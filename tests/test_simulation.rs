use std::any::Any;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;
use std::{fs, thread};

use nohash_hasher::IntMap;
use tracing::info;

use rust_q_sim::simulation::config::{CommandLineArgs, Config, RoutingMode};
use rust_q_sim::simulation::controller::{get_numbered_output_filename, partition_input};
use rust_q_sim::simulation::id;
use rust_q_sim::simulation::io::xml_events::XmlEventsWriter;
use rust_q_sim::simulation::messaging::communication::communicators::{
    ChannelSimCommunicator, SimCommunicator,
};
use rust_q_sim::simulation::messaging::communication::message_broker::NetMessageBroker;
use rust_q_sim::simulation::messaging::events::{EventsPublisher, EventsSubscriber};
use rust_q_sim::simulation::network::global_network::Network;
use rust_q_sim::simulation::network::sim_network::SimNetworkPartition;
use rust_q_sim::simulation::population::population::Population;
use rust_q_sim::simulation::replanning::replanner::{
    DummyReplanner, ReRouteTripReplanner, Replanner,
};
use rust_q_sim::simulation::replanning::routing::travel_time_collector::TravelTimeCollector;
use rust_q_sim::simulation::simulation::Simulation;
use rust_q_sim::simulation::vehicles::garage::Garage;
use rust_q_sim::simulation::wire_types::events::Event;

pub fn execute_sim_with_channels(config_args: CommandLineArgs, expected_events: &str) {
    let config = Config::from_file(&config_args);
    let comms = ChannelSimCommunicator::create_n_2_n(config.partitioning().num_parts);
    let mut receiver = ReceivingSubscriber::new_with_events_from_file(&expected_events);

    let mut handles: IntMap<u32, JoinHandle<()>> = comms
        .into_iter()
        .map(|comm| {
            let config_args_clone = config_args.clone();
            let subscr = SendingSubscriber {
                rank: comm.rank(),
                sender: receiver.channel.0.clone(),
            };
            (
                comm.rank(),
                thread::spawn(move || execute_sim(comm, Box::new(subscr), config_args_clone)),
            )
        })
        .collect();

    // create another thread for the receiver, so that the main thread doesn't block.
    let receiver_handle = thread::spawn(move || receiver.start_listen());
    handles.insert(handles.len() as u32, receiver_handle);

    try_join(handles);
}

pub fn execute_sim<C: SimCommunicator + 'static>(
    comm: C,
    test_subscriber: Box<dyn EventsSubscriber + Send>,
    config_args: CommandLineArgs,
) {
    let rank = comm.rank();

    let config = Config::from_file(&config_args);

    let output_path = PathBuf::from(&config.output().output_dir);
    fs::create_dir_all(&output_path).expect("Failed to create output path");

    let temp_network_file = get_numbered_output_filename(
        &output_path,
        &PathBuf::from(config.proto_files().network),
        config.partitioning().num_parts,
    );

    id::load_from_file(&PathBuf::from(config.proto_files().ids));

    if rank == 0 {
        info!("#{rank} preparing to create input for partitions.");
        //this call also loads the ids from the file.
        partition_input(&config);
    } else {
        //apply busy waiting until first process has created all files
        while !all_temp_files_created(&temp_network_file) {
            thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let network = Network::from_file_as_is(&temp_network_file);
    let mut garage = Garage::from_file(&PathBuf::from(config.proto_files().vehicles));

    //let population: Population = Population::from_file(&temp_population_file, &mut garage);
    let population: Population = Population::from_file_filtered_part(
        &PathBuf::from(config.proto_files().population),
        &network,
        &mut garage,
        comm.rank(),
    );
    let sim_net = SimNetworkPartition::from_network(&network, rank, config.simulation());

    let events = Rc::new(RefCell::new(EventsPublisher::new()));
    events.borrow_mut().add_subscriber(test_subscriber);
    events
        .borrow_mut()
        .add_subscriber(Box::new(TravelTimeCollector::new()));

    let rc = Rc::new(comm);
    let broker = NetMessageBroker::new(rc.clone(), &network, &sim_net);

    let replanner: Box<dyn Replanner> = if config.routing().mode == RoutingMode::AdHoc {
        Box::new(ReRouteTripReplanner::new(
            &network,
            &sim_net,
            &garage,
            Rc::clone(&rc),
        ))
    } else {
        Box::new(DummyReplanner {})
    };

    let mut sim = Simulation::new(
        config, sim_net, garage, population, broker, events, replanner,
    );

    sim.run();
}

fn all_temp_files_created(temp_network_file: &PathBuf) -> bool {
    temp_network_file.exists()
}

/// Have this more complicated join logic, so that threads in the back of the handle vec can also
/// cause the main thread to panic.
fn try_join(mut handles: IntMap<u32, JoinHandle<()>>) {
    while !handles.is_empty() {
        let mut finished = Vec::new();
        for (i, handle) in handles.iter() {
            if handle.is_finished() {
                finished.push(*i);
            }
        }
        for i in finished {
            let handle = handles.remove(&i).unwrap();
            handle.join().expect("Error in a thread");
        }
    }
}

pub struct TestSubscriber {
    next_index: usize,
    expected_events: Vec<String>,
}

struct ReceivingSubscriber {
    test_subscriber: TestSubscriber,
    channel: (Sender<String>, Receiver<String>),
}

struct SendingSubscriber {
    #[allow(dead_code)]
    rank: u32,
    sender: Sender<String>,
}

impl EventsSubscriber for SendingSubscriber {
    fn receive_event(&mut self, time: u32, event: &Event) {
        let event_string = XmlEventsWriter::event_2_string(time, event);
        self.sender
            .send(event_string)
            .expect("Failed on sending event message!");
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl ReceivingSubscriber {
    fn new_with_events_from_file(events_file: &str) -> Self {
        Self {
            test_subscriber: TestSubscriber::new_with_events_from_file(events_file),
            channel: channel(),
        }
    }

    fn start_listen(&mut self) {
        while self.test_subscriber.next_index < self.test_subscriber.expected_events.len() {
            let event_string = self
                .channel
                .1
                .recv()
                .expect("Something went wrong while listening for events");
            self.test_subscriber.receive_event_string(event_string);
        }
    }
}

impl TestSubscriber {
    pub fn new_with_events_from_file(events_file: &str) -> Self {
        Self {
            next_index: 0,
            expected_events: Self::expected_events_from_file(events_file),
        }
    }

    /// Load an external file with expected events. Instead of loading proto buf files this has two advantages:
    /// 1. The expected events are in a human readable format.
    /// 2. The expected events consist of the external ids.
    fn expected_events_from_file(events_file: &str) -> Vec<String> {
        let file = File::open(events_file).expect("Failed to open events file.");
        let reader = BufReader::new(file);

        // Prepare the expected events. Since the file is an xml events file, we do not want to compare other lines than the
        // event lines. Also, we need to append \n to each line since the reader strips it.
        reader
            .lines()
            .map(|l| l.unwrap())
            .filter(|s| s.starts_with("<event "))
            .map(|s| s + "\n")
            .collect()
    }
}

impl TestSubscriber {
    fn receive_event_string(&mut self, event: String) {
        let expected_value = self.expected_events.get(self.next_index).unwrap();
        self.next_index += 1;
        assert_eq!(expected_value, &event);
    }
}

impl EventsSubscriber for TestSubscriber {
    fn receive_event(&mut self, time: u32, event: &Event) {
        self.receive_event_string(XmlEventsWriter::event_2_string(time, event));
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}
