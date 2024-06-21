use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use std::{fs, thread};

use clap::Parser;
use mpi::traits::{Communicator, CommunicatorCollectives};
use nohash_hasher::IntMap;
use tracing::info;

use crate::simulation::config::{
    CommandLineArgs, Config, PartitionMethod, RoutingMode, WriteEvents,
};
use crate::simulation::io::proto_events::ProtoEventsWriter;
use crate::simulation::messaging::communication::communicators::{
    ChannelSimCommunicator, MpiSimCommunicator, SimCommunicator,
};
use crate::simulation::messaging::communication::message_broker::NetMessageBroker;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::global_network::Network;
use crate::simulation::network::sim_network::SimNetworkPartition;
use crate::simulation::population::population::Population;
use crate::simulation::replanning::replanner::{DummyReplanner, ReRouteTripReplanner, Replanner};
use crate::simulation::replanning::routing::travel_time_collector::TravelTimeCollector;
use crate::simulation::simulation::Simulation;
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::{id, logging};

pub fn run_channel() {
    let args = CommandLineArgs::parse();
    let config = Config::from_file(&args);

    let _guards = logging::init_logging(
        &config,
        config.partitioning().num_parts.to_string().as_str(),
    );

    info!(
        "Starting Multithreaded Simulation with {} partitions.",
        config.partitioning().num_parts
    );
    let comms = ChannelSimCommunicator::create_n_2_n(config.partitioning().num_parts);

    let handles: IntMap<u32, JoinHandle<()>> = comms
        .into_iter()
        .map(|comm| {
            let config_path = args.clone();
            (
                comm.rank(),
                thread::Builder::new()
                    .name(comm.rank().to_string())
                    .spawn(move || execute_partition(comm, &config_path))
                    .unwrap(),
            )
        })
        .collect();

    try_join(handles);
}

pub fn run_mpi() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let comm = MpiSimCommunicator {
        mpi_communicator: world,
    };

    let mut args = CommandLineArgs::parse();
    // override the num part argument, with the number of processes mpi has started.
    args.num_parts = Some(world.size() as u32);
    let config = Config::from_file(&args);

    let _guards = logging::init_logging(&config, comm.rank().to_string().as_str());

    info!(
        "Starting MPI Simulation with {} partitions",
        config.partitioning().num_parts
    );
    execute_partition(comm, &args);

    info!("#{} at barrier.", world.rank());
    universe.world().barrier();
    info!("Process #{} finishing.", world.rank());
}

fn execute_partition<C: SimCommunicator + 'static>(comm: C, args: &CommandLineArgs) {
    let config = Config::from_file(args);

    let rank = comm.rank();
    let size = config.partitioning().num_parts;

    let output_path = PathBuf::from(&config.output().output_dir);
    fs::create_dir_all(&output_path).expect("Failed to create output path");

    if rank == 0 {
        info!("#{rank} preparing to create input for partitions.");
        partition_input(&config);
    }

    info!("Process #{rank} of {size} has started. Waiting for other processes to arrive at initial barrier. ");
    // send emtpy travel times to everybody as a barrier.
    //comm.send_receive_travel_times(0, std::collections::HashMap::new());
    comm.barrier();

    id::load_from_file(&PathBuf::from(config.proto_files().ids));
    let network = Network::from_file_as_is(&get_numbered_output_filename(
        &output_path,
        &PathBuf::from(config.proto_files().network),
        config.partitioning().num_parts,
    ));
    let mut garage = Garage::from_file(&PathBuf::from(config.proto_files().vehicles));

    let population = Population::from_file_filtered_part(
        &PathBuf::from(config.proto_files().population),
        &network,
        &mut garage,
        comm.rank(),
    );

    let network_partition = SimNetworkPartition::from_network(&network, rank, config.simulation());
    info!(
        "Partition #{rank} network has: {} nodes and {} links. Population has {} agents",
        network_partition.nodes.len(),
        network_partition.links.len(),
        population.persons.len()
    );

    let events = Rc::new(RefCell::new(EventsPublisher::new()));

    if config.output().write_events == WriteEvents::Proto {
        let events_file = format!("events.{rank}.binpb");
        let events_path = output_path.join(events_file);
        events.borrow_mut().add_subscriber(Box::new(ProtoEventsWriter::new(&events_path)));
    }
    let travel_time_collector = Box::new(TravelTimeCollector::new());
    events.borrow_mut().add_subscriber(travel_time_collector);

    let rc = Rc::new(comm);

    let replanner: Box<dyn Replanner> = if config.routing().mode == RoutingMode::AdHoc {
        Box::new(ReRouteTripReplanner::new(
            &network,
            &network_partition,
            &garage,
            Rc::clone(&rc),
        ))
    } else {
        Box::new(DummyReplanner {})
    };
    let net_message_broker = NetMessageBroker::new(rc, &network, &network_partition);

    let mut simulation: Simulation<C> = Simulation::new(
        config,
        network_partition,
        garage,
        population,
        net_message_broker,
        events,
        replanner,
    );

    simulation.run();
}

/// Have this more complicated join logic, so that threads in the back of the handle vec can also
/// cause the main thread to panic.
fn try_join(mut handles: IntMap<u32, JoinHandle<()>>) {
    while !handles.is_empty() {
        sleep(Duration::from_secs(1)); // test for finished threads once a second
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

pub fn partition_input(config: &Config) {
    id::load_from_file(&PathBuf::from(config.proto_files().ids));
    let _net = if let PartitionMethod::Metis(_) = config.partitioning().method {
        info!("Config param Partition method was set to metis. Loading input network, running metis conversion and then store it into output folder");
        partition_network(config)
    } else {
        info!("Config param Partition method was set to none. Loading network from input, assuming it has partitioning information");
        copy_network_into_output(config)
    };
}

fn partition_network(config: &Config) -> Network {
    let net_in_path = PathBuf::from(config.proto_files().network);
    let num_parts = config.partitioning().num_parts;
    let network = Network::from_file_path(&net_in_path, num_parts, config.partitioning().method);

    let mut net_out_path =
        create_output_filename(&PathBuf::from(config.output().output_dir), &net_in_path);
    net_out_path = insert_number_in_proto_filename(&net_out_path, num_parts);
    network.to_file(&net_out_path);
    network
}

fn copy_network_into_output(config: &Config) -> Network {
    let net_in_path = PathBuf::from(config.proto_files().network);
    let num_parts = config.partitioning().num_parts;
    let network = Network::from_file_as_is(&net_in_path);
    let mut net_out_path =
        create_output_filename(&PathBuf::from(config.output().output_dir), &net_in_path);
    net_out_path = insert_number_in_proto_filename(&net_out_path, num_parts);
    network.to_file(&net_out_path);
    network
}

pub fn get_numbered_output_filename(output_dir: &Path, input_file: &Path, part: u32) -> PathBuf {
    let out = create_output_filename(output_dir, input_file);
    insert_number_in_proto_filename(&out, part)
}

fn create_output_filename(output_dir: &Path, input_file: &Path) -> PathBuf {
    let filename = input_file.file_name().unwrap();
    output_dir.join(filename)
}

fn insert_number_in_proto_filename(path: &Path, part: u32) -> PathBuf {
    let filename = path.file_name().unwrap().to_str().unwrap();
    let mut stripped = filename.strip_suffix(".binpb").unwrap();
    if let Some(s) = stripped.strip_suffix(format!(".{part}").as_str()) {
        stripped = s;
    }
    let new_filename = format!("{stripped}.{part}.binpb");
    path.parent().unwrap().join(new_filename)
}
