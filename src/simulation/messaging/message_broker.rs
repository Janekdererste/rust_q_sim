use mpi::collective::CommunicatorCollectives;
use mpi::datatype::PartitionMut;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};

use mpi::topology::SystemCommunicator;
use mpi::traits::{Communicator, Destination, Source};
use mpi::{Count, Rank};

use crate::simulation::messaging::messages::proto::{StorageCap, SyncMessage, Vehicle};
use crate::simulation::network::global_network::Network;
use crate::simulation::network::sim_network::{SimNetworkPartition, SplitStorage};

pub trait SimCommunicator {
    fn send_receive_vehicles<F>(
        &self,
        vehicles: HashMap<u32, SyncMessage>,
        expected_vehicle_messages: &mut HashSet<u32>,
        now: u32,
        on_msg: F,
    ) where
        F: FnMut(SyncMessage);

    fn send_receive_travel_times<F>(&self, now: u32, travel_times: HashMap<u64, u32>, on_msg: F)
    where
        F: FnMut(Vec<TravelTimesMessage>);

    fn rank(&self) -> u32;
}

pub struct DummySimCommunicator();

impl SimCommunicator for DummySimCommunicator {
    fn send_receive_vehicles<F>(
        &self,
        _vehicles: HashMap<u32, SyncMessage>,
        _expected_vehicle_messages: &mut HashSet<u32>,
        _now: u32,
        _on_msg: F,
    ) where
        F: FnMut(SyncMessage),
    {
    }

    fn send_receive_travel_times<F>(
        &self,
        _now: u32,
        travel_times: HashMap<u64, u32>,
        mut on_msg: F,
    ) where
        F: FnMut(Vec<TravelTimesMessage>),
    {
        //process own travel times messages
        on_msg(vec![TravelTimesMessage::from(travel_times)])
    }

    fn rank(&self) -> u32 {
        0
    }
}

pub struct ChannelSimCommunicator {
    receiver: Receiver<SyncMessage>,
    senders: Vec<Sender<SyncMessage>>,
    rank: u32,
}

impl ChannelSimCommunicator {
    pub fn create_n_2_n(num_parts: u32) -> Vec<ChannelSimCommunicator> {
        let mut senders: Vec<_> = Vec::new();
        let mut comms: Vec<_> = Vec::new();

        for rank in 0..num_parts {
            let (sender, receiver) = channel();
            let comm = ChannelSimCommunicator {
                receiver,
                senders: vec![],
                rank,
            };
            senders.push(sender);
            comms.push(comm);
        }

        for comm in &mut comms {
            for sender in &senders {
                comm.senders.push(sender.clone());
            }
        }

        comms
    }
}

impl SimCommunicator for ChannelSimCommunicator {
    fn send_receive_vehicles<F>(
        &self,
        vehicles: HashMap<u32, SyncMessage>,
        expected_vehicle_messages: &mut HashSet<u32>,
        now: u32,
        mut on_msg: F,
    ) where
        F: FnMut(SyncMessage),
    {
        // send messages to everyone
        for (target, msg) in vehicles {
            let sender = self.senders.get(target as usize).unwrap();
            sender
                .send(msg)
                .expect("Failed to send message in message broker");
        }

        // receive messages from everyone
        while !expected_vehicle_messages.is_empty() {
            let received_msg = self
                .receiver
                .recv()
                .expect("Error while receiving messages");
            let from_rank = received_msg.from_process;

            // If a message was received from a neighbor partition for this very time step, remove
            // that partition from expected messages which indicates which partitions we are waiting
            // for
            if received_msg.time == now {
                expected_vehicle_messages.remove(&from_rank);
            }

            // publish the received message to the message broker
            on_msg(received_msg);
        }
    }

    fn send_receive_travel_times<F>(&self, now: u32, travel_times: HashMap<u64, u32>, mut on_msg: F)
    where
        F: FnMut(Vec<TravelTimesMessage>),
    {
        todo!()
    }

    fn rank(&self) -> u32 {
        self.rank
    }
}

pub struct MpiSimCommunicator {
    pub mpi_communicator: SystemCommunicator,
}

impl SimCommunicator for MpiSimCommunicator {
    fn send_receive_vehicles<F>(
        &self,
        out_messages: HashMap<u32, SyncMessage>,
        expected_vehicle_messages: &mut HashSet<u32>,
        now: u32,
        mut on_msg: F,
    ) where
        F: FnMut(SyncMessage),
    {
        let buf_msg: Vec<_> = out_messages.values().map(|m| (m, m.serialize())).collect();

        // we have to use at least immediate send here. Otherwise we risk blocking on send as explained
        // in https://paperpile.com/app/p/e209e0b3-9bdb-08c7-8a62-b1180a9ac954 chapter 4.3, 4.4 and 4.12.
        // The underlying mpi-implementation may wait for the receiver to call a recv variant, and provide
        // a buffer, where the buffer used for the send operation can be written into. If process 1 and 2
        // want to send with MPI_Send, which is a blocking operation, both processes will wait, that
        // the other calls MPI_Recv, which never happens, because both processes are stuck at MPI_Send
        //
        // With immediate_send (MPI_Isend) we tell MPI that we are ready to send away the message buffer,
        // then the same process immediately calls MPI_Recv (blocking) which makes room for a message
        // buffer. In the case of the above example, both processes are calling MPI_Recv and provide
        // a buffer to write the message into, which was issued in MPI_Isend.
        //
        // The rsmpi library wraps non-blocking mpi-communication into a scope, so that the compiler
        // can ensure that a buffer is not moved while the request is in progress.
        mpi::request::multiple_scope(buf_msg.len(), |scope, reqs| {
            // ------- Send Part ---------
            for (message, buf) in buf_msg.iter() {
                let req = self
                    .mpi_communicator
                    .process_at_rank(message.to_process as Rank)
                    .immediate_send(scope, buf);
                reqs.add(req);
            }

            // Use blocking MPI_recv here, since we don't have anything to do if there are no other
            // messages.
            while !expected_vehicle_messages.is_empty() {
                let (encoded_msg, _status) = self.mpi_communicator.any_process().receive_vec();
                let msg = SyncMessage::deserialize(&encoded_msg);
                let from_rank = msg.from_process;

                // If a message was received from a neighbor partition for this very time step, remove
                // that partition from expected messages which indicates which partitions we are waiting
                // for
                if msg.time == now {
                    expected_vehicle_messages.remove(&from_rank);
                }

                on_msg(msg);
            }

            // wait here, so that all requests finish. This is necessary, because a process might send
            // more messages than it receives. This happens, if a process sends messages to remote
            // partitions (teleported legs) but only receives messages from neighbor partitions.
            reqs.wait_all(&mut Vec::new());
        });
    }

    fn send_receive_travel_times<F>(&self, now: u32, travel_times: HashMap<u64, u32>, mut on_msg: F)
    where
        F: FnMut(Vec<TravelTimesMessage>),
    {
        let travel_times_message = TravelTimesMessage::from(travel_times);
        let serial_travel_times_message = travel_times_message.serialize();

        let messages: Vec<TravelTimesMessage> =
            self.gather_travel_times(&serial_travel_times_message);

        on_msg(messages);
    }

    fn rank(&self) -> u32 {
        self.mpi_communicator.rank() as u32
    }
}

impl MpiSimCommunicator {
    fn gather_travel_times(&self, travel_times_message: &Vec<u8>) -> Vec<TravelTimesMessage> {
        // ------- Gather traffic info lengths -------
        let mut travel_times_length_buffer = vec![0i32; self.mpi_communicator.size() as usize];
        self.mpi_communicator.all_gather_into(
            &(travel_times_message.len() as i32),
            &mut travel_times_length_buffer[..],
        );

        // ------- Gather traffic info -------
        if travel_times_length_buffer.iter().sum::<i32>() <= 0 {
            // if there is no traffic data to be sent, we do not actually perform mpi communication
            // because mpi would crash
            return Vec::new();
        }

        let mut travel_times_buffer =
            vec![0u8; travel_times_length_buffer.iter().sum::<i32>() as usize];
        let info_displs = Self::get_travel_times_displs(&mut travel_times_length_buffer);
        let mut partition = PartitionMut::new(
            &mut travel_times_buffer,
            travel_times_length_buffer.clone(),
            &info_displs[..],
        );
        self.mpi_communicator
            .all_gather_varcount_into(&travel_times_message[..], &mut partition);

        Self::deserialize_travel_times(travel_times_buffer, travel_times_length_buffer)
    }

    fn get_travel_times_displs(all_travel_times_message_lengths: &mut Vec<i32>) -> Vec<Count> {
        // this is copied from rsmpi example immediate_all_gather_varcount
        all_travel_times_message_lengths
            .iter()
            .scan(0, |acc, &x| {
                let tmp = *acc;
                *acc += x;
                Some(tmp)
            })
            .collect()
    }

    fn deserialize_travel_times(
        all_travel_times_messages: Vec<u8>,
        lengths: Vec<i32>,
    ) -> Vec<TravelTimesMessage> {
        let mut result = Vec::new();
        let mut last_end_index = 0usize;
        for len in lengths {
            let begin_index = last_end_index;
            let end_index = last_end_index + len as usize;
            result.push(TravelTimesMessage::deserialize(
                &all_travel_times_messages[begin_index..end_index as usize],
            ));
            last_end_index = end_index;
        }
        result
    }
}

pub struct TravelTimesMessageBroker<C>
where
    C: SimCommunicator,
{
    communicator: Rc<C>,
}

impl<C> TravelTimesMessageBroker<C>
where
    C: SimCommunicator,
{
    pub fn new(communicator: Rc<C>) -> Self {
        TravelTimesMessageBroker { communicator }
    }

    pub fn rank(&self) -> u32 {
        self.communicator.rank()
    }

    pub fn send_recv(&self, now: u32, travel_times: HashMap<u64, u32>) -> Vec<TravelTimesMessage> {
        let mut res = Vec::new();
        self.communicator
            .send_receive_travel_times(now, travel_times, |m| res = m);
        res
    }
}

pub struct NetMessageBroker<C>
where
    C: SimCommunicator,
{
    //communicator: SystemCommunicator,
    communicator: Rc<C>,
    out_messages: HashMap<u32, SyncMessage>,
    in_messages: BinaryHeap<SyncMessage>,
    // store link mapping with internal ids instead of id structs, because vehicles only store internal
    // ids (usize) and this way we don't need to keep a reference to the global network's id store
    link_mapping: HashMap<u64, u32>,
    neighbors: HashSet<u32>,
}

impl<C> NetMessageBroker<C>
where
    C: SimCommunicator,
{
    pub fn new(comm: Rc<C>, net: &SimNetworkPartition, global_network: &Network) -> Self {
        let neighbors = net.neighbors().iter().copied().collect();
        let link_mapping = global_network
            .links
            .iter()
            .map(|link| (link.id.internal(), link.partition))
            .collect();

        Self {
            communicator: comm,
            out_messages: Default::default(),
            in_messages: Default::default(),
            link_mapping,
            neighbors,
        }
    }

    pub fn rank(&self) -> u32 {
        self.communicator.rank()
    }

    pub fn rank_for_link(&self, link_id: u64) -> u32 {
        *self.link_mapping.get(&(link_id)).unwrap()
    }

    pub fn add_veh(&mut self, vehicle: Vehicle, now: u32) {
        let link_id = vehicle.curr_link_id().unwrap();
        let partition = *self.link_mapping.get(&link_id).unwrap();
        let rank = self.rank();
        let message = self
            .out_messages
            .entry(partition)
            .or_insert_with(|| SyncMessage::new(now, rank, partition));
        message.add_veh(vehicle);
    }

    pub fn add_cap(&mut self, cap: SplitStorage, now: u32) {
        let rank = self.rank();
        let message = self
            .out_messages
            .entry(cap.from_part)
            .or_insert_with(|| SyncMessage::new(now, rank, cap.from_part));
        message.add_storage_cap(StorageCap {
            link_id: cap.link_id,
            value: cap.used,
        });
    }

    pub fn send_recv(&mut self, now: u32) -> Vec<SyncMessage> {
        let vehicles = self.prepare_send_recv_vehicles(now);
        let mut result: Vec<SyncMessage> = Vec::new();
        let mut expected_vehicle_messages = self.neighbors.clone();

        self.pop_from_cache(&mut expected_vehicle_messages, &mut result, now);

        // get refs to communicator and in_messages, so that we can have mut refs to both, instead
        // of passing self around, which would lock them because we would hold multiple mut refs to self
        let comm_ref = &self.communicator;
        let in_msgs_ref = &mut self.in_messages;

        comm_ref.send_receive_vehicles(vehicles, &mut expected_vehicle_messages, now, |msg| {
            Self::handle_incoming_msg(msg, &mut result, in_msgs_ref, now)
        });

        result
    }

    fn handle_incoming_msg(
        msg: SyncMessage,
        result: &mut Vec<SyncMessage>,
        in_messages: &mut BinaryHeap<SyncMessage>,
        now: u32,
    ) {
        if msg.time <= now {
            result.push(msg);
        } else {
            in_messages.push(msg);
        }
    }

    fn pop_from_cache(
        &mut self,
        expected_messages: &mut HashSet<u32>,
        messages: &mut Vec<SyncMessage>,
        now: u32,
    ) {
        while let Some(msg) = self.in_messages.peek() {
            if msg.time <= now {
                expected_messages.remove(&msg.from_process);
                messages.push(self.in_messages.pop().unwrap())
            } else {
                break; // important! otherwise this is an infinite loop
            }
        }
    }

    fn prepare_send_recv_vehicles(&mut self, now: u32) -> HashMap<u32, SyncMessage> {
        let capacity = self.out_messages.len();
        let mut messages =
            std::mem::replace(&mut self.out_messages, HashMap::with_capacity(capacity));

        for partition in &self.neighbors {
            let neighbor_rank = *partition;
            messages
                .entry(neighbor_rank)
                .or_insert_with(|| SyncMessage::new(now, self.rank(), neighbor_rank));
        }
        messages
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use crate::simulation::id::Id;
    use crate::simulation::messaging::message_broker::{
        ChannelSimCommunicator, NetMessageBroker, TravelTimesMessageBroker,
    };
    use crate::simulation::messaging::messages::proto::Vehicle;
    use crate::simulation::network::global_network::{Link, Network, Node};
    use crate::simulation::network::sim_network::{SimNetworkPartition, SplitStorage};
    use crate::test_utils::create_agent;

    #[test]
    fn send_recv_empty_msgs() {
        let sends = Arc::new(AtomicUsize::new(0));

        execute_test(move |communicator| {
            let mut broker = create_message_broker(communicator);

            sends.fetch_add(1, Ordering::Relaxed);
            let result = broker.send_recv(0);

            // all threads should block on receive. Therefore, the send count should be equal to 3, as
            // 0,1 have 3 as a remote neighbor. It is possible for 0 and 1 to move on before 3 has
            // increased the send count. Most of the time it should be 4 though. I don't know how
            // good this test is in this case. I guess the remaining asserts are also fine.
            assert!(
                3 <= sends.load(Ordering::Relaxed),
                "# {} Failed on send count of {}",
                broker.rank(),
                sends.load(Ordering::Relaxed)
            );

            // the different partitions expect varying numbers of sync messags.
            match broker.rank() {
                0 | 1 => assert_eq!(2, result.len()),
                2 => assert_eq!(3, result.len()),
                3 => assert_eq!(1, result.len()),
                _ => panic!("Not expecting this rank!"),
            };

            for msg in result {
                assert!(msg.vehicles.is_empty());
            }
        });
    }

    /// This test moves a vehicle from partition 0 to 2 and then to partition 3. The test involves
    /// Two send_recv steps.
    #[test]
    fn send_recv_local_vehicle_msg() {
        execute_test(|communicator| {
            let mut broker = create_message_broker(communicator);

            // place vehicle into partition 0
            if broker.rank() == 0 {
                let agent = create_agent(0, vec![2, 6]);
                let vehicle = Vehicle::new(0, 0, 0., 0., Some(agent));
                broker.add_veh(vehicle.clone(), 0);
            }

            // do sync step for all partitions
            let result_0 = broker.send_recv(0);

            // we expect broker 2 to have received the vehicle all other messages should have no vehicles
            if broker.rank() == 2 {
                let mut msg = result_0
                    .into_iter()
                    .find(|msg| msg.from_process == 0)
                    .unwrap();
                assert_eq!(0, msg.time);
                assert_eq!(1, msg.vehicles.len());
                let mut vehicle = msg.vehicles.remove(0);
                vehicle.advance_route_index();
                broker.add_veh(vehicle, 1);
            } else {
                for msg in result_0 {
                    assert!(msg.vehicles.is_empty());
                }
            }

            // do second sync step for all partitions
            let result_1 = broker.send_recv(1);

            // we expect broker 3 to have received the vehicle all other messages should have no vehicles
            if broker.rank() == 3 {
                let mut msg = result_1
                    .into_iter()
                    .find(|msg| msg.from_process == 2)
                    .unwrap();
                assert_eq!(1, msg.time);
                assert_eq!(1, msg.vehicles.len());
                let vehicle = msg.vehicles.remove(0);
                broker.add_veh(vehicle, 1);
            } else {
                for msg in result_1 {
                    assert!(msg.vehicles.is_empty());
                }
            }
        });
    }

    #[test]
    fn send_recv_remote_message() {
        execute_test(|communicator| {
            let mut broker = create_message_broker(communicator);

            // place vehicle into partition 0 with a future timestamp
            if broker.rank() == 0 {
                let agent = create_agent(0, vec![6]);
                let vehicle = Vehicle::new(0, 0, 0., 0., Some(agent));
                broker.add_veh(vehicle, 1);
            }

            // do sync step for all partitions for "current" time step
            let result_0 = broker.send_recv(0);

            for msg in result_0 {
                assert_eq!(0, msg.time);
                assert!(msg.vehicles.is_empty());
            }

            // do sync step for all partitions for "future" time step
            let result_1 = broker.send_recv(1);

            for msg in result_1 {
                if broker.rank() == 3 && msg.from_process == 0 {
                    assert_eq!(1, msg.vehicles.len());
                }

                assert_eq!(1, msg.time);
            }
        });
    }

    #[test]
    fn send_recv_local_and_remote_msg() {
        execute_test(|communicator| {
            let mut broker = create_message_broker(communicator);

            if broker.rank() == 0 {
                // place vehicle into partition 0 with a future timestamp with remote destination
                let agent = create_agent(0, vec![6]);
                let vehicle = Vehicle::new(0, 0, 0., 0., Some(agent));
                broker.add_veh(vehicle, 1);
            }

            // do sync step for all partitions for "current" time step
            let result_0 = broker.send_recv(0);

            for msg in result_0 {
                assert_eq!(0, msg.time);
                assert!(msg.vehicles.is_empty());
            }

            if broker.rank() == 2 {
                // place vehicle into partition 2 with a current timestamp with neighbor destination
                let agent = create_agent(1, vec![6]);
                let vehicle = Vehicle::new(1, 0, 0., 0., Some(agent));
                broker.add_veh(vehicle, 1);
            }

            // do sync step for all partitions for "future" time step
            let result_1 = broker.send_recv(1);

            for msg in result_1 {
                if broker.rank() == 3 && msg.from_process == 0 {
                    assert_eq!(1, msg.vehicles.len());
                    assert_eq!(0, msg.vehicles.first().unwrap().id);
                } else if broker.rank() == 3 && msg.from_process == 2 {
                    assert_eq!(1, msg.vehicles.len());
                    assert_eq!(1, msg.vehicles.first().unwrap().id);
                } else {
                    assert_eq!(0, msg.vehicles.len());
                }

                assert_eq!(1, msg.time);
            }
        });
    }

    fn create_message_broker(
        communicator: ChannelSimCommunicator,
    ) -> NetMessageBroker<ChannelSimCommunicator> {
        let rank = communicator.rank;
        let mut broker = NetMessageBroker::new(
            Rc::new(communicator),
            &SimNetworkPartition::from_network(&create_network(), rank,1.0),
        );
        broker
    }

    #[test]
    fn send_recv_storage_cap() {
        execute_test(|communicator| {
            let mut broker = create_message_broker(communicator);
            // add a storage cap message for link 4, which connects parts 1 -> 2
            if broker.rank() == 2 {
                broker.add_cap(
                    SplitStorage {
                        link_id: 4,
                        used: 42.0,
                        from_part: 1,
                    },
                    0,
                );
            }

            // do sync step
            let result_0 = broker.send_recv(0);

            // broker 1 should have received the StorageCap message
            // all others should not have any storage cap messages.
            for msg in result_0 {
                if msg.from_process == 2 && msg.to_process == 1 {
                    assert_eq!(1, msg.storage_capacities.len(), "{msg:?}")
                } else {
                    assert!(msg.storage_capacities.is_empty(), "{msg:?}");
                }
            }
        });
    }

    fn execute_test<F>(test: F)
    where
        F: Fn(ChannelSimCommunicator) + Send + Sync + 'static,
    {
        let network = create_network();
        let communicators = ChannelSimCommunicator::create_n_2_n(network.nodes.len() as u32);

        let mut join_handles = Vec::new();

        let test_ref = Arc::new(test);

        for c in communicators {
            let cloned_test_ref = test_ref.clone();
            let handle = thread::spawn(move || cloned_test_ref(c));
            join_handles.push(handle)
        }

        for handle in join_handles {
            handle.join().expect("Some thread crashed");
        }
    }

    /// use example with four partitions
    /// 0 --- 2 --- 3
    /// |   /
    /// 1--/
    /// 0, 1, 2, are neighbors, 3 is only neighbor to 2
    fn create_network() -> Network {
        let mut result = Network::new();
        result.add_node(create_node(0, 0));
        result.add_node(create_node(1, 1));
        result.add_node(create_node(2, 2));
        result.add_node(create_node(3, 3));

        // connection 0 <-> 1
        result.add_link(create_link(0, Id::new_internal(0), Id::new_internal(1), 1));
        result.add_link(create_link(1, Id::new_internal(1), Id::new_internal(0), 0));

        // connection 0 <-> 2
        result.add_link(create_link(2, Id::new_internal(0), Id::new_internal(2), 2));
        result.add_link(create_link(3, Id::new_internal(2), Id::new_internal(0), 0));

        // connection 1 <-> 2
        result.add_link(create_link(4, Id::new_internal(1), Id::new_internal(2), 2));
        result.add_link(create_link(5, Id::new_internal(2), Id::new_internal(1), 1));

        // connection 2 <-> 3
        result.add_link(create_link(6, Id::new_internal(2), Id::new_internal(3), 3));
        result.add_link(create_link(7, Id::new_internal(3), Id::new_internal(2), 2));

        result
    }

    fn create_node(id: u64, partition: u32) -> Node {
        let mut node = Node::new(Id::new_internal(id), 0., 0.);
        node.partition = partition;
        node
    }

    fn create_link(id: u64, from: Id<Node>, to: Id<Node>, partition: u32) -> Link {
        Link {
            id: Id::new_internal(id),
            from,
            to,
            length: 0.0,
            capacity: 1.0,
            freespeed: 0.0,
            permlanes: 0.0,
            modes: Default::default(),
            partition,
        }
    }
}
