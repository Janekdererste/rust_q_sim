use crate::simulation::messaging::messages::proto::{Vehicle, VehicleMessage};

use crate::simulation::performance_profiling::measure_duration;
use mpi::topology::SystemCommunicator;
use mpi::traits::{Communicator, Destination, Source};
use mpi::Rank;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

pub trait MessageBroker {
    fn send_recv(&mut self, now: u32) -> Vec<VehicleMessage>;
    fn add_veh(&mut self, vehicle: Vehicle, now: u32);
}
pub struct MpiMessageBroker {
    pub rank: Rank,
    communicator: SystemCommunicator,
    neighbors: HashSet<u32>,
    out_messages: HashMap<u32, VehicleMessage>,
    in_messages: BinaryHeap<VehicleMessage>,
    link_id_mapping: Arc<HashMap<usize, usize>>,
}

impl MessageBroker for MpiMessageBroker {
    fn send_recv(&mut self, now: u32) -> Vec<VehicleMessage> {
        // preparation of vehicle messages
        let vehicle_messages = self.prepare_send_recv_vehicles(now);
        let buf_msg: Vec<_> = vehicle_messages
            .values()
            .map(|m| (m, m.serialize()))
            .collect();

        // prepare the receiving here.
        let mut expected_vehicle_messages = self.neighbors.clone();
        let mut received_vehicle_messages = Vec::new();

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
            measure_duration(Some(now), "mpi_send", None, || {
                for (message, buf) in buf_msg.iter() {
                    let req = self
                        .communicator
                        .process_at_rank(message.to_process as Rank)
                        .immediate_send(scope, buf);
                    reqs.add(req);
                }
            });

            // ------ Receive Part --------
            self.pop_from_cache(
                &mut expected_vehicle_messages,
                &mut received_vehicle_messages,
                now,
            );

            // Use blocking MPI_recv here, since we don't have anything to do if there are no other
            // messages.
            measure_duration(Some(now), "mpi_receive", None, || {
                while !expected_vehicle_messages.is_empty() {
                    let (encoded_msg, _status) = self.communicator.any_process().receive_vec();
                    let msg = VehicleMessage::deserialize(&encoded_msg);
                    let from_rank = msg.from_process;

                    // If a message was received from a neighbor partition for this very time step, remove
                    // that partition from expected messages which indicates which partitions we are waiting
                    // for
                    if msg.time == now {
                        expected_vehicle_messages.remove(&from_rank);
                    }
                    // In case a message is for a future time step store it in the message cache, until
                    // this process reaches the time step of that message. Otherwise store it in received
                    // messages and use it in the simulation
                    if msg.time <= now {
                        received_vehicle_messages.push(msg);
                    } else {
                        self.in_messages.push(msg);
                    }
                }
            });

            // wait here, so that all requests finish. This is necessary, because a process might send
            // more messages than it receives. This happens, if a process sends messages to remote
            // partitions (teleported legs) but only receives messages from neighbor partitions.
            measure_duration(Some(now), "mpi_wait_all", None, || {
                reqs.wait_all(&mut Vec::new());
            });
        });

        received_vehicle_messages
    }

    fn add_veh(&mut self, vehicle: Vehicle, now: u32) {
        let link_id = vehicle.curr_link_id().unwrap();
        let partition = *self.link_id_mapping.get(&link_id).unwrap() as u32;
        let message = self
            .out_messages
            .entry(partition)
            .or_insert_with(|| VehicleMessage::new(now, self.rank as u32, partition));
        message.add(vehicle);
    }
}

impl MpiMessageBroker {
    pub fn new(
        communicator: SystemCommunicator,
        rank: Rank,
        neighbors: HashSet<u32>,
        link_id_mapping: Arc<HashMap<usize, usize>>,
    ) -> Self {
        MpiMessageBroker {
            out_messages: HashMap::new(),
            in_messages: BinaryHeap::new(),
            communicator,
            neighbors,
            rank,
            link_id_mapping,
        }
    }

    pub fn rank_for_link(&self, link_id: u64) -> u64 {
        *self.link_id_mapping.get(&(link_id as usize)).unwrap() as u64
    }

    fn pop_from_cache(
        &mut self,
        expected_messages: &mut HashSet<u32>,
        messages: &mut Vec<VehicleMessage>,
        now: u32,
    ) {
        while let Some(msg) = self.in_messages.peek() {
            //  info!("; {}; {}; pop cache ", self.rank, now);
            if msg.time <= now {
                //  info!(
                //      "; {}; {now}; pop_cache; {}; {}; {}; {expected_messages:?}",
                //      self.rank, msg.from_process, msg.to_process, msg.time
                //  );
                expected_messages.remove(&msg.from_process);
                messages.push(self.in_messages.pop().unwrap())
            } else {
                //  info!("; {}; {}; break in cache ", self.rank, now);
                break; // important! otherwise this is an infinite loop
            }
        }
    }

    fn prepare_send_recv_vehicles(&mut self, now: u32) -> HashMap<u32, VehicleMessage> {
        let capacity = self.out_messages.len();
        let mut messages =
            std::mem::replace(&mut self.out_messages, HashMap::with_capacity(capacity));

        for partition in &self.neighbors {
            let neighbor_rank = *partition;
            messages
                .entry(neighbor_rank)
                .or_insert_with(|| VehicleMessage::new(now, self.rank as u32, neighbor_rank));
        }
        messages
    }
}
