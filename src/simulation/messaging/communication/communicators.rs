use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Barrier};

use mpi::collective::CommunicatorCollectives;
use mpi::datatype::PartitionMut;
use mpi::point_to_point::{Destination, Source};
use mpi::topology::{Communicator, SystemCommunicator};
use mpi::{Count, Rank};
use tracing::{debug, info, instrument, span, Level};

use crate::simulation::wire_types::messages::{SimMessage, SyncMessage, TravelTimesMessage};

pub trait SimCommunicator {
    fn send_receive_vehicles<F>(
        &self,
        vehicles: HashMap<u32, SyncMessage>,
        expected_vehicle_messages: &mut HashSet<u32>,
        now: u32,
        on_msg: F,
    ) where
        F: FnMut(SyncMessage);

    fn send_receive_travel_times(
        &self,
        now: u32,
        travel_times: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage>;

    fn barrier(&self);

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

    fn send_receive_travel_times(
        &self,
        _now: u32,
        travel_times: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage> {
        //process own travel times messages
        vec![TravelTimesMessage::from(travel_times)]
    }

    fn barrier(&self) {
        info!("Barrier was called on DummySimCommunicator, which doesn't do anything.")
    }

    fn rank(&self) -> u32 {
        0
    }
}

pub struct ChannelSimCommunicator {
    receiver: Receiver<SimMessage>,
    senders: Vec<Sender<SimMessage>>,
    tt_receiver: Receiver<SimMessage>,
    tt_senders: Vec<Sender<SimMessage>>,
    rank: u32,
    barrier: Arc<Barrier>,
}

impl ChannelSimCommunicator {
    pub fn create_n_2_n(num_parts: u32) -> Vec<ChannelSimCommunicator> {
        let mut senders: Vec<_> = Vec::new();
        let mut tt_senders: Vec<_> = Vec::new();
        let mut comms: Vec<_> = Vec::new();
        let barrier = Arc::new(Barrier::new(num_parts as usize));

        for rank in 0..num_parts {
            let (sender, receiver) = channel();
            let (tt_sender, tt_receiver) = channel();
            let comm = ChannelSimCommunicator {
                receiver,
                tt_receiver,
                senders: vec![],
                tt_senders: vec![],
                rank,
                barrier: barrier.clone(),
            };
            senders.push(sender);
            tt_senders.push(tt_sender);
            comms.push(comm);
        }

        for comm in &mut comms {
            for sender in &senders {
                comm.senders.push(sender.clone());
            }

            for sender in &tt_senders {
                comm.tt_senders.push(sender.clone());
            }
        }

        comms
    }

    pub fn rank(&self) -> u32 {
        self.rank
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
                .send(SimMessage::from_sync_message(msg))
                .expect("Failed to send vehicle message in message broker");
        }

        // receive messages from everyone
        while !expected_vehicle_messages.is_empty() {
            let received_msg = self
                .receiver
                .recv()
                .expect("Error while receiving messages")
                .sync_message();
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

    fn send_receive_travel_times(
        &self,
        _now: u32,
        travel_times: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage> {
        //Waiting for all processes to enter this stage. This is to make sure, that all processes only receive
        //the travel time messages for one mode. Otherwise messages could be mixed up due to buffering of the receiver.
        self.barrier.wait();

        let message = TravelTimesMessage::from(travel_times);
        //send to each
        for sender in &self.tt_senders {
            sender
                .send(SimMessage::from_travel_times_message(message.clone()))
                .expect("Failed to send travel times message in message broker");
        }

        let mut result = Vec::new();
        while result.len() < self.tt_senders.len() {
            let sim_message = self
                .tt_receiver
                .recv()
                .expect("Error while receiving messages");

            let received_msg = sim_message.travel_times_message();
            result.push(received_msg);
        }
        result
    }

    fn barrier(&self) {
        self.barrier.wait();
    }

    fn rank(&self) -> u32 {
        self.rank
    }
}

pub struct MpiSimCommunicator {
    pub mpi_communicator: SystemCommunicator,
}

impl SimCommunicator for MpiSimCommunicator {
    #[instrument(level = "trace", skip(self, on_msg), fields(rank = self.rank()))]
    fn send_receive_vehicles<F>(
        &self,
        out_messages: HashMap<u32, SyncMessage>,
        expected_vehicle_messages: &mut HashSet<u32>,
        now: u32,
        mut on_msg: F,
    ) where
        F: FnMut(SyncMessage),
    {
        let send_span = span!(Level::TRACE, "send_msgs", rank = self.rank(), now = now);
        let send_time = send_span.enter();
        let buf_msg: Vec<_> = out_messages
            .into_iter()
            .map(|(to, m)| (to, SimMessage::from_sync_message(m).serialize()))
            .collect();

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
                    .process_at_rank(*message as Rank)
                    .immediate_send(scope, buf);
                reqs.add(req);
            }
            drop(send_time);

            let receive_span = span!(Level::TRACE, "receive_msgs", rank = self.rank(), now = now);
            let handle_span = span!(Level::TRACE, "handle_msgs", rank = self.rank(), now = now);
            // Use blocking MPI_recv here, since we don't have anything to do if there are no other
            // messages.
            while !expected_vehicle_messages.is_empty() {
                // measure the wait time for receiving
                let receive_time = receive_span.enter();
                let (encoded_msg, _status) = self.mpi_communicator.any_process().receive_vec();
                drop(receive_time);

                let handle_time = handle_span.enter();
                let msg = SimMessage::deserialize(&encoded_msg).sync_message();
                let from_rank = msg.from_process;

                // If a message was received from a neighbor partition for this very time step, remove
                // that partition from expected messages which indicates which partitions we are waiting
                // for
                if msg.time == now {
                    expected_vehicle_messages.remove(&from_rank);
                }

                on_msg(msg);
                drop(handle_time);
            }

            // wait here, so that all requests finish. This is necessary, because a process might send
            // more messages than it receives. This happens, if a process sends messages to remote
            // partitions (teleported legs) but only receives messages from neighbor partitions.
            // this also accounts for wait times
            let receive_time = receive_span.enter();
            reqs.wait_all(&mut Vec::new());
            drop(receive_time)
        });
    }

    fn send_receive_travel_times(
        &self,
        _now: u32,
        travel_times: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage> {
        let travel_times_message =
            SimMessage::from_travel_times_message(TravelTimesMessage::from(travel_times));
        let serial_travel_times_message = travel_times_message.serialize();

        debug!(
            "Sending travel times message with {} bytes.",
            serial_travel_times_message.len()
        );

        let messages: Vec<TravelTimesMessage> =
            self.gather_travel_times(&serial_travel_times_message, _now);

        messages
    }

    fn barrier(&self) {
        self.mpi_communicator.barrier();
    }

    fn rank(&self) -> u32 {
        self.mpi_communicator.rank() as u32
    }
}

impl MpiSimCommunicator {
    fn gather_travel_times(
        &self,
        sim_travel_times_message: &Vec<u8>,
        _now: u32,
    ) -> Vec<TravelTimesMessage> {
        // ------- Gather traffic info lengths -------
        let mut travel_times_length_buffer =
            self.gather_travel_time_lengths(&sim_travel_times_message, _now);

        // ------- Gather traffic info -------
        if travel_times_length_buffer.iter().sum::<i32>() <= 0 {
            // if there is no traffic data to be sent, we do not actually perform mpi communication
            // because mpi would crash
            return Vec::new();
        }

        let travel_times_buffer = self.gather_travel_times_var_count(
            &sim_travel_times_message,
            &mut travel_times_length_buffer,
            _now,
        );

        debug!(
            "Received travel times message with {} bytes.",
            travel_times_buffer.len()
        );

        self.deserialize_travel_times(travel_times_buffer, travel_times_length_buffer, _now)
    }

    #[instrument(level = "trace", skip(self, sim_travel_times_message, travel_times_length_buffer), fields(rank = self.rank()))]
    fn gather_travel_times_var_count(
        &self,
        sim_travel_times_message: &&Vec<u8>,
        mut travel_times_length_buffer: &mut Vec<i32>,
        _now: u32,
    ) -> Vec<u8> {
        let mut travel_times_buffer =
            vec![0u8; travel_times_length_buffer.iter().sum::<i32>() as usize];
        let info_displs = Self::get_travel_times_displs(&mut travel_times_length_buffer);
        let mut partition = PartitionMut::new(
            &mut travel_times_buffer,
            travel_times_length_buffer.clone(),
            &info_displs[..],
        );
        self.mpi_communicator
            .all_gather_varcount_into(&sim_travel_times_message[..], &mut partition);
        travel_times_buffer
    }

    #[instrument(level = "trace", skip(self, sim_travel_times_message), fields(rank = self.rank()))]
    fn gather_travel_time_lengths(
        &self,
        sim_travel_times_message: &&Vec<u8>,
        _now: u32,
    ) -> Vec<i32> {
        let mut travel_times_length_buffer = vec![0i32; self.mpi_communicator.size() as usize];
        self.mpi_communicator.all_gather_into(
            &(sim_travel_times_message.len() as i32),
            &mut travel_times_length_buffer[..],
        );
        travel_times_length_buffer
    }

    fn get_travel_times_displs(all_travel_times_message_lengths: &mut [i32]) -> Vec<Count> {
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

    #[instrument(level = "trace", skip_all, fields(rank = self.rank()))]
    fn deserialize_travel_times(
        &self,
        all_travel_times_messages: Vec<u8>,
        lengths: Vec<i32>,
        _now: u32,
    ) -> Vec<TravelTimesMessage> {
        let mut result = Vec::new();
        let mut last_end_index = 0usize;
        for len in lengths {
            let begin_index = last_end_index;
            let end_index = last_end_index + len as usize;
            result.push(SimMessage::deserialize(
                &all_travel_times_messages[begin_index..end_index],
            ));
            last_end_index = end_index;
        }
        result
            .into_iter()
            .map(|s| s.travel_times_message())
            .collect()
    }
}
