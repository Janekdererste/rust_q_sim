use std::collections::HashMap;

use mpi::{Count, Rank};
use mpi::collective::CommunicatorCollectives;
use mpi::datatype::PartitionMut;
use mpi::topology::{Communicator, SystemCommunicator};

use crate::simulation::messaging::messages::proto::TravelTimesMessage;

pub struct TravelTimesMessageBroker {
    pub rank: Rank,
    communicator: SystemCommunicator,
}

impl TravelTimesMessageBroker {
    pub fn new(communicator: SystemCommunicator, rank: Rank) -> Self {
        TravelTimesMessageBroker { rank, communicator }
    }

    pub fn send_recv(
        &mut self,
        now: u32,
        travel_times: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage> {
        let travel_times_message = TravelTimesMessage::from(travel_times);
        let serial_travel_times_message = travel_times_message.serialize();

        self.gather_travel_times(&serial_travel_times_message)
    }

    fn gather_travel_times(&mut self, travel_times_message: &Vec<u8>) -> Vec<TravelTimesMessage> {
        // ------- Gather traffic info lengths -------
        let mut travel_times_length_buffer = vec![0i32; self.communicator.size() as usize];
        self.communicator.all_gather_into(
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
        self.communicator
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
