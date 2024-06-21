use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;

use prost::Message;

use crate::simulation::time_queue::EndTime;
use crate::simulation::wire_types::messages::sim_message::Type;
use crate::simulation::wire_types::messages::{
    Empty, SimMessage, StorageCap, SyncMessage, TravelTimesMessage, Vehicle,
};
use crate::simulation::wire_types::population::Person;

impl SimMessage {
    pub fn sync_message(self) -> SyncMessage {
        match self.r#type.unwrap() {
            Type::Sync(m) => m,
            _ => panic!("That message is no sync message."),
        }
    }

    pub fn travel_times_message(self) -> TravelTimesMessage {
        match self.r#type.unwrap() {
            Type::TravelTimes(t) => t,
            _ => panic!("That message is no travel times message."),
        }
    }

    pub fn from_sync_message(m: SyncMessage) -> SimMessage {
        SimMessage {
            r#type: Some(Type::Sync(m)),
        }
    }

    pub fn from_travel_times_message(m: TravelTimesMessage) -> SimMessage {
        SimMessage {
            r#type: Some(Type::TravelTimes(m)),
        }
    }

    pub fn from_empty(m: Empty) -> SimMessage {
        SimMessage {
            r#type: Some(Type::Barrier(m)),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buffer).unwrap();
        buffer
    }

    pub fn deserialize(buffer: &[u8]) -> SimMessage {
        SimMessage::decode(&mut Cursor::new(buffer)).unwrap()
    }
}

impl SyncMessage {
    pub fn new(time: u32, from: u32, to: u32) -> Self {
        Self {
            time,
            from_process: from,
            to_process: to,
            vehicles: Vec::new(),
            storage_capacities: Vec::new(),
        }
    }

    pub fn add_veh(&mut self, vehicle: Vehicle) {
        self.vehicles.push(vehicle);
    }

    pub fn add_storage_cap(&mut self, storage_cap: StorageCap) {
        self.storage_capacities.push(storage_cap);
    }
}

impl TravelTimesMessage {
    pub fn new() -> Self {
        TravelTimesMessage {
            travel_times_by_link_id: HashMap::new(),
        }
    }

    pub fn from(map: HashMap<u64, u32>) -> Self {
        TravelTimesMessage {
            travel_times_by_link_id: map,
        }
    }

    pub fn add_travel_time(&mut self, link: u64, travel_time: u32) {
        self.travel_times_by_link_id.insert(link, travel_time);
    }
}

// Implementation for ordering, so that vehicle messages can be put into a message queue sorted by time
impl PartialOrd for SyncMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SyncMessage {}

impl Ord for SyncMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time)
    }
}

impl Vehicle {
    // todo, fix type and mode
    pub fn new(id: u64, veh_type: u64, max_v: f32, pce: f32, driver: Option<Person>) -> Vehicle {
        Vehicle {
            id,
            driver,
            curr_route_elem: 0,
            r#type: veh_type,
            max_v,
            pce,
            passengers: vec![],
        }
    }

    pub fn driver(&self) -> &Person {
        self.driver.as_ref().unwrap()
    }

    pub fn passengers(&self) -> &Vec<Person> {
        &self.passengers
    }

    pub fn id(&self) -> usize {
        self.id as usize
    }

    pub fn advance_route_index(&mut self) {
        self.curr_route_elem += 1;
    }

    /// This method advances the pointer to the last element of the route. We need this in case of
    /// teleported legs. Advancing the route pointer to the last element directly ensures that teleporting
    /// the vehicle is independent of whether the leg has a Generic-Teleportation route or a network
    /// route.
    pub fn route_index_to_last(&mut self) {
        let route_len = self.driver().curr_leg().route.as_ref().unwrap().route.len() as u32;
        self.curr_route_elem = route_len - 1;
    }

    pub fn curr_link_id(&self) -> Option<u64> {
        let leg = self.driver().curr_leg();
        let route = leg.route.as_ref().unwrap();
        let index = self.curr_route_elem as usize;
        route.route.get(index).copied()
    }

    // todo same as above
    pub fn is_current_link_last(&self) -> bool {
        let leg = self.driver().curr_leg();
        let route = leg.route.as_ref().unwrap();
        self.curr_route_elem + 1 >= route.route.len() as u32
    }

    pub fn peek_next_route_element(&self) -> Option<u64> {
        let route = self.driver().curr_leg().route.as_ref().unwrap();
        let next_i = self.curr_route_elem as usize + 1;
        route.route.get(next_i).copied()
    }
}

impl EndTime for Vehicle {
    fn end_time(&self, now: u32) -> u32 {
        self.driver().end_time(now)
    }
}
