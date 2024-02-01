use ahash::HashMapExt;
use nohash_hasher::IntMap;
use std::any::Any;
use std::collections::HashMap;

use crate::simulation::messaging::events::EventsSubscriber;
use crate::simulation::wire_types::events::event::Type;
use crate::simulation::wire_types::events::{
    Event, LinkEnterEvent, LinkLeaveEvent, PersonLeavesVehicleEvent,
};

pub struct TravelTimeCollector {
    travel_times_by_link: IntMap<u64, Vec<u32>>,
    cache_enter_time_by_vehicle: IntMap<u64, u32>,
}

impl Default for TravelTimeCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TravelTimeCollector {
    pub fn new() -> Self {
        TravelTimeCollector {
            travel_times_by_link: IntMap::new(),
            cache_enter_time_by_vehicle: IntMap::new(),
        }
    }

    fn process_link_enter_event(&mut self, time: u32, event: &LinkEnterEvent) {
        // link enter events will always be stored
        self.cache_enter_time_by_vehicle.insert(event.vehicle, time);
    }

    fn process_link_leave_event(&mut self, time: u32, event: &LinkLeaveEvent) {
        // remove traffic information of link enter and compute travel time (assumes that a vehicle will leave a link before it enters the same again)
        // if it's None, the LinkLeaveEvent is the begin of a leg, thus no travel time can be computed
        if let Some(t) = self.cache_enter_time_by_vehicle.remove(&event.vehicle) {
            self.travel_times_by_link
                .entry(event.link)
                .or_insert(Vec::new())
                .push(time - t)
        }
    }

    fn process_person_leaves_vehicle_event(
        &mut self,
        _time: u32,
        event: &PersonLeavesVehicleEvent,
    ) {
        self.cache_enter_time_by_vehicle.remove(&event.vehicle);
    }

    pub fn get_travel_time_of_link(&self, link: u64) -> Option<u32> {
        match self.travel_times_by_link.get(&link) {
            None => None,
            Some(travel_times) => {
                let sum: u32 = travel_times.iter().sum();
                let len = travel_times.len();
                Some(sum / (len as u32))
            }
        }
    }

    pub fn get_travel_times(&self) -> HashMap<u64, u32> {
        self.travel_times_by_link
            .keys()
            .map(|id| (*id, self.get_travel_time_of_link(*id)))
            .filter(|(_, travel_time)| travel_time.is_some())
            .map(|(id, travel_time)| (id, travel_time.unwrap()))
            .collect::<HashMap<u64, u32>>()
    }

    pub fn flush(&mut self) {
        // Collected travel times will be dropped, but cached values not.
        // Vehicles of cached values haven't left the corresponding links yet.
        // A travel time of a link is considered when a vehicle leaves the link.
        self.travel_times_by_link = IntMap::new();
    }
}

impl EventsSubscriber for TravelTimeCollector {
    fn receive_event(&mut self, time: u32, event: &Event) {
        match event.r#type.as_ref().unwrap() {
            Type::LinkEnter(e) => self.process_link_enter_event(time, e),
            Type::LinkLeave(e) => self.process_link_leave_event(time, e),
            Type::PersonLeavesVeh(e) => self.process_person_leaves_vehicle_event(time, e),
            _ => {}
        }
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use crate::simulation::messaging::events::EventsSubscriber;
    use crate::simulation::replanning::routing::travel_time_collector::TravelTimeCollector;
    use crate::simulation::wire_types::events::Event;

    #[test]
    fn test_one_vehicle() {
        let mut collector = TravelTimeCollector::new();
        collector.receive_event(1, &Event::new_link_leave(1, 1));
        collector.receive_event(2, &Event::new_link_enter(2, 1));
        collector.receive_event(4, &Event::new_link_leave(2, 1));

        assert_eq!(collector.get_travel_time_of_link(2), Some(2));
        assert_eq!(collector.get_travel_time_of_link(1), None);
        assert_eq!(collector.get_travel_times().keys().len(), 1);
        assert_eq!(collector.get_travel_times().get(&2), Some(&2u32));

        assert_eq!(collector.cache_enter_time_by_vehicle.get(&1), None)
    }

    #[test]
    /// Tests travel time collection with two vehicles passing link 2.
    /// Vehicle 1: 2s
    /// Vehicle 2: 4s
    fn test_two_vehicles() {
        let mut collector = TravelTimeCollector::new();
        collector.receive_event(1, &Event::new_link_leave(1, 1));
        // vehicle 1 enters
        collector.receive_event(2, &Event::new_link_enter(2, 1));
        // vehicle 2 enters
        collector.receive_event(3, &Event::new_link_enter(2, 2));
        // vehicle 1 leaves
        collector.receive_event(4, &Event::new_link_leave(2, 1));
        // vehicle 3 enters
        collector.receive_event(5, &Event::new_link_enter(2, 3));
        // vehicle 2 leaves
        collector.receive_event(7, &Event::new_link_leave(2, 2));

        // The average travel time on link 2 is 3
        assert_eq!(Some(3), collector.get_travel_time_of_link(2));
        assert_eq!(None, collector.get_travel_time_of_link(1));

        assert_eq!(collector.get_travel_times().keys().len(), 1);
        assert_eq!(collector.get_travel_times().get(&2), Some(&3u32));

        // vehicle 1 and 2 have no cached traffic information
        assert_eq!(collector.cache_enter_time_by_vehicle.get(&1), None);
        assert_eq!(collector.cache_enter_time_by_vehicle.get(&2), None);

        // vehicle 3 has cached traffic information
        assert_eq!(collector.cache_enter_time_by_vehicle.get(&3).unwrap(), &5)
    }

    #[test]
    /// Tests whether PersonLeavesVehicleEvent discards travel time
    fn test_with_person_leaves_vehicle() {
        let mut collector = TravelTimeCollector::new();
        collector.receive_event(0, &Event::new_link_enter(1, 1));
        collector.receive_event(2, &Event::new_person_leaves_veh(1, 1));
        collector.receive_event(4, &Event::new_link_leave(1, 1));

        assert_eq!(collector.get_travel_time_of_link(1), None);
        assert_eq!(collector.cache_enter_time_by_vehicle.get(&1), None);
    }

    #[test]
    /// Tests whether PersonLeavesVehicleEvent discards travel time
    fn test_with_person_leaves_vehicle_complex() {
        let mut collector = TravelTimeCollector::new();
        collector.receive_event(0, &Event::new_link_enter(1, 1));

        //intermediate veh 2 enters link 1
        collector.receive_event(1, &Event::new_link_enter(1, 2));

        collector.receive_event(2, &Event::new_person_leaves_veh(1, 1));

        //intermediate veh 2 leaves link 1
        collector.receive_event(3, &Event::new_link_leave(1, 2));

        collector.receive_event(10, &Event::new_link_leave(1, 1));
        collector.receive_event(10, &Event::new_link_enter(2, 1));
        collector.receive_event(20, &Event::new_link_leave(2, 1));

        assert_eq!(collector.get_travel_time_of_link(1), Some(2));
        assert_eq!(collector.get_travel_time_of_link(2), Some(10));
        assert_eq!(collector.cache_enter_time_by_vehicle.get(&1), None);
    }
}
