use std::collections::VecDeque;
use std::fmt::Debug;

use crate::simulation::config;
use crate::simulation::id::Id;
use crate::simulation::network::flow_cap::Flowcap;
use crate::simulation::network::global_network::Node;
use crate::simulation::network::sim_network::StorageUpdate;
use crate::simulation::network::storage_cap::StorageCap;
use crate::simulation::network::stuck_timer::StuckTimer;
use crate::simulation::wire_types::messages::Vehicle;

use super::global_network::Link;

#[derive(Debug, Clone)]
pub enum SimLink {
    Local(LocalLink),
    In(SplitInLink),
    Out(SplitOutLink),
}

impl SimLink {
    pub fn id(&self) -> &Id<Link> {
        match self {
            SimLink::Local(ll) => &ll.id,
            SimLink::In(il) => &il.local_link.id,
            SimLink::Out(ol) => &ol.id,
        }
    }

    pub fn from(&self) -> &Id<Node> {
        match self {
            SimLink::Local(l) => l.from(),
            SimLink::In(l) => l.local_link.from(),
            SimLink::Out(_) => {
                panic!("There is no from_id of a split out link.")
            }
        }
    }

    pub fn to(&self) -> &Id<Node> {
        match self {
            SimLink::Local(l) => l.to(),
            SimLink::In(l) => l.local_link.to(),
            SimLink::Out(_) => {
                panic!("There is no from_id of a split out link.")
            }
        }
    }

    pub fn neighbor_part(&self) -> u32 {
        match self {
            SimLink::Local(_) => {
                panic!("local links don't have information about neighbor partitions")
            }
            SimLink::In(il) => il.from_part,
            SimLink::Out(ol) => ol.to_part,
        }
    }

    pub fn flow_cap(&self) -> f32 {
        match self {
            SimLink::Local(l) => l.flow_cap.capacity(),
            SimLink::In(il) => il.local_link.flow_cap.capacity(),
            SimLink::Out(_) => {
                panic!("no flow cap for out links")
            }
        }
    }

    pub fn offers_veh(&self, now: u32) -> Option<&Vehicle> {
        match self {
            SimLink::Local(ll) => ll.q_front(now),
            SimLink::In(il) => il.local_link.q_front(now),
            SimLink::Out(_) => {
                panic!("can't query out links to offer vehicles.")
            }
        }
    }

    pub fn is_veh_stuck(&self, now: u32) -> bool {
        match self {
            SimLink::Local(ll) => ll.stuck_timer.is_stuck(now),
            SimLink::In(il) => il.local_link.stuck_timer.is_stuck(now),
            SimLink::Out(_) => {
                panic!("Out links don't offer vehicles. ")
            }
        }
    }

    pub fn is_available(&self) -> bool {
        match self {
            SimLink::Local(ll) => ll.is_available(),
            SimLink::In(_) => {
                panic!("In Links can't accept vehicles")
            }
            SimLink::Out(ol) => ol.storage_cap.is_available(),
        }
    }

    pub fn used_storage(&self) -> f32 {
        match self {
            SimLink::Local(ll) => ll.storage_cap.currently_used(),
            SimLink::In(il) => il.local_link.storage_cap.currently_used(),
            SimLink::Out(ol) => ol.storage_cap.currently_used(),
        }
    }

    pub fn push_veh(&mut self, vehicle: Vehicle, now: u32) {
        match self {
            SimLink::Local(l) => l.push_veh(vehicle, now),
            SimLink::In(il) => il.local_link.push_veh(vehicle, now),
            SimLink::Out(ol) => ol.push_veh(vehicle),
        }
    }

    pub fn pop_veh(&mut self) -> Vehicle {
        match self {
            SimLink::Local(ll) => ll.pop_front(),
            SimLink::In(il) => il.local_link.pop_front(),
            SimLink::Out(_) => {
                panic!("Can't pop vehicle from out link")
            }
        }
    }

    pub fn update_flow_cap(&mut self, now: u32) {
        match self {
            SimLink::Local(ll) => ll.update_flow_cap(now),
            SimLink::In(il) => il.local_link.update_flow_cap(now),
            SimLink::Out(_) => {
                panic!("can't update flow cap on out links.")
            }
        }
    }

    pub fn update_released_storage_cap(&mut self) {
        match self {
            SimLink::Local(l) => l.apply_storage_cap_updates(),
            SimLink::In(l) => l.local_link.apply_storage_cap_updates(),
            SimLink::Out(_) => {
                panic!("Can't update storage capapcity on out link.")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalLink {
    pub id: Id<Link>,
    q: VecDeque<VehicleQEntry>,
    length: f64,
    free_speed: f32,
    storage_cap: StorageCap,
    flow_cap: Flowcap,
    stuck_timer: StuckTimer,
    pub from: Id<Node>,
    pub to: Id<Node>,
}

#[derive(Debug, Clone)]
struct VehicleQEntry {
    vehicle: Vehicle,
    earliest_exit_time: u32,
}

impl LocalLink {
    pub fn from_link(link: &Link, effective_cell_size: f32, config: config::Simulation) -> Self {
        LocalLink::new(
            link.id.clone(),
            link.capacity,
            link.freespeed,
            link.permlanes,
            link.length,
            effective_cell_size,
            config,
            link.from.clone(),
            link.to.clone(),
        )
    }

    pub fn new_with_defaults(id: Id<Link>, from: Id<Node>, to: Id<Node>) -> Self {
        LocalLink {
            id,
            q: VecDeque::new(),
            length: 1.0,
            free_speed: 1.0,
            storage_cap: StorageCap::new(0., 1., 1., 1.0, 7.5),
            flow_cap: Flowcap::new(3600., 1.0),
            stuck_timer: StuckTimer::new(u32::MAX),
            from,
            to,
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Id<Link>,
        capacity_h: f32,
        free_speed: f32,
        perm_lanes: f32,
        length: f64,
        effective_cell_size: f32,
        config: config::Simulation,
        from: Id<Node>,
        to: Id<Node>,
    ) -> Self {
        let storage_cap = StorageCap::new(
            length,
            perm_lanes,
            capacity_h,
            config.sample_size,
            effective_cell_size,
        );

        LocalLink {
            id,
            q: VecDeque::new(),
            length,
            free_speed,
            storage_cap,
            flow_cap: Flowcap::new(capacity_h, config.sample_size),
            stuck_timer: StuckTimer::new(config.stuck_threshold),
            from,
            to,
        }
    }

    pub fn push_veh(&mut self, vehicle: Vehicle, now: u32) {
        let speed = self.free_speed.min(vehicle.max_v);
        let duration = 1.max((self.length / speed as f64) as u32); // at least 1 second per link
        let earliest_exit_time = now + duration;

        // update state
        self.storage_cap.consume(vehicle.pce);
        self.q.push_back(VehicleQEntry {
            vehicle,
            earliest_exit_time,
        });
    }

    pub fn pop_front(&mut self) -> Vehicle {
        let veh = self.q.pop_front().unwrap_or_else(|| panic!("There was no vehicle in the queue. Use 'offers_veh' to test if a vehicle is present first."));
        self.flow_cap.consume_capacity(veh.vehicle.pce);
        self.storage_cap.release(veh.vehicle.pce);
        self.stuck_timer.reset();
        veh.vehicle
    }

    pub fn update_flow_cap(&mut self, now: u32) {
        // increase flow cap if new time step
        self.flow_cap.update_capacity(now);
    }

    pub fn q_front(&self, now: u32) -> Option<&Vehicle> {
        // check if we have flow cap left for current time step, otherwise abort
        if !self.flow_cap.has_capacity() {
            return None;
        }

        // peek if fist vehicle in queue can leave
        if let Some(entry) = self.q.front() {
            if entry.earliest_exit_time <= now {
                self.stuck_timer.start(now);
                return Some(&entry.vehicle);
            }
        }

        None
    }

    pub fn veh_count(&self) -> usize {
        self.q.len()
    }

    pub fn is_available(&self) -> bool {
        self.storage_cap.is_available()
    }

    pub fn apply_storage_cap_updates(&mut self) {
        self.storage_cap.apply_updates();
    }

    pub fn used_storage(&self) -> f32 {
        self.storage_cap.currently_used()
    }

    pub fn from(&self) -> &Id<Node> {
        &self.from
    }

    pub fn to(&self) -> &Id<Node> {
        &self.to
    }
}

#[derive(Debug, Clone)]
pub struct SplitOutLink {
    pub id: Id<Link>,
    pub to_part: u32,
    q: VecDeque<Vehicle>,
    storage_cap: StorageCap,
}

impl SplitOutLink {
    pub fn new(
        link: &Link,
        effective_cell_size: f32,
        sample_size: f32,
        to_part: u32,
    ) -> SplitOutLink {
        let storage_cap = StorageCap::new(
            link.length,
            link.permlanes,
            link.capacity,
            sample_size,
            effective_cell_size,
        );

        SplitOutLink {
            id: link.id.clone(),
            to_part,
            q: VecDeque::default(),
            storage_cap,
        }
    }

    pub fn apply_storage_cap_update(&mut self, released: f32) {
        self.storage_cap.consume(-released);
        self.storage_cap.apply_updates();
    }

    pub fn take_veh(&mut self) -> VecDeque<Vehicle> {
        self.storage_cap.apply_updates();
        std::mem::take(&mut self.q)
    }

    pub fn push_veh(&mut self, veh: Vehicle) {
        self.storage_cap.consume(veh.pce);
        self.q.push_back(veh);
    }
}

#[derive(Debug, Clone)]
pub struct SplitInLink {
    pub from_part: u32,
    pub local_link: LocalLink,
}

impl SplitInLink {
    pub fn new(from_part: u32, local_link: LocalLink) -> Self {
        SplitInLink {
            from_part,
            local_link,
        }
    }

    pub fn storage_cap_updates(&self) -> Option<StorageUpdate> {
        if self.has_released() {
            let released = self.local_link.storage_cap.released();
            Some(StorageUpdate {
                link_id: self.local_link.id.internal(),
                released,
                from_part: self.from_part,
            })
        } else {
            None
        }
    }

    pub fn has_released(&self) -> bool {
        self.local_link.storage_cap.released() > 0.
    }
}

#[cfg(test)]
mod sim_link_tests {
    use assert_approx_eq::assert_approx_eq;

    use crate::simulation::config;
    use crate::simulation::id::Id;
    use crate::simulation::network::link::{LocalLink, SimLink};
    use crate::simulation::wire_types::messages::Vehicle;
    use crate::test_utils;
    use crate::test_utils::create_agent;

    #[test]
    fn storage_cap_consumed() {
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            3600.,
            10.,
            3.,
            100.,
            7.5,
            test_utils::config(),
            Id::new_internal(1),
            Id::new_internal(2),
        ));
        let agent = create_agent(1, vec![]);
        let vehicle = Vehicle::new(1, 0, 10., 1.5, Some(agent));

        link.push_veh(vehicle, 0);

        // storage capacity should be consumed immediately. The expected value is max_storage_cap - pce of the vehicle
        assert_eq!(1.5, link.used_storage())
    }

    #[test]
    fn storage_cap_released() {
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            3600.,
            10.,
            3.,
            100.,
            7.5,
            test_utils::config(),
            Id::new_internal(1),
            Id::new_internal(2),
        ));
        let agent = create_agent(1, vec![]);
        let vehicle = Vehicle::new(1, 0, 10., 1.5, Some(agent));

        link.push_veh(vehicle, 0);
        let _vehicle = link.pop_veh();

        // after the vehicle is removed from the link, the available storage_cap should NOT be updated
        // immediately
        assert_eq!(1.5, link.used_storage());

        // by calling release, the accumulated released storage cap, should be freed.
        link.update_released_storage_cap();
        assert_eq!(0., link.used_storage());
        if let SimLink::Local(ll) = link {
            assert_eq!(0., ll.storage_cap.released()); // test internal prop here, because I am too lazy for a more complex test
        }
    }

    #[test]
    fn flow_cap_accumulates() {
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            360.,
            10.,
            3.,
            100.,
            7.5,
            test_utils::config(),
            Id::new_internal(1),
            Id::new_internal(2),
        ));

        let agent1 = create_agent(1, vec![]);
        let vehicle1 = Vehicle::new(1, 0, 10., 1.5, Some(agent1));
        let agent2 = create_agent(2, vec![]);
        let vehicle2 = Vehicle::new(2, 0, 10., 1.5, Some(agent2));

        link.push_veh(vehicle1, 0);
        link.push_veh(vehicle2, 0);
        link.update_flow_cap(10);
        // this should reduce the flow capacity, so that no other vehicle can leave during this time step
        let popped1 = link.pop_veh();
        assert_eq!(1, popped1.id);

        // as the flow cap is 0.1/s the next vehicle can leave the link 15s after the first
        for now in 11..24 {
            link.update_flow_cap(now);
            assert!(link.offers_veh(now).is_none());
        }

        link.update_flow_cap(25);
        if let Some(popped2) = link.offers_veh(25) {
            assert_eq!(2, popped2.id);
        } else {
            panic!("Expected vehicle2 to be available at t=30")
        }
    }

    #[test]
    fn calculates_exit_time() {
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            3600.,
            10.,
            3.,
            100.,
            7.5,
            test_utils::config(),
            Id::new_internal(1),
            Id::new_internal(2),
        ));

        let agent1 = create_agent(1, vec![]);
        let vehicle1 = Vehicle::new(1, 0, 10., 1.5, Some(agent1));

        link.push_veh(vehicle1, 0);

        // this is also implicitly tested above, but we'll do it here again, so that we have descriptive
        // test naming
        for now in 0..9 {
            assert!(link.offers_veh(now).is_none());
        }

        assert!(link.offers_veh(10).is_some())
    }

    #[test]
    fn fifo_ordering() {
        let id1 = 42;
        let id2 = 43;
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            1.,
            1.,
            1.,
            15.0,
            10.0,
            test_utils::config(),
            Id::new_internal(0),
            Id::new_internal(0),
        ));

        let agent1 = create_agent(1, vec![]);
        let vehicle1 = Vehicle::new(id1, 0, 10., 1., Some(agent1));
        let agent2 = create_agent(1, vec![]);
        let vehicle2 = Vehicle::new(id2, 0, 10., 1., Some(agent2));

        link.push_veh(vehicle1, 0);
        assert_approx_eq!(1., link.used_storage());
        assert!(link.is_available());

        link.push_veh(vehicle2, 0);
        assert_approx_eq!(2.0, link.used_storage());
        assert!(!link.is_available());

        // make sure that vehicles are added ad the end of the queue
        let popped_vehicle1 = link.pop_veh();
        assert_eq!(id1, popped_vehicle1.id);

        let popped_vehicle2 = link.pop_veh();
        assert_eq!(id2, popped_vehicle2.id);
    }

    #[test]
    pub fn stuck_time() {
        let stuck_threshold = 10;
        let config = config::Simulation {
            start_time: 0,
            end_time: 0,
            sample_size: 1.0,
            stuck_threshold,
        };
        let mut link = SimLink::Local(LocalLink::new(
            Id::create("stuck-link"),
            1.,
            1.,
            1.0,
            10.0,
            7.5,
            config,
            Id::create("from-node"),
            Id::create("to-node"),
        ));

        let vehicle = Vehicle::new(1, 0, 10., 1., None);
        link.push_veh(vehicle, 0);

        // earliest exit is at 10. Therefore this call should not trigger the stuck timer
        let offers = link.offers_veh(9);
        assert!(offers.is_none());
        assert!(!link.is_veh_stuck(9));

        // this should trigger the stuck timer
        let expected_timer_start = 10;
        let offers = link.offers_veh(expected_timer_start);
        assert!(offers.is_some());
        assert!(!link.is_veh_stuck(expected_timer_start + stuck_threshold - 1));
        assert!(link.is_veh_stuck(expected_timer_start + stuck_threshold));
    }

    #[test]
    pub fn stuck_time_reset() {
        let stuck_threshold = 10;
        let earliest_exit: u32 = 10;
        let config = config::Simulation {
            start_time: 0,
            end_time: 0,
            sample_size: 1.0,
            stuck_threshold,
        };
        let mut link = SimLink::Local(LocalLink::new(
            Id::create("stuck-link"),
            36000.,
            1.,
            1.0,
            earliest_exit as f64,
            7.5,
            config,
            Id::create("from-node"),
            Id::create("to-node"),
        ));

        let vehicle1 = Vehicle::new(1, 0, earliest_exit as f32, 1., None);
        let vehicle2 = Vehicle::new(2, 0, earliest_exit as f32, 1., None);
        link.push_veh(vehicle1, 0);
        link.push_veh(vehicle2, 0);

        // trigger stuck timer
        assert!(link.offers_veh(earliest_exit).is_some());
        // check that stuck timer works as expected
        let now = earliest_exit + stuck_threshold;
        assert!(link.is_veh_stuck(now));
        // fetch the stuck vehicle, which should reset the timer, so that the next veh is not stuck
        let _ = link.pop_veh();
        assert!(!link.is_veh_stuck(now));
        // the next vehicle should be ready to leave the link as well.
        // This call should trigger the stuck timer again.
        assert!(link.offers_veh(now).is_some());
        let now = now + stuck_threshold;
        assert!(!link.is_veh_stuck(now - 1));
        assert!(link.is_veh_stuck(now));
    }
}

#[cfg(test)]
mod out_link_tests {
    use crate::simulation::id::Id;
    use crate::simulation::network::link::{SimLink, SplitOutLink};
    use crate::simulation::network::storage_cap::StorageCap;
    use crate::simulation::wire_types::messages::Vehicle;
    use crate::test_utils::create_agent;

    #[test]
    fn push_and_take() {
        let mut link = SimLink::Out(SplitOutLink {
            id: Id::new_internal(0),
            to_part: 1,
            q: Default::default(),
            storage_cap: StorageCap::new(100., 1., 1., 1., 1.),
        });
        let id1 = 42;
        let id2 = 43;
        let agent1 = create_agent(1, vec![]);
        let vehicle1 = Vehicle::new(id1, 0, 10., 1., Some(agent1));
        let agent2 = create_agent(1, vec![]);
        let vehicle2 = Vehicle::new(id2, 0, 10., 1., Some(agent2));

        link.push_veh(vehicle1, 0);
        link.push_veh(vehicle2, 0);

        // storage should be consumed
        assert_eq!(2., link.used_storage());

        if let SimLink::Out(ref mut ol) = link {
            let mut result = ol.take_veh();

            // make sure, that vehicles have correct order
            assert_eq!(2, result.len());
            let taken_1 = result.pop_front().unwrap();
            assert_eq!(id1, taken_1.id);
            let taken_2 = result.pop_front().unwrap();
            assert_eq!(id2, taken_2.id);

            // make sure storage capacity is not released
            assert_eq!(2., link.used_storage());
        } else {
            panic!("expected out link")
        }
    }

    #[test]
    fn update_storage_caps() {
        // set up the link, so that we consume two units of storage.
        let mut cap = StorageCap::new(100., 1., 1., 1., 1.);
        cap.consume(2.);
        cap.apply_updates();
        let mut out_link = SplitOutLink {
            id: Id::new_internal(0),
            to_part: 1,
            q: Default::default(),
            storage_cap: cap,
        };

        assert_eq!(2., out_link.storage_cap.currently_used());
        out_link.apply_storage_cap_update(2.);

        assert_eq!(0., out_link.storage_cap.currently_used());
    }
}
