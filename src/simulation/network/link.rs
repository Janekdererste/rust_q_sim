use std::collections::VecDeque;
use std::fmt::Debug;

use crate::simulation::id::Id;
use crate::simulation::messaging::messages::proto::Vehicle;
use crate::simulation::network::flow_cap::Flowcap;
use crate::simulation::network::global_network::Node;
use crate::simulation::network::storage_cap::StorageCap;

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
            SimLink::Local(ll) => ll.storage_cap.used,
            SimLink::In(il) => il.local_link.storage_cap.used,
            SimLink::Out(ol) => ol.storage_cap.used,
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
            SimLink::Local(l) => l.update_released_storage_cap(),
            SimLink::In(l) => l.local_link.update_released_storage_cap(),
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
    pub from: Id<Node>,
    pub to: Id<Node>,
}

#[derive(Debug, Clone)]
struct VehicleQEntry {
    vehicle: Vehicle,
    earliest_exit_time: u32,
}

impl LocalLink {
    pub fn from_link(link: &Link, sample_size: f32, effective_cell_size: f32) -> Self {
        LocalLink::new(
            link.id.clone(),
            link.capacity,
            link.freespeed,
            link.permlanes,
            link.length,
            sample_size,
            effective_cell_size,
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
            flow_cap: Flowcap::new(1.0),
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
        sample_size: f32,
        effective_cell_size: f32,
        from: Id<Node>,
        to: Id<Node>,
    ) -> Self {
        let flow_cap_s = capacity_h * sample_size / 3600.;
        let storage_cap = StorageCap::new(
            length,
            perm_lanes,
            flow_cap_s,
            sample_size,
            effective_cell_size,
        );

        LocalLink {
            id,
            q: VecDeque::new(),
            length,
            free_speed,
            storage_cap,
            flow_cap: Flowcap::new(flow_cap_s),
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

    pub fn update_released_storage_cap(&mut self) {
        self.storage_cap.apply_released();
    }

    pub fn used_storage(&self) -> f32 {
        self.storage_cap.used
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
        let flow_cap_s = link.capacity * sample_size / 3600.;
        let storage_cap = StorageCap::new(
            link.length,
            link.permlanes,
            flow_cap_s,
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

    pub fn set_used_storage_cap(&mut self, value: f32) {
        self.storage_cap.clear();
        self.storage_cap.consume(value);
    }

    pub fn take_veh(&mut self) -> VecDeque<Vehicle> {
        self.storage_cap.clear();
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
}

#[cfg(test)]
mod sim_link_tests {
    use assert_approx_eq::assert_approx_eq;

    use crate::simulation::id::Id;
    use crate::simulation::messaging::messages::proto::Vehicle;
    use crate::simulation::network::link::{LocalLink, SimLink};
    use crate::test_utils::create_agent;

    #[test]
    fn storage_cap_consumed() {
        let mut link = SimLink::Local(LocalLink::new(
            Id::new_internal(1),
            3600.,
            10.,
            3.,
            100.,
            1.0,
            7.5,
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
            1.0,
            7.5,
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
            assert_eq!(0., ll.storage_cap.released); // test internal prop here, because I am too lazy for a more complex test
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
            1.0,
            7.5,
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
            1.0,
            7.5,
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
            1.,
            10.0,
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
}

#[cfg(test)]
mod local_link_tests {
    use crate::simulation::id::Id;
    use crate::simulation::network::link::LocalLink;
    use assert_approx_eq::assert_approx_eq;

    #[test]
    fn storage_cap_initialized_default() {
        let link = LocalLink::new(
            Id::new_internal(1),
            1.,
            1.,
            3.,
            100.,
            0.2,
            7.5,
            Id::new_internal(1),
            Id::new_internal(2),
        );

        // we expect a storage size of 100 * 3 * 0.2 / 7.5 = 8
        assert_approx_eq!(8., link.storage_cap.max);
    }

    #[test]
    fn storage_cap_initialized_large_flow() {
        let link = LocalLink::new(
            Id::new_internal(1),
            360000.,
            1.,
            3.,
            100.,
            0.2,
            7.5,
            Id::new_internal(1),
            Id::new_internal(2),
        );

        // we expect a storage size of 20. because it the flow cap/s is 20 (36000 * 0.2 / 3600)
        assert_eq!(20., link.storage_cap.max);
    }

    #[test]
    fn flow_cap_initialized() {
        let link = LocalLink::new(
            Id::new_internal(1),
            3600.,
            1.,
            3.,
            100.,
            0.2,
            7.5,
            Id::new_internal(1),
            Id::new_internal(2),
        );

        assert_eq!(0.2, link.flow_cap.capacity())
    }
}

#[cfg(test)]
mod out_link_tests {
    use crate::simulation::id::Id;
    use crate::simulation::messaging::messages::proto::Vehicle;
    use crate::simulation::network::link::{SimLink, SplitOutLink};
    use crate::simulation::network::storage_cap::StorageCap;
    use crate::test_utils::create_agent;

    #[test]
    fn push_and_take() {
        let mut link = SimLink::Out(SplitOutLink {
            id: Id::new_internal(0),
            to_part: 1,
            q: Default::default(),
            storage_cap: StorageCap {
                max: 100.,
                released: 0.0,
                used: 0.0,
            },
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

            // make sure storage capacity is released
            assert_eq!(0., link.used_storage());
        } else {
            panic!("expected out link")
        }
    }
}
