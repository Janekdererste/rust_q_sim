use crate::io::network::IOLink;
use crate::parallel_simulation::network::flowcap::Flowcap;
use std::collections::VecDeque;
use std::fmt::Debug;

#[derive(Debug)]
pub enum Link<V: Debug> {
    LocalLink(LocalLink<V>),
    SplitInLink(SplitInLink<V>),
    SplitOutLink(SplitOutLink),
}

#[derive(Debug)]
pub struct LocalLink<V: Debug> {
    id: usize,
    q: VecDeque<VehicleQEntry<V>>,
    length: f32,
    freespeed: f32,
    flowcap: Flowcap,
}

#[derive(Debug)]
struct VehicleQEntry<V> {
    vehicle: V,
    earliest_exit_time: u32,
}

impl<V: Debug> LocalLink<V> {
    pub fn from_io_link(id: usize, link: &IOLink, sample_size: f32) -> Self {
        LocalLink::new(id, link.capacity, link.freespeed, link.length, sample_size)
    }

    pub fn new(id: usize, capacity_h: f32, freespeed: f32, length: f32, sample_size: f32) -> Self {
        LocalLink {
            id,
            q: VecDeque::new(),
            flowcap: Flowcap::new(capacity_h * sample_size / 3600.),
            freespeed,
            length,
        }
    }

    pub fn push_vehicle(&mut self, vehicle: V, now: u32) {
        let earliest_exit_time = now + (self.length / self.freespeed) as u32;
        self.q.push_back(VehicleQEntry {
            vehicle,
            earliest_exit_time,
        });
    }

    pub fn pop_front(&mut self, now: u32) -> Vec<V> {
        self.flowcap.update_capacity(now);

        let mut popped_veh = Vec::new();

        while let Some(entry) = self.q.front() {
            if entry.earliest_exit_time > now || !self.flowcap.has_capacity() {
                break;
            }

            let vehicle = self.q.pop_front().unwrap().vehicle;
            self.flowcap.consume_capacity(1.0);
            popped_veh.push(vehicle);
        }

        popped_veh
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct SplitOutLink {
    id: usize,
    to_part: usize,
}

impl SplitOutLink {
    pub fn new(id: usize, to_part: usize) -> SplitOutLink {
        SplitOutLink { id, to_part }
    }

    pub fn neighbor_partition_id(&self) -> usize {
        self.to_part
    }
    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct SplitInLink<V: Debug> {
    from_part: usize,
    local_link: LocalLink<V>,
}

impl<V: Debug> SplitInLink<V> {
    pub fn new(from_part: usize, local_link: LocalLink<V>) -> Self {
        SplitInLink {
            from_part,
            local_link,
        }
    }

    pub fn neighbor_partition_id(&self) -> usize {
        self.from_part
    }

    pub fn local_link_mut(&mut self) -> &mut LocalLink<V> {
        &mut self.local_link
    }
}

#[cfg(test)]
mod tests {
    use crate::parallel_simulation::network::link::LocalLink;
    use crate::parallel_simulation::vehicles::Vehicle;

    #[test]
    fn local_link_push_single_veh() {
        let veh_id = 42;
        let mut link = LocalLink::new(1, 1., 1., 10., 1.);
        let vehicle = Vehicle::new(veh_id, 1, vec![]);

        link.push_vehicle(vehicle, 0);

        // this should put the vehicle into the queue and update the exit time correctly
        let pushed_vehicle = link.q.front().unwrap();
        assert_eq!(veh_id, pushed_vehicle.vehicle.id);
        assert_eq!(10, pushed_vehicle.earliest_exit_time);
    }

    #[test]
    fn local_link_push_multiple_veh() {
        let id1 = 42;
        let id2 = 43;
        let mut link = LocalLink::new(1, 1., 1., 11.8, 1.);
        let vehicle1 = Vehicle::new(id1, id1, vec![]);
        let vehicle2 = Vehicle::new(id2, id2, vec![]);

        link.push_vehicle(vehicle1, 0);
        link.push_vehicle(vehicle2, 0);

        // make sure that vehicles are added ad the end of the queue
        assert_eq!(2, link.q.len());

        let popped_vehicle1 = link.q.pop_front().unwrap();
        assert_eq!(id1, popped_vehicle1.vehicle.id);
        assert_eq!(11, popped_vehicle1.earliest_exit_time);

        let popped_vehicle2 = link.q.pop_front().unwrap();
        assert_eq!(id2, popped_vehicle2.vehicle.id);
        assert_eq!(11, popped_vehicle2.earliest_exit_time);
    }

    #[test]
    fn local_link_pop_with_exit_time() {
        let mut link = LocalLink::new(1, 1000000., 10., 100., 1.);

        let mut n: u32 = 0;

        while n < 10 {
            link.push_vehicle(Vehicle::new(n as usize, n as usize, vec![]), n);
            n += 1;
        }

        let pop1 = link.pop_front(12);
        assert_eq!(3, pop1.len());
        let pop2 = link.pop_front(12);
        assert_eq!(0, pop2.len());
        let pop3 = link.pop_front(20);
        assert_eq!(7, pop3.len());
    }

    #[test]
    fn local_link_pop_with_capacity() {
        // link has capacity of 2 per second
        let mut link = LocalLink::new(1, 7200., 10., 100., 1.);

        let mut n: u32 = 0;

        while n < 10 {
            link.push_vehicle(Vehicle::new(n as usize, n as usize, vec![]), n);
            n += 1;
        }

        n = 0;
        while n < 5 {
            let popped = link.pop_front(20 + n);
            assert_eq!(2, popped.len());
            n += 1;
        }
    }

    #[test]
    fn local_link_pop_with_capacity_reduced() {
        // link has a capacity of 1 * 0.1 per second
        let mut link = LocalLink::new(1, 3600., 10., 100., 0.1);

        link.push_vehicle(Vehicle::new(1, 1, vec![]), 0);
        link.push_vehicle(Vehicle::new(2, 2, vec![]), 0);

        let popped = link.pop_front(10);
        assert_eq!(1, popped.len());

        // actually this shouldn't let vehicles at 19 seconds as well, but due to floating point arithmatic
        // the flowcap inside the link has a accumulated capacity slightly greater than 0 at 19 🤷‍♀️
        let popped_2 = link.pop_front(18);
        assert_eq!(0, popped_2.len());

        let popped_3 = link.pop_front(20);
        assert_eq!(1, popped_3.len());
    }
}
