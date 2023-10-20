use nohash_hasher::{BuildNoHashHasher, IntMap, IntSet};
use rand::distributions::Distribution;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use rand_distr::WeightedAliasIndex;

use crate::simulation::messaging::messages::proto::StorageCap;
use crate::simulation::messaging::{
    events::{proto::Event, EventsPublisher},
    messages::proto::Vehicle,
};

use super::{
    global_network::{Link, Network, Node},
    link::{LocalLink, SimLink, SplitInLink, SplitOutLink},
};

pub struct SplitStorage {
    pub link_id: u64,
    pub from_part: u32,
    pub used: f32,
}

#[derive(Debug)]
pub struct SimNetworkPartition<'n> {
    pub nodes: Vec<SimNode>,
    // use int map as hash map variant with stable order
    pub links: IntMap<u64, SimLink>,
    pub global_network: &'n Network<'n>,
    rnd: ThreadRng,
}

#[derive(Debug)]
pub struct SimNode {
    id: u64,
    in_links: Vec<u64>,
    in_links_weights: Option<WeightedAliasIndex<f32>>,
}
impl<'n> SimNetworkPartition<'n> {
    pub fn from_network(global_network: &'n Network, partition: u32) -> Self {
        let nodes: Vec<&Node> = global_network
            .nodes
            .iter()
            .filter(|n| n.partition == partition)
            .collect();

        let link_ids: Vec<_> = nodes
            .iter()
            .flat_map(|n| n.in_links.iter().chain(n.out_links.iter()))
            .collect(); // collect here to get each link id only once

        let sim_links: IntMap<_, _> = link_ids
            .iter()
            .map(|link_id| global_network.get_link(link_id))
            .map(|link| {
                (
                    link.id.internal(),
                    Self::create_sim_link(
                        link,
                        partition,
                        global_network.effective_cell_size,
                        global_network,
                    ),
                )
            })
            .collect();

        let sim_nodes: Vec<_> = nodes
            .iter()
            .map(|n| Self::create_sim_node(n, global_network))
            .collect();

        Self::new(sim_nodes, sim_links, global_network)
    }

    fn create_sim_node(node: &Node, network: &Network) -> SimNode {
        let id = node.id.internal();
        let in_links: Vec<u64> = node.in_links.iter().map(|l_id| l_id.internal()).collect();
        let capacities: Vec<f32> = node
            .in_links
            .iter()
            .map(|l_id| network.links.get(l_id.internal() as usize).unwrap())
            .map(|link| link.capacity)
            .collect();

        let in_links_weights = if capacities.is_empty() {
            None
        } else {
            Some(WeightedAliasIndex::new(capacities).unwrap())
        };

        SimNode {
            id,
            in_links,
            in_links_weights,
        }
    }

    fn create_sim_link(
        link: &Link,
        partition: u32,
        effective_cell_size: f32,
        global_network: &Network,
    ) -> SimLink {
        let from_part = global_network.get_node(&link.from).partition; //all_nodes.get(link.from.internal()).unwrap().partition;
        let to_part = global_network.get_node(&link.to).partition; //all_nodes.get(link.to.internal()).unwrap().partition;

        if from_part == to_part {
            SimLink::Local(LocalLink::from_link(link, 1.0, effective_cell_size))
        } else if to_part == partition {
            let local_link = LocalLink::from_link(link, 1.0, 7.5);
            SimLink::In(SplitInLink::new(from_part, local_link))
        } else {
            SimLink::Out(SplitOutLink::new(link, effective_cell_size, 1.0, to_part))
        }
    }

    pub fn new(
        nodes: Vec<SimNode>,
        links: IntMap<u64, SimLink>,
        global_network: &'n Network,
    ) -> Self {
        SimNetworkPartition {
            nodes,
            links,
            global_network,
            rnd: thread_rng(),
        }
    }

    pub fn neighbors(&self) -> IntSet<u32> {
        let distinct_partitions: IntSet<u32> = self
            .links
            .values()
            .filter(|link| match link {
                SimLink::Local(_) => false,
                SimLink::In(_) => true,
                SimLink::Out(_) => true,
            })
            .map(|link| link.neighbor_part())
            .collect();
        distinct_partitions
    }

    pub fn send_veh_en_route(&mut self, vehicle: Vehicle, now: u32) {
        let link_id = vehicle.curr_link_id().unwrap_or_else(|| {
            panic!("Vehicle is expected to have a current link id if it is sent onto the network")
        });
        let link = self.links.get_mut(&link_id).unwrap();
        link.push_veh(vehicle, now);
    }

    pub fn update_storage_caps(&mut self, storage_caps: Vec<StorageCap>) {
        for cap in storage_caps {
            if let SimLink::Out(link) = self.links.get_mut(&cap.link_id).unwrap() {
                link.set_used_storage_cap(cap.value);
            } else {
                panic!("only expecting ids for split out links ")
            }
        }
    }

    pub fn move_links(&mut self, now: u32) -> (Vec<Vehicle>, Vec<SplitStorage>) {
        let mut storage_cap: Vec<_> = Vec::new();
        let mut vehicles: Vec<_> = Vec::new();

        for link in self.links.values_mut() {
            match link {
                // update the used storage capacity by the number of pce we have released in the previous
                // move_nodes step and update flow cap
                SimLink::Local(_) => {
                    link.update_flow_cap(now);
                    link.update_released_storage_cap();
                }
                // update storage cap as with the local link and capture the used storage cap
                // so that we can send it to the neighbor partition and update flow cap
                SimLink::In(il) => {
                    let from_part = il.from_part;

                    link.update_flow_cap(now);
                    link.update_released_storage_cap();

                    // put storage cap information into the result if there is anything to report.
                    if link.used_storage() > 0. {
                        let used_storage = link.used_storage();
                        let id = link.id().internal();

                        storage_cap.push(SplitStorage {
                            link_id: id,
                            used: used_storage,
                            from_part,
                        });
                    }
                }
                // take all vehicles in the out link so that we can send them to the next partition
                SimLink::Out(ol) => {
                    let out_q = ol.take_veh();
                    for veh in out_q {
                        vehicles.push(veh);
                    }
                }
            }
        }

        (vehicles, storage_cap)
    }

    pub fn move_nodes(&mut self, events: &mut EventsPublisher, now: u32) -> Vec<Vehicle> {
        let mut exited_vehicles = Vec::new();

        for node in &self.nodes {
            Self::move_node(
                node,
                &mut self.links,
                &mut exited_vehicles,
                events,
                &mut self.rnd,
                now,
            );
        }

        exited_vehicles
    }

    fn move_node(
        node: &SimNode,
        links: &mut IntMap<u64, SimLink>,
        exited_vehicles: &mut Vec<Vehicle>,
        events: &mut EventsPublisher,
        rnd: &mut ThreadRng,
        now: u32,
    ) {
        let mut unavailable_links =
            IntSet::with_capacity_and_hasher(node.in_links.len(), BuildNoHashHasher::default());

        while unavailable_links.len() < node.in_links.len() {
            // draw a link id weighted on capacity and fetch that link
            let link_index = node.in_links_weights.as_ref().unwrap().sample(rnd);
            let in_link_id = node.in_links.get(link_index).unwrap();
            // if the link is already unavailable draw again.
            if unavailable_links.contains(in_link_id) {
                continue;
            }
            // if the link has a vehicle which can move, do it.
            else if Self::should_veh_move_out(in_link_id, links, now) {
                // get the mut ref here again, so that the borrow checker lets us borrow the links map
                // for the method above.
                let in_link = links.get_mut(&in_link_id).unwrap();
                let veh = in_link.pop_veh();

                if veh.peek_next_route_element().is_some() {
                    Self::move_vehicle(veh, links, events, now);
                } else {
                    exited_vehicles.push(veh);
                }
            }
            // if the vehicle can't move, the in_link is blocked. Add that link to the set of
            // unavailable links, and keep trying to move vehicles.
            else {
                unavailable_links.insert(*in_link_id);
            }
        }

        /* for link_id in &node.in_links {
            while Self::should_veh_move_out(link_id, links, now) {
                // get the mut ref here again, so that the borrow checker lets us borrow the links map
                // for the method above.
                let in_link = links.get_mut(&link_id).unwrap();
                let veh = in_link.pop_veh();

                if veh.peek_next_route_element().is_some() {
                    Self::move_vehicle(veh, links, events, now);
                } else {
                    exited_vehicles.push(veh);
                }
            }
        }

        */
    }

    fn should_veh_move_out(in_id: &u64, links: &IntMap<u64, SimLink>, now: u32) -> bool {
        let in_link = links.get(in_id).unwrap();
        if let Some(veh_ref) = in_link.offers_veh(now) {
            return if let Some(next_id_int) = veh_ref.peek_next_route_element() {
                // if the vehicle has a next link id, it should move out of the current link, if the
                // next link is free.
                let out_link = links.get(&next_id_int).unwrap();
                out_link.is_available()
            } else {
                // if there is no next link, the vehicle is done with its route and we can take it out
                // of the network
                true
            };
        }
        // if the link doesn't have a vehicle to offer, we don't have to do anything.
        false
    }

    fn move_vehicle(
        mut vehicle: Vehicle,
        links: &mut IntMap<u64, SimLink>,
        events: &mut EventsPublisher,
        now: u32,
    ) {
        events.publish_event(
            now,
            &Event::new_link_leave(vehicle.curr_route_elem as u64, vehicle.id),
        );
        vehicle.advance_route_index();
        let link_id = vehicle.curr_link_id().unwrap();
        let link = links.get_mut(&link_id).unwrap();
        events.publish_event(
            now,
            &Event::new_link_enter(link.id().internal(), vehicle.id),
        );

        link.push_veh(vehicle, now);
    }
}

#[cfg(test)]
mod tests {
    use assert_approx_eq::assert_approx_eq;
    use itertools::Itertools;

    use crate::simulation::id::Id;
    use crate::simulation::messaging::messages::proto::Route;
    use crate::simulation::vehicles::garage::Garage;
    use crate::simulation::{
        messaging::{
            events::EventsPublisher,
            messages::proto::{Activity, Agent, Leg, Plan, Vehicle},
        },
        network::{
            global_network::{Link, Network, Node},
            link::SimLink,
        },
    };

    use super::SimNetworkPartition;

    #[test]
    fn from_network() {
        let mut network = Network::new();
        let mut sim_nets = create_three_node_sim_network_with_partition(&mut network);
        let net1 = sim_nets.get_mut(0).unwrap();

        // we expect two nodes
        assert_eq!(2, net1.nodes.len());
        // we expect two links one local and one out link
        assert_eq!(2, net1.links.len());
        let local_link = net1.links.get(&0).unwrap();
        assert!(matches!(local_link, SimLink::Local(_)));
        let out_link = net1.links.get(&1).unwrap();
        assert!(matches!(out_link, SimLink::Out(_)));

        let net2 = sim_nets.get_mut(1).unwrap();
        // we expect one node
        assert_eq!(1, net2.nodes.len());
        // we expect one in link
        assert_eq!(1, net2.links.len());
        let in_link = net2.links.get(&1).unwrap();
        assert!(matches!(in_link, SimLink::In(_)));
    }

    #[test]
    fn vehicle_travels_local() {
        let mut publisher = EventsPublisher::new();
        let mut garage = Garage::new();
        let global_net = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            "metis",
            &mut garage,
        );
        let mut network = SimNetworkPartition::from_network(&global_net, 0);
        let agent = create_agent(1, vec![0, 1, 2]);
        let vehicle = Vehicle::new(1, 0, 10., 1., Some(agent));
        network.send_veh_en_route(vehicle, 0);

        for i in 0..120 {
            let result = network.move_nodes(&mut publisher, i);

            if i == 120 {
                assert!(!result.is_empty());
                let veh = result.first().unwrap();
                assert!(veh.curr_link_id().is_none());
            }
        }
    }

    #[test]
    fn vehicle_reaches_boundary() {
        let mut publisher = EventsPublisher::new();
        let mut garage = Garage::new();
        let global_net = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            2,
            "metis",
            &mut garage,
        );
        let mut network = SimNetworkPartition::from_network(&global_net, 1);
        let agent = create_agent(1, vec![0, 1, 2]);
        let vehicle = Vehicle::new(1, 0, 10., 100., Some(agent));
        network.send_veh_en_route(vehicle, 0);

        for now in 0..20 {
            let node_result = network.move_nodes(&mut publisher, now);
            assert!(node_result.is_empty());

            let (vehicles, storage_caps) = network.move_links(now);
            assert_eq!(0, storage_caps.len()); // we expect no out links here

            // when the vehicle moves from link1 to link2, it will be placed on an out link.
            // the stored vehicles on out links should be collected during move links.
            if now == 10 {
                assert_eq!(1, vehicles.len());
            } else {
                assert!(vehicles.is_empty());
            }
        }
    }

    #[test]
    fn move_nodes_flow_cap_constraint() {
        let mut publisher = EventsPublisher::new();
        let mut garage = Garage::new();
        let global_net = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            "metis",
            &mut garage,
        );
        let mut network = SimNetworkPartition::from_network(&global_net, 0);

        // place 100 vehicles on first link
        for i in 0..100 {
            let agent = create_agent(i, vec![0]);
            let vehicle = Vehicle::new(i, 0, 10., 1., Some(agent));
            network.send_veh_en_route(vehicle, 0);
        }

        // all vehicle only have to traverse link1. Link1 can release one vehicle/s, first one at t=10
        // this way we should have 10 vehicles released at t=20
        let mut counter = 0;
        for now in 0..110 {
            let result = network.move_nodes(&mut publisher, now);
            let _ = network.move_links(now);
            if now < 10 {
                assert!(result.is_empty());
            } else {
                assert_eq!(1, result.len());
                counter += 1;
            }
        }
        assert_eq!(100, counter);
    }

    #[test]
    fn move_nodes_storage_cap_constraint() {
        let mut publisher = EventsPublisher::new();
        let mut garage = Garage::new();
        let mut global_net = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            "metis",
            &mut garage,
        );
        global_net.effective_cell_size = 10.;

        let id_1 = global_net.link_ids.get_from_ext("link1");
        let id_2 = global_net.link_ids.get_from_ext("link2");
        let mut network = SimNetworkPartition::from_network(&global_net, 0);

        //place 10 vehicles on link2 so that it is jammed
        // vehicles are very slow, so that the first vehicle should leave link2 at t=1000
        for i in 0..10 {
            let agent = create_agent(i, vec![id_2.internal(), 2]);
            let vehicle = Vehicle::new(i, 0, 1., 10., Some(agent));
            network.send_veh_en_route(vehicle, 0);
        }

        // place 1 vehicle onto link1 which has to wait until link2 has free storage cap
        // as the first vehicle leaves link2 at t=1000 this vehicle can leave link1 and enter link2 at
        // the next timestep at t=1001
        let agent = create_agent(11, vec![id_1.internal(), 1, 2]);
        let vehicle = Vehicle::new(11, 0, 10., 1., Some(agent));
        network.send_veh_en_route(vehicle, 0);

        for now in 0..1010 {
            network.move_nodes(&mut publisher, now);
            network.move_links(now);

            let link1 = network.links.get(&id_1.internal()).unwrap();
            if (10..1001).contains(&now) {
                assert!(link1.offers_veh(now).is_some());
            } else {
                assert!(link1.offers_veh(now).is_none());
            }
        }
    }

    #[test]
    fn move_nodes_transition_logic() {
        let mut net = Network::new();
        let node1 = Node {
            x: 0.0,
            y: 0.0,
            id: Id::new_internal(0),
            attrs: vec![],
            in_links: vec![],
            out_links: vec![],
            partition: 0,
        };
        let node2 = Node {
            id: Id::new_internal(1),
            ..node1.clone()
        };
        let node3 = Node {
            id: Id::new_internal(2),
            ..node1.clone()
        };
        let node4 = Node {
            id: Id::new_internal(3),
            ..node1.clone()
        };
        net.add_node(node1);
        net.add_node(node2);
        net.add_node(node3);
        net.add_node(node4);

        net.add_link(Link {
            id: Id::new_internal(0),
            from: Id::new_internal(0),
            to: Id::new_internal(2),
            length: 1.0,
            capacity: 7200.,
            freespeed: 100.,
            permlanes: 1.0,
            modes: Default::default(),
            attributes: vec![],
            partition: 0,
        });
        net.add_link(Link {
            id: Id::new_internal(1),
            from: Id::new_internal(1),
            to: Id::new_internal(2),
            length: 1.0,
            capacity: 3600.,
            freespeed: 100.0,
            permlanes: 1.0,
            modes: Default::default(),
            attributes: vec![],
            partition: 0,
        });
        net.add_link(Link {
            id: Id::new_internal(2),
            from: Id::new_internal(2),
            to: Id::new_internal(3),
            length: 75.,
            capacity: 3600.,
            freespeed: 100.0,
            permlanes: 1.0,
            modes: Default::default(),
            attributes: vec![],
            partition: 0,
        });
        let mut sim_net = SimNetworkPartition::from_network(&net, 0);

        //place 10 vehicles on 2, so that it is jammed. The link should release 1 veh per time step.
        for i in 2000..2010 {
            let agent = create_agent(i, vec![2]);
            let vehicle = Vehicle::new(i, 0, 100., 1., Some(agent));
            sim_net.send_veh_en_route(vehicle, 0);
        }

        //place 1000 vehicles on 0
        for i in 0..1000 {
            let agent = create_agent(i, vec![0, 2]);
            let vehicle = Vehicle::new(i, 0, 100., 1., Some(agent));
            sim_net.send_veh_en_route(vehicle, 0);
        }

        //place 1000 vehicles on 1
        for i in 1000..2000 {
            let agent = create_agent(i, vec![1, 2]);
            let vehicle = Vehicle::new(i, 0, 100., 1., Some(agent));
            sim_net.send_veh_en_route(vehicle, 0);
        }

        let mut publisher = EventsPublisher::new();
        for now in 0..1000 {
            let result = sim_net.move_nodes(&mut publisher, now);
            let move_links_result = sim_net.move_links(now);
        }

        let link1 = sim_net.links.get(&0).unwrap().used_storage();
        let link2 = sim_net.links.get(&1).unwrap().used_storage();

        assert_approx_eq!(link1 * 2., link2, 100.);
    }

    #[test]
    fn storage_cap_over_boundaries() {
        // use programmed network here, to avoid instabilities with metis algorithm for small
        // network graphs
        let mut network = Network::new();
        let mut sim_nets = create_three_node_sim_network_with_partition(&mut network);
        let net2 = sim_nets.get_mut(1).unwrap();

        let split_link_id = net2.global_network.link_ids.get_from_ext("link-2");
        let agent = create_agent(1, vec![split_link_id.internal(), 2]);
        let vehicle = Vehicle::new(1, 0, 10., 100., Some(agent));

        // collect empty storage caps
        let (_, storage_caps) = net2.move_links(0);
        assert_eq!(0, storage_caps.len());

        // now place vehicle on network and collect storage caps again.
        net2.send_veh_en_route(vehicle, 0);
        let (_, storage_caps) = net2.move_links(0);
        assert_eq!(1, storage_caps.len());
        let storage_cap = storage_caps.first().unwrap();
        assert_eq!(split_link_id.internal(), storage_cap.link_id);
        assert_approx_eq!(100., storage_cap.used, 0.0001);
    }

    #[test]
    fn neighbors() {
        let mut net = Network::new();
        let mut node = Node::new(net.node_ids.create_id("node-1"), -0., 0.);
        node.partition = 0;

        let mut node_1_1 = Node::new(net.node_ids.create_id("node-1-1"), -0., 0.);
        node_1_1.partition = 1;
        let mut node_1_2 = Node::new(net.node_ids.create_id("node-1-2"), -0., 0.);
        node_1_2.partition = 1;

        let mut node_2_1 = Node::new(net.node_ids.create_id("node-2-1"), -0., 0.);
        node_2_1.partition = 2;
        let mut node_3_1 = Node::new(net.node_ids.create_id("node-3-1"), -0., 0.);
        node_3_1.partition = 3;
        let mut node_4_1 = Node::new(net.node_ids.create_id("not-a-neighbor"), 0., 0.);
        node_4_1.partition = 4;

        // create in links from partitions 1 and 2. 2 incoming links from partition 1, one incoming from
        // partition 2
        let in_link_1_1 =
            Link::new_with_default(net.link_ids.create_id("in-link-1-1"), &node_1_1, &node);
        let in_link_1_2 =
            Link::new_with_default(net.link_ids.create_id("in-link-1-2"), &node_1_2, &node);
        let in_link_2_1 =
            Link::new_with_default(net.link_ids.create_id("in-link-2-1"), &node_2_1, &node);

        // create out links to partitions 1 and 3
        let out_link_1_1 =
            Link::new_with_default(net.link_ids.create_id("out-link-1-1"), &node, &node_1_1);
        let out_link_1_2 =
            Link::new_with_default(net.link_ids.create_id("out-link-1-2"), &node, &node_1_2);
        let out_link_3_1 =
            Link::new_with_default(net.link_ids.create_id("out-link-3-1"), &node, &node_3_1);

        net.add_node(node);
        net.add_node(node_1_1);
        net.add_node(node_1_2);
        net.add_node(node_2_1);
        net.add_node(node_3_1);
        net.add_node(node_4_1);
        net.add_link(in_link_1_1);
        net.add_link(in_link_1_2);
        net.add_link(in_link_2_1);
        net.add_link(out_link_1_1);
        net.add_link(out_link_1_2);
        net.add_link(out_link_3_1);

        let sim_net = SimNetworkPartition::from_network(&net, 0);

        let neighbors = sim_net.neighbors();
        assert_eq!(3, neighbors.len());
        assert!(neighbors.contains(&1));
        assert!(neighbors.contains(&2));
        assert!(neighbors.contains(&3));
        assert!(!neighbors.contains(&4));
    }

    fn init_three_node_network(network: &mut Network) {
        let node1 = Node::new(network.node_ids.create_id("node-1"), -100., 0.);
        let node2 = Node::new(network.node_ids.create_id("node-2"), 0., 0.);
        let node3 = Node::new(network.node_ids.create_id("node-3"), 100., 0.);
        let mut link1 =
            Link::new_with_default(network.link_ids.create_id("link-1"), &node1, &node2);
        link1.capacity = 3600.;
        link1.freespeed = 10.;
        let mut link2 =
            Link::new_with_default(network.link_ids.create_id("link-2"), &node2, &node3);
        link2.capacity = 3600.;
        link2.freespeed = 10.;

        network.add_node(node1);
        network.add_node(node2);
        network.add_node(node3);
        network.add_link(link1);
        network.add_link(link2);
    }

    fn create_three_node_sim_network_with_partition<'n>(
        network: &'n mut Network,
    ) -> Vec<SimNetworkPartition<'n>> {
        init_three_node_network(network);
        let node3 = network.nodes.get_mut(2).unwrap();
        node3.partition = 1;
        let link2 = network.links.get_mut(1).unwrap();
        link2.partition = 1;
        vec![
            SimNetworkPartition::from_network(network, 0),
            SimNetworkPartition::from_network(network, 1),
        ]
    }

    fn create_agent(id: u64, route: Vec<u64>) -> Agent {
        let route = Route {
            veh_id: id,
            distance: 0.0,
            route,
        };
        let leg = Leg::new(route, 0, 0, None);
        let act = Activity::new(0., 0., 0, 1, None, None, None);
        let mut plan = Plan::new();
        plan.add_act(act);
        plan.add_leg(leg);
        let mut agent = Agent::new(id, plan);
        agent.advance_plan();

        agent
    }
}
