use std::rc::Rc;

use tracing::debug;

use crate::simulation::id::Id;
use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::global_network::{Link, Network};
use crate::simulation::network::sim_network::SimNetworkPartition;
use crate::simulation::replanning::routing::router::NetworkRouter;
use crate::simulation::replanning::routing::travel_times_collecting_alt_router::TravelTimesCollectingAltRouter;
use crate::simulation::replanning::teleported_router::{BeeLineDistanceRouter, TeleportedRouter};
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::wire_types::messages::Vehicle;
use crate::simulation::wire_types::population::{Activity, Leg, Person};
use crate::simulation::wire_types::vehicles::{LevelOfDetail, VehicleType};

pub trait Replanner {
    fn update_time(&mut self, now: u32, events: &mut EventsPublisher);
    fn replan(&self, now: u32, agent: &mut Person, garage: &Garage);
}

#[derive(Eq, PartialEq)]
enum LegType {
    TripPlaceholder,
    AccessEgress,
    MainTeleported,
    MainNetwork,
}

pub struct DummyReplanner {}

impl Replanner for DummyReplanner {
    fn update_time(&mut self, _now: u32, _events: &mut EventsPublisher) {}

    fn replan(&self, _now: u32, _agent: &mut Person, _garage: &Garage) {}
}

#[derive(Debug)]
pub struct ReRouteTripReplanner {
    network_router: Box<dyn NetworkRouter>,
    teleported_router: Box<dyn TeleportedRouter>,
    global_network: Network,
    rank: u32,
}

impl Replanner for ReRouteTripReplanner {
    fn update_time(&mut self, now: u32, events: &mut EventsPublisher) {
        self.network_router.next_time_step(now, events)
    }

    // #[tracing::instrument(level = "trace", skip(self, agent, garage), fields(rank = self.rank))]
    fn replan(&self, _now: u32, agent: &mut Person, garage: &Garage) {
        let leg_type = Self::get_leg_type(agent, garage);
        if leg_type == LegType::TripPlaceholder {
            self.insert_access_egress(agent, garage);
        }

        match leg_type {
            // in case of trip placeholder: we have inserted access and egress legs before
            // => we must now replan the respective access leg
            LegType::AccessEgress | LegType::TripPlaceholder => {
                self.replan_access_egress(_now, agent, garage)
            }
            LegType::MainNetwork => self.replan_main(_now, agent, garage),
            LegType::MainTeleported => self.replan_teleported_main(_now, agent, garage),
        };
    }
}

impl ReRouteTripReplanner {
    pub fn new<C: SimCommunicator + 'static>(
        global_network: &Network,
        sim_network: &SimNetworkPartition,
        garage: &Garage,
        communicator: Rc<C>,
    ) -> ReRouteTripReplanner {
        let forward_backward_graph_by_veh_type =
            TravelTimesCollectingAltRouter::<C>::get_forward_backward_graph_by_veh_type(
                global_network,
                &garage.vehicle_types,
            );

        let rank = communicator.rank();

        let router: Box<dyn NetworkRouter> = Box::new(TravelTimesCollectingAltRouter::new(
            forward_backward_graph_by_veh_type,
            communicator,
            sim_network.get_link_ids(),
        ));

        let teleported_router: Box<dyn TeleportedRouter> = Box::new(BeeLineDistanceRouter::new());

        ReRouteTripReplanner {
            network_router: router,
            teleported_router,
            global_network: global_network.clone(),
            rank,
        }
    }

    fn insert_access_egress(&self, agent: &mut Person, garage: &Garage) {
        // So far, we have:
        // act (current) - leg (next) - act (next)
        //
        // We want:
        // act (current) - walk (next)  - interaction act (next) - leg - interaction act - walk - act
        //
        // Thus, we need to
        // 1. insert 2 interaction activities between current and next activity
        // 2. insert access and egress walking legs before and after main leg
        let main_leg_veh_type_id = agent.next_leg().vehicle_type_id(garage);
        let interaction_id =
            Id::<String>::get_from_ext(&format!("{} interaction", main_leg_veh_type_id.external()));

        let new_acts = vec![
            Activity::interaction(agent.curr_act().link_id, interaction_id.internal()),
            Activity::interaction(agent.next_act().link_id, interaction_id.internal()),
        ];
        agent.add_act_after_curr(new_acts);

        //"walk" as default access egress vehicle type is hard coded here. Could also be optional
        let walk_vehicle_type = garage
            .vehicle_types
            .get(&Id::<VehicleType>::get_from_ext("walk"))
            .expect("No walk vehicle type");

        let walk_mode_id = walk_vehicle_type.net_mode;

        //replace current leg()
        let access = Leg::access_eggress(walk_mode_id, walk_vehicle_type.id);
        let egress = Leg::access_eggress(walk_mode_id, walk_vehicle_type.id);

        //we have: last leg (current) - main leg (next)
        //we want: last leg (current) - walk access leg (next) - main leg - walk egress leg
        agent.replace_next_leg(vec![access, agent.next_leg().clone(), egress]);
    }

    #[tracing::instrument(level = "trace", skip(self, agent, garage), fields(rank = self.rank))]
    fn replan_main(&self, _now: u32, agent: &mut Person, garage: &Garage) {
        let curr_act = agent.curr_act();

        let veh_type_id = garage
            .vehicles
            .get(&Id::<Vehicle>::get(
                agent.next_leg().route.as_ref().unwrap().veh_id,
            ))
            .unwrap();

        let (route, travel_time) = self.find_route(agent.curr_act(), agent.next_act(), veh_type_id);
        let dep_time = curr_act.end_time;

        let vehicle_type_id = agent.next_leg().vehicle_type_id(garage);

        let veh_id = garage.veh_id(&Id::<Person>::get(agent.id), vehicle_type_id);

        let distance = self.calculate_distance(&route);

        agent.update_next_leg(
            dep_time,
            travel_time.unwrap(),
            route,
            distance,
            veh_id.internal(),
        );
    }

    // #[tracing::instrument(level = "trace", skip(self, agent, garage), fields(rank = self.rank))]
    fn replan_access_egress(&self, _now: u32, agent: &mut Person, garage: &Garage) {
        let curr_act = agent.curr_act();
        let next_act = agent.next_act();

        assert_eq!(curr_act.link_id, next_act.link_id);

        let veh_type_id = agent.next_leg().vehicle_type_id(garage);
        let access_egress_speed = garage.vehicle_types.get(veh_type_id).unwrap().max_v;

        let dep_time;
        let walk = if curr_act.is_interaction() {
            dep_time = curr_act.end_time;
            // curr activity is interaction => it's an egress leg => next activity has location
            self.teleported_router.query_access_egress(
                next_act,
                access_egress_speed,
                &self.global_network,
            )
        } else {
            dep_time = curr_act.end_time;
            // curr activity is an actual activity => it's an access leg => curr activity has location
            self.teleported_router.query_access_egress(
                curr_act,
                access_egress_speed,
                &self.global_network,
            )
        };

        let vehicle_id = garage.veh_id(&Id::<Person>::get(agent.id), veh_type_id);

        agent.update_next_leg(
            dep_time,
            walk.duration,
            vec![agent.curr_act().link_id, agent.curr_act().link_id],
            walk.distance,
            vehicle_id.internal(),
        );
    }

    // #[tracing::instrument(level = "trace", skip(self, agent, garage), fields(rank = self.rank))]
    fn replan_teleported_main(&self, _now: u32, agent: &mut Person, garage: &Garage) {
        let curr_act = agent.curr_act();
        let next_act = agent.next_act();

        let veh_type_id = agent.next_leg().vehicle_type_id(garage);
        let speed = garage.vehicle_types.get(veh_type_id).unwrap().max_v;

        let dep_time = curr_act.end_time;
        let teleportation = self
            .teleported_router
            .query_between_acts(curr_act, next_act, speed);

        let vehicle_id = garage.veh_id(&Id::<Person>::get(agent.id), veh_type_id);
        agent.update_next_leg(
            dep_time,
            teleportation.duration,
            vec![agent.curr_act().link_id, agent.next_act().link_id],
            teleportation.distance,
            vehicle_id.internal(),
        );
    }

    // #[tracing::instrument(level = "trace", skip(self, from_act, to_act, veh_type_id))]
    fn find_route(
        &self,
        from_act: &Activity,
        to_act: &Activity,
        veh_type_id: &Id<VehicleType>,
    ) -> (Vec<u64>, Option<u32>) {
        let query_result =
            self.network_router
                .query_links(from_act.link_id, to_act.link_id, veh_type_id);

        let route = query_result.path.expect("There is no route!");
        let travel_time = query_result.travel_time;

        if route.is_empty() {
            debug!("Route between {:?} and {:?} is empty.", from_act, to_act);
        }

        (route, travel_time)
    }

    #[allow(clippy::if_same_then_else)]
    fn get_leg_type(agent: &Person, garage: &Garage) -> LegType {
        //act - leg - interaction act => walk
        if !agent.curr_act().is_interaction() && agent.next_act().is_interaction() {
            LegType::AccessEgress
        }
        //interaction act - leg - act => walk
        else if agent.curr_act().is_interaction() && !agent.next_act().is_interaction() {
            LegType::AccessEgress
        }
        //interaction act - leg - interaction act => main leg if network, generic if teleported
        else if agent.curr_act().is_interaction() && agent.next_act().is_interaction() {
            let level_of_detail = LevelOfDetail::try_from(
                garage
                    .vehicle_types
                    .get(agent.next_leg().vehicle_type_id(garage))
                    .unwrap()
                    .lod,
            )
            .expect("Unknown level of detail");
            match level_of_detail {
                LevelOfDetail::Network => LegType::MainNetwork,
                LevelOfDetail::Teleported => LegType::MainTeleported,
            }
        }
        //act - leg - act => trip placeholder if network, generic if teleported
        else if !agent.curr_act().is_interaction() && !agent.next_act().is_interaction() {
            let level_of_detail = LevelOfDetail::try_from(
                garage
                    .vehicle_types
                    .get(agent.next_leg().vehicle_type_id(garage))
                    .unwrap()
                    .lod,
            )
            .expect("Unknown level of detail");
            match level_of_detail {
                LevelOfDetail::Network => LegType::TripPlaceholder,
                LevelOfDetail::Teleported => LegType::MainTeleported,
            }
        } else {
            panic!("Computing a leg between two main activities should never happen.")
        }
    }

    fn calculate_distance(&self, route: &[u64]) -> f64 {
        let distance: f64 = route
            .iter()
            .map(|id| self.global_network.get_link(&Id::<Link>::get(*id)))
            .map(|l| l.length)
            .sum();
        distance
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::rc::Rc;

    use crate::simulation::config::{MetisOptions, PartitionMethod};
    use crate::simulation::id::Id;
    use crate::simulation::messaging::communication::communicators::DummySimCommunicator;
    use crate::simulation::network::global_network::Network;
    use crate::simulation::network::sim_network::SimNetworkPartition;
    use crate::simulation::population::population::Population;
    use crate::simulation::replanning::replanner::{ReRouteTripReplanner, Replanner};
    use crate::simulation::vehicles::garage::Garage;
    use crate::simulation::wire_types::population::{Person, Route};
    use crate::simulation::wire_types::vehicles::VehicleType;
    use crate::test_utils;

    #[test]
    fn test_trip_placeholder_leg() {
        //prepare
        let network = Network::from_file(
            "./assets/adhoc_routing/no_updates/network.xml",
            1,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let mut garage = Garage::from_file(&PathBuf::from("./assets/3-links/vehicles.xml"));
        let mut population = Population::part_from_file(
            &PathBuf::from("./assets/adhoc_routing/no_updates/agents.xml"),
            &network,
            &mut garage,
            0,
        );
        let sim_net = SimNetworkPartition::from_network(&network, 0, test_utils::config());
        let agent_id = Id::get_from_ext("100");
        let mut agent = population.persons.get_mut(&agent_id).unwrap();

        let replanner =
            ReRouteTripReplanner::new(&network, &sim_net, &garage, Rc::new(DummySimCommunicator()));

        //do change
        replanner.replan(0, &mut agent, &garage);

        //check activities
        assert_eq!(agent.plan.as_ref().unwrap().acts.len(), 4);
        assert_eq!(get_act_type_id(&agent, 1).external(), "car interaction");
        assert_eq!(get_act_type_id(&agent, 2).external(), "car interaction");

        //check legs
        assert_eq!(agent.plan.as_ref().unwrap().legs.len(), 3);
        assert_eq!(get_veh_type_id(&agent, 0, &garage).external(), "walk");
        assert_eq!(get_veh_type_id(&agent, 1, &garage).external(), "car");
        assert_eq!(get_veh_type_id(&agent, 2, &garage).external(), "walk");
    }

    #[test]
    fn test_update_walk_leg() {
        //prepare
        let network = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let mut garage = Garage::from_file(&PathBuf::from("./assets/3-links/vehicles.xml"));
        let mut population = Population::part_from_file(
            &PathBuf::from("./assets/3-links/1-agent-trip-leg.xml"),
            &network,
            &mut garage,
            0,
        );
        let sim_net = SimNetworkPartition::from_network(&network, 0, test_utils::config());
        let agent_id = Id::get_from_ext("100");
        let mut agent = population.persons.get_mut(&agent_id).unwrap();

        let replanner =
            ReRouteTripReplanner::new(&network, &sim_net, &garage, Rc::new(DummySimCommunicator()));

        //do change
        replanner.replan(0, &mut agent, &garage);

        //check activities
        assert_eq!(agent.plan.as_ref().unwrap().acts.len(), 4);
        assert_eq!(get_act_type_id(&agent, 1).external(), "car interaction");
        assert_eq!(get_act_type_id(&agent, 2).external(), "car interaction");

        //check legs
        assert_eq!(agent.plan.as_ref().unwrap().legs.len(), 3);
        assert_eq!(get_veh_type_id(&agent, 0, &garage).external(), "walk");
        assert_eq!(get_veh_type_id(&agent, 1, &garage).external(), "car");
        assert_eq!(get_veh_type_id(&agent, 2, &garage).external(), "walk");

        let access_leg = agent.plan.as_ref().unwrap().legs.get(0);

        assert_eq!(
            access_leg.unwrap().trav_time,
            (access_leg.unwrap().route.as_ref().unwrap().distance / 0.85) as u32
        );
        assert_eq!(
            access_leg
                .as_ref()
                .unwrap()
                .route
                .as_ref()
                .unwrap()
                .distance,
            10.
        );
    }

    #[test]
    fn test_update_teleported_main() {
        //prepare
        let network = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let mut garage = Garage::from_file(&PathBuf::from("./assets/3-links/vehicles.xml"));
        let mut population = Population::part_from_file(
            &PathBuf::from("./assets/3-links/1-agent-generic-leg.xml"),
            &network,
            &mut garage,
            0,
        );

        let sim_net = SimNetworkPartition::from_network(&network, 0, test_utils::config());
        let agent_id = Id::get_from_ext("100");
        let mut agent = population.persons.get_mut(&agent_id).unwrap();

        let replanner =
            ReRouteTripReplanner::new(&network, &sim_net, &garage, Rc::new(DummySimCommunicator()));

        //do change
        replanner.replan(0, &mut agent, &garage);

        //check activities
        assert_eq!(agent.plan.as_ref().unwrap().acts.len(), 2);

        //check legs
        assert_eq!(agent.plan.as_ref().unwrap().legs.len(), 1);
        assert_eq!(get_veh_type_id(&agent, 0, &garage).external(), "walk");
        let route = agent
            .plan
            .as_ref()
            .unwrap()
            .legs
            .get(0)
            .unwrap()
            .route
            .as_ref()
            .unwrap();

        //distance from (0,0) to (12,5) is 13
        assert_eq!(13., route.distance);
        assert_eq!(
            (13. / 0.85) as u32,
            agent.plan.as_ref().unwrap().legs.get(0).unwrap().trav_time
        )
    }

    #[test]
    fn test_update_main_leg() {
        //prepare
        let mut garage = Garage::from_file(&PathBuf::from("./assets/3-links/vehicles.xml"));

        let population = advance_plan_and_update_main_leg(&mut garage);
        let agent_id = Id::get_from_ext("100");
        let agent = population.persons.get(&agent_id).unwrap();

        //check main leg
        let main_leg = agent.plan.as_ref().unwrap().legs.get(1);
        assert_eq!(
            main_leg.unwrap().route.as_ref().unwrap(),
            &Route {
                veh_id: 0,
                distance: 1200.,
                route: vec![0, 1, 2],
            }
        );
    }

    #[test]
    fn test_update_main_leg_with_same_net_modes_in_veh_type() {
        //prepare
        let mut garage = Garage::from_file(&PathBuf::from(
            "./assets/3-links/vehicles_same_net_mode.xml",
        ));

        let population = advance_plan_and_update_main_leg(&mut garage);
        let agent_id = Id::get_from_ext("100");
        let agent = population.persons.get(&agent_id).unwrap();

        //check main leg
        let main_leg = agent.plan.as_ref().unwrap().legs.get(1);
        assert_eq!(
            main_leg.unwrap().route.as_ref().unwrap(),
            &Route {
                veh_id: 0,
                distance: 1200.,
                route: vec![0, 1, 2],
            }
        );
    }

    fn advance_plan_and_update_main_leg(mut garage: &mut Garage) -> Population {
        let network = Network::from_file(
            "./assets/3-links/3-links-network.xml",
            1,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let mut population = Population::part_from_file(
            &PathBuf::from("./assets/3-links/1-agent-trip-leg.xml"),
            &network,
            &mut garage,
            0,
        );
        let sim_net = SimNetworkPartition::from_network(&network, 0, test_utils::config());
        let agent_id = Id::get_from_ext("100");
        let mut agent = population.persons.get_mut(&agent_id).unwrap();

        let replanner =
            ReRouteTripReplanner::new(&network, &sim_net, &garage, Rc::new(DummySimCommunicator()));

        //do change of walk leg
        replanner.replan(0, &mut agent, &garage);

        //agent is on leg now
        agent.advance_plan();

        //agent is performing car interaction
        agent.advance_plan();

        //do change
        replanner.replan(0, &mut agent, &garage);
        population
    }

    fn get_act_type_id(agent: &Person, act_index: usize) -> Id<String> {
        Id::<String>::get(
            agent
                .plan
                .as_ref()
                .unwrap()
                .acts
                .get(act_index)
                .unwrap()
                .act_type,
        )
    }

    fn get_veh_type_id(agent: &Person, leg_index: usize, garage: &Garage) -> Id<VehicleType> {
        agent
            .plan
            .as_ref()
            .unwrap()
            .legs
            .get(leg_index)
            .unwrap()
            .vehicle_type_id(garage)
            .clone()
    }
}
