use crate::simulation::config::Config;
use crate::simulation::id_mapping::MatsimIdMappings;
use crate::simulation::io::vehicle_definitions::VehicleDefinitions;
use crate::simulation::messaging::events::proto::Event;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::messaging::message_broker::{MessageBroker, MpiMessageBroker};
use crate::simulation::messaging::messages::proto::leg::Route;
use crate::simulation::messaging::messages::proto::{
    Activity, Agent, GenericRoute, Vehicle, VehicleType,
};
use crate::simulation::network::link::Link;
use crate::simulation::network::network_partition::NetworkPartition;
use crate::simulation::network::node::ExitReason;
use crate::simulation::performance_profiling::measure_duration;
use crate::simulation::population::Population;
use crate::simulation::routing::router::Router;
use crate::simulation::routing::walk_leg_updater::WalkLegUpdater;
use crate::simulation::time_queue::TimeQueue;
use tracing::{debug, info};

pub struct Simulation<'sim> {
    activity_q: TimeQueue<Agent>,
    teleportation_q: TimeQueue<Vehicle>,
    network: NetworkPartition,
    message_broker: MpiMessageBroker,
    events: EventsPublisher,
    router: Option<Box<dyn Router>>,
    vehicle_definitions: Option<VehicleDefinitions>,
    id_mappings: &'sim MatsimIdMappings,
    walk_leg_updater: Option<Box<dyn WalkLegUpdater>>,
}

impl<'sim> Simulation<'sim> {
    pub fn new(
        config: &Config,
        id_mappings: &'sim MatsimIdMappings,
        network: NetworkPartition,
        population: Population,
        message_broker: MpiMessageBroker,
        events: EventsPublisher,
        router: Option<Box<dyn Router>>,
        vehicle_definitions: Option<VehicleDefinitions>,
        walk_leg_updater: Option<Box<dyn WalkLegUpdater>>,
    ) -> Self {
        let mut activity_q = TimeQueue::new();
        for agent in population.agents.into_values() {
            activity_q.add(agent, config.start_time);
        }

        Simulation {
            network,
            teleportation_q: TimeQueue::new(),
            activity_q,
            message_broker,
            events,
            router,
            vehicle_definitions,
            id_mappings,
            walk_leg_updater,
        }
    }

    pub fn run(&mut self, start_time: u32, end_time: u32) {
        // use fixed start and end times
        let mut now = start_time;
        info!(
            "Starting #{}. Network neighbors: {:?}, Start time {start_time}, End time {end_time}",
            self.message_broker.rank,
            self.network.neighbors(),
        );

        while now <= end_time {
            if self.message_broker.rank == 0 && now % 600 == 0 {
                let _hour = now / 3600;
                let _min = (now % 3600) / 60;
                info!("#{} of Qsim at {_hour}:{_min}", self.message_broker.rank);
            }
            self.wakeup(now);

            measure_duration(
                Some(now),
                "qsim_step",
                None, //TODO
                || {
                    self.terminate_teleportation(now);
                    self.move_nodes(now);
                    self.send_receive(now);
                },
            );

            if let Some(router) = self.router.as_mut() {
                router.next_time_step(now, &mut self.events)
            }

            now += 1;
        }

        // maybe this belongs into the controller? Then this would have to be a &mut instead of owned.
        self.events.finish();
    }

    fn wakeup(&mut self, now: u32) {
        let agents = self.activity_q.pop(now);

        for mut agent in agents {
            let agent_id = agent.id;
            self.events.publish_event(
                now,
                &Event::new_act_end(
                    agent_id,
                    agent.curr_act().link_id,
                    agent.curr_act().act_type.clone(),
                ),
            );

            if self.router.is_some() {
                if (!agent.curr_act().is_interaction() && agent.next_act().is_interaction())
                    || (agent.curr_act().is_interaction() && !agent.next_act().is_interaction())
                {
                    self.update_walk_leg(&mut agent);
                } else if agent.curr_act().is_interaction() && agent.next_act().is_interaction() {
                    self.update_main_leg(&mut agent, now);
                } else {
                    panic!("Computing a leg between two main activities should never happen.")
                }
            }

            //here, current element counter is going to be increased
            agent.advance_plan();

            assert_ne!(agent.curr_plan_elem % 2, 0);

            let leg = agent.curr_leg();

            match leg.route.as_ref().unwrap() {
                Route::GenericRoute(route) => {
                    self.events.publish_event(
                        now,
                        &Event::new_departure(agent_id, route.start_link, leg.mode.clone()),
                    );

                    if Simulation::is_local_route(route, &self.message_broker) {
                        let veh = Vehicle::new(
                            agent.id,
                            VehicleType::Teleported,
                            leg.mode.clone(),
                            agent,
                        );
                        self.teleportation_q.add(veh, now);
                    } else {
                        let veh = Vehicle::new(
                            agent.id,
                            VehicleType::Teleported,
                            leg.mode.clone(),
                            agent,
                        );
                        self.message_broker.add_veh(veh, now);
                    }
                }
                Route::NetworkRoute(route) => {
                    let link_id = route.route.get(0).unwrap();
                    self.events.publish_event(
                        now,
                        &Event::new_departure(agent_id, *link_id, leg.mode.clone()),
                    );

                    self.events.publish_event(
                        now,
                        &Event::new_person_enters_veh(agent_id, route.vehicle_id),
                    );

                    let veh = Vehicle::new(
                        route.vehicle_id,
                        VehicleType::Network,
                        leg.mode.clone(),
                        agent,
                    );
                    self.veh_onto_network(veh, true, now);
                }
            }
        }
    }

    fn update_main_leg(&mut self, agent: &mut Agent, now: u32) {
        let curr_act = agent.curr_act();
        let next_act = agent.next_act();
        let mode = agent.next_leg().mode.as_str();

        let (route, travel_time) = measure_duration(Some(now), "query", None, || {
            self.find_route(agent.curr_act(), agent.next_act(), mode)
        });
        let dep_time = curr_act.end_time;

        agent.update_next_leg(
            dep_time,
            travel_time,
            route,
            None,
            curr_act.link_id,
            next_act.link_id,
        );
    }

    fn update_walk_leg(&self, agent: &mut Agent) {
        self.walk_leg_updater
            .as_ref()
            .expect("WalkLegFinder must be set, if routing is enabled.")
            .as_ref()
            .update_walk_leg(agent, &self.network);
    }

    fn find_route(
        &mut self,
        from_act: &Activity,
        to_act: &Activity,
        mode: &str,
    ) -> (Vec<u64>, Option<u32>) {
        let query_result =
            self.router
                .as_mut()
                .unwrap()
                .query_links(from_act.link_id, to_act.link_id, mode);

        let route = query_result.path.expect("There is no route!");
        let travel_time = query_result.travel_time;

        if route.is_empty() {
            debug!("Route between {:?} and {:?} is empty.", from_act, to_act);
        }

        (route, travel_time)
    }

    fn veh_onto_network(&mut self, vehicle: Vehicle, from_act: bool, now: u32) {
        let link_id = vehicle.curr_link_id().unwrap(); // in this case there should always be a link id.
        let link = self.network.links.get_mut(&link_id).expect(&*format!(
            "Cannot find link for link_id {:?} and vehicle {:?}",
            link_id, vehicle
        ));

        if !from_act {
            self.events
                .publish_event(now, &Event::new_link_enter(link_id as u64, vehicle.id));
        }
        match link {
            Link::LocalLink(link) => {
                link.push_vehicle(vehicle, now, self.vehicle_definitions.as_ref())
            }
            Link::SplitInLink(in_link) => {
                let local_link = in_link.local_link_mut();
                local_link.push_vehicle(vehicle, now, self.vehicle_definitions.as_ref())
            }
            Link::SplitOutLink(_) => {
                panic!("Vehicles should not start on out links...")
            }
        }
    }

    fn terminate_teleportation(&mut self, now: u32) {
        let teleportation_vehicles = self.teleportation_q.pop(now);
        for vehicle in teleportation_vehicles {
            // handle travelled
            let mut agent = vehicle.agent.unwrap();
            let leg = agent.curr_leg();
            if let Route::GenericRoute(route) = &leg.route.as_ref().unwrap() {
                self.events.publish_event(
                    now,
                    &Event::new_travelled(agent.id, route.distance, leg.mode.clone()),
                );
            }
            agent.advance_plan();
            self.activity_q.add(agent, now);
        }
    }

    fn move_nodes(&mut self, now: u32) {
        for node in NetworkPartition::get_local_nodes(self.network.nodes.values()) {
            let exited_vehicles = node.move_vehicles(
                &mut self.network.links,
                now,
                &mut self.events,
                self.vehicle_definitions.as_ref(),
            );

            for exit_reason in exited_vehicles {
                match exit_reason {
                    ExitReason::FinishRoute(vehicle) => {
                        let veh_id = vehicle.id;
                        let mut agent = vehicle.agent.unwrap();
                        let leg_mode = agent.curr_leg().mode.clone();

                        self.events
                            .publish_event(now, &Event::new_person_leaves_veh(agent.id, veh_id));

                        agent.advance_plan();
                        let act = agent.curr_act();

                        self.events.publish_event(
                            now,
                            &Event::new_arrival(agent.id, act.link_id, leg_mode),
                        );

                        self.events.publish_event(
                            now,
                            &Event::new_act_start(agent.id, act.link_id, act.act_type.clone()),
                        );
                        self.activity_q.add(agent, now);
                    }
                    ExitReason::ReachedBoundary(vehicle) => {
                        self.message_broker.add_veh(vehicle, now);
                    }
                }
            }
        }
    }

    fn send_receive(&mut self, now: u32) {
        let vehicle_update_messages = self.message_broker.send_recv(now);

        let vehicles = vehicle_update_messages
            .into_iter()
            .flat_map(|msg| msg.vehicles)
            .collect::<Vec<Vehicle>>();

        for vehicle in vehicles {
            match vehicle.r#type() {
                VehicleType::Teleported => {
                    self.teleportation_q.add(vehicle, now);
                }
                VehicleType::Network => {
                    self.veh_onto_network(vehicle, false, now);
                }
            }
        }
    }

    fn is_local_route(route: &GenericRoute, message_broker: &MpiMessageBroker) -> bool {
        let (from, to) = Simulation::process_ids_for_generic_route(route, message_broker);
        from == to
    }

    fn process_ids_for_generic_route(
        route: &GenericRoute,
        message_broker: &MpiMessageBroker,
    ) -> (u64, u64) {
        let from_rank = message_broker.rank_for_link(route.start_link);
        let to_rank = message_broker.rank_for_link(route.end_link);
        (from_rank, to_rank)
    }

    fn get_router_ref(&self) -> &dyn Router {
        self.router.as_ref().unwrap().as_ref()
    }
}
