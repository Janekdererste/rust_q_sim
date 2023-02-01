use crate::config::{Config, RoutingMode};
use crate::io::proto_events::ProtoEventsWriter;
use crate::mpi::events::proto::Event;
use crate::mpi::events::EventsPublisher;
use crate::parallel_simulation::agent_q::AgentQ;
use crate::parallel_simulation::messaging::MessageBroker;
use crate::parallel_simulation::network::link::{Link, LocalLink};
use crate::parallel_simulation::network::network_partition::NetworkPartition;
use crate::parallel_simulation::network::node::ExitReason;
use crate::parallel_simulation::network::routing_kit_network::RoutingKitNetwork;
use crate::parallel_simulation::routing::router::Router;
use crate::parallel_simulation::splittable_population::NetworkRoute;
use crate::parallel_simulation::splittable_population::PlanElement;
use crate::parallel_simulation::splittable_population::Route;
use crate::parallel_simulation::splittable_population::{Agent, Leg};
use crate::parallel_simulation::splittable_scenario::{Scenario, ScenarioPartition};
use crate::parallel_simulation::vehicles::Vehicle;
use log::info;
use rust_road_router::algo::customizable_contraction_hierarchy::CCH;
use std::path::PathBuf;

mod agent_q;
pub mod events;
pub mod id_mapping;
mod messages;
mod messaging;
pub mod network;
pub mod partition_info;
pub mod routing;
pub mod splittable_population;
pub mod splittable_scenario;
mod vehicles;

pub struct Simulation {
    scenario: ScenarioPartition<Vehicle>,
    activity_q: AgentQ,
    teleportation_q: AgentQ,
    events: EventsPublisher,
    start_time: u32,
    end_time: u32,
    routing_mode: RoutingMode,
    output_dir: String,
    routing_kit_network: RoutingKitNetwork,
}

impl Simulation {
    fn new(
        config: &Config,
        scenario: ScenarioPartition<Vehicle>,
        events: EventsPublisher,
        routing_kit_network: RoutingKitNetwork,
    ) -> Simulation {
        let mut q = AgentQ::new();
        for (_, agent) in scenario.population.agents.iter() {
            q.add(agent, 0);
        }

        Simulation {
            scenario,
            activity_q: q,
            teleportation_q: AgentQ::new(),
            events,
            start_time: config.start_time,
            end_time: config.end_time,
            routing_mode: config.routing_mode,
            output_dir: config.output_dir.to_owned(),
            routing_kit_network,
        }
    }

    pub fn create_simulation_partitions(
        config: &Config,
        scenario: Scenario<Vehicle>,
        routing_kit_network: RoutingKitNetwork,
    ) -> Vec<Simulation> {
        let simulations: Vec<_> = scenario
            .scenarios
            .into_iter()
            .map(|partition| {
                let id = partition.msg_broker.id;
                let mut events = EventsPublisher::new();
                let output_path = PathBuf::from(&config.output_dir);
                let events_file = format!("events.{id}.pbf");
                let events_path = output_path.join(events_file);
                let writer = ProtoEventsWriter::new(&events_path);
                events.add_subscriber(Box::new(writer));
                Simulation::new(config, partition, events, routing_kit_network.clone())
            })
            .collect();

        simulations
    }

    pub fn run(&mut self) {
        // use fixed start and end times
        let mut now = self.start_time;
        info!(
            "Starting #{}. Network neighbors: {:?}",
            self.scenario.msg_broker.id,
            self.scenario.network.neighbors(),
        );

        let mut router: Option<Router> = None;
        let cch: CCH;
        if self.routing_mode == RoutingMode::AdHoc {
            cch = Router::perform_preprocessing(
                &self.routing_kit_network,
                self.get_temp_output_folder().as_str(),
            );
            router = Some(Router::new(&cch, &self.routing_kit_network));
        };

        // conceptually this should do the following in the main loop:

        // put received agents onto out part of the split links
        // move nodes
        // send agents to neighbours
        // receive vehicles and teleported agents from neighbours - this must be at the end, so that the loop can start.
        //   add received agents to agent_q
        //   put agents from agent_q onto links -- wakeup

        // this will shut down the thread once everybody has left to other
        // simulation parts. Think about better sync method, so that a thread only
        // terminates, if all threads are done.
        while self.active_agents() > 0 && now <= self.end_time {
            if now % 3600 == 0 {
                let hour = now / 3600;
                info!(
                    "Simulation #{} at {hour}:00:00",
                    self.scenario.msg_broker.id
                );
            }

            self.wakeup(now, &mut router);
            self.teleportation_arrivals(now);
            self.move_nodes(now);
            self.send(now);
            self.receive(now);
            now += 1;
        }

        self.events.finish();
        info!("Partition #{} finished.", self.scenario.msg_broker.id,);
    }

    fn wakeup(&mut self, now: u32, router: &mut Option<Router>) {
        let agents_2_link = self.activity_q.wakeup(now);

        for id in agents_2_link {
            let agent = self.scenario.population.agents.get_mut(&id).unwrap();
            if let PlanElement::Activity(act) = agent.current_plan_element() {
                self.events.publish_event(
                    now,
                    &Event::new_act_end(agent.id as u64, act.link_id as u64, act.act_type.clone()),
                )
            }

            if let Some(router) = router {
                let end_activity = match agent.plan.elements.get(agent.current_element).unwrap() {
                    PlanElement::Activity(a) => a,
                    PlanElement::Leg(_) => todo!(),
                };

                let new_activity = match agent.plan.elements.get(agent.current_element + 1).unwrap()
                {
                    PlanElement::Activity(a) => a,
                    PlanElement::Leg(_) => todo!(),
                };

                let query_result = router.query_coordinates(
                    end_activity.x,
                    end_activity.y,
                    new_activity.x,
                    new_activity.y,
                );

                let leg = PlanElement::Leg(Leg {
                    mode: "car".to_string(),
                    dep_time: end_activity.end_time,
                    trav_time: query_result.travel_time,
                    route: Route::NetworkRoute(NetworkRoute {
                        vehicle_id: 0, //TODO is a counter feasible?
                        route: query_result
                            .path
                            .expect("There is no route!")
                            .into_iter()
                            .map(|e| e as usize)
                            .collect(),
                    }),
                });

                agent.plan.elements.insert(agent.current_element + 1, leg);
            }

            //here, current element counter is going to be increased
            agent.advance_plan();

            if let PlanElement::Leg(leg) = agent.current_plan_element() {
                match &leg.route {
                    Route::NetworkRoute(net_route) => {
                        self.events.publish_event(
                            now,
                            &Event::new_departure(
                                agent.id as u64,
                                *net_route.route.first().unwrap() as u64,
                                leg.mode.clone(),
                            ),
                        );
                        Simulation::push_onto_network(
                            &mut self.scenario.network,
                            &mut self.events,
                            net_route,
                            0,
                            agent.id,
                            now,
                        );
                    }
                    Route::GenericRoute(gen_route) => {
                        self.events.publish_event(
                            now,
                            &Event::new_departure(
                                agent.id as u64,
                                gen_route.start_link as u64,
                                leg.mode.clone(),
                            ),
                        );
                        if Simulation::is_local_teleportation(agent, &self.scenario.msg_broker) {
                            self.teleportation_q.add(agent, now);
                        } else {
                            // copy the id here, so that the reference to agent can be dropped before we
                            // attempt to own the agent to move it to customs.
                            let id = agent.id;
                            let agent = self.scenario.population.agents.remove(&id).unwrap();
                            self.scenario.msg_broker.prepare_teleported(agent);
                        }
                    }
                }
            }
        }
    }

    fn teleportation_arrivals(&mut self, now: u32) {
        let agents_2_activity = self.teleportation_q.wakeup(now);
        for id in agents_2_activity {
            let agent = self.scenario.population.agents.get_mut(&id).unwrap();
            if let PlanElement::Leg(leg) = agent.current_plan_element() {
                if let Route::GenericRoute(route) = &leg.route {
                    self.events.publish_event(
                        now,
                        &Event::new_travelled(agent.id as u64, route.distance, leg.mode.clone()),
                    )
                }
            }

            agent.advance_plan();
            self.activity_q.add(agent, now);
        }
    }

    fn move_nodes(&mut self, now: u32) {
        for node in self.scenario.network.nodes.values() {
            let exited_vehicles =
                node.move_vehicles(&mut self.scenario.network.links, now, &mut self.events);

            for exit_reason in exited_vehicles {
                match exit_reason {
                    ExitReason::FinishRoute(vehicle) => {
                        let agent = self
                            .scenario
                            .population
                            .agents
                            .get_mut(&vehicle.driver_id)
                            .unwrap();

                        self.events.publish_event(
                            now,
                            &Event::new_person_leaves_veh(agent.id as u64, vehicle.id as u64),
                        );

                        let leg_mode = if let PlanElement::Leg(leg) = agent.current_plan_element() {
                            leg.mode.clone()
                        } else {
                            panic!("Expected leg");
                        };

                        agent.advance_plan();

                        if let PlanElement::Activity(act) = agent.current_plan_element() {
                            self.events.publish_event(
                                now,
                                &Event::new_arrival(agent.id as u64, act.link_id as u64, leg_mode),
                            );
                            self.events.publish_event(
                                now,
                                &Event::new_act_start(
                                    agent.id as u64,
                                    act.link_id as u64,
                                    act.act_type.clone(),
                                ),
                            );
                        }
                        self.activity_q.add(agent, now);
                    }
                    ExitReason::ReachedBoundary(vehicle) => {
                        let agent = self
                            .scenario
                            .population
                            .agents
                            .remove(&vehicle.driver_id)
                            .unwrap();
                        self.scenario.msg_broker.prepare_routed(agent, vehicle);
                    }
                }
            }
        }
    }

    fn send(&mut self, now: u32) {
        self.scenario.msg_broker.send(now);
    }

    fn receive(&mut self, now: u32) {
        let messages = self.scenario.msg_broker.receive(now);
        for message in messages {
            for vehicle in message.vehicles {
                let agent = vehicle.0;
                let route_index = vehicle.1;
                if let PlanElement::Leg(leg) = agent.current_plan_element() {
                    match &leg.route {
                        Route::NetworkRoute(net_route) => {
                            Simulation::push_onto_network(
                                &mut self.scenario.network,
                                &mut self.events,
                                net_route,
                                route_index,
                                agent.id,
                                now,
                            );
                            self.scenario.population.agents.insert(agent.id, agent);
                        }
                        Route::GenericRoute(_) => {
                            self.teleportation_q.add(&agent, now);
                        }
                    }
                }
            }
        }
    }

    fn active_agents(&self) -> usize {
        1 // this needs something else maybe a counter would do.
          //self.scenario.population.agents.len() - self.activity_q.finished_agents()
    }

    fn push_onto_network(
        network: &mut NetworkPartition<Vehicle>,
        events: &mut EventsPublisher,
        route: &NetworkRoute,
        route_index: usize,
        driver_id: usize,
        now: u32,
    ) -> Option<Vehicle> {
        let mut vehicle = Vehicle::new(route.vehicle_id, driver_id, route.route.clone());
        vehicle.route_index = route_index;
        let link_id = route.route.get(route_index).unwrap();
        let link = network.links.get_mut(link_id).unwrap();

        match link {
            Link::LocalLink(local_link) => Self::push_onto_link(events, now, vehicle, local_link),
            Link::SplitInLink(split_link) => {
                let local_link = split_link.local_link_mut();
                Self::push_onto_link(events, now, vehicle, local_link)
            }
            // I am not sure whether this is even possible.
            Link::SplitOutLink(_) => Some(vehicle),
        }
    }

    fn push_onto_link(
        events: &mut EventsPublisher,
        now: u32,
        vehicle: Vehicle,
        local_link: &mut LocalLink<Vehicle>,
    ) -> Option<Vehicle> {
        if vehicle.route_index == 0 {
            events.publish_event(
                now,
                &Event::new_person_enters_veh(vehicle.driver_id as u64, vehicle.id as u64),
            );
        } else {
            events.publish_event(
                now,
                &Event::new_link_enter(local_link.id() as u64, vehicle.id as u64),
            );
        }

        local_link.push_vehicle(vehicle, now);
        None
    }

    fn is_local_teleportation(agent: &Agent, customs: &MessageBroker) -> bool {
        let (start_thread, end_thread) =
            Simulation::get_thread_ids_for_generic_route(agent, customs);
        start_thread == end_thread
    }

    fn get_thread_ids_for_generic_route(agent: &Agent, customs: &MessageBroker) -> (usize, usize) {
        if let PlanElement::Leg(leg) = agent.current_plan_element() {
            if let Route::GenericRoute(route) = &leg.route {
                let start_thread = *customs.part_id(&route.start_link);
                let end_thread = *customs.part_id(&route.end_link);
                return (start_thread, end_thread);
            }
        }
        panic!("This should not happen!!!")
    }

    fn get_temp_output_folder(&self) -> String {
        format!(
            "{}{}{}{}",
            self.output_dir.to_owned(),
            "/routing/",
            self.scenario.msg_broker.id,
            "/"
        )
    }
}
/*
#[cfg(test)]
mod test {
    use crate::io::network::IONetwork;
    use crate::io::non_blocking_io::NonBlocking;
    use crate::io::population::IOPopulation;
    use crate::parallel_simulation::events::Events;
    use crate::parallel_simulation::splittable_scenario::Scenario;
    use crate::parallel_simulation::Simulation;
    use flexi_logger::{detailed_format, FileSpec, Logger, LoggerHandle, WriteMode};
    use log::info;
    use std::path::Path;
    use std::thread;
    use std::thread::JoinHandle;

    /// This creates a scenario with three links and one agent. The scenario is not split up, therefore
    /// a single threaded simulation is run. This test exists to see whether the logic of the simulation
    /// without passing messages to other simulation slices works.
    #[test]
    fn run_single_agent_single_slice() {
        // let _logger =
        //     get_file_logger("./test_output/parallel_simulation/run_single_agent_single_slice/");

        let network = IONetwork::from_file("./assets/3-links/3-links-network.xml");
        let population = IOPopulation::from_file("./assets/3-links/1-agent.xml");

        let scenario = Scenario::from_io(&network, &population, 1);

        let out_network = scenario.as_network(&network);
        out_network.to_file(Path::new(
            "./test_output/parallel_simulation/run_single_agent_single_slice/output_network.xml.gz",
        ));
        let (writer, _guard) = NonBlocking::from_file(
            "./test_output/parallel_simulation/run_single_agent_single_slice/output_events.xml",
        );
        let mut events = Events::new(writer);
        let mut simulations = Simulation::create_simulation_partitions(scenario, &events);

        assert_eq!(1, simulations.len());
        let mut simulation = simulations.remove(0);
        simulation.run();
        events.finish();
    }

    /// This creates a scenario with three links and one agent. The scenario is split into two domains.
    /// The scenario should contain one split link "link2". Nodes 1 and 2 should be in the first, 3 and 4
    /// should end up in the second domain. The agent starts at link1, enters link2, gets passed to
    /// the other domain, leaves link2, enters link3 and finishes its route on link3
    #[test]
    fn run_single_agent_two_slices() {
        //let _logger =
        //     get_file_logger("./test_output/parallel_simulation/run_single_agent_two_slices/");
        let network = IONetwork::from_file("./assets/3-links/3-links-network.xml");
        let population = IOPopulation::from_file("./assets/3-links/1-agent.xml");
        let scenario = Scenario::from_io(&network, &population, 2);

        let out_network = scenario.as_network(&network);
        out_network.to_file(Path::new(
            "./test_output/parallel_simulation/run_single_agent_two_slices/output_network.xml.gz",
        ));
        let (writer, _guard) = NonBlocking::from_file(
            "./test_output/parallel_simulation/run_single_agent_two_slices/output_events.xml",
        );
        let mut events = Events::new(writer);
        let simulations = Simulation::create_simulation_partitions(scenario, &events);

        let join_handles: Vec<_> = simulations
            .into_iter()
            .map(|mut simulation| thread::spawn(move || simulation.run()))
            .collect();

        for handle in join_handles {
            handle.join().unwrap();
        }
        events.finish();
    }

    #[test]
    fn run_equil_scenario() {
        let _logger = get_file_logger("./test_output/parallel_simulation/run_equil_scenario/");

        // load input files
        let network = IONetwork::from_file("./assets/equil-network.xml");
        let population = IOPopulation::from_file("./assets/equil-plans.xml.gz");

        // convert input into simulation
        let scenario = Scenario::from_io(&network, &population, 2);

        let out_network = scenario.as_network(&network);
        out_network.to_file(Path::new(
            "./test_output/parallel_simulation/run_equil_scenario/output_network.xml.gz",
        ));
        let (writer, _guard) = NonBlocking::from_file(
            "./test_output/parallel_simulation/run_equil_scenario/output_events.xml",
        );
        let mut events = Events::new(writer);
        let simulations = Simulation::create_simulation_partitions(scenario, &events);

        // create threads and start them
        let join_handles: Vec<JoinHandle<()>> = simulations
            .into_iter()
            .map(|mut simulation| thread::spawn(move || simulation.run()))
            .collect();

        // wait for all threads to finish
        for handle in join_handles {
            handle.join().unwrap();
        }
        events.finish();

        info!("all simulation threads have finished. ")
    }

    #[test]
    #[ignore]
    fn run_berlin_scenario() {
        let _logger = get_file_logger("./test_output/parallel_simulation/run_berlin_scenario/");
        let network = IONetwork::from_file("/home/janek/test-files/berlin-test-network.xml.gz");
        let population =
            IOPopulation::from_file("/home/janek/test-files/berlin-all-plans-without-pt.xml.gz");

        let scenario = Scenario::from_io(&network, &population, 16);

        let out_network = scenario.as_network(&network);
        out_network.to_file(Path::new(
            "./test_output/parallel_simulation/run_berlin_scenario/output_network.xml.gz",
        ));
        let (writer, _guard) = NonBlocking::from_file(
            "./test_output/parallel_simulation/run_berlin_scenario/output_events.xml",
        );
        let mut events = Events::new(writer);
        let simulations = Simulation::create_simulation_partitions(scenario, &events);

        // create threads and start them
        let join_handles: Vec<JoinHandle<()>> = simulations
            .into_iter()
            .map(|mut simulation| thread::spawn(move || simulation.run()))
            .collect();

        // wait for all threads to finish
        for handle in join_handles {
            handle.join().unwrap();
        }

        events.finish();

        info!("all simulation threads have finished. ")
    }

    fn get_file_logger(directory: &str) -> LoggerHandle {
        Logger::try_with_str("info")
            .unwrap()
            .log_to_file(
                FileSpec::default()
                    .suppress_timestamp()
                    .directory(directory),
            )
            .format_for_files(detailed_format)
            .write_mode(WriteMode::Async)
            .start()
            .unwrap()
    }
}

 */
