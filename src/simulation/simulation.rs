use std::fmt::Debug;
use std::fmt::Formatter;

use tracing::{info, instrument};

use crate::simulation::config::Config;
use crate::simulation::id::Id;
use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::messaging::communication::message_broker::NetMessageBroker;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::sim_network::SimNetworkPartition;
use crate::simulation::population::population::Population;
use crate::simulation::replanning::replanner::Replanner;
use crate::simulation::time_queue::TimeQueue;
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::wire_types::events::Event;
use crate::simulation::wire_types::messages::Vehicle;
use crate::simulation::wire_types::population::Person;
use crate::simulation::wire_types::vehicles::LevelOfDetail;

pub struct Simulation<C>
    where
        C: SimCommunicator,
{
    activity_q: TimeQueue<Person>,
    teleportation_q: TimeQueue<Vehicle>,
    network: SimNetworkPartition,
    garage: Garage,
    net_message_broker: NetMessageBroker<C>,
    events: EventsPublisher,
    replanner: Box<dyn Replanner>,
    start_time: u32,
    end_time: u32,
}

impl<C> Simulation<C>
    where
        C: SimCommunicator + 'static,
{
    pub fn new(
        config: Config,
        network: SimNetworkPartition,
        garage: Garage,
        mut population: Population,
        net_message_broker: NetMessageBroker<C>,
        events: EventsPublisher,
        replanner: Box<dyn Replanner>,
    ) -> Self {
        let mut activity_q = TimeQueue::new();

        // take Persons and copy them into queues. This way we can keep population around to translate
        // ids for events processing...
        let agents = std::mem::take(&mut population.persons);

        for agent in agents.into_values() {
            activity_q.add(agent, config.simulation().start_time);
        }

        Simulation {
            network,
            garage,
            teleportation_q: TimeQueue::new(),
            activity_q,
            net_message_broker,
            events,
            replanner,
            start_time: config.simulation().start_time,
            end_time: config.simulation().end_time,
        }
    }

    //#[tracing::instrument(level = "info", skip(self), fields(rank = self.net_message_broker.rank()))]
    pub fn run(&mut self) {
        // use fixed start and end times
        let mut now = self.start_time;
        info!(
            "Starting #{}. Network neighbors: {:?}, Start time {}, End time {}",
            self.net_message_broker.rank(),
            self.network.neighbors(),
            self.start_time,
            self.end_time,
        );

        while now <= self.end_time {
            if self.net_message_broker.rank() == 0 && now % 3600 == 0 {
                let _hour = now / 3600;
                let _min = (now % 3600) / 60;
                info!(
                    "#{} of Qsim at {_hour:02}:{_min:02}; Active Nodes: {}, Active Links: {}, Vehicles on Network Partition: {}",
                    self.net_message_broker.rank(),
                    self.network.active_nodes(),
                    self.network.active_links(),
                    self.network.veh_on_net()
                );
            }
            self.wakeup(now);
            self.terminate_teleportation(now);
            self.move_nodes(now);
            self.move_links(now);

            self.replanner.update_time(now, &mut self.events);

            now += 1;
        }

        // maybe this belongs into the controller? Then this would have to be a &mut instead of owned.
        self.events.finish();
    }

    //#[tracing::instrument(level = "trace", skip(self), fields(rank = self.net_message_broker.rank()))]
    fn wakeup(&mut self, now: u32) {
        let agents = self.activity_q.pop(now);

        for mut agent in agents {
            self.update_agent(&mut agent, now);

            let act_type: Id<String> = Id::get(agent.curr_act().act_type);
            self.events.publish_event(
                now,
                &Event::new_act_end(agent.id, agent.curr_act().link_id, act_type.internal()),
            );

            let mut vehicle = self.departure(agent, now);
            let veh_type_id = Id::get(vehicle.r#type);
            let veh_type = self.garage.vehicle_types.get(&veh_type_id).unwrap();

            match veh_type.lod() {
                LevelOfDetail::Network => {
                    self.events.publish_event(
                        now,
                        &Event::new_person_enters_veh(vehicle.agent().id, vehicle.id),
                    );
                    //we don't pass the event publisher because a link enter event should not be published
                    self.network.send_veh_en_route(vehicle, None, now);
                }
                LevelOfDetail::Teleported => {
                    if Simulation::is_local_route(&vehicle, &self.net_message_broker) {
                        self.teleportation_q.add(vehicle, now);
                    } else {
                        // set the pointer of the route to the last element, so that the current link
                        // is the destination of this leg. Setting this to the last element makes this
                        // logic independent of whether the agent has a Generic-Route with only start
                        // and end link or a full Network-Route, which is often the case for ride modes.
                        vehicle.route_index_to_last();
                        self.net_message_broker.add_veh(vehicle, now);
                    }
                }
            }
        }
    }

    fn departure(&mut self, mut agent: Person, now: u32) -> Vehicle {
        //here, current element counter is going to be increased
        agent.advance_plan();

        assert_ne!(agent.curr_plan_elem % 2, 0);

        let leg = agent.curr_leg();
        let route = leg.route.as_ref().unwrap();
        let leg_mode: Id<String> = Id::get(leg.mode);
        self.events.publish_event(
            now,
            &Event::new_departure(agent.id, route.start_link(), leg_mode.internal()),
        );

        let veh_id = Id::get(route.veh_id);
        self.garage.unpark_veh(agent, &veh_id)
    }

    fn update_agent(&mut self, agent: &mut Person, now: u32) {
        self.replanner.replan(now, agent, &self.garage)
    }

    //#[instrument(level = "trace", skip(self), fields(rank = self.net_message_broker.rank()))]
    fn terminate_teleportation(&mut self, now: u32) {
        let teleportation_vehicles = self.teleportation_q.pop(now);
        for vehicle in teleportation_vehicles {
            // park the vehice - get the agent out of the vehicle
            let mut agent = self.garage.park_veh(vehicle);

            // emmit travelled
            let leg = agent.curr_leg();
            let route = leg.route.as_ref().unwrap();
            let mode: Id<String> = Id::get(leg.mode);
            self.events.publish_event(
                now,
                &Event::new_travelled(agent.id, route.distance, mode.internal()),
            );

            // emmit arrival
            self.events.publish_event(
                now,
                &Event::new_arrival(agent.id, route.end_link(), mode.internal()),
            );

            // advance plan to activity and put agent into activity q.
            agent.advance_plan();

            // emmit act start event
            let act = agent.curr_act();
            let act_type: Id<String> = Id::get(act.act_type);
            self.events.publish_event(
                now,
                &Event::new_act_start(agent.id, act.link_id, act_type.internal()),
            );
            self.activity_q.add(agent, now);
        }
    }

    //#[instrument(level = "trace", skip(self), fields(rank = self.net_message_broker.rank()))]
    fn move_nodes(&mut self, now: u32) {
        let exited_vehicles = self.network.move_nodes(&mut self.events, now);

        for veh in exited_vehicles {
            self.events
                .publish_event(now, &Event::new_person_leaves_veh(veh.agent().id, veh.id));
            let veh_type_id = Id::get(veh.r#type);
            let veh_type = self.garage.vehicle_types.get(&veh_type_id).unwrap();
            let mode = veh_type.net_mode;
            let mut agent = self.garage.park_veh(veh);

            // move to next activity
            agent.advance_plan();
            let act = agent.curr_act();
            self.events
                .publish_event(now, &Event::new_arrival(agent.id, act.link_id, mode));
            let act_type: Id<String> = Id::get(act.act_type);
            self.events.publish_event(
                now,
                &Event::new_act_start(agent.id, act.link_id, act_type.internal()),
            );
            self.activity_q.add(agent, now);
        }
    }

    //#[instrument(level = "trace", skip(self), fields(rank = self.net_message_broker.rank()))]
    fn move_links(&mut self, now: u32) {
        let (vehicles, storage_cap_updates) = self.network.move_links(now);

        for veh in vehicles {
            self.net_message_broker.add_veh(veh, now);
        }

        for cap in storage_cap_updates {
            self.net_message_broker.add_cap_update(cap, now);
        }

        let sync_messages = self.net_message_broker.send_recv(now);

        for msg in sync_messages {
            self.network
                .apply_storage_cap_updates(msg.storage_capacities);

            for veh in msg.vehicles {
                let veh_type_id = Id::get(veh.r#type);
                let veh_type = self.garage.vehicle_types.get(&veh_type_id).unwrap();
                match veh_type.lod() {
                    LevelOfDetail::Network => {
                        self.network
                            .send_veh_en_route(veh, Some(&mut self.events), now)
                    }
                    LevelOfDetail::Teleported => self.teleportation_q.add(veh, now),
                }
            }
        }
    }

    fn is_local_route(veh: &Vehicle, message_broker: &NetMessageBroker<C>) -> bool {
        let leg = veh.agent.as_ref().unwrap().curr_leg();
        let route = leg.route.as_ref().unwrap();
        let to = message_broker.rank_for_link(route.end_link());
        message_broker.rank() == to
    }
}

impl<C: SimCommunicator> Debug for Simulation<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Simulation with Rank #{}",
            self.net_message_broker.rank()
        )
    }
}
