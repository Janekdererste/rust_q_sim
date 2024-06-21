use std::cell::RefCell;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::rc::Rc;

use tracing::info;

use crate::simulation::config::Config;
use crate::simulation::engines::activity_engine::ActivityEngine;
use crate::simulation::engines::leg_engine::LegEngine;
use crate::simulation::engines::{AgentStateTransitionLogic, ReplanEngine};
use crate::simulation::id::Id;
use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::messaging::communication::message_broker::NetMessageBroker;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::sim_network::SimNetworkPartition;
use crate::simulation::population::population::Population;
use crate::simulation::replanning::replanner::Replanner;
use crate::simulation::time_queue::TimeQueue;
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::wire_types::messages::Vehicle;

pub struct Simulation<C: SimCommunicator> {
    activity_engine: Rc<RefCell<ActivityEngine<C>>>,
    leg_engine: Rc<RefCell<LegEngine<C>>>,
    internal_interface: Rc<RefCell<AgentStateTransitionLogic<C>>>,
    events: Rc<RefCell<EventsPublisher>>,
    replan_engines: Vec<Box<dyn ReplanEngine>>,
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
        events: Rc<RefCell<EventsPublisher>>,
        replanner: Box<dyn Replanner>,
    ) -> Self {
        let mut activity_q = TimeQueue::new();

        // take Persons and copy them into queues. This way we can keep population around to translate
        // ids for events processing...
        let agents = std::mem::take(&mut population.persons);

        for agent in agents.into_values() {
            activity_q.add(agent, config.simulation().start_time);
        }

        let activity_engine: Rc<RefCell<ActivityEngine<C>>> = Rc::new(RefCell::new(
            ActivityEngine::new(activity_q, events.clone()),
        ));

        let passenger_modes = config
            .simulation()
            .passenger_modes
            .iter()
            .map(|mode| Id::<String>::get_from_ext(mode).internal())
            .collect();

        let leg_engine = Rc::new(RefCell::new(LegEngine::new(
            network,
            garage,
            net_message_broker,
            events.clone(),
            passenger_modes,
        )));

        let internal_interface = Rc::new(RefCell::new(AgentStateTransitionLogic::new(
            activity_engine.clone(),
            leg_engine.clone(),
        )));

        activity_engine
            .borrow_mut()
            .set_agent_state_transition_logic(Rc::downgrade(&internal_interface));
        leg_engine
            .borrow_mut()
            .set_agent_state_transition_logic(Rc::downgrade(&internal_interface));

        Simulation {
            activity_engine,
            leg_engine,
            internal_interface,
            events,
            replan_engines: vec![],
            start_time: config.simulation().start_time,
            end_time: config.simulation().end_time,
        }
    }

    #[tracing::instrument(level = "info", skip(self), fields(rank = self.leg_engine.borrow().net_message_broker().rank()))]
    pub fn run(&mut self) {
        // use fixed start and end times
        let mut now = self.start_time;
        info!(
            "Starting #{}. Network neighbors: {:?}, Start time {}, End time {}",
            self.leg_engine.borrow().net_message_broker().rank(),
            self.leg_engine.borrow().network().neighbors(),
            self.start_time,
            self.end_time,
        );

        while now <= self.end_time {
            if self.leg_engine.borrow().net_message_broker().rank() == 0 && now % 3600 == 0 {
                let _hour = now / 3600;
                let _min = (now % 3600) / 60;
                info!(
                    "#{} of Qsim at {_hour:02}:{_min:02}; Active Nodes: {}, Active Links: {}, Vehicles on Network Partition: {}",
                    self.leg_engine.borrow().net_message_broker().rank(),
                    self.leg_engine.borrow().network().active_nodes(),
                    self.leg_engine.borrow().network().active_links(),
                    self.leg_engine.borrow().network().veh_on_net()
                );
            }

            // let mut act_ref = self.activity_engine.borrow_mut();
            // let mut leg_ref = self.leg_engine.borrow_mut();

            // let mut agents = act_ref.agents();
            // agents.extend(leg_ref.agents());
            //
            // for engine in &mut self.replan_engines {
            //     engine.do_sim_step(now, &agents);
            // }

            self.activity_engine.borrow_mut().do_step(now);
            self.leg_engine.borrow_mut().do_step(now);

            //TODO
            // self.replanner.update_time(now, &mut self.events);

            now += 1;
        }

        // maybe this belongs into the controller? Then this would have to be a &mut instead of owned.
        self.events.borrow_mut().finish();
    }

    pub(crate) fn is_local_route(veh: &Vehicle, message_broker: &NetMessageBroker<C>) -> bool {
        let leg = veh.driver.as_ref().unwrap().curr_leg();
        let route = leg.route.as_ref().unwrap();
        let to = message_broker.rank_for_link(route.end_link());
        message_broker.rank() == to
    }
}

impl<C: SimCommunicator + 'static> Debug for Simulation<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Simulation with Rank #{}",
            self.leg_engine.borrow().net_message_broker().rank()
        )
    }
}
