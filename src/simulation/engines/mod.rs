use crate::simulation::engines::activity_engine::ActivityEngine;
use crate::simulation::engines::leg_engine::LegEngine;
use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::population::population_data::State;
use crate::simulation::wire_types::population::Person;
use std::cell::RefCell;
use std::rc::Rc;

pub mod activity_engine;
pub mod leg_engine;
pub mod network_engine;
pub mod teleportation_engine;

pub trait ReplanEngine {
    fn do_sim_step(&mut self, now: u32, agents: &Vec<&mut Person>);
}

pub struct AgentStateTransitionLogic<C: SimCommunicator> {
    activity_engine: Rc<RefCell<ActivityEngine<C>>>,
    pub leg_engine: Rc<RefCell<LegEngine<C>>>,
}

impl<C: SimCommunicator + 'static> AgentStateTransitionLogic<C> {
    fn arrange_next_agent_state(&self, now: u32, agent: Person) {
        match agent.state() {
            State::ACTIVITY => self.activity_engine.borrow_mut().receive_agent(now, agent),
            State::LEG => self.leg_engine.borrow_mut().receive_agent(now, agent),
        }
    }

    pub(crate) fn new(
        activity_engine: Rc<RefCell<ActivityEngine<C>>>,
        teleportation_engine: Rc<RefCell<LegEngine<C>>>,
    ) -> Self {
        AgentStateTransitionLogic {
            activity_engine,
            leg_engine: teleportation_engine,
        }
    }
}
