use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::messaging::communication::message_broker::NetMessageBroker;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::sim_network::SimNetworkPartition;
use crate::simulation::wire_types::messages::Vehicle;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::rc::Rc;

pub struct NetworkEngine {
    pub(crate) network: SimNetworkPartition,
    pub events: Rc<RefCell<EventsPublisher>>,
}

impl NetworkEngine {
    pub fn new(network: SimNetworkPartition, events: Rc<RefCell<EventsPublisher>>) -> Self {
        NetworkEngine { network, events }
    }

    pub fn receive_vehicle(&mut self, now: u32, vehicle: Vehicle, route_begin: bool) {
        let events = if route_begin {
            //if route has just begun, no link enter event should be published
            None
        } else {
            //if route is already in progress, this method gets vehicles from another partition and should publish link enter event
            //this is because the receiving partition is the owner of this link and should publish the event
            Some(self.events.clone())
        };
        self.network.send_veh_en_route(vehicle, events, now)
    }

    pub(crate) fn move_nodes(&mut self, now: u32) -> Vec<Vehicle> {
        let exited_vehicles = self
            .network
            .move_nodes(self.events.borrow_mut().deref_mut(), now);
        exited_vehicles
    }

    pub(crate) fn move_links<C: SimCommunicator + 'static>(
        &mut self,
        now: u32,
        net_message_broker: &mut NetMessageBroker<C>,
    ) {
        let (vehicles, storage_cap_updates) = self.network.move_links(now);

        for veh in vehicles {
            net_message_broker.add_veh(veh, now);
        }

        for cap in storage_cap_updates {
            net_message_broker.add_cap_update(cap, now);
        }
    }
}
