use std::{collections::HashMap, path::PathBuf};

use mpi::{topology::SystemCommunicator, Rank};

use crate::simulation::{messaging::messages::proto::{Agent, Activity, Plan, Leg}, io::{network::IONetwork, vehicle_definitions::VehicleDefinitions}, id_mapping::MatsimIdMappings};

use super::{road_router::RoadRouter, network_converter::NetworkConverter};

struct TripPlanner<'router> {
    routers: HashMap<String, RoadRouter<'router>>
}

struct Trip<'a> {
    start_act : &'a Activity,
    end_activity:  &'a Activity,
    legs: Vec<Leg>,
    interactions: Vec<Activity>
}

struct RoutingRequest<'a> {
    start_act: &'a Activity,
    end_act: &'a Activity,
    dep_time: u32,
    agent: &'a Agent
}

impl<'router> TripPlanner<'router> {
    
    pub fn new(io_network: IONetwork, //TODO change it to matsim internal network representation
               id_mappings: Option<&MatsimIdMappings>,
               communicator: SystemCommunicator,
               rank: Rank,
               output_dir: PathBuf,
               vehicle_definitions: Option<VehicleDefinitions>,) -> Self {
        
        // TODO this is terrible and should be done in a more consise way
        let routers = if let Some(vehicle_definitions) = vehicle_definitions.as_ref() {
            NetworkConverter::convert_io_network_with_vehicle_definitions(
                io_network,
                id_mappings,
                vehicle_definitions,
            )
            .iter()
            .map(|(m, r)| (m.clone(), RoadRouter::new(r, output_dir.join(m))))
            .collect::<HashMap<_, _>>()
        } else {
            let mut map = HashMap::new();
            map.insert(
                Plan::DEFAULT_ROUTING_MODE.to_string(),
                RoadRouter::new(
                    &NetworkConverter::convert_io_network(io_network, id_mappings, None, None),
                    output_dir,
                ),
            );
            map
        };
        
        TripPlanner { routers }
    }
    
    fn get_router(& self, mode : &str) -> & RoadRouter<'router> {
        self.routers.get(mode).expect(format!("no router for mode {mode}").as_str())
    }
    
    fn plan_next_trip<'a>(&self, req: RoutingRequest<'a>) -> Trip<'a> {
        
        let start_act = req.agent.curr_act();
        let end_act = req.agent.next_main_act();
        let next_leg = req.agent.next_leg();
        // in case of access-main-egress legs, this will yield the wrong mode. Let's get this right later and assume that all
        // trips are single legs before they are passed into the router.
        let router = self.get_router(&next_leg.mode);
        
        // find closest link to start_act
        let node_id = router.initial_network.find_nearest_node_id(start_act.x, start_act.y);
        //TODO: What now. We need to find an appropriate link after we have found the right node
            // create interaction activity at link
            // create walk leg from start_act to interaction activity
        
        // find closest link to end_act
            // create interaction activity at link
            // create walk leg from interaction activity to end_act
        
        // route from link to link
            // create routed leg
        
        // remove elements between start and end activity from plan
        // insert walk-leg, interaction act, routed-leg, interaction act, walk-leg into agent
    }
}