use std::fs;
use std::path::PathBuf;

use crate::simulation::config;
use crate::simulation::id::Id;
use crate::simulation::wire_types::population::{Activity, Leg, Person, Plan, Route};
use crate::simulation::wire_types::vehicles::{LevelOfDetail, VehicleType};

pub fn create_agent(id: u64, route: Vec<u64>) -> Person {
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
    let mut agent = Person::new(id, plan);
    agent.advance_plan();

    agent
}

pub fn create_folders(path: PathBuf) -> PathBuf {
    fs::create_dir_all(&path)
        .unwrap_or_else(|_e| panic!("Failed to create folders for path {path:?}"));
    path
}

pub fn create_vehicle_type(id: &Id<VehicleType>, net_mode: Id<String>) -> VehicleType {
    VehicleType {
        id: id.internal(),
        length: 0.0,
        width: 0.0,
        max_v: 0.0,
        pce: 0.0,
        fef: 0.0,
        net_mode: net_mode.internal(),
        lod: LevelOfDetail::Network as i32,
    }
}

pub fn config() -> config::Simulation {
    config::Simulation {
        start_time: 0,
        end_time: 0,
        sample_size: 1.0,
        stuck_threshold: u32::MAX,
    }
}
