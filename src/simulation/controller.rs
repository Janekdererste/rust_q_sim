use crate::simulation::config::{Config, RoutingMode};
use crate::simulation::id_mapping::MatsimIdMappings;
use crate::simulation::io::network::IONetwork;
use crate::simulation::io::population::IOPopulation;
use crate::simulation::io::proto_events::ProtoEventsWriter;
use crate::simulation::io::vehicle_definitions::{IOVehicleDefinitions, VehicleDefinitions};
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::messaging::message_broker::MpiMessageBroker;
use crate::simulation::messaging::travel_time_collector::TravelTimeCollector;
use crate::simulation::network::network::Network;
use crate::simulation::partition_info::PartitionInfo;
use crate::simulation::performance_profiling::measure_duration;
use crate::simulation::population::Population;
use crate::simulation::routing::router::Router;
use crate::simulation::routing::travel_times_collecting_road_router::TravelTimesCollectingRoadRouter;
use crate::simulation::routing::walk_leg_updater::{EuclideanWalkLegUpdater, WalkLegUpdater};
use crate::simulation::simulation::Simulation;
use mpi::topology::SystemCommunicator;
use mpi::traits::{Communicator, CommunicatorCollectives};
use serde_json::json;
use std::ffi::c_int;
use std::fs;
use std::fs::remove_dir_all;
use std::ops::Sub;
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;

pub fn run(world: SystemCommunicator, config: Config) {
    let rank = world.rank();
    let size = world.size();

    info!("Process #{rank} of {size}");

    let output_path = PathBuf::from(&config.output_dir);
    fs::create_dir_all(&output_path).expect("Failed to create output path");

    let io_network = IONetwork::from_file(config.network_file.as_ref());
    let io_population = IOPopulation::from_file(config.population_file.as_ref());
    let id_mappings = MatsimIdMappings::from_io(&io_network, &io_population);
    let partition_info = PartitionInfo::from_io_network(&io_network, &id_mappings, size as usize);
    let mut network: Network = Network::from_io(
        &io_network,
        size as usize,
        config.sample_size,
        |node| partition_info.get_partition(node),
        &id_mappings,
    );

    // write network with new ids to output but only once.
    if rank == 0 {
        let out_network =
            io_network.clone_with_internal_ids(&network, &id_mappings.links, &id_mappings.nodes);
        out_network.to_file(&output_path.join("output_network.xml.gz"));

        let id_mappings_string = serde_json::to_string(id_mappings.links.as_ref()).unwrap();
        fs::write(
            config.output_dir.to_owned() + "/id_mappings.json",
            id_mappings_string,
        )
        .expect("Unable to write file");
        info!("Written id mappings file!");
    }

    let population = Population::from_io(
        &io_population,
        &id_mappings,
        rank as usize,
        &network,
        config.routing_mode,
    );

    let mut vehicle_definitions: Option<VehicleDefinitions> = None;
    if let Some(vehicle_definitions_file_path) = &config.vehicle_definitions_file {
        let io_vehicle_definitions =
            IOVehicleDefinitions::from_file(vehicle_definitions_file_path.as_ref());
        vehicle_definitions = Some(VehicleDefinitions::from_io(io_vehicle_definitions));
    }

    let routing_kit_network_by_mode =
        TravelTimesCollectingRoadRouter::get_routing_kit_network_by_mode(
            &network,
            vehicle_definitions.as_ref(),
        );

    let network_partition = network.partitions.remove(rank as usize);
    info!(
        "Partition #{rank} network has: {} nodes and {} links. Population has {} agents",
        network_partition.len_local_nodes(),
        network_partition.links.len(),
        population.agents.len()
    );

    let neighbors = network_partition
        .neighbors()
        .iter()
        // cast this here. change the api to not use usize all the time, since with mpi and protobuf
        // we have to use u32 or u64.
        .map(|u| *u as u32)
        .collect();
    let link_id_mapping = network.links_2_partition;

    let message_broker = MpiMessageBroker::new(world.clone(), rank, neighbors, link_id_mapping);
    let mut events = EventsPublisher::new();

    let events_file = format!("events.{rank}.pbf");
    let events_path = output_path.join(events_file);
    events.add_subscriber(Box::new(ProtoEventsWriter::new(&events_path)));
    let travel_time_collector = Box::new(TravelTimeCollector::new());
    events.add_subscriber(travel_time_collector);
    //events.add_subscriber(Box::new(EventsLogger {}));

    let mut router: Option<Box<dyn Router>> = None;
    let mut walk_leg_finder: Option<Box<dyn WalkLegUpdater>> = None;
    if config.routing_mode == RoutingMode::AdHoc {
        router = Some(Box::new(TravelTimesCollectingRoadRouter::new(
            routing_kit_network_by_mode,
            world.clone(),
            rank,
            get_temp_output_folder(&output_path, rank),
            network_partition.get_link_ids(),
        )));

        let walking_speed_in_m_per_sec = 1.2;
        walk_leg_finder = Some(Box::new(EuclideanWalkLegUpdater::new(
            walking_speed_in_m_per_sec,
        )))
    }

    let local_links = network_partition.len_local_links();
    let split_in_links = network_partition.len_split_in_links();
    let split_out_links = network_partition.len_split_out_links();
    let neighbors = network_partition
        .neighbors()
        .iter()
        .map(|&n| n as u64)
        .collect::<Vec<u64>>();

    let mut simulation = Simulation::new(
        &config,
        &id_mappings,
        network_partition,
        population,
        message_broker,
        events,
        router,
        vehicle_definitions,
        walk_leg_finder,
    );

    let metadata = json!({
        "localLinks": local_links,
        "splitInLinks": split_in_links,
        "splitOutLinks": split_out_links,
        "neighbors": neighbors
    });

    let start = Instant::now();
    measure_duration(None, "simulation", Some(metadata), || {
        simulation.run(config.start_time, config.end_time)
    });
    let end = Instant::now();
    let duration = end.sub(start).as_millis() / 1000;
    info!("#{rank} took: {duration}s");

    info!("output dir: {:?}", config.output_dir);

    if rank == 0 && config.routing_mode == RoutingMode::AdHoc {
        remove_dir_all(output_path.join("routing"))
            .expect("Wasn't able to delete temporary routing output.")
    }

    info!("#{rank} at barrier.");
    world.barrier();
    info!("Process #{rank} finishing.");
}

fn get_temp_output_folder(output_dir: &PathBuf, rank: c_int) -> PathBuf {
    output_dir.join("routing").join(format!("{}", rank))
}
