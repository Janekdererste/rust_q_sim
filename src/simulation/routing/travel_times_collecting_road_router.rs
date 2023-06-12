use crate::simulation::io::vehicle_definitions::VehicleDefinitions;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::messaging::messages::proto::{Plan, TravelTimesMessage};
use crate::simulation::messaging::travel_time_collector::TravelTimeCollector;
use crate::simulation::network::network::Network;
use crate::simulation::network::routing_kit_network::RoutingKitNetwork;
use crate::simulation::performance_profiling::measure_duration;
use crate::simulation::routing::network_converter::NetworkConverter;
use crate::simulation::routing::road_router::RoadRouter;
use crate::simulation::routing::router::{CustomQueryResult, Router};
use crate::simulation::routing::travel_times_message_broker::TravelTimesMessageBroker;
use itertools::Itertools;
use mpi::topology::SystemCommunicator;
use mpi::Rank;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use tracing::debug;

pub struct TravelTimesCollectingRoadRouter<'router> {
    router_by_mode: BTreeMap<String, RoadRouter<'router>>,
    traffic_message_broker: TravelTimesMessageBroker,
    link_ids_of_process: HashSet<u64>,
}

impl<'router> Router for TravelTimesCollectingRoadRouter<'router> {
    fn query_links(&mut self, from_link: u64, to_link: u64, mode: &str) -> CustomQueryResult {
        self.get_router_by_mode(mode)
            .expect(&*format!(
                "There is no router for mode {:?}. Check the vehicle definitions.",
                mode
            ))
            .query_links(from_link, to_link)
    }

    fn next_time_step(&mut self, now: u32, events: &mut EventsPublisher) {
        let traffic_update_interval_in_min = 15;
        if !(now % (60 * traffic_update_interval_in_min) == 0) {
            return;
        }

        let _hour = now / 3600;
        let _min = (now % 3600) / 60;
        debug!(
            "#{:?} Traffic update triggered at {_hour}:{_min}",
            self.traffic_message_broker.rank
        );

        //get travel times
        let collected_travel_times =
            measure_duration(Some(now), "travel_time_aggregating", None, || {
                events
                    .get_subscriber::<TravelTimeCollector>()
                    .map(|travel_time_collector| travel_time_collector.get_travel_times())
                    .unwrap()
            });

        //compute all updates of partition
        let send_package = measure_duration(
            Some(now),
            "travel_time_augmenting",
            Some(json!({"updates": collected_travel_times.len()})),
            || self.get_travel_times_by_mode_to_send(&collected_travel_times),
        );

        self.get_travel_times_by_mode_to_send(&collected_travel_times);

        let received_messages_by_mode = send_package
            .into_iter()
            .map(|(mode, updates)| {
                let number_of_updates = updates.len() as u64;
                let received_messages = measure_duration(
                    Some(now),
                    "travel_time_communicating",
                    Some(json!({ "mode": mode, "updates": number_of_updates })),
                    || self.traffic_message_broker.send_recv(now, updates),
                );
                (mode, received_messages)
            })
            .collect::<BTreeMap<String, Vec<TravelTimesMessage>>>();

        //handle travel times
        for (mode, message) in received_messages_by_mode.into_iter() {
            let number_of_updates: usize = message
                .iter()
                .map(|m| m.travel_times_by_link_id.len())
                .sum();
            measure_duration(
                Some(now),
                "travel_time_handling",
                Some(json!({ "mode": mode, "updates": number_of_updates })),
                || {
                    self.handle_traffic_info_messages(now, mode, message);
                },
            );
        }

        //reset travel times
        events
            .get_subscriber::<TravelTimeCollector>()
            .expect("There is no TravelTimeCollector as EventSubscriber.")
            .flush();
    }
}

impl<'router> TravelTimesCollectingRoadRouter<'router> {
    pub fn new(
        routing_kit_network_by_mode: HashMap<String, RoutingKitNetwork>,
        communicator: SystemCommunicator,
        rank: Rank,
        output_dir: PathBuf,
        link_ids_of_process: HashSet<u64>,
    ) -> Self {
        let router_by_mode = routing_kit_network_by_mode
            .iter()
            .map(|(m, r)| (m.clone(), RoadRouter::new(r, output_dir.join(m))))
            .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
            .collect::<BTreeMap<_, _>>();

        TravelTimesCollectingRoadRouter {
            router_by_mode,
            traffic_message_broker: TravelTimesMessageBroker::new(communicator, rank),
            link_ids_of_process,
        }
    }

    fn handle_traffic_info_messages(
        &mut self,
        now: u32,
        mode: String,
        traffic_info_messages: Vec<TravelTimesMessage>,
    ) {
        if traffic_info_messages.is_empty() {
            return;
        }

        let travel_times_by_link = traffic_info_messages
            .iter()
            .map(|info| &info.travel_times_by_link_id)
            .fold(HashMap::new(), |result, value| {
                result.into_iter().chain(value).collect()
            });

        let number_of_links_with_traffic_info = traffic_info_messages
            .iter()
            .map(|info| info.travel_times_by_link_id.len())
            .sum::<usize>();

        assert_eq!(
            number_of_links_with_traffic_info,
            travel_times_by_link.len()
        );

        let new_network = self
            .router_by_mode
            .get(&*mode)
            .unwrap()
            .get_current_network()
            .clone_with_new_travel_times_by_link(travel_times_by_link);

        measure_duration(
            Some(now),
            "router_customization",
            Some(json!({ "mode": mode })),
            || {
                self.router_by_mode
                    .get_mut(&*mode)
                    .unwrap()
                    .customize(new_network);
            },
        )
    }

    fn get_router_by_mode(&mut self, mode: &str) -> Option<&mut RoadRouter<'router>> {
        self.router_by_mode.get_mut(mode)
    }

    fn get_travel_times_by_mode_to_send(
        &mut self,
        collected_travel_times: &HashMap<u64, u32>,
    ) -> BTreeMap<String, HashMap<u64, u32>> {
        let mut result = BTreeMap::new();
        for (mode, router) in self.router_by_mode.iter_mut() {
            let mut extended_travel_times_by_link_id = HashMap::new();
            for id in &self.link_ids_of_process {
                if let Some(travel_time) = collected_travel_times.get(&id) {
                    // for each collected travel time: add if currently known travel time is different
                    let new_travel_time = (*travel_time).max(router.get_initial_travel_time(*id));
                    if new_travel_time != router.get_current_travel_time(*id) {
                        extended_travel_times_by_link_id.insert(*id, new_travel_time);
                    }
                } else {
                    // for each link which has no new travel time: add initial travel time if currently known travel time is different
                    let initial_travel_time = router.get_initial_travel_time(*id);
                    if router.get_current_travel_time(*id) != initial_travel_time {
                        extended_travel_times_by_link_id.insert(*id, initial_travel_time);
                    }
                }
            }
            result.insert(String::from(mode), extended_travel_times_by_link_id);
        }
        result
    }

    pub fn get_routing_kit_network_by_mode(
        network: &Network,
        vehicle_definitions: Option<&VehicleDefinitions>,
    ) -> HashMap<String, RoutingKitNetwork> {
        if let Some(vehicle_definitions) = vehicle_definitions {
            NetworkConverter::convert_network_with_vehicle_definitions(network, vehicle_definitions)
        } else {
            let mut map = HashMap::new();
            map.insert(
                Plan::DEFAULT_ROUTING_MODE.to_string(),
                NetworkConverter::convert_network(network, None, None),
            );
            map
        }
    }
}
