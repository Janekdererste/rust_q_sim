use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;

use nohash_hasher::IntMap;
use tracing::{debug, info};

use crate::simulation::id::Id;
use crate::simulation::messaging::communication::communicators::SimCommunicator;
use crate::simulation::messaging::communication::message_broker::TravelTimesMessageBroker;
use crate::simulation::messaging::events::EventsPublisher;
use crate::simulation::network::global_network::Network;
use crate::simulation::replanning::routing::alt_router::AltRouter;
use crate::simulation::replanning::routing::graph::ForwardBackwardGraph;
use crate::simulation::replanning::routing::network_converter::NetworkConverter;
use crate::simulation::replanning::routing::router::{CustomQueryResult, NetworkRouter};
use crate::simulation::replanning::routing::travel_time_collector::TravelTimeCollector;
use crate::simulation::wire_types::messages::TravelTimesMessage;
use crate::simulation::wire_types::vehicles::VehicleType;

pub struct TravelTimesCollectingAltRouter<C: SimCommunicator> {
    router_by_veh_type: BTreeMap<Id<VehicleType>, AltRouter>,
    traffic_message_broker: TravelTimesMessageBroker<C>,
    link_ids_of_process: HashSet<u64>,
}

impl<C: SimCommunicator> Debug for TravelTimesCollectingAltRouter<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TravelTimesCollectingAltRouter")
    }
}

impl<C: SimCommunicator> NetworkRouter for TravelTimesCollectingAltRouter<C> {
    fn query_links(
        &self,
        from_link: u64,
        to_link: u64,
        veh_type_id: &Id<VehicleType>,
    ) -> CustomQueryResult {
        self.get_router_by_mode(veh_type_id)
            .unwrap_or_else(|| {
                panic!(
                    "There is no router for mode {:?}. Check the vehicle definitions.",
                    veh_type_id
                )
            })
            .query_links(from_link, to_link)
    }

    fn next_time_step(&mut self, now: u32, events: &mut EventsPublisher) {
        let traffic_update_interval_in_min = 15;
        if now % (60 * traffic_update_interval_in_min) != 0 {
            return;
        }

        let _hour = now / 3600;
        let _min = (now % 3600) / 60;
        debug!(
            "#{:?} Traffic update triggered at {_hour}:{_min}",
            self.traffic_message_broker.rank()
        );

        //get travel times
        let collected_travel_times = Self::collect_travel_times(events, now);

        //compute all updates of partition
        let send_package = self.get_travel_times_by_mode_to_send(&collected_travel_times, now);

        let received_messages_by_veh_type_id = send_package
            .into_iter()
            .map(|(mode, updates)| {
                let received_messages = self.communicate_travel_times(now, updates);
                (mode, received_messages)
            })
            .collect::<BTreeMap<u64, Vec<TravelTimesMessage>>>();

        //handle travel times
        for (veh_type_id, message) in received_messages_by_veh_type_id.into_iter() {
            self.handle_traffic_info_messages(now, veh_type_id, message);
        }

        //reset travel times
        events
            .get_subscriber::<TravelTimeCollector>()
            .expect("There is no TravelTimeCollector as EventSubscriber.")
            .flush();
    }
}

impl<C: SimCommunicator> TravelTimesCollectingAltRouter<C> {
    #[tracing::instrument(level = "trace", skip(events))]
    fn collect_travel_times(events: &mut EventsPublisher, _now: u32) -> HashMap<u64, u32> {
        events
            .get_subscriber::<TravelTimeCollector>()
            .map(|travel_time_collector| travel_time_collector.get_travel_times())
            .unwrap()
    }

    #[tracing::instrument(level = "trace", skip(updates))]
    fn communicate_travel_times(
        &mut self,
        now: u32,
        updates: HashMap<u64, u32>,
    ) -> Vec<TravelTimesMessage> {
        self.traffic_message_broker.send_recv(now, updates)
    }
}

impl<C: SimCommunicator> TravelTimesCollectingAltRouter<C> {
    pub fn new(
        forward_backward_graph_by_mode: IntMap<Id<VehicleType>, ForwardBackwardGraph>,
        communicator: Rc<C>,
        link_ids_of_process: HashSet<u64>,
    ) -> Self {
        let router_by_vehicle_type = forward_backward_graph_by_mode
            .iter()
            .map(|(m, g)| (m.clone(), AltRouter::new(g.clone())))
            .collect::<BTreeMap<_, _>>();

        info!(
            "Created TravelTimesCollectingAltRouter with vehicle types: {:?}",
            router_by_vehicle_type
                .keys()
                .map(|id| (id.internal(), id.external()))
                .collect::<BTreeMap<u64, &str>>()
        );

        TravelTimesCollectingAltRouter {
            router_by_veh_type: router_by_vehicle_type,
            traffic_message_broker: TravelTimesMessageBroker::new(communicator),
            link_ids_of_process,
        }
    }

    #[tracing::instrument(level = "trace", skip(veh_type_id_internal, traffic_info_messages))]
    fn handle_traffic_info_messages(
        &mut self,
        _now: u32,
        veh_type_id_internal: u64,
        traffic_info_messages: Vec<TravelTimesMessage>,
    ) {
        if traffic_info_messages.is_empty() {
            return;
        }

        let travel_times_by_link: IntMap<u64, u32> = traffic_info_messages
            .into_iter()
            .flat_map(|info| info.travel_times_by_link_id.into_iter())
            .collect();

        // let number_of_links_with_traffic_info = traffic_info_messages
        //     .iter()
        //     .map(|info| info.travel_times_by_link_id.len())
        //     .sum::<usize>();

        // debug_assert_eq!(
        //     number_of_links_with_traffic_info,
        //     travel_times_by_link.len()
        // );

        let veh_type_id = Id::<VehicleType>::get(veh_type_id_internal);

        self.router_by_veh_type
            .get_mut(&veh_type_id)
            .unwrap()
            .current_graph_mut()
            .insert_new_travel_times_by_link(travel_times_by_link);
    }

    fn get_router_by_mode(&self, veh_type_id: &Id<VehicleType>) -> Option<&AltRouter> {
        self.router_by_veh_type.get(veh_type_id)
    }

    #[tracing::instrument(level = "trace", skip(self, collected_travel_times))]
    fn get_travel_times_by_mode_to_send(
        &mut self,
        collected_travel_times: &HashMap<u64, u32>,
        _now: u32,
    ) -> BTreeMap<u64, HashMap<u64, u32>> {
        let mut result = BTreeMap::new();
        for (mode, router) in self.router_by_veh_type.iter_mut() {
            let mut extended_travel_times_by_link_id = HashMap::new();
            for id in &self.link_ids_of_process {
                if let Some(travel_time) = collected_travel_times.get(id) {
                    // add collected travel time
                    let initial = router.get_initial_travel_time(*id);

                    if initial.is_none() {
                        continue;
                    }

                    let new_travel_time = (*travel_time).max(initial.unwrap());
                    let current_travel_time = router.get_current_travel_time(*id).expect("If there is an initial travel time, there also must be a current travel time");
                    if new_travel_time != current_travel_time {
                        extended_travel_times_by_link_id.insert(*id, new_travel_time);
                    }
                } else {
                    // add initial travel time for each link which has no new travel time
                    let initial = router.get_initial_travel_time(*id);

                    if initial.is_none() {
                        continue;
                    }

                    if router.get_current_travel_time(*id) != initial {
                        extended_travel_times_by_link_id.insert(*id, initial.unwrap());
                    }
                }
            }
            result.insert(mode.internal(), extended_travel_times_by_link_id);
        }
        result
    }

    pub fn get_forward_backward_graph_by_veh_type(
        network: &Network,
        vehicle_types: &IntMap<Id<VehicleType>, VehicleType>,
    ) -> IntMap<Id<VehicleType>, ForwardBackwardGraph> {
        NetworkConverter::convert_network_with_vehicle_types(network, vehicle_types)
    }
}
