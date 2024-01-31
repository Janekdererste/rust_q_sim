use keyed_priority_queue::Entry;

use crate::simulation::replanning::routing::alt_landmark_data::AltLandmarkData;
use crate::simulation::replanning::routing::dijsktra::{Dijkstra, Distance};
use crate::simulation::replanning::routing::graph::ForwardBackwardGraph;
use crate::simulation::replanning::routing::router::CustomQueryResult;

#[derive(PartialEq, Debug)]
struct AltQueryResult {
    travel_time: Option<u32>,
    node_path: Option<Vec<usize>>,
}

impl AltQueryResult {
    fn empty() -> Self {
        Self {
            travel_time: None,
            node_path: None,
        }
    }

    fn node_path(self) -> Option<Vec<usize>> {
        self.node_path
    }
}

pub struct AltRouter {
    pub landmark_data: AltLandmarkData,
    pub current_graph: ForwardBackwardGraph,
    pub initial_graph: ForwardBackwardGraph,
}

impl AltRouter {
    pub fn new(graph: ForwardBackwardGraph) -> Self {
        let landmark_data = AltLandmarkData::new(&graph);
        AltRouter {
            landmark_data,
            current_graph: graph.clone(),
            initial_graph: graph,
        }
    }

    pub fn query_links(&self, from_link: u64, to_link: u64) -> CustomQueryResult {
        let travel_time;
        let result_edge_path;
        {
            let result = self.query(self.get_end_node(from_link), self.get_start_node(to_link));
            travel_time = result.travel_time;
            result_edge_path = result.node_path();
        }
        let edge_path = result_edge_path
            .map(|node_path| Self::get_edge_path(node_path, &self.current_graph))
            .map(|mut path| {
                //add from link at the beginning and to link at the end
                path.insert(0, from_link);
                path.push(to_link);
                path
            });

        CustomQueryResult {
            travel_time,
            path: edge_path,
        }
    }

    fn query(&self, from: usize, to: usize) -> AltQueryResult {
        let number_of_nodes = self.current_graph.forward_first_out().len() - 1;
        let (mut queue, mut distances) = Dijkstra::get_initial_queue(number_of_nodes, from);
        let mut parents: Vec<Option<usize>> = (0..number_of_nodes).map(|_| None).collect();

        while let Some((current_id, _)) = queue.pop() {
            let current_distance = distances[current_id];

            if current_distance == u32::MAX {
                //The smallest value in queue was unreachable. So abort here.
                return AltQueryResult::empty();
            }

            if current_id == to {
                return AltQueryResult {
                    travel_time: Some(current_distance),
                    node_path: Some(Self::extract_path(to, parents)),
                };
            }

            let begin_index_adjacent_nodes = self.current_graph.forward_graph.first_out[current_id];
            let end_index_adjacent_nodes =
                self.current_graph.forward_graph.first_out[current_id + 1];

            for i in begin_index_adjacent_nodes..end_index_adjacent_nodes {
                //we need an update_or_insert + parent update here instead of push always.
                let neighbour = self.current_graph.forward_graph.head[i];

                if let Entry::Vacant(_) = queue.entry(neighbour) {
                    continue;
                }

                let neighbour_distance =
                    current_distance + self.current_graph.forward_graph.travel_time[i];

                if distances[neighbour] > neighbour_distance {
                    //perform update
                    distances[neighbour] = neighbour_distance;

                    match queue.entry(neighbour) {
                        Entry::Occupied(e) => {
                            e.set_priority(Distance(
                                neighbour_distance
                                    + Self::heuristic(neighbour, to, &self.landmark_data),
                            ));
                        }
                        Entry::Vacant(_) => {
                            unreachable!()
                        }
                    }

                    parents[neighbour] = Some(current_id);
                }
            }
        }
        AltQueryResult::empty()
    }

    fn heuristic(node: usize, goal: usize, landmark_data: &AltLandmarkData) -> u32 {
        /* The ALT algorithm uses two lower bounds for each Landmark:
         * given: source node S, target node T, landmark L
         * then, due to the triangle inequality:
         *  1) ST + TL >= SL --> ST >= SL - TL (forward estimate)
         *  2) LS + ST >= LT --> ST >= LT - LS (backward estimate)
         * The algorithm is interested in the largest possible value of (SL-TL) and (LT-LS),
         * as this gives the closest approximation for the minimal travel time required to
         * go from S to T.
         */
        let mut h = 0;
        for l in landmark_data.travel_times_to_all() {
            let node_distance = l[node]; // (SL,LS)
            let goal_distance = l[goal]; // (LT,TL)

            let forward_estimate = node_distance.0 as i32 - goal_distance.1 as i32;
            let backward_estimate = goal_distance.0 as i32 - node_distance.1 as i32;

            h = h.max(forward_estimate.max(backward_estimate))
        }
        if h < 0 {
            0
        } else {
            h as u32
        }
    }

    fn extract_path(to: usize, parent: Vec<Option<usize>>) -> Vec<usize> {
        let mut path = Vec::new();
        let mut current = to;

        path.push(to);
        while let Some(father) = parent[current] {
            path.push(father);
            current = father;
        }

        path.reverse();
        path
    }

    pub fn update(&mut self, new_graph: ForwardBackwardGraph) {
        self.current_graph = new_graph;
    }

    fn get_end_node(&self, link_id: u64) -> usize {
        let link_id_index = *self
            .current_graph
            .forward_link_id_pos()
            .get(&link_id)
            .unwrap_or_else(|| {
                panic!(
                    "There is no link with id {} in the current mode graph.",
                    link_id
                )
            });
        *self
            .current_graph
            .forward_head()
            .get(link_id_index)
            .unwrap()
    }

    fn get_start_node(&self, link_id: u64) -> usize {
        let link_id_index = *self
            .current_graph
            .forward_link_id_pos()
            .get(&link_id)
            .unwrap_or_else(|| {
                panic!(
                    "There is no link with id {} in the current mode graph.",
                    link_id
                )
            });

        let mut result = None;
        for i in 0..self.current_graph.forward_first_out().len() {
            if link_id_index >= *self.current_graph.forward_first_out().get(i).unwrap()
                && link_id_index < *self.current_graph.forward_first_out().get(i + 1).unwrap()
            {
                result = Some(i);
            }
        }

        result.unwrap()
    }

    pub fn current_graph_mut(&mut self) -> &mut ForwardBackwardGraph {
        &mut self.current_graph
    }

    pub fn get_initial_travel_time(&self, link_id: u64) -> Option<u32> {
        self.initial_graph
            .get_forward_travel_time_by_link_id(link_id)
    }

    pub fn get_current_travel_time(&self, link_id: u64) -> Option<u32> {
        self.current_graph
            .get_forward_travel_time_by_link_id(link_id)
    }

    fn get_edge_path(path: Vec<usize>, graph: &ForwardBackwardGraph) -> Vec<u64> {
        let mut res = Vec::new();
        let mut last_node: Option<usize> = None;
        for node in path {
            match last_node {
                None => last_node = Some(node),
                Some(n) => {
                    let first_out_index = *graph.forward_first_out().get(n).unwrap();
                    let last_out_index = graph.forward_first_out().get(n + 1).unwrap() - 1;
                    res.push(Self::find_edge_id_of_outgoing(
                        first_out_index,
                        last_out_index,
                        node,
                        graph,
                    ));
                    last_node = Some(node)
                }
            }
        }
        res
    }

    fn find_edge_id_of_outgoing(
        first_out_index: usize,
        last_out_index: usize,
        next_node: usize,
        graph: &ForwardBackwardGraph,
    ) -> u64 {
        assert!(
            last_out_index as i64 - first_out_index as i64 >= 0,
            "No outgoing edges!"
        );
        let mut result = None;
        for i in first_out_index..=last_out_index {
            if *graph.forward_head().get(i).unwrap() == next_node {
                result = Some(*graph.forward_link_ids().get(i).unwrap());
                break;
            }
        }
        result.expect("No outgoing edge found!")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::simulation::config::{MetisOptions, PartitionMethod};
    use crate::simulation::id::Id;
    use crate::simulation::network::global_network::Network;
    use crate::simulation::replanning::routing::alt_router::{AltQueryResult, AltRouter};
    use crate::simulation::replanning::routing::graph::tests::get_triangle_test_graph;
    use crate::simulation::replanning::routing::network_converter::NetworkConverter;
    use crate::simulation::vehicles::garage::Garage;
    use crate::simulation::wire_types::vehicles::VehicleType;

    fn query_and_check(
        router: &AltRouter,
        from: usize,
        to: usize,
        expected_travel_time: Option<u32>,
        expected_path: Option<Vec<usize>>,
    ) {
        let result = router.query(from, to);
        assert_eq!(
            result,
            AltQueryResult {
                travel_time: expected_travel_time,
                node_path: expected_path,
            }
        )
    }

    #[test]
    fn test_simple_alt_routing() {
        let graph = get_triangle_test_graph();
        let router = AltRouter::new(graph);

        query_and_check(&router, 2, 1, Some(6), Some(vec![2, 3, 1]));
        query_and_check(&router, 3, 2, Some(3), Some(vec![3, 1, 2]));
        query_and_check(&router, 2, 3, Some(4), Some(vec![2, 3]));
        query_and_check(&router, 0, 1, None, None);
    }

    #[test]
    fn test_mode_alt_routing() {
        let network = Network::from_file(
            "./assets/adhoc_routing/no_updates/network.xml",
            1,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let garage = Garage::from_file(&PathBuf::from("./assets/adhoc_routing/vehicles.xml"));

        let graph_by_vehicle_type =
            NetworkConverter::convert_network_with_vehicle_types(&network, &garage.vehicle_types);

        let bike_id = &Id::<VehicleType>::get_from_ext("bike");
        let car_id = &Id::<VehicleType>::get_from_ext("car");

        let router_by_vehicle_type = graph_by_vehicle_type
            .into_iter()
            .map(|(id, g)| (id, AltRouter::new(g)))
            .collect::<HashMap<_, _>>();

        // check routing for bike
        query_and_check(
            &router_by_vehicle_type.get(bike_id).unwrap(),
            0,
            5,
            Some(280),
            Some(vec![0, 1, 2, 3, 4, 5]),
        );

        // check routing for car
        query_and_check(
            &router_by_vehicle_type.get(car_id).unwrap(),
            0,
            5,
            Some(120),
            Some(vec![0, 1, 6, 4, 5]),
        )
    }
}
