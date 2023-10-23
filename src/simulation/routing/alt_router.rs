use crate::simulation::routing::alt_landmark_data::AltLandmarkData;
use crate::simulation::routing::graph::ForwardBackwardGraph;
use crate::simulation::routing::router::CustomQueryResult;

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

    pub fn query_links(&mut self, from_link: u64, to_link: u64) -> CustomQueryResult {
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

    fn query(&mut self, from: usize, to: usize) -> AltQueryResult {
        let number_of_nodes = self.current_graph.forward_first_out().len() - 1;

        let mut distances: Vec<u32> = (0..number_of_nodes).map(|_| u32::MAX).collect();
        let mut f_score: Vec<u32> = (0..number_of_nodes).map(|_| u32::MAX).collect();
        let mut parents: Vec<Option<usize>> = (0..number_of_nodes).map(|_| None).collect();

        let mut traversed: Vec<bool> = (0..number_of_nodes).map(|_| false).collect();

        //update start node
        f_score[from] = 0;
        distances[from] = 0;

        while let Some(current) = Self::get_next_node(&mut f_score, &mut traversed) {
            let current_id = current.0;
            let current_distance = distances[current_id];

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

                if traversed[neighbour] {
                    continue;
                }

                let neighbour_distance =
                    current_distance + self.current_graph.forward_graph.travel_time[i];

                if distances[neighbour] > neighbour_distance {
                    //perform update
                    distances[neighbour] = neighbour_distance;
                    f_score[neighbour] =
                        neighbour_distance + Self::heuristic(neighbour, to, &self.landmark_data);
                    parents[neighbour] = Some(current_id);
                }
            }
            traversed[current_id] = true;
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
            let node_distance = l[node];
            let goal_distance = l[goal];

            let forward_estimate = node_distance.0 as i32 - goal_distance.0 as i32;
            let backward_estimate = goal_distance.1 as i32 - node_distance.1 as i32;

            h = h.max(forward_estimate.max(backward_estimate))
        }
        if h < 0 {
            0
        } else {
            h as u32
        }
    }

    fn get_next_node<'a>(
        travel_times: &'a Vec<u32>,
        traversed: &'a Vec<bool>,
    ) -> Option<(usize, (&'a u32, &'a bool))> {
        let result = travel_times
            .iter()
            .zip(traversed.iter())
            .enumerate()
            .filter(|(_, (_, &t))| !t)
            .min_by(|a, b| a.1 .0.cmp(b.1 .0));

        if result.is_none() {
            return None;
        }

        if result.map(|(_, (t, _))| t).unwrap() >= &u32::MAX {
            return None;
        }

        result
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
        let link_id_index = self
            .current_graph
            .forward_link_ids()
            .iter()
            .position(|&id| id == link_id)
            .unwrap();
        *self
            .current_graph
            .forward_head()
            .get(link_id_index)
            .unwrap() as usize
    }

    fn get_start_node(&self, link_id: u64) -> usize {
        let link_id_index = self
            .current_graph
            .forward_link_ids()
            .iter()
            .position(|&id| id == link_id)
            .unwrap();

        let mut result = None;
        for i in 0..self.current_graph.forward_first_out().len() {
            if link_id_index >= *self.current_graph.forward_first_out().get(i).unwrap() as usize
                && link_id_index
                    < *self.current_graph.forward_first_out().get(i + 1).unwrap() as usize
            {
                result = Some(i as usize);
            }
        }

        result.unwrap()
    }

    pub fn current_graph(&self) -> &ForwardBackwardGraph {
        &self.current_graph
    }

    pub fn get_initial_travel_time(&self, link_id: u64) -> u32 {
        self.initial_graph
            .get_forward_travel_time_by_link_id(link_id)
    }

    pub fn get_current_travel_time(&self, link_id: u64) -> u32 {
        self.current_graph
            .get_forward_travel_time_by_link_id(link_id)
    }

    fn get_edge_path(path: Vec<usize>, graph: &ForwardBackwardGraph) -> Vec<u64> {
        let mut res = Vec::new();
        let mut last_node: Option<usize> = None;
        for node in path {
            match last_node {
                None => last_node = Some(node as usize),
                Some(n) => {
                    let first_out_index = *graph.forward_first_out().get(n).unwrap() as usize;
                    let last_out_index =
                        (graph.forward_first_out().get(n + 1).unwrap() - 1) as usize;
                    res.push(Self::find_edge_id_of_outgoing(
                        first_out_index,
                        last_out_index,
                        node,
                        graph,
                    ));
                    last_node = Some(node as usize)
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
                result = Some(graph.forward_link_ids().get(i).unwrap().clone());
                break;
            }
        }
        result.expect("No outgoing edge found!") as u64
    }
}
