use rust_road_router::datastr::graph::{EdgeId, NodeId, Weight};
use std::collections::HashMap;
use tracing::debug;

#[derive(Debug, Clone, PartialEq)]
pub struct RoutingKitNetwork {
    //CSR graph representation
    pub(crate) first_out: Vec<EdgeId>,
    pub(crate) head: Vec<NodeId>,
    pub(crate) travel_time: Vec<Weight>,
    pub(crate) link_ids: Vec<u64>,
    pub(crate) x: Vec<f32>,
    pub(crate) y: Vec<f32>,
}

impl RoutingKitNetwork {
    pub(crate) fn new() -> RoutingKitNetwork {
        RoutingKitNetwork {
            first_out: Vec::new(),
            head: Vec::new(),
            travel_time: Vec::new(),
            link_ids: Vec::new(),
            x: Vec::new(),
            y: Vec::new(),
        }
    }

    pub fn get_travel_time_by_link_id(&self, link_id: u64) -> u32 {
        let index = self.link_ids.iter().position(|&l| l == link_id);
        index
            .map(|i| {
                *self
                    .travel_time
                    .get(i)
                    .expect(&*format!("There is no travel time for link {:?}", link_id))
                    as u32
            })
            .unwrap()
    }

    pub(crate) fn clone_with_new_travel_times(&self, travel_times: Vec<u32>) -> RoutingKitNetwork {
        let mut result = self.clone();
        result.travel_time = travel_times;
        result
    }

    pub fn clone_with_new_travel_times_by_link(
        &self,
        new_travel_times_by_link: HashMap<u64, u32>,
    ) -> RoutingKitNetwork {
        let mut new_travel_time_vector = Vec::new();

        assert_eq!(self.link_ids.len(), self.travel_time.len());
        for (index, &id) in self.link_ids.iter().enumerate() {
            if let Some(&new_travel_time) = new_travel_times_by_link.get(&(id as u64)) {
                new_travel_time_vector.push(new_travel_time);
            } else {
                new_travel_time_vector.push(*self.travel_time.get(index).unwrap())
            }
        }

        self.clone_with_new_travel_times(new_travel_time_vector)
    }

    pub fn has_different_travel_times(&self, other: &RoutingKitNetwork) -> bool {
        self.travel_time != other.travel_time
    }
}

#[cfg(test)]
mod test {
    //TODO
}
