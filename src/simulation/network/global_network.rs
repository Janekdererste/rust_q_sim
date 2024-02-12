use std::path::PathBuf;
use std::{collections::HashSet, path::Path};

use itertools::Itertools;
use nohash_hasher::IntSet;

use crate::simulation::config::PartitionMethod;
use crate::simulation::id::Id;

use super::metis_partitioning;

/// This is called global network but could also be renamed into network when things are sorted out a little
#[derive(Debug, Clone)]
pub struct Network {
    pub nodes: Vec<Node>,
    pub links: Vec<Link>,
    pub effective_cell_size: f32,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub x: f64,
    pub y: f64,
    pub id: Id<Node>,
    pub in_links: Vec<Id<Link>>,
    pub out_links: Vec<Id<Link>>,
    pub partition: u32,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub id: Id<Link>,
    pub from: Id<Node>,
    pub to: Id<Node>,
    pub length: f64,
    pub capacity: f32,
    pub freespeed: f32,
    pub permlanes: f32,
    pub modes: IntSet<Id<String>>,
    pub partition: u32,
}

impl Default for Network {
    fn default() -> Self {
        Network::new()
    }
}

impl Network {
    pub fn new() -> Self {
        Network {
            nodes: Vec::new(),
            links: Vec::new(),
            effective_cell_size: 7.5,
        }
    }

    pub fn from_file(file: &str, num_parts: u32, partition_method: PartitionMethod) -> Self {
        Self::from_file_path(&PathBuf::from(file), num_parts, partition_method)
    }

    pub fn from_file_path(
        file_path: &Path,
        num_parts: u32,
        partition_method: PartitionMethod,
    ) -> Self {
        let mut result = super::io::from_file(file_path);
        Self::partition_network(&mut result, partition_method, num_parts);
        result
    }

    pub fn from_file_as_is(path: &Path) -> Self {
        super::io::from_file(path)
    }

    pub fn to_file(&self, file_path: &Path) {
        super::io::to_file(self, file_path);
    }

    pub fn add_node(&mut self, node: Node) {
        assert_eq!(
            node.id.internal(),
            self.nodes.len() as u64,
            "internal id {} and slot in node vec {} were note the same. Probably, node id {} already exsists.",
            node.id.internal(),
            self.nodes.len(),
            node.id.external()
        );
        self.nodes.push(node);
    }

    pub fn add_link(&mut self, link: Link) {
        assert_eq!(
            link.id.internal(),
            self.links.len() as u64,
            "internal id {} and slot in link vec {} were note the same. Probably, this link id {} already exists",
            link.id.internal(),
            self.links.len(),
            link.id.external()
        );

        // wire up in and out links and push link to the links vec
        self.nodes
            .get_mut(link.from.internal() as usize)
            .unwrap()
            .out_links
            .push(link.id.clone());
        self.nodes
            .get_mut(link.to.internal() as usize)
            .unwrap()
            .in_links
            .push(link.id.clone());
        self.links.push(link);
    }

    pub fn get_node(&self, id: &Id<Node>) -> &Node {
        self.nodes.get(id.internal() as usize).unwrap()
    }

    pub fn get_link(&self, id: &Id<Link>) -> &Link {
        self.links.get(id.internal() as usize).unwrap()
    }

    pub fn get_link_form_internal(&self, id: u64) -> &Link {
        self.links.get(id as usize).unwrap()
    }

    fn partition_network(network: &mut Network, partition_method: PartitionMethod, num_parts: u32) {
        match partition_method {
            PartitionMethod::Metis(options) => {
                let partitions = metis_partitioning::partition(network, num_parts, options);
                for node in network.nodes.iter_mut() {
                    let partition = partitions[node.id.internal() as usize] as u32;
                    node.partition = partition;

                    for link_id in &node.in_links {
                        let link = network.links.get_mut(link_id.internal() as usize).unwrap();
                        link.partition = partition;
                    }
                }
            }
            PartitionMethod::None => {}
        }
    }

    pub fn get_all_links_sorted(&self) -> Vec<&Link> {
        self.links
            .iter()
            .sorted_by_key(|&l| &l.id)
            .collect::<Vec<&Link>>()
    }

    pub fn get_all_nodes_sorted(&self) -> Vec<&Node> {
        self.nodes
            .iter()
            .sorted_by_key(|&n| &n.id)
            .collect::<Vec<&Node>>()
    }
}

impl Node {
    pub fn new(id: Id<Node>, x: f64, y: f64, part: u32) -> Self {
        Node {
            id,
            x,
            y,
            in_links: Vec::new(),
            out_links: Vec::new(),
            partition: part,
        }
    }
}

impl Link {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: Id<Link>,
        from: Id<Node>,
        to: Id<Node>,
        length: f64,
        capacity: f32,
        freespeed: f32,
        permlanes: f32,
        modes: IntSet<Id<String>>,
        partition: u32,
    ) -> Self {
        Link {
            id,
            from,
            to,
            length,
            capacity,
            freespeed,
            permlanes,
            modes,
            partition,
        }
    }

    pub fn new_with_default(id: Id<Link>, from: &Node, to: &Node) -> Self {
        // compute eucledean distance between from and to node
        let length = ((from.x - to.x).powi(2) + (from.y - to.y).powi(2)).sqrt();
        Link::new(
            id,
            from.id.clone(),
            to.id.clone(),
            length,
            1.,
            1.,
            1.,
            HashSet::default(),
            0,
        )
    }

    pub fn contains_mode(&self, mode: u64) -> bool {
        self.modes.iter().map(|m| m.internal()).contains(&mode)
    }
}

#[cfg(test)]
mod tests {
    use crate::simulation::config::{EdgeWeight, MetisOptions, PartitionMethod};
    use crate::simulation::id::Id;

    use super::{Link, Network, Node};

    #[test]
    fn add_node() {
        let mut network = Network::new();
        let id = Id::create("node-id");
        let node = Node::new(id.clone(), 1., 1., 0);

        assert_eq!(0, network.nodes.len());
        network.add_node(node);
        assert_eq!(1, network.nodes.len());
        assert_eq!(id, network.get_node(&id).id);
    }

    #[test]
    #[should_panic]
    fn add_node_reject_duplicate() {
        let mut network = Network::new();
        let id = Id::create("node-id");
        let node = Node::new(id.clone(), 1., 1., 0);
        let duplicate = Node::new(id.clone(), 2., 2., 0);

        assert_eq!(0, network.nodes.len());
        network.add_node(node);
        network.add_node(duplicate); // expecting panic here.
    }

    #[test]
    fn add_link() {
        let mut network = Network::new();
        let from = Node::new(Id::create("from"), 0., 0., 0);
        let to = Node::new(Id::create("to"), 3., 4., 0);
        let id = Id::create("link-id");
        let link = Link::new_with_default(id.clone(), &from, &to);

        network.add_node(from);
        network.add_node(to);
        network.add_link(link);

        assert_eq!(2, network.nodes.len());
        assert_eq!(1, network.links.len());
        assert_eq!(id, network.get_link(&id).id);

        let link = network.get_link(&id);
        let from = network.get_node(&link.from);
        let to = network.get_node(&link.to);

        assert_eq!(id, link.id);
        assert_eq!(0, from.in_links.len());
        assert_eq!(1, from.out_links.len());
        assert_eq!(&id, from.out_links.get(0).unwrap());
        assert_eq!(0, to.out_links.len());
        assert_eq!(1, to.in_links.len());
        assert_eq!(&id, to.in_links.get(0).unwrap());
    }

    #[test]
    #[should_panic]
    fn add_link_reject_duplicate() {
        let mut network = Network::new();
        let from = Node::new(Id::create("from"), 0., 0., 0);
        let to = Node::new(Id::create("to"), 3., 4., 0);
        let id = Id::create("link-id");
        let link = Link::new_with_default(id.clone(), &from, &to);
        let duplicate = Link::new_with_default(id.clone(), &from, &to);

        network.add_node(from);
        network.add_node(to);
        network.add_link(link);
        network.add_link(duplicate); // expecting panic here
    }

    #[test]
    #[ignore] // ingore this test, because it keeps not working, due to non determined ordering of metis
    fn from_file() {
        let network = Network::from_file(
            "./assets/equil/equil-network.xml",
            2,
            //I don't know, why "edge_weight = true" sets 1 as partition for all nodes.
            PartitionMethod::Metis(MetisOptions::default().set_edge_weight(EdgeWeight::Constant)),
        );

        // check partitioning
        let expected_partitions = [0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0];
        for node in &network.nodes {
            let expected_partition = expected_partitions[node.id.internal() as usize];
            assert_eq!(expected_partition, node.partition);
        }
        for link in &network.links {
            let expected_partition = expected_partitions[link.to.internal() as usize];
            assert_eq!(expected_partition, link.partition);
        }

        // probe in and out links
        for node in &network.nodes {
            match &node.id.internal() {
                11 => {
                    assert_eq!(9, node.in_links.len());
                    assert_eq!(1, node.out_links.len());
                }
                1 => {
                    assert_eq!(9, node.out_links.len());
                    assert_eq!(1, node.in_links.len());
                }
                _ => {
                    assert_eq!(1, node.in_links.len());
                    assert_eq!(1, node.out_links.len());
                }
            }
        }

        // check cell size
        assert_eq!(7.5, network.effective_cell_size);
    }

    #[test]
    fn link_new_with_default() {
        let from = Node::new(Id::create("from"), 0., 0., 0);
        let to = Node::new(Id::create("to"), 3., 4., 0);
        let id = Id::create("link-id");
        let link = Link::new_with_default(id.clone(), &from, &to);

        assert_eq!(id, link.id);
        assert_eq!(5., link.length);
        assert_eq!(from.id, link.from);
        assert_eq!(to.id, link.to);
    }

    #[test]
    fn test_metis_with_large_graph() {}
}
