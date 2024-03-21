use metis::{Graph, Idx};
use tracing::info;

use crate::simulation::config::{EdgeWeight, MetisOptions, VertexWeight};

use super::global_network::{Link, Network, Node};

pub fn partition(network: &Network, num_parts: u32, options: MetisOptions) -> Vec<Idx> {
    if num_parts <= 1 {
        return vec![0; network.nodes.len()];
    }

    let mut xadj: Vec<Idx> = Vec::from([0]);
    let mut adjncy: Vec<Idx> = Vec::new();
    let mut adjwgt: Vec<Idx> = Vec::new();
    let mut vwgt: Vec<Idx> = Vec::new();
    let mut result: Vec<Idx> = vec![0x00; network.nodes.len()];

    info!("Converting network into Metis format");
    for node in &network.nodes {
        let num_out_links = node.out_links.len() as Idx;
        let num_in_links = node.in_links.len() as Idx;
        let next_adjacency_index = xadj.last().unwrap() + num_out_links + num_in_links;
        xadj.push(next_adjacency_index);

        // Add vertex weights
        add_vwgt(network, &options, &mut vwgt, node);

        for id in &node.out_links {
            let link = &network.links[id.internal() as usize];
            adjncy.push(link.to.internal() as Idx);
            adjwgt.push(get_adjwgt(&options, link) as Idx);
        }

        for id in &node.in_links {
            let link = &network.links[id.internal() as usize];
            adjncy.push(link.from.internal() as Idx);
            adjwgt.push(get_adjwgt(&options, link) as Idx);
        }
    }

    let ncon = if options.vertex_weight.is_empty() {
        1
    } else {
        options.vertex_weight.len() as Idx
    };

    info!("Calling Metis Partitioning Library");
    let mut graph = Graph::new(ncon, num_parts as Idx, &mut xadj, &mut adjncy)
        .set_option(metis::option::UFactor(options.ufactor() as Idx))
        .set_option(metis::option::Seed(4711))
        .set_adjwgt(&mut adjwgt);

    if !vwgt.is_empty() {
        graph = graph.set_vwgt(&mut vwgt);
    }

    graph.part_kway(&mut result).unwrap();

    result
}

fn add_vwgt(network: &Network, options: &MetisOptions, vwgt: &mut Vec<Idx>, node: &Node) {
    for weight in options.vertex_weight.iter() {
        match weight {
            VertexWeight::InLinkCapacity => {
                vwgt.push(
                    node.in_links
                        .iter()
                        .map(|id| network.links[id.internal() as usize].capacity as Idx)
                        .sum(),
                );
            }
            VertexWeight::InLinkCount => {
                vwgt.push(node.in_links.len() as Idx);
            }
            VertexWeight::Constant => {
                vwgt.push(1);
            }
            VertexWeight::PreComputed => {
                vwgt.push(node.cmp_weight as Idx);
            }
        }
    }
}

fn get_adjwgt(options: &MetisOptions, link: &Link) -> f32 {
    match options.edge_weight {
        EdgeWeight::Capacity => link.capacity,
        EdgeWeight::Constant => 1.,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::simulation::config::{MetisOptions, PartitionMethod, VertexWeight};
    use crate::simulation::id::Id;
    use crate::simulation::network::global_network::{Link, Network, Node};
    use crate::simulation::network::metis_partitioning::partition;

    #[test]
    fn simple_graph() {
        let mut net = Network::new();
        let from_id = Id::create("from");
        let to_id = Id::create("to");
        net.add_node(Node::new(from_id.clone(), 0., 0., 0, 1));
        net.add_node(Node::new(to_id.clone(), 100., 0., 0, 1));
        let link_id = Id::create("link");
        net.add_link(Link::new_with_default(
            link_id,
            net.get_node(&from_id),
            net.get_node(&to_id),
        ));

        for _n in 0..100 {
            let _partition_result = partition(&net, 2, MetisOptions::default());
        }
    }

    #[test]
    fn test_andorra_with_default() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        println!("=== Default ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    #[test]
    fn test_andorra_with_capacity() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(
                MetisOptions::default()
                    .add_vertex_weight(VertexWeight::InLinkCapacity)
                    .set_imbalance_factor(0.),
            ),
        );
        println!("=== Capacity ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    #[test]
    fn test_andorra_with_inlinkcount() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(
                MetisOptions::default()
                    .add_vertex_weight(VertexWeight::InLinkCount)
                    .set_imbalance_factor(0.),
            ),
        );
        println!("=== InLinkCount ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    #[test]
    fn test_andorra_with_inlinkcount_and_capacity() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(
                MetisOptions::default()
                    .add_vertex_weight(VertexWeight::InLinkCapacity)
                    .add_vertex_weight(VertexWeight::InLinkCount)
                    .set_imbalance_factor(0.),
            ),
        );
        println!("=== Capacity & InLinkCount ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    #[test]
    fn test_andorra_with_vertex_constant() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(
                MetisOptions::default()
                    .add_vertex_weight(VertexWeight::Constant)
                    .set_imbalance_factor(0.),
            ),
        );
        println!("=== Constant Vertex ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    #[test]
    fn test_andorra_with_vertex_constant_and_inlinkcount() {
        let network = Network::from_file(
            "./assets/andorra-network.xml.gz",
            5,
            PartitionMethod::Metis(
                MetisOptions::default()
                    .add_vertex_weight(VertexWeight::Constant)
                    .add_vertex_weight(VertexWeight::InLinkCount)
                    .set_imbalance_factor(0.)
                    .set_iteration_number(100),
            ),
        );
        println!("=== Constant Vertex & InLinkCount ===");
        let _node_count = node_count(&network);
        let _edge_count = edge_count(network);
    }

    fn node_count(network: &Network) -> BTreeMap<u32, usize> {
        let map = network.nodes.iter().map(|n| n.partition).fold(
            BTreeMap::<u32, usize>::new(),
            |mut m, x| {
                *m.entry(x).or_default() += 1;
                m
            },
        );
        println!("Node count per partition: {:?}", map);
        map
    }

    fn edge_count(network: Network) -> BTreeMap<u32, usize> {
        let map = network.links.iter().map(|l| l.partition).fold(
            BTreeMap::<u32, usize>::new(),
            |mut m, x| {
                *m.entry(x).or_default() += 1;
                m
            },
        );
        println!("Edge count per partition: {:?}", map);
        map
    }
}
