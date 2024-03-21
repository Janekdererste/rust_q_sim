use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use nohash_hasher::IntSet;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::simulation::id::Id;
use crate::simulation::io::attributes::{Attr, Attrs};
use crate::simulation::io::matsim_id::MatsimId;
use crate::simulation::io::xml;
use crate::simulation::network::global_network::{Link, Network, Node};

pub fn from_file(path: &Path) -> Network {
    if path.extension().unwrap().eq("binpb") {
        load_from_proto(path)
    } else if path.extension().unwrap().eq("xml") || path.extension().unwrap().eq("gz") {
        load_from_xml(path)
    } else {
        panic!("Tried to load {path:?}. File format not supported. Either use `.xml`, `.xml.gz`, or `.binpb` as extension");
    }
}

pub fn to_file(network: &Network, path: &Path) {
    if path.extension().unwrap().eq("binpb") {
        write_to_proto(network, path);
    } else if path.extension().unwrap().eq("xml") || path.extension().unwrap().eq("gz") {
        write_to_xml(network, path);
    } else {
        panic!("Tried to write {path:?} . File format not supported. Either use `.xml`, `.xml.gz`, or `.binpb` as extension");
    }
}

fn load_from_xml(path: &Path) -> Network {
    let mut result = Network::new();
    let io_net = IONetwork::from_file(path.to_str().unwrap());

    for io_node in &io_net.nodes.nodes {
        add_io_node(&mut result, io_node);
    }

    for io_link in &io_net.links.links {
        add_io_link(&mut result, io_link);
    }

    result.effective_cell_size = io_net.effective_cell_size();

    result
}

fn write_to_xml(network: &Network, path: &Path) {
    let mut result = IONetwork::new(None);

    for node in &network.nodes {
        let attributes = Attrs {
            attributes: vec![
                Attr {
                    name: "partition".to_string(),
                    value: node.partition.to_string(),
                    class: "java.lang.Integer".to_string(),
                },
                Attr {
                    name: "cmp_weight".to_string(),
                    class: "java.lang.Integer".to_string(),
                    value: node.cmp_weight.to_string(),
                },
            ],
        };
        let io_node = IONode {
            id: node.id.external().to_string(),
            x: node.x,
            y: node.y,
            attributes: Some(attributes),
        };
        result.nodes_mut().push(io_node);
    }

    for link in &network.links {
        let modes = link
            .modes
            .iter()
            .map(|m| m.external().to_string())
            .reduce(|modes, mode| format!("{modes},{mode}"))
            .unwrap();
        let attributes = Attrs {
            attributes: vec![Attr {
                name: String::from("partition"),
                value: link.partition.to_string(),
                class: String::from("java.lang.Integer"),
            }],
        };

        let io_link = IOLink {
            id: link.id.external().to_string(),
            from: link.from.external().to_string(),
            to: link.to.external().to_string(),
            length: link.length,
            capacity: link.capacity,
            freespeed: link.freespeed,
            permlanes: link.permlanes,
            modes,
            attributes: Some(attributes),
        };
        result.links.effective_cell_size = Some(network.effective_cell_size);
        result.links_mut().push(io_link);
    }

    result.to_file(path);
}

fn load_from_proto(path: &Path) -> Network {
    let wire_net: crate::simulation::wire_types::network::Network =
        crate::simulation::io::proto::read_from_file(path);
    let mut result = Network::new();
    result.effective_cell_size = wire_net.effective_cell_size;
    for wn in &wire_net.nodes {
        let node = Node::new(Id::get(wn.id), wn.x, wn.y, wn.partition, wn.cmp_weight);
        result.add_node(node);
    }
    for wl in &wire_net.links {
        let modes: IntSet<Id<String>> = wl.modes.iter().map(|id| Id::get(*id)).collect();

        let link = Link::new(
            Id::get(wl.id),
            Id::get(wl.from),
            Id::get(wl.to),
            wl.length,
            wl.capacity,
            wl.freespeed,
            wl.permlanes,
            modes,
            wl.partition,
        );
        result.add_link(link);
    }
    info!("Finished converting protobuf wire type into Network");
    result
}

fn write_to_proto(network: &Network, path: &Path) {
    info!("Converting Network into wire format");
    let nodes: Vec<_> = network
        .nodes
        .iter()
        .map(|n| crate::simulation::wire_types::network::Node {
            id: n.id.internal(),
            x: n.x,
            y: n.y,
            partition: n.partition,
            cmp_weight: n.cmp_weight,
        })
        .collect();
    let links: Vec<_> = network
        .links
        .iter()
        .map(|l| crate::simulation::wire_types::network::Link {
            id: l.id.internal(),
            from: l.from.internal(),
            to: l.to.internal(),
            length: l.length,
            capacity: l.capacity,
            freespeed: l.freespeed,
            permlanes: l.permlanes,
            modes: l.modes.iter().map(|id| id.internal()).collect(),
            partition: l.partition,
        })
        .collect();

    let wire_network = crate::simulation::wire_types::network::Network {
        nodes,
        links,
        effective_cell_size: network.effective_cell_size,
    };
    crate::simulation::io::proto::write_to_file(wire_network, path);
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct IONode {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub attributes: Option<Attrs>,
}

impl MatsimId for IONode {
    fn id(&self) -> &str {
        self.id.as_str()
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Default, Clone)]
pub struct IOLink {
    pub id: String,
    pub from: String,
    pub to: String,
    pub length: f64,
    pub capacity: f32,
    pub freespeed: f32,
    pub permlanes: f32,
    #[serde(default)]
    pub modes: String,
    pub attributes: Option<Attrs>,
}

impl MatsimId for IOLink {
    fn id(&self) -> &str {
        self.id.as_str()
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
struct Nodes {
    #[serde(rename = "node", default)]
    pub nodes: Vec<IONode>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
struct Links {
    #[serde(rename = "link", default)]
    pub links: Vec<IOLink>,
    #[serde(rename = "effectivecellsize")]
    pub effective_cell_size: Option<f32>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename = "network")]
struct IONetwork {
    pub name: Option<String>,
    pub nodes: Nodes,
    pub links: Links,
}

impl IONetwork {
    pub fn new(name: Option<String>) -> IONetwork {
        IONetwork {
            links: Links {
                links: Vec::new(),
                effective_cell_size: Some(7.5),
            },
            nodes: Nodes { nodes: Vec::new() },
            name,
        }
    }
    pub fn nodes(&self) -> &Vec<IONode> {
        &self.nodes.nodes
    }

    pub fn nodes_mut(&mut self) -> &mut Vec<IONode> {
        &mut self.nodes.nodes
    }

    pub fn links(&self) -> &Vec<IOLink> {
        &self.links.links
    }

    pub fn links_mut(&mut self) -> &mut Vec<IOLink> {
        &mut self.links.links
    }

    pub fn effective_cell_size(&self) -> f32 {
        self.links.effective_cell_size.unwrap_or(7.5)
    }

    pub fn from_file(file_path: &str) -> IONetwork {
        let network: IONetwork = xml::read_from_file(file_path);
        info!(
            "IONetwork:: Finished reading network. It contains {} nodes and {} links.",
            network.nodes().len(),
            network.links().len()
        );

        network
    }

    pub fn to_file(&self, path: &Path) {
        xml::write_to_file(
            self,
            path,
            "<!DOCTYPE network SYSTEM \"http://www.matsim.org/files/dtd/network_v2.dtd\">",
        );
    }
}

fn add_io_node(network: &mut Network, io_node: &IONode) {
    let id = Id::create(&io_node.id);
    let part_attr = Attrs::find_or_else_opt(&io_node.attributes, "partition", || "0");
    let cmp_weight_attr = Attrs::find_or_else_opt(&io_node.attributes, "cmp_weight", || "1");
    let partition = u32::from_str(part_attr).unwrap();
    let cmp_weight = u32::from_str(cmp_weight_attr).unwrap();

    let mut node = Node::new(id, io_node.x, io_node.y, partition, cmp_weight);
    node.partition = partition;
    network.add_node(node);
}

fn add_io_link(network: &mut Network, io_link: &IOLink) {
    let id = Id::create(&io_link.id);
    let part_attr = Attrs::find_or_else_opt(&io_link.attributes, "partition", || "0");
    let partition = u32::from_str(part_attr).unwrap();
    let modes: IntSet<Id<String>> = io_link
        .modes
        .split(',')
        .map(|s| s.trim())
        .map(Id::create)
        .collect();
    let from_id = Id::get_from_ext(&io_link.from);
    let to_id = Id::get_from_ext(&io_link.to);

    let link = Link::new(
        id,
        from_id,
        to_id,
        io_link.length,
        io_link.capacity,
        io_link.freespeed,
        io_link.permlanes,
        modes,
        partition,
    );
    network.add_link(link);
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;

    use quick_xml::de::from_str;

    use crate::simulation::id::Id;
    use crate::simulation::network::global_network::Network;
    use crate::simulation::network::io::{add_io_link, add_io_node, IOLink, IONetwork, IONode};

    static OUTPUT_FOLDER: &str = "./test_output/io/network/";

    fn get_output_folder(name: &str) -> PathBuf {
        let path = format!("{OUTPUT_FOLDER}{name}/");
        PathBuf::from(&path)
    }

    fn clear_output_folder(name: &str) {
        let folder_path = get_output_folder(name);

        if let Ok(iter) = fs::read_dir(folder_path) {
            for entry in iter {
                fs::remove_file(entry.unwrap().path()).unwrap();
            }
        }
    }

    #[test]
    fn write_and_read_simple_network() {
        // set up
        let test_name = "write_and_read_simple_network";
        clear_output_folder(test_name);

        let xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
                <!DOCTYPE network SYSTEM \"http://www.matsim.org/files/dtd/network_v1.dtd\">
                <network name=\"test network\">
                    <nodes>
                        <node id=\"1\" x=\"-20000\" y=\"0\"/>
                    </nodes>
                    <links effectivecellsize=\"385.3\">
                        <link id=\"23\" from=\"15\" to=\"1\" length=\"10000.00\" capacity=\"36000\" freespeed=\"27.78\" permlanes=\"1\" modes=\"car,bike\"  />
                    </links>
                </network>
            ";

        let network: IONetwork = from_str(xml).unwrap();
        let file_path = get_output_folder(test_name).join("network.xml.gz");
        network.to_file(&file_path);

        let result = IONetwork::from_file(file_path.to_str().unwrap());
        assert_eq!(network, result);
    }

    #[test]
    fn parse_simple_network() -> Result<(), Box<dyn Error>> {
        let xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
                <!DOCTYPE network SYSTEM \"http://www.matsim.org/files/dtd/network_v1.dtd\">
                <network name=\"test network\">
                    <nodes>
                        <node id=\"1\" x=\"-20000\" y=\"0\">
                            <attributes>
                                
                            </attributes>
                        </node>
                    </nodes>
                    <links effectivecellsize=\"42.0\">
                        <link id=\"23\" from=\"15\" to=\"1\" length=\"10000.00\" capacity=\"36000\" freespeed=\"27.78\" permlanes=\"1\" modes=\"car, bike\" />
                    </links>
                </network>
            ";

        let result: IONetwork = from_str(xml)?;

        // test overall structure of network
        assert_eq!("test network", result.name.as_ref().unwrap());
        assert_eq!(1, result.nodes().len());
        assert_eq!(1, result.links().len());
        assert_eq!(42., result.effective_cell_size());

        // test node structure
        let node = result.nodes().first().unwrap();
        assert_eq!("1", node.id);
        assert_eq!(-20000., node.x);
        assert_eq!(0.0, node.y);

        // test the link structure
        let link = result.links().first().unwrap();
        let modes: Vec<_> = link.modes.split(',').map(|s| s.trim()).collect();
        assert_eq!("23", link.id);
        assert_eq!("15", link.from);
        assert_eq!("1", link.to);
        assert_eq!(10000.0, link.length);
        assert_eq!(36000.0, link.capacity);
        assert_eq!(27.78, link.freespeed);
        assert_eq!(1.0, link.permlanes);
        assert_eq!(vec!["car", "bike"], modes);

        Ok(())
    }

    #[test]
    fn read_simple_network() {
        let file_path = "./assets/io_network_tests/simple-network.xml";
        let network: IONetwork = IONetwork::from_file(file_path);

        assert_eq!("simple network", network.name.as_ref().unwrap());
        assert_eq!(2, network.nodes().len());
        assert_eq!(2, network.links().len());

        for node in network.nodes() {
            match &node.attributes {
                None => {
                    assert_eq!("node-without-attr", node.id);
                }
                Some(attrs) => {
                    assert_eq!(1, attrs.attributes.len());
                    let attr = attrs.attributes.get(0).unwrap();
                    assert_eq!("test", attr.name);
                    assert_eq!("value", attr.value);
                }
            }
        }

        for link in network.links() {
            match &link.attributes {
                None => {
                    assert_eq!("link-without-attr", link.id);
                }
                Some(attrs) => {
                    assert_eq!("link-with-attr", link.id);
                    assert_eq!(1, attrs.attributes.len());
                    let attr = attrs.attributes.get(0).unwrap();
                    assert_eq!("test", attr.name);
                    assert_eq!("value", attr.value);
                }
            }
        }
    }

    #[test]
    fn read_example_file() {
        let file_path = "./assets/equil/equil-network.xml";
        let network: IONetwork = IONetwork::from_file(file_path);

        // only test some metadata here
        assert_eq!("equil test network", network.name.as_ref().unwrap());
        assert_eq!(15, network.nodes().len());
        assert_eq!(23, network.links().len());
        // this network doesn't have an effectivecellsize set so test, whether the default works
        assert_eq!(7.5, network.effective_cell_size());
    }

    #[test]
    fn read_example_file_gzipped() {
        let network: IONetwork = IONetwork::from_file("./assets/andorra-network.xml.gz");

        assert_eq!(2259, network.nodes().len());
        assert_eq!(4288, network.links().len());
    }

    #[test]
    fn test_add_io_node() {
        let external_id = String::from("some-id");
        let x = 1.;
        let y = 2.;
        let io_node = IONode {
            id: external_id.clone(),
            x,
            y,
            attributes: None,
        };
        let mut network = Network::new();

        add_io_node(&mut network, &io_node);

        // the node should be in nodes vec and there should be a node id
        let id = Id::get_from_ext(&external_id);
        assert_eq!(0, id.internal());
        assert_eq!(external_id, id.external());

        let node = network.get_node(&id);
        assert_eq!(x, node.x);
        assert_eq!(y, node.y);
        assert_eq!(id, node.id);
    }

    #[test]
    fn test_add_io_link() {
        let ext_from_id = String::from("from");
        let ext_to_id = String::from("to");
        let ext_link_id = String::from("link");

        let io_from = IONode {
            id: ext_from_id.clone(),
            x: 0.,
            y: 0.,
            attributes: None,
        };
        let io_to = IONode {
            id: ext_to_id.clone(),
            x: 100.,
            y: 100.,
            attributes: None,
        };
        let io_link = IOLink {
            id: ext_link_id.clone(),
            from: ext_from_id.clone(),
            to: ext_to_id.clone(),
            length: 100.,
            capacity: 100.,
            freespeed: 10.,
            permlanes: 2.,
            modes: String::from("car,ride, bike"),
            attributes: None,
        };

        let mut network = Network::new();
        add_io_node(&mut network, &io_from);
        add_io_node(&mut network, &io_to);
        add_io_link(&mut network, &io_link);

        let from = network.get_node(&Id::get_from_ext(&ext_from_id));
        let to = network.get_node(&Id::get_from_ext(&ext_to_id));
        let link = network.get_link(&Id::get_from_ext(&ext_link_id));

        assert_eq!(from.id, link.from);
        assert_eq!(to.id, link.to);
        assert_eq!(ext_link_id, link.id.external());
        assert_eq!(io_link.length, link.length);
        assert_eq!(io_link.capacity, link.capacity);
        assert_eq!(io_link.freespeed, link.freespeed);
        assert_eq!(io_link.permlanes, link.permlanes);

        assert!(link.modes.contains(&Id::get_from_ext("car")));
        assert!(link.modes.contains(&Id::get_from_ext("ride")));
        assert!(link.modes.contains(&Id::get_from_ext("bike")));
    }
}
