use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use ahash::HashSet;
use clap::Parser;
use tracing::info;

use rust_q_sim::simulation::config::PartitionMethod;
use rust_q_sim::simulation::logging::init_std_out_logging;
use rust_q_sim::simulation::network::global_network::Network;
use rust_q_sim::simulation::network::sim_network::SimNetworkPartition;
use rust_q_sim::simulation::{config, id};

fn main() {
    init_std_out_logging();
    let args = InputArgs::parse();

    id::load_from_file(&args.id_store);
    info!("Loading network from {:?}", args.network);
    let net = Network::from_file_path(&args.network, 1, PartitionMethod::None);
    let distinct_partitions: HashSet<u32> = net.nodes.iter().map(|n| n.partition).collect();

    let mut writer =
        BufWriter::new(File::create(&args.output).expect("Could not open output file."));
    writer
        .write("rank,neighbors\n".as_bytes())
        .expect("failed to write header");

    for partition in distinct_partitions {
        let net_partition =
            SimNetworkPartition::from_network(&net, partition, config::Simulation::default());
        let neighbors = net_partition.neighbors().len();
        let serialized = format!("{},{}\n", partition, neighbors);
        writer
            .write(serialized.as_bytes())
            .expect("Failed to write entry.");
    }

    writer.flush().unwrap();
    info!("Finished writing output file to: {:?}", args.output)
}

#[derive(Parser, Debug)]
struct InputArgs {
    #[arg(short, long)]
    pub id_store: PathBuf,
    #[arg(short, long)]
    pub network: PathBuf,
    #[arg(short, long)]
    pub output: PathBuf,
}
