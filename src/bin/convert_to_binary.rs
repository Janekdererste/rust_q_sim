use std::path::PathBuf;

use ahash::HashMapExt;
use clap::Parser;
use nohash_hasher::IntMap;
use tracing::info;

use rust_q_sim::simulation::config::PartitionMethod;
use rust_q_sim::simulation::network::global_network::Network;
use rust_q_sim::simulation::population::population::Population;
use rust_q_sim::simulation::vehicles::garage::Garage;

#[derive(Parser, Debug)]
struct InputArgs {
    #[arg(short, long)]
    pub network: PathBuf,
    #[arg(short, long)]
    pub population: PathBuf,
    #[arg(short, long)]
    pub vehicles: PathBuf,
    #[arg(short, long)]
    pub output_dir: PathBuf,
    #[arg(short, long)]
    pub run_id: String,
}

fn main() {
    rust_q_sim::simulation::logging::init_std_out_logging();
    let args = InputArgs::parse();

    let mut net = Network::from_file_path(&args.network, 1, PartitionMethod::None);
    let mut veh = Garage::from_file(&args.vehicles);
    let pop = Population::from_file(&args.population, &mut veh);

    let cmp_weights = compute_computational_weights(&pop);
    assign_computational_weights(&mut net, cmp_weights);

    rust_q_sim::simulation::id::store_to_file(&create_file_path(&args, "ids"));
    net.to_file(&create_file_path(&args, "network"));
    veh.to_file(&create_file_path(&args, "vehicles"));
    pop.to_file(&create_file_path(&args, "plans"));
}

fn create_file_path(args: &InputArgs, extension: &str) -> PathBuf {
    args.output_dir
        .join(format!("{}.{}.binpb", args.run_id, extension))
}

fn compute_computational_weights(pop: &Population) -> IntMap<u64, u32> {
    info!("Computing computational weights based on routes in plans file");
    let result: IntMap<u64, u32> = pop
        .persons
        .values()
        .flat_map(|p| p.plan.as_ref().unwrap().legs.iter())
        .flat_map(|leg| leg.route.as_ref().unwrap().route.iter())
        .fold(IntMap::new(), |mut map, link_id| {
            map.entry(*link_id)
                .and_modify(|counter| *counter += 1)
                .or_insert(1u32);
            map
        });
    info!("Finished computing computational weights");
    result
}

fn assign_computational_weights(net: &mut Network, cmp_weights: IntMap<u64, u32>) {
    for (link_id, weight) in cmp_weights {
        let link = &net.links[link_id as usize];
        let node = &mut net.nodes[link.to.internal() as usize];
        node.cmp_weight = weight;
    }
}
