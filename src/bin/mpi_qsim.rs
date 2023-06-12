use clap::Parser;
use mpi::traits::Communicator;
use rust_q_sim::simulation::config::Config;
use rust_q_sim::simulation::logging;
use tracing::info;

fn main() {
    let universe = mpi::initialize().unwrap();
    let rank = universe.world().rank();
    let config = Config::parse();
    let _guards = logging::init_non_blocking_logging(config.output_dir.as_ref(), rank.to_string());

    info!("{}", mpi::environment::library_version().unwrap());

    rust_q_sim::simulation::controller::run(universe.world(), config);
}
