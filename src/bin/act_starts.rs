use clap::Parser;
use csv::Writer;
use itertools::Itertools;
use rust_q_sim::simulation::config::PartitionMethod;
use rust_q_sim::simulation::id;
use rust_q_sim::simulation::id::Id;
use rust_q_sim::simulation::logging::init_std_out_logging;
use rust_q_sim::simulation::messaging::events::EventsSubscriber;
use rust_q_sim::simulation::network::global_network::Network;
use rust_q_sim::simulation::wire_types::events::event::Type;
use rust_q_sim::simulation::wire_types::events::Event;
use rust_q_sim::simulation::wire_types::population::Activity;
use std::any::Any;
use std::collections::HashMap;
use std::io::Error;
use std::path::PathBuf;
use tracing::warn;

struct StatefulEventsReader {
    output_path: PathBuf,
    partition_by_link: HashMap<u64, u32>,
    data: HashMap<(u32, u32), u32>,
}

mod proto2xml;

impl StatefulEventsReader {
    pub fn new(output_path: PathBuf, partition_by_link: HashMap<u64, u32>) -> Self {
        StatefulEventsReader {
            output_path,
            partition_by_link,
            data: Default::default(),
        }
    }

    fn process_event(&mut self, time: u32, event: &Event) {
        match event.r#type.as_ref().unwrap() {
            Type::Generic(_) => {}
            Type::ActStart(_) => {}
            Type::ActEnd(a) => {
                if !Id::<String>::get(a.act_type)
                    .external()
                    .contains("interaction")
                {
                    self.data
                        .entry((time, *self.partition_by_link.get(&a.link).unwrap()))
                        .and_modify(|counter| *counter += 1)
                        .or_insert(1);
                };
            }
            Type::LinkEnter(_) => {}
            Type::LinkLeave(_) => {}
            Type::PersonEntersVeh(_) => {}
            Type::PersonLeavesVeh(_) => {}
            Type::Departure(_) => {}
            Type::Arrival(_) => {}
            Type::Travelled(_) => {}
        }
    }

    fn write_csv(&self) -> Result<(), Error> {
        let file_path = self.output_path.join("act_starts.csv");
        let mut writer = Writer::from_path(&file_path)?;

        let data = self
            .data
            .iter()
            .map(|((time, partition), count)| (*time, *partition, *count))
            .sorted_by(|(time_a, partition_a, _), (time_b, partition_b, _)| {
                time_a.cmp(time_b).then(partition_a.cmp(partition_b))
            })
            .collect::<Vec<(u32, u32, u32)>>();

        writer.write_record(&["time", "partition", "count"])?;

        for &(col1, col2, col3) in &data {
            writer.write_record(&[col1.to_string(), col2.to_string(), col3.to_string()])?;
        }
        writer.flush()?;

        println!("Created csv file: {}", file_path.to_str().unwrap());

        Ok(())
    }
}

impl EventsSubscriber for StatefulEventsReader {
    fn receive_event(&mut self, time: u32, event: &Event) {
        self.process_event(time, event);
    }

    fn finish(&mut self) {
        self.write_csv().unwrap();
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

fn main() {
    init_std_out_logging();
    let args = InputArgs::parse();

    id::load_from_file(&PathBuf::from(&args.id_store));

    let net_path = PathBuf::from(&args.network);
    let net = Network::from_file_path(net_path.as_path(), 1, PartitionMethod::None);

    let partition_by_link = net
        .links
        .iter()
        .map(|link| (link.id.internal(), link.partition))
        .collect::<HashMap<u64, u32>>();

    let reader = Box::new(StatefulEventsReader::new(
        PathBuf::from(&args.output),
        partition_by_link,
    ));
    proto2xml::convert(args.events, args.num_parts, vec![reader]);
}

#[derive(Parser, Debug)]
struct InputArgs {
    #[arg(short, long)]
    pub id_store: String,
    #[arg(short, long)]
    pub events: String,
    #[arg(short, long)]
    pub network: String,
    #[arg(short, long)]
    pub output: String,
    #[arg(long)]
    pub num_parts: u32,
}
