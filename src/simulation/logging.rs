use std::io;
use tracing::level_filters::LevelFilter;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

pub fn init_non_blocking_logging(
    directory: &str,
    file_discriminant: String,
) -> (WorkerGuard, WorkerGuard) {
    let mut file_name = String::from("mpi_qsim_");
    file_name.push_str(file_discriminant.as_str());

    let log_file_appender = rolling::never(directory, &file_name);
    let (log_file, _guard_log) = non_blocking(log_file_appender);

    let mut performance_directory = String::from(directory);
    performance_directory.push_str("/trace");

    let performance_file_appender = rolling::never(performance_directory, &file_name);
    let (performance_file, _guard_performance) = non_blocking(performance_file_appender);

    let collector = tracing_subscriber::registry()
        .with(
            fmt::Layer::new()
                .with_writer(io::stdout)
                .with_filter(LevelFilter::INFO),
        )
        .with(
            fmt::Layer::new()
                .with_writer(log_file)
                .json()
                .with_ansi(false)
                .with_filter(LevelFilter::DEBUG),
        )
        .with(
            fmt::Layer::new()
                .with_writer(performance_file.with_min_level(Level::TRACE))
                .json(),
        );
    tracing::subscriber::set_global_default(collector).expect("Unable to set a global collector");
    (_guard_log, _guard_performance)
}

pub fn init_blocking_logging(directory: &str, file_discriminant: String) {
    let mut file_name = String::from("mpi_qsim_");
    file_name.push_str(file_discriminant.as_str());

    let log_file = rolling::never(directory, &file_name);

    let mut performance_directory = String::from(directory);
    performance_directory.push_str("/trace");

    let performance_file = rolling::never(performance_directory, &file_name);

    let collector = tracing_subscriber::registry()
        .with(
            fmt::Layer::new()
                .with_writer(io::stdout)
                .with_filter(LevelFilter::INFO),
        )
        .with(
            fmt::Layer::new()
                .with_writer(log_file)
                .json()
                .with_ansi(false)
                .with_filter(LevelFilter::DEBUG),
        )
        .with(
            fmt::Layer::new()
                .with_writer(performance_file.with_min_level(Level::TRACE))
                .json(),
        );
    tracing::subscriber::set_global_default(collector).expect("Unable to set a global collector");
}
