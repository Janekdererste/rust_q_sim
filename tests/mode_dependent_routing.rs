use crate::event_test_utils::{compare_events, run_mpi_simulation_and_convert_events};
use serial_test::serial;

mod event_test_utils;

#[test]
#[serial]
fn test_mode_dependent_routing_no_significant_updates() {
    let output_dir = "test_output/mpi_test/mode_dependent_routing/no_updates/";
    run_mpi_simulation_and_convert_events(
        2,
        "assets/mode_dependent_routing/no_updates/network.xml",
        "assets/mode_dependent_routing/no_updates/agents.xml",
        output_dir,
        "ad-hoc",
        Some("assets/mode_dependent_routing/vehicle_definitions.xml"),
    );
    compare_events(
        output_dir,
        "tests/resources/mode_dependent_routing/no_updates",
    )
}

#[test]
#[serial]
#[ignore]
fn test_adhoc_routing_with_updates() {
    let output_dir = "test_output/mpi_test/adhoc_routing/with_updates/";
    run_mpi_simulation_and_convert_events(
        2,
        "assets/mode_dependent_routing/with_updates/network.xml",
        "assets/mode_dependent_routing/with_updates/agents_no_leg.xml",
        output_dir,
        "ad-hoc",
        None,
    );
    compare_events(output_dir, "tests/resources/adhoc_routing/with_updates")
}