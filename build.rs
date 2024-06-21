extern crate prost_build;
extern crate protobuf_src;

fn main() {
    // we use the protobuf-src which provides the protoc compiler. This line makes it available
    // to prost-build
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let proto_files = [
        "src/simulation/wire_types/messages.proto",
        "src/simulation/wire_types/events.proto",
        "src/simulation/wire_types/ids.proto",
        "src/simulation/wire_types/network.proto",
        "src/simulation/wire_types/population.proto",
        "src/simulation/wire_types/vehicles.proto",
    ];

    // tell cargo to rerun this build script if any of the proto files change
    for proto in &proto_files {
        println!("cargo:rerun-if-changed={}", proto);
    }

    // Compiling the protobuf files
    prost_build::compile_protos(&proto_files, &["src/"]).unwrap();
}
