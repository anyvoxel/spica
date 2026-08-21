// Compiles spica.proto into Rust (prost messages + tonic service/client/server traits) at build
// time. The generated code lands in OUT_DIR and is brought in by `tonic::include_proto!` in lib.rs.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/spica.proto";
    let dir = "proto";
    // Emit both server and client traits: the server crate implements the service, the spica CLI
    // (and tests) builds clients against the generated stubs.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &[dir])?;
    // Rebuild when the contract changes.
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
