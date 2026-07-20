fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile(&["proto/stg_core.proto"], &["proto"])?;
    Ok(())
}
