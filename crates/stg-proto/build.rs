fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile stg_core.proto into Rust structs at build-time.
    // Disable client generation because "Connect" RPC conflicts with the generated client's built-in "connect" method.
    tonic_build::configure()
        .build_client(false)
        .compile(&["proto/stg_core.proto"], &["proto"])?;
    Ok(())
}
