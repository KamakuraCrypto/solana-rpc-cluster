fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Yellowstone Geyser + Solana storage protos together
    // They must be compiled in the same call because geyser.proto imports solana-storage.proto
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/geyser.proto", "proto/solana-storage.proto"],
            &["proto/"],
        )?;

    // Compile aRPC v1 proto
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/arpc_v1.proto"], &["proto/"])?;

    // Compile aRPC v2 proto
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/arpc_v2.proto"], &["proto/"])?;

    Ok(())
}
