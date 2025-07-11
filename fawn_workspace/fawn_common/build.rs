fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src")
        .compile(
                &["proto/fawn_backend_api.proto", "proto/fawn_frontend_api.proto"],
                &["proto"],
        )?;
    Ok(())
}