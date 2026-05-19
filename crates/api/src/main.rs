use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

// Proto-generated code will live here once the .proto files are defined.
// Each RPC service gets its own module under src/services/.
//
// Example layout once protos are added:
//   pub mod trident { tonic::include_proto!("trident"); }
//   mod services { pub mod events; }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let addr: SocketAddr = std::env::var("GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".into())
        .parse()?;

    tracing::info!(%addr, "Trident gRPC server listening");

    // TODO: implement the EventsService trait generated from protos
    // TODO: register service with tonic::transport::Server
    // TODO: add tls_config if TLS is required

    tonic::transport::Server::builder()
        // .add_service(EventsServer::new(EventsService::new(db, redis)))
        .serve(addr)
        .await?;

    Ok(())
}
