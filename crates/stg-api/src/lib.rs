pub mod grpc;

use std::collections::HashMap;
use std::sync::Arc;
use stg_application::{EconomyService, ConversionService, PlayerService, EnergyService};
use stg_proto::stg::v1::stg_backend_server::StgBackendServer;
use tonic::transport::Server;
use crate::grpc::auth::auth_interceptor;
use crate::grpc::handlers::StgGrpcServer;

pub async fn run_server(
    address: std::net::SocketAddr,
    player_service: Arc<PlayerService>,
    economy_service: Arc<EconomyService>,
    conversion_service: Arc<ConversionService>,
    energy_service: Arc<EnergyService>,
    server_tokens: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_tokens = Arc::new(server_tokens);
    let grpc_server = StgGrpcServer {
        player_service,
        economy_service,
        conversion_service,
        energy_service,
    };

    let svc = StgBackendServer::with_interceptor(grpc_server, move |req| {
        auth_interceptor(req, server_tokens.clone())
    });

    Server::builder()
        .add_service(svc)
        .serve(address)
        .await?;

    Ok(())
}
