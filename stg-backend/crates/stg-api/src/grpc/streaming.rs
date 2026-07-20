// STG-Core Streaming RPC Implementation
//
// This module implements the STGStreaming gRPC service (bidirectional
// realtime communication between game server and backend).

use std::sync::Arc;
use tonic::{Request, Response, Status};

use stg_proto::stg::v1::{
    stg_streaming_server::StgStreaming,
    BackendMessage, ServerMessage,
};

use crate::grpc::unary::StgUnaryImpl;

/// Thin adapter: implements STGStreaming by delegating to StgUnaryImpl.
/// This allows registering a separate gRPC service for streaming only.
pub struct StgStreamingAdapter {
    pub unary: Arc<StgUnaryImpl>,
}

impl StgStreamingAdapter {
    pub fn new(unary: Arc<StgUnaryImpl>) -> Self {
        Self { unary }
    }
}

#[tonic::async_trait]
impl StgStreaming for StgStreamingAdapter {
    async fn connect(
        &self,
        request: Request<tonic::Streaming<ServerMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        // Delegates to the unary implementation's shared connect_inner method.
        self.unary.connect_inner(request).await
    }
}
