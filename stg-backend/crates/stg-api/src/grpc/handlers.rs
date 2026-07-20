// DEPRECATED: Legacy StgBackend handler implementation.
//
// The old single-service StgBackend has been split into three services:
//   - STGAuth (crate::grpc::auth::StgAuthHandler)
//   - STGUnary (crate::grpc::unary::StgUnaryImpl)
//   - STGStreaming (crate::grpc::streaming::StgStreamingAdapter)
//
// This module is kept only to prevent breaking existing imports.
// New code should use the three-service modules directly.
//
// The old StgGrpcServer and StgBackend implementation have been removed.
