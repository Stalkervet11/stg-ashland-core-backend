use tonic::{Request, Status};
use std::collections::HashMap;
use std::sync::Arc;

pub fn auth_interceptor(mut request: Request<()>, server_tokens: Arc<HashMap<String, String>>) -> Result<Request<()>, Status> {
    let (token, server_id) = {
        let metadata = request.metadata();
        let token = match metadata.get("authorization") {
            Some(t) => t.to_str().map_err(|_| Status::unauthenticated("Invalid token format"))?.to_string(),
            None => return Err(Status::unauthenticated("Missing authorization metadata")),
        };
        
        let server_id = match metadata.get("x-server-id") {
            Some(s) => s.to_str().map_err(|_| Status::unauthenticated("Invalid server id format"))?.to_string(),
            None => return Err(Status::unauthenticated("Missing x-server-id metadata")),
        };
        (token, server_id)
    };

    if let Some(expected_token) = server_tokens.get(&server_id) {
        let token = token.strip_prefix("Bearer ").unwrap_or(&token);
        if token == expected_token {
            request.extensions_mut().insert(AuthenticatedServer(server_id.to_string()));
            return Ok(request);
        }
    }

    Err(Status::unauthenticated("Invalid server identity or token"))
}

#[derive(Clone, Debug)]
pub struct AuthenticatedServer(pub String);
