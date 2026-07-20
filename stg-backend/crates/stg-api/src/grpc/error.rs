use stg_domain::DomainError;
use stg_proto::stg::v1::{Error as ProtoError, ErrorCode};
use tonic::Status;

pub fn map_domain_error(e: DomainError) -> Status {
    let code = match &e {
        DomainError::PlayerNotFound(_) => ErrorCode::PlayerNotFound,
        DomainError::InvalidAmount(_) => ErrorCode::InvalidAmount,
        DomainError::InsufficientFunds { .. } => ErrorCode::InsufficientFunds,
        DomainError::RevisionConflict(_, _, _) => ErrorCode::RevisionConflict,
        DomainError::EnergyNodeNotFound(_) => ErrorCode::EnergyNodeNotFound,
        DomainError::IdempotencyConflict { .. } => ErrorCode::DuplicateRequest,
        DomainError::ConversionReservationNotFound(_) => ErrorCode::ReservationExpired, // Close enough or INTERNAL
        DomainError::ConversionReservationInvalidState(_) => ErrorCode::ReservationAlreadyCommitted,
        DomainError::ConversionReservationExpired(_) => ErrorCode::ReservationExpired,
        DomainError::ConversionRuleNotFound => ErrorCode::ConversionRuleNotFound,
        DomainError::ServerIdentityMismatch { .. } => ErrorCode::ServerUnauthorized,
        DomainError::DuplicateSession(_) => ErrorCode::TransitionAlreadyActive,
        DomainError::SessionNotFound(_) => ErrorCode::TransitionNotFound,
        DomainError::SessionTerminated(_) => ErrorCode::TransitionAlreadyActive,
        DomainError::SessionExpired(_) => ErrorCode::ReservationExpired,
        DomainError::DuplicateTick(_)
        | DomainError::TickNotFound(_)
        | DomainError::SchedulerAlreadyRunning
        | DomainError::SubsystemFailure { .. } => ErrorCode::InternalError,
        DomainError::InternalStateError(_)
        | DomainError::LedgerImbalance
        | DomainError::ArithmeticOverflow => ErrorCode::InternalError,
    };

    let _proto_err = ProtoError {
        code: code.into(),
        message: e.to_string(),
    };

    // We can embed the Error protobuf into a tonic Status details if we want,
    // or just map to Tonic status directly.
    let tonic_code = match code {
        ErrorCode::PlayerNotFound => tonic::Code::NotFound,
        ErrorCode::InvalidAmount | ErrorCode::InsufficientFunds => tonic::Code::InvalidArgument,
        ErrorCode::RevisionConflict | ErrorCode::DuplicateRequest => tonic::Code::Aborted,
        ErrorCode::EnergyNodeNotFound => tonic::Code::NotFound,
        ErrorCode::ServerUnauthorized => tonic::Code::PermissionDenied,
        ErrorCode::TransitionAlreadyActive => tonic::Code::AlreadyExists,
        ErrorCode::TransitionNotFound => tonic::Code::NotFound,
        ErrorCode::ReservationExpired => tonic::Code::DeadlineExceeded,
        _ => tonic::Code::Internal,
    };

    Status::new(tonic_code, e.to_string())
}
