use std::fmt;

#[derive(Debug)]
pub enum ProxyError {
    Unauthorized,
    RateLimited { is_tps: bool },
    UpstreamError(String),
    NoHealthyNodes,
    AllUpstreamsFailed,
    BodyTooLarge,
    InvalidRequest(String),
    Internal(String),
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyError::Unauthorized => write!(f, "Unauthorized"),
            ProxyError::RateLimited { is_tps } => {
                if *is_tps {
                    write!(f, "TPS rate limit exceeded")
                } else {
                    write!(f, "RPS rate limit exceeded")
                }
            }
            ProxyError::UpstreamError(e) => write!(f, "Upstream error: {}", e),
            ProxyError::NoHealthyNodes => write!(f, "No healthy upstream nodes"),
            ProxyError::AllUpstreamsFailed => write!(f, "All upstream nodes failed"),
            ProxyError::BodyTooLarge => write!(f, "Request body too large"),
            ProxyError::InvalidRequest(e) => write!(f, "Invalid request: {}", e),
            ProxyError::Internal(e) => write!(f, "Internal error: {}", e),
        }
    }
}

impl std::error::Error for ProxyError {}

impl From<hyper::Error> for ProxyError {
    fn from(e: hyper::Error) -> Self {
        ProxyError::UpstreamError(e.to_string())
    }
}

impl From<hyper_util::client::legacy::Error> for ProxyError {
    fn from(e: hyper_util::client::legacy::Error) -> Self {
        ProxyError::UpstreamError(e.to_string())
    }
}
