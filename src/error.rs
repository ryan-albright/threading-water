use std::{fmt, io};

#[derive(Debug)]
pub enum PoolFailure {
    InvalidSize,
    Creation(io::Error),
    ShuttingDown,
}

impl fmt::Display for PoolFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolFailure::InvalidSize => write!(f, "Pool size cannot be zero"),
            PoolFailure::Creation(_) => write!(f, "Pool instantiation failed"),
            PoolFailure::ShuttingDown => write!(f, "Pool shutting down"),
        }
    }
}

impl std::error::Error for PoolFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PoolFailure::Creation(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for PoolFailure {
    fn from(value: io::Error) -> Self {
        PoolFailure::Creation(value)
    }
}
