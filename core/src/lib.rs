pub mod agent;
pub mod models;
pub mod traits;

pub use agent::Agent;
pub use models::{AgentConfig, AgentError, Finding, ResultStatus, ScanResult, ScanTask};
pub use traits::{AgentHooks, ResultSink, ScanEngine, TaskQueue};

/// Library version (sourced from Cargo.toml at compile time).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Mock implementations for use in tests.
///
/// Available when compiling tests (`cargo test`).
#[cfg(test)]
pub mod mocks {
    pub use crate::agent::testing::*;
}
