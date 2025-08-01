//! Generate TPCH data as Apache Iceberg tables

pub mod catalog;
pub mod config;
pub mod generator;

pub use generator::IcebergGenerator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for Iceberg catalog connectivity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Type of catalog (rest, hive, glue, etc.)
    pub catalog_type: String,
    /// URI or connection string for the catalog
    pub uri: String,
    /// Base location for the warehouse
    pub warehouse: String,
    /// Namespace for Iceberg tables
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Additional credentials and configuration
    pub properties: HashMap<String, String>,
}

fn default_namespace() -> String {
    "tpch".to_string()
}

/// Error type for Iceberg operations
#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iceberg::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
}

pub type Result<T> = std::result::Result<T, IcebergError>;
