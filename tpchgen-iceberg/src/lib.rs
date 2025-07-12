//! Generate TPCH data as Apache Iceberg tables
//!
//! This crate provides generators for TPCH tables that directly produces
//! Apache Iceberg tables stored in object storage (S3, Azure Blob, GCS).
//! The generated tables are partitioned and include metadata for efficient querying.
//!
//! # Example
//! ```no_run
//! # use std::collections::HashMap;
//! # use tpchgen_iceberg::{IcebergGenerator, IcebergConfig};
//! # use tokio;
//! # 
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create configuration for Iceberg catalog
//! let config = IcebergConfig {
//!     catalog_type: "rest".to_string(),
//!     catalog_uri: "http://localhost:8181".to_string(),
//!     warehouse_location: "s3://my-bucket/warehouse".to_string(),
//!     credentials: HashMap::new(),
//! };
//! 
//! // Create generator and generate all tables for scale factor 1
//! let generator = IcebergGenerator::new(config).await?;
//! generator.generate_all_tables(1.0).await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod generator;
pub mod catalog;
pub mod tables;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Remove the export since IcebergConfig is already defined here
pub use generator::IcebergGenerator;

/// Configuration for Iceberg catalog connectivity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Type of catalog (rest, hive, glue, etc.)
    pub catalog_type: String,
    /// URI or connection string for the catalog
    pub catalog_uri: String,
    /// Base location for the warehouse
    pub warehouse_location: String,
    /// Additional credentials and configuration
    pub credentials: HashMap<String, String>,
}

/// Error type for Iceberg operations
#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iceberg::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, IcebergError>;