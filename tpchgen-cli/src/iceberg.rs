//! Iceberg format support for TPC-H data generation
//!
//! This module provides functionality to generate TPC-H data directly into
//! Apache Iceberg tables using the tpchgen-iceberg crate.

use crate::Table;
use log::info;
use std::path::PathBuf;
use tpchgen_arrow::RecordBatchIterator;
use tpchgen_iceberg::{IcebergConfig, IcebergError, IcebergGenerator};

/// Load Iceberg configuration from JSON file
pub fn load_config(config_path: &PathBuf) -> Result<IcebergConfig, IcebergError> {
    let config_content = std::fs::read_to_string(config_path)?;
    let config: IcebergConfig = serde_json::from_str(&config_content)?;
    Ok(config)
}

/// Generate TPC-H data into Iceberg tables
pub async fn generate_iceberg_table<I>(
    table: &Table,
    config: IcebergConfig,
    sources: I,
) -> Result<(), IcebergError>
where
    I: Iterator<Item: RecordBatchIterator> + 'static,
{
    info!("Initializing Iceberg generator with config {:?}", config);
    let generator = IcebergGenerator::new(config).await?;

    info!("Generating table: {}", table.name());
    generator
        .generate_table(table.name(), Box::new(sources))
        .await?;

    info!("Iceberg table generation complete!");
    Ok(())
}
