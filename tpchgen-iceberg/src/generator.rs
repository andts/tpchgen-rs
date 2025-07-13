//! Main Iceberg generator orchestration

use crate::catalog::IcebergCatalog;
use crate::tables::*;
use crate::{IcebergConfig, IcebergError, Result};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::TableIdent;
use std::sync::Arc;

/// Main generator for TPC-H data in Iceberg format
pub struct IcebergGenerator {
    catalog: IcebergCatalog,
    batch_size: usize,
    max_concurrent_tables: usize,
}

impl IcebergGenerator {
    /// Create a new Iceberg generator
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        let catalog = IcebergCatalog::new(&config).await?;

        Ok(IcebergGenerator {
            catalog,
            batch_size: 8192, // Default batch size
            max_concurrent_tables: 4, // Default concurrency
        })
    }

    /// Set the batch size for Arrow record batches
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of concurrent table generations
    pub fn with_max_concurrent_tables(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent_tables = max_concurrent;
        self
    }

    /// Generate all TPC-H tables
    pub async fn generate_all_tables(&self, scale_factor: f64, parts: usize) -> Result<()> {
        self.generate_tables(get_all_table_names(), scale_factor, parts).await
    }

    /// Generate specified tables
    pub async fn generate_tables(
        &self,
        table_names: Vec<&str>,
        scale_factor: f64,
        parts: usize,
        // Ensure namespace exists
    ) -> Result<()> {
        log::debug!("generate tables for {}", table_names.join(", "));

        self.catalog.ensure_namespace().await?;
        log::debug!("namespace ensured");

        // Generate data for tables with controlled concurrency
        // let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();
        // let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_tables));

        for table_name in table_names {
            let arrow_schema = get_schema(table_name);

            let iceberg_schema = arrow_schema_to_schema(arrow_schema.as_ref())?;

            // dbg!(&arrow_schema);
            // dbg!(&iceberg_schema);

            // log::debug!("creating table {}", table_name);
            self.catalog.create_table(table_name, iceberg_schema, vec![]).await?;
            // log::debug!("created table {}", table_name);

            let catalog = self.catalog.catalog().clone();
            let namespace = self.catalog.namespace().clone();
            let table_name = table_name.to_string();
            let batch_size = self.batch_size;
            // let permit = semaphore.clone().acquire_owned().await.unwrap();

            // let handle = tokio::spawn(async move {
            //     let _permit = permit; // Hold permit for the duration of the task

            // Get table reference
            let table_ident = TableIdent::new(namespace.name().clone(), table_name.clone());
            let table = Arc::new(catalog.load_table(&table_ident).await?);

            // Generate data based on table type
            match table_name.as_str() {
                "region" => generate_region_table_data(table, catalog.clone()).await?,
                "nation" => generate_nation_table_data(table, catalog.clone()).await?,
                "customer" => generate_customer_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                "supplier" => generate_supplier_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                "part" => generate_part_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                "partsupp" => generate_partsupp_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                "orders" => generate_orders_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                "lineitem" => generate_lineitem_table(table, catalog.clone(), scale_factor, batch_size, parts).await?,
                _ => Err(IcebergError::Config(format!("Unknown table: {}", table_name)))?,
            }
            // });

            // handles.push(handle);
        }

        // Wait for all generation tasks to complete
        // let results: Result<Vec<_>> = try_join_all(handles).await
        //     .map_err(|e| IcebergError::Config(format!("Task join error: {}", e)))?
        //     .into_iter()
        //     .collect();

        // results?;
        Ok(())
    }

    /// List all available tables in the catalog
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.catalog.catalog().list_tables(&self.catalog.namespace().name().clone()).await?;
        let table_names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
        Ok(table_names)
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let table_ident = TableIdent::new(self.catalog.namespace().name().clone(), table_name.to_string());
        Ok(self.catalog.catalog().table_exists(&table_ident).await?)
    }

    /// Get statistics for a table
    pub async fn get_table_stats(&self, table_name: &str) -> Result<String> {
        let table_ident = TableIdent::new(self.catalog.namespace().name().clone(), table_name.to_string());
        let table = self.catalog.catalog().load_table(&table_ident).await?;

        let metadata = table.metadata();
        let current_snapshot = metadata.current_snapshot();

        if let Some(snapshot) = current_snapshot {
            let summary = snapshot.summary();
            Ok(format!(
                "Table: {}\n\
                 Snapshot ID: {}\n\
                 Total Records: {}\n\
                 Total Data Files: {}\n\
                 Total Size: {} bytes",
                table_name,
                snapshot.snapshot_id(),
                summary.additional_properties.get("total-records").unwrap_or(&"unknown".to_string()),
                summary.additional_properties.get("total-data-files").unwrap_or(&"unknown".to_string()),
                summary.additional_properties.get("total-size").unwrap_or(&"unknown".to_string()),
            ))
        } else {
            Ok(format!("Table: {} (no data)", table_name))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IcebergConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_generator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = IcebergConfig::local_filesystem(temp_dir.path());

        let generator = IcebergGenerator::new(config).await.unwrap();
        assert_eq!(generator.batch_size, 8192);
        assert_eq!(generator.max_concurrent_tables, 4);
    }

    #[tokio::test]
    async fn test_generator_with_custom_settings() {
        let temp_dir = TempDir::new().unwrap();
        let config = IcebergConfig::local_filesystem(temp_dir.path());

        let generator = IcebergGenerator::new(config).await
            .unwrap()
            .with_batch_size(1024)
            .with_max_concurrent_tables(2);

        assert_eq!(generator.batch_size, 1024);
        assert_eq!(generator.max_concurrent_tables, 2);
    }

    #[tokio::test]
    async fn test_small_scale_generation() {
        // let temp_dir = TempDir::new().unwrap();
        let config =
            IcebergConfig::rest_catalog("http://localhost:8181/catalog".to_string(), "testwarehouse".to_string())
                .with_property("s3.access-key-id".to_string(), "minioadmin".to_string())
                .with_property("s3.secret-access-key".to_string(), "minioadmin".to_string());

        let generator = IcebergGenerator::new(config).await.unwrap();

        // Generate only small tables for testing
        generator.generate_all_tables(1.0, 1).await.unwrap();

        let tables = generator.list_tables().await.unwrap();
        assert!(tables.contains(&"region".to_string()));
        assert!(tables.contains(&"nation".to_string()));
    }
}