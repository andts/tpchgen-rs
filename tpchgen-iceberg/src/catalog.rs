//! Iceberg catalog management

use crate::{IcebergConfig, IcebergError, Result};
use iceberg::io::FileIOBuilder;
use iceberg::spec::Schema;
use iceberg::{Catalog, Namespace, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_catalog_rest::RestCatalog;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Wrapper around Iceberg catalog operations
pub struct IcebergCatalog {
    catalog: Arc<dyn Catalog>,
    namespace: Namespace,
}

impl IcebergCatalog {
    /// Create a new catalog instance from configuration
    pub async fn new(config: &IcebergConfig) -> Result<Self> {
        config.validate()?;

        let catalog = match config.catalog_type.as_str() {
            "filesystem" => Self::create_filesystem_catalog(config)?,
            "rest" => Self::create_rest_catalog(config).await?,
            "glue" => Self::create_glue_catalog(config).await?,
            _ => {
                return Err(IcebergError::Config(format!(
                    "Unsupported catalog type: {}",
                    config.catalog_type
                )));
            }
        };

        // Create or use configured namespace
        let namespace_ident = iceberg::NamespaceIdent::new(config.namespace.clone());
        let namespace = Namespace::new(namespace_ident);

        Ok(IcebergCatalog { catalog, namespace })
    }

    /// Create a filesystem catalog (using memory catalog for local testing)
    fn create_filesystem_catalog(_config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        // For now, use MemoryCatalog as a placeholder for filesystem catalog
        // In a real implementation, this would use a proper filesystem catalog
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let warehouse_location = Self::temp_path();

        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location));
        Ok(Arc::new(catalog))
    }

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    /// Create a REST catalog
    async fn create_rest_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        // properties.insert("uri".to_string(), config.catalog_uri.clone());
        // properties.insert("warehouse".to_string(), config.warehouse_location.clone());
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let config = iceberg_catalog_rest::RestCatalogConfig::builder()
            .uri(config.uri.clone())
            .warehouse(config.warehouse.clone())
            .props(properties)
            .build();
        let catalog = RestCatalog::new(config);
        Ok(Arc::new(catalog))
    }

    /// Create a Glue catalog (using memory catalog as placeholder)
    async fn create_glue_catalog(_config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        // For now, use MemoryCatalog as a placeholder for Glue catalog
        // In a real implementation, this would use iceberg-catalog-glue crate
        //TODO add real implementation
        let file_io = iceberg::io::FileIO::from_path("memory://")?.build()?;
        let catalog = MemoryCatalog::new(file_io, None);
        Ok(Arc::new(catalog))
    }

    /// Ensure namespace exists
    pub async fn ensure_namespace(&self) -> Result<()> {
        let namespace_ident = self.namespace.name().clone();
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?;
            log::debug!("create namespace '{namespace_ident}' in catalog");
        }
        log::debug!("namespace ensured");
        Ok(())
    }

    /// Create a table with the given schema
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        partition_specs: Vec<String>,
    ) -> Result<()> {
        let table_ident = TableIdent::new(self.namespace.name().clone(), table_name.to_string());

        // Check if table already exists
        if self.catalog.table_exists(&table_ident).await? {
            log::info!("Table {} already exists, skipping creation", table_name);
            return Ok(());
        }

        let table_creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema);

        // Add partition specs if provided
        if !partition_specs.is_empty() {
            // For now, we'll skip partitioning and implement it later
            log::warn!("Partitioning not yet implemented, creating unpartitioned table");
        }

        let table = table_creation.build();
        self.catalog
            .create_table(&self.namespace.name().clone(), table)
            .await?;

        Ok(())
    }

    /// Get catalog reference
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Get namespace
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
}
