//! Configuration management for Iceberg catalog connections

use crate::{IcebergConfig, IcebergError, Result};
use std::collections::HashMap;
use std::path::Path;

impl IcebergConfig {
    /// Load configuration from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: IcebergConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Create a configuration for REST catalog
    pub fn rest_catalog(uri: String, warehouse: String, namespace: String) -> Self {
        IcebergConfig {
            catalog_type: "rest".to_string(),
            uri,
            warehouse,
            namespace,
            properties: HashMap::new(),
        }
    }

    /// Set the namespace for the configuration
    pub fn with_namespace(mut self, namespace: String) -> Self {
        self.namespace = namespace;
        self
    }

    /// Add a credential to the configuration
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.catalog_type.is_empty() {
            return Err(IcebergError::Config(
                "catalog_type cannot be empty".to_string(),
            ));
        }
        if self.uri.is_empty() {
            return Err(IcebergError::Config(
                "uri cannot be empty".to_string(),
            ));
        }
        if self.warehouse.is_empty() {
            return Err(IcebergError::Config(
                "warehouse cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}
