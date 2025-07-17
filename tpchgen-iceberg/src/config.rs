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

    /// Create a configuration for AWS Glue catalog
    pub fn glue_catalog(region: String, warehouse: String) -> Self {
        let mut credentials = HashMap::new();
        credentials.insert("aws.region".to_string(), region);

        IcebergConfig {
            catalog_type: "glue".to_string(),
            uri: "glue".to_string(),
            warehouse,
            namespace: "tpch_gen_1".to_string(),
            properties: credentials,
        }
    }

    /// Create a configuration for local filesystem catalog
    pub fn local_filesystem<P: AsRef<Path>>(warehouse_path: P) -> Self {
        IcebergConfig {
            catalog_type: "filesystem".to_string(),
            uri: "filesystem".to_string(),
            warehouse: warehouse_path.as_ref().to_string_lossy().to_string(),
            namespace: "tpch_gen_1".to_string(),
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
            return Err(IcebergError::Config("catalog_type cannot be empty".to_string()));
        }
        if self.uri.is_empty() {
            return Err(IcebergError::Config("catalog_uri cannot be empty".to_string()));
        }
        if self.warehouse.is_empty() {
            return Err(IcebergError::Config("warehouse_location cannot be empty".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_rest_catalog_config() {
        let config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string(),
            "tpch".to_string()
        );
        assert_eq!(config.catalog_type, "rest");
        assert_eq!(config.uri, "http://localhost:8181");
        assert_eq!(config.warehouse, "s3://bucket/warehouse");
    }

    #[test]
    fn test_config_validation() {
        let valid_config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string(),
            "tpch".to_string()
        );
        assert!(valid_config.validate().is_ok());

        let invalid_config = IcebergConfig {
            catalog_type: "".to_string(),
            uri: "".to_string(),
            warehouse: "".to_string(),
            namespace: "tpch_gen_1".to_string(),
            properties: HashMap::new(),
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_config_from_file() {
        let config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string(),
            "tpch".to_string()
        );

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "{}", serde_json::to_string(&config).unwrap()).unwrap();

        let loaded_config = IcebergConfig::from_file(temp_file.path()).unwrap();
        assert_eq!(loaded_config.catalog_type, config.catalog_type);
        assert_eq!(loaded_config.uri, config.uri);
        assert_eq!(loaded_config.warehouse, config.warehouse);
    }
}
