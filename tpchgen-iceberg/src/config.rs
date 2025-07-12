//! Configuration management for Iceberg catalog connections

use crate::{IcebergConfig, IcebergError, Result};
// use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

impl IcebergConfig {
    /// Load configuration from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: IcebergConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Create a configuration for local filesystem catalog (for testing)
    pub fn local_filesystem<P: AsRef<Path>>(warehouse_path: P) -> Self {
        IcebergConfig {
            catalog_type: "filesystem".to_string(),
            catalog_uri: format!("file://{}", warehouse_path.as_ref().display()),
            warehouse_location: format!("file://{}", warehouse_path.as_ref().display()),
            credentials: HashMap::new(),
        }
    }

    /// Create a configuration for REST catalog
    pub fn rest_catalog(uri: String, warehouse_location: String) -> Self {
        IcebergConfig {
            catalog_type: "rest".to_string(),
            catalog_uri: uri,
            warehouse_location,
            credentials: HashMap::new(),
        }
    }

    /// Create a configuration for AWS Glue catalog
    pub fn glue_catalog(region: String, warehouse_location: String) -> Self {
        let mut credentials = HashMap::new();
        credentials.insert("aws.region".to_string(), region);

        IcebergConfig {
            catalog_type: "glue".to_string(),
            catalog_uri: "glue".to_string(),
            warehouse_location,
            credentials,
        }
    }

    /// Add a credential to the configuration
    pub fn with_credential(mut self, key: String, value: String) -> Self {
        self.credentials.insert(key, value);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.catalog_type.is_empty() {
            return Err(IcebergError::Config("catalog_type cannot be empty".to_string()));
        }
        if self.catalog_uri.is_empty() {
            return Err(IcebergError::Config("catalog_uri cannot be empty".to_string()));
        }
        if self.warehouse_location.is_empty() {
            return Err(IcebergError::Config("warehouse_location cannot be empty".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_local_filesystem_config() {
        let config = IcebergConfig::local_filesystem("/tmp/warehouse");
        assert_eq!(config.catalog_type, "filesystem");
        assert!(config.catalog_uri.starts_with("file://"));
        assert!(config.warehouse_location.starts_with("file://"));
    }

    #[test]
    fn test_rest_catalog_config() {
        let config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string()
        );
        assert_eq!(config.catalog_type, "rest");
        assert_eq!(config.catalog_uri, "http://localhost:8181");
        assert_eq!(config.warehouse_location, "s3://bucket/warehouse");
    }

    #[test]
    fn test_config_validation() {
        let valid_config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string()
        );
        assert!(valid_config.validate().is_ok());

        let invalid_config = IcebergConfig {
            catalog_type: "".to_string(),
            catalog_uri: "".to_string(),
            warehouse_location: "".to_string(),
            credentials: HashMap::new(),
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_config_from_file() {
        let config = IcebergConfig::rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://bucket/warehouse".to_string()
        );
        
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "{}", serde_json::to_string(&config).unwrap()).unwrap();
        
        let loaded_config = IcebergConfig::from_file(temp_file.path()).unwrap();
        assert_eq!(loaded_config.catalog_type, config.catalog_type);
        assert_eq!(loaded_config.catalog_uri, config.catalog_uri);
        assert_eq!(loaded_config.warehouse_location, config.warehouse_location);
    }
}