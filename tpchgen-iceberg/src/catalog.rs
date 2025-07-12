//! Iceberg catalog management

use crate::{IcebergConfig, IcebergError, Result};
use iceberg::{Catalog, Namespace, TableCreation, TableIdent};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_catalog_rest::RestCatalog;
use std::collections::HashMap;
use std::sync::Arc;

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
            "filesystem" => {
                Self::create_filesystem_catalog(config).await?
            }
            "rest" => {
                Self::create_rest_catalog(config).await?
            }
            "glue" => {
                Self::create_glue_catalog(config).await?
            }
            _ => {
                return Err(IcebergError::Config(format!(
                    "Unsupported catalog type: {}",
                    config.catalog_type
                )));
            }
        };

        // Create or use default namespace
        let namespace_ident = iceberg::NamespaceIdent::new("tpch".to_string());
        let namespace = Namespace::new(namespace_ident);

        Ok(IcebergCatalog { catalog, namespace })
    }

    /// Create a filesystem catalog (using memory catalog for local testing)
    async fn create_filesystem_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        // For now, use MemoryCatalog as a placeholder for filesystem catalog
        // In a real implementation, this would use a proper filesystem catalog
        let file_io = iceberg::io::FileIO::from_path("memory://")?.build()?;
        let catalog = MemoryCatalog::new(file_io, None);
        Ok(Arc::new(catalog))
    }

    /// Create a REST catalog
    async fn create_rest_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), config.catalog_uri.clone());
        properties.insert("warehouse".to_string(), config.warehouse_location.clone());
        for (key, value) in &config.credentials {
            properties.insert(key.clone(), value.clone());
        }

        let config = iceberg_catalog_rest::RestCatalogConfig::builder()
            .uri(config.catalog_uri.clone())
            .build();
        let catalog = RestCatalog::new(config);
        Ok(Arc::new(catalog))
    }

    /// Create a Glue catalog (using memory catalog as placeholder)
    async fn create_glue_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        // For now, use MemoryCatalog as a placeholder for Glue catalog
        // In a real implementation, this would use iceberg-catalog-glue crate
        let file_io = iceberg::io::FileIO::from_path("memory://")?.build()?;
        let catalog = MemoryCatalog::new(file_io, None);
        Ok(Arc::new(catalog))
    }

    /// Ensure namespace exists
    pub async fn ensure_namespace(&self) -> Result<()> {
        let namespace_ident = self.namespace.name().clone();
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog.create_namespace(&namespace_ident, HashMap::new()).await?;
        }
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
        self.catalog.create_table(&self.namespace.name().clone(), table).await?;
        
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

/// Helper function to create TPC-H table schemas
pub fn create_tpch_schemas() -> HashMap<String, Schema> {
    let mut schemas = HashMap::new();

    // Region table schema
    schemas.insert("region".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "r_regionkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "r_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "r_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // Nation table schema
    schemas.insert("nation".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "n_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "n_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(3, "n_regionkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(4, "n_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // Customer table schema
    schemas.insert("customer".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "c_custkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "c_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(3, "c_address", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(4, "c_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(5, "c_phone", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(6, "c_acctbal", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(7, "c_mktsegment", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(8, "c_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // Supplier table schema
    schemas.insert("supplier".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "s_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "s_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(3, "s_address", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(4, "s_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(5, "s_phone", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(6, "s_acctbal", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(7, "s_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // Part table schema
    schemas.insert("part".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "p_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "p_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(3, "p_mfgr", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(4, "p_brand", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(5, "p_type", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(6, "p_size", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(7, "p_container", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(8, "p_retailprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(9, "p_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // PartSupp table schema
    schemas.insert("partsupp".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "ps_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "ps_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(3, "ps_availqty", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(4, "ps_supplycost", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(5, "ps_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // Orders table schema
    schemas.insert("orders".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "o_orderkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "o_custkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(3, "o_orderstatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(4, "o_totalprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(5, "o_orderdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(6, "o_orderpriority", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(7, "o_clerk", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(8, "o_shippriority", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(9, "o_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    // LineItem table schema
    schemas.insert("lineitem".to_string(), Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(1, "l_orderkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "l_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(3, "l_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(4, "l_linenumber", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(5, "l_quantity", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(6, "l_extendedprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(7, "l_discount", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(8, "l_tax", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(9, "l_returnflag", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(10, "l_linestatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(11, "l_shipdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(12, "l_commitdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(13, "l_receiptdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(14, "l_shipinstruct", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(15, "l_shipmode", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(16, "l_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build().unwrap());

    schemas
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_filesystem_catalog() {
        let temp_dir = TempDir::new().unwrap();
        let config = IcebergConfig::local_filesystem(temp_dir.path());
        
        let catalog = IcebergCatalog::new(&config).await.unwrap();
        catalog.ensure_namespace().await.unwrap();
        
        let schemas = create_tpch_schemas();
        let region_schema = schemas.get("region").unwrap().clone();
        
        catalog.create_table("region", region_schema, vec![]).await.unwrap();
    }

    #[test]
    fn test_tpch_schemas() {
        let schemas = create_tpch_schemas();
        assert_eq!(schemas.len(), 8);
        assert!(schemas.contains_key("region"));
        assert!(schemas.contains_key("nation"));
        assert!(schemas.contains_key("customer"));
        assert!(schemas.contains_key("supplier"));
        assert!(schemas.contains_key("part"));
        assert!(schemas.contains_key("partsupp"));
        assert!(schemas.contains_key("orders"));
        assert!(schemas.contains_key("lineitem"));
    }
}