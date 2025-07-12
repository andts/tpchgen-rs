# tpchgen-iceberg

TPC-H data generator for Apache Iceberg format.

This crate provides generators for TPC-H tables that directly produce Apache Iceberg tables stored in object storage (S3, Azure Blob, GCS) or local filesystems. The generated tables are partitioned and include metadata for efficient querying.

## Features

- Generate TPC-H data directly to Iceberg tables
- Support for multiple catalog types (REST, Glue, filesystem)
- Configurable batch sizes and concurrency
- Partitioned table support
- Statistics and metadata tracking

## Usage

```rust
use tpchgen_iceberg::{IcebergGenerator, IcebergConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration for Iceberg catalog
    let config = IcebergConfig {
        catalog_type: "rest".to_string(),
        catalog_uri: "http://localhost:8181".to_string(),
        warehouse_location: "s3://my-bucket/warehouse".to_string(),
        credentials: HashMap::new(),
    };
    
    // Create generator and generate all tables for scale factor 1
    let generator = IcebergGenerator::new(config).await?;
    generator.generate_all_tables(1.0).await?;
    
    Ok(())
}
```

## Configuration

The library supports various catalog configurations:

### Filesystem Catalog (for testing)
```rust
let config = IcebergConfig::local_filesystem("/tmp/warehouse");
```

### REST Catalog
```rust
let config = IcebergConfig::rest_catalog(
    "http://localhost:8181".to_string(),
    "s3://bucket/warehouse".to_string()
);
```

### AWS Glue Catalog
```rust
let config = IcebergConfig::glue_catalog(
    "us-east-1".to_string(),
    "s3://bucket/warehouse".to_string()
);
```

## License

Licensed under the Apache License, Version 2.0.