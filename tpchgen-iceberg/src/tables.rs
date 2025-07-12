//! Table-specific Iceberg data generation

use crate::{IcebergError, Result};
use arrow::array::RecordBatch;
use iceberg::table::Table;
use iceberg::writer::IcebergWriter;
// use iceberg::writer::base_writer::data_file_writer::DataFileWriter;
use std::sync::Arc;
use iceberg::spec::DataFile;
use tpchgen_arrow::*;
use tpchgen::generators::*;

/// Writer for TPCH tables in Iceberg format
pub struct IcebergTableWriter {
    table: Arc<Table>,
    writer: Option<Box<dyn IcebergWriter<arrow::array::RecordBatch>>>,
}

impl IcebergTableWriter {
    /// Create a new Iceberg table writer
    pub async fn new(table: Arc<Table>) -> Result<Self> {
        Ok(IcebergTableWriter {
            table,
            writer: None,
        })
    }

    /// Initialize the writer
    pub async fn initialize(&mut self) -> Result<()> {
        // For now, we'll skip the actual writer initialization
        // This is a placeholder until we can determine the correct API
        Ok(())
    }

    /// Write a batch of records to the table
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.write(batch).await?;
        } else {
            return Err(IcebergError::Config("Writer not initialized".to_string()));
        }
        Ok(())
    }

    /// Close the writer and commit the data
    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        if let Some(mut writer) = self.writer.take() {
            Ok(writer.close().await.unwrap())
        } else {
            Err(IcebergError::Config("Writer already closed".to_string()))
        }
    }
}

/// Generate and write Region table data
pub async fn generate_region_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = RegionGenerator::new(scale_factor, 1, 1);
    let mut arrow_generator = RegionArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;

    //TODO create and commit an iceberg transaction to append created DataFile(s) to the table

    Ok(())
}

/// Generate and write Nation table data
pub async fn generate_nation_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = NationGenerator::new(scale_factor, 1, 1);
    let mut arrow_generator = NationArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write Customer table data
pub async fn generate_customer_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = CustomerGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = CustomerArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write Supplier table data
pub async fn generate_supplier_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = SupplierGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = SupplierArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write Part table data
pub async fn generate_part_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = PartGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = PartArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write PartSupp table data
pub async fn generate_partsupp_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = PartSuppGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = PartSuppArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write Orders table data
pub async fn generate_orders_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = OrderGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = OrderArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Generate and write LineItem table data
pub async fn generate_lineitem_table(
    table: Arc<Table>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
    part: usize,
) -> Result<()> {
    let mut writer = IcebergTableWriter::new(table).await?;
    writer.initialize().await?;

    let generator = LineItemGenerator::new(scale_factor, parts as i32, part as i32);
    let mut arrow_generator = LineItemArrow::new(generator).with_batch_size(batch_size);

    while let Some(batch) = arrow_generator.next() {
        writer.write_batch(batch).await?;
    }

    writer.close().await?;
    Ok(())
}

/// Table generation functions mapped by table name
pub fn get_table_generator(table_name: &str) -> Option<fn(Arc<Table>, f64, usize, usize, usize) -> Result<()>> {
    match table_name {
        "region" => Some(|table, sf, bs, _, _| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_region_table(table, sf, bs))
            })
        }),
        "nation" => Some(|table, sf, bs, _, _| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_nation_table(table, sf, bs))
            })
        }),
        "customer" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_customer_table(table, sf, bs, parts, part))
            })
        }),
        "supplier" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_supplier_table(table, sf, bs, parts, part))
            })
        }),
        "part" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_part_table(table, sf, bs, parts, part))
            })
        }),
        "partsupp" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_partsupp_table(table, sf, bs, parts, part))
            })
        }),
        "orders" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_orders_table(table, sf, bs, parts, part))
            })
        }),
        "lineitem" => Some(|table, sf, bs, parts, part| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(generate_lineitem_table(table, sf, bs, parts, part))
            })
        }),
        _ => None,
    }
}

/// Get all TPC-H table names
pub fn get_all_table_names() -> Vec<&'static str> {
    vec![
        "region",
        "nation", 
        "customer",
        "supplier",
        "part",
        "partsupp",
        "orders",
        "lineitem",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_table_names() {
        let names = get_all_table_names();
        assert_eq!(names.len(), 8);
        assert!(names.contains(&"region"));
        assert!(names.contains(&"lineitem"));
    }

    #[test]
    fn test_get_table_generator() {
        assert!(get_table_generator("region").is_some());
        assert!(get_table_generator("lineitem").is_some());
        assert!(get_table_generator("nonexistent").is_none());
    }
}