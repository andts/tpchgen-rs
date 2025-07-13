//! Table-specific Iceberg data generation

use crate::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tpchgen::generators::*;
use tpchgen_arrow::*;

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

pub fn get_schema(table_name: &str) -> SchemaRef {
    let generator: Box<dyn RecordBatchIterator> = match table_name {
        "region" => {
            Box::new(RegionArrow::new(RegionGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "nation" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "customer" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "supplier" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "part" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "partsupp" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "orders" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        "lineitem" => {
            Box::new(NationArrow::new(NationGenerator::default())) as Box<dyn RecordBatchIterator>
        }
        &_ => {
            panic!("Unknown table '{}'", table_name)
        }
    };

    generator.schema().clone()
}

/// Generate and write Region table data
pub async fn generate_region_table_data(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
) -> Result<()> {
    let generator = RegionGenerator::default();
    let mut arrow_generator = RegionArrow::new(generator);

    generate_table_data(&mut arrow_generator, table, catalog).await?;

    Ok(())
}

/// Generate and write Nation table data
pub async fn generate_nation_table_data(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
) -> Result<()> {
    let generator = NationGenerator::default();
    let mut arrow_generator = NationArrow::new(generator);

    generate_table_data(&mut arrow_generator, table, catalog).await?;

    Ok(())
}

/// Generate and write Customer table data
pub async fn generate_customer_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = CustomerGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = CustomerArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

/// Generate and write Supplier table data
pub async fn generate_supplier_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = SupplierGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = SupplierArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

/// Generate and write Part table data
pub async fn generate_part_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = PartGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = PartArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

/// Generate and write PartSupp table data
pub async fn generate_partsupp_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = PartSuppGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = PartSuppArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

/// Generate and write Orders table data
pub async fn generate_orders_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = OrderGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = OrderArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

/// Generate and write LineItem table data
pub async fn generate_lineitem_table(
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    scale_factor: f64,
    batch_size: usize,
    parts: usize,
) -> Result<()> {
    //TODO First generate all data files, then append them to a table in a tx
    for part in 1..=parts {
        let generator = LineItemGenerator::new(scale_factor, parts as i32, part as i32);
        let mut arrow_generator = LineItemArrow::new(generator).with_batch_size(batch_size);

        generate_table_data(&mut arrow_generator, table.clone(), catalog.clone()).await?;
    }
    Ok(())
}

async fn generate_table_data(
    source_data_iter: &mut dyn RecordBatchIterator,
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
) -> Result<()> {
    let mut batches = Vec::new();
    while let Some(batch) = source_data_iter.next() {
        batches.push(batch);
    }

    let schema = source_data_iter.schema();

    if !batches.is_empty() {
        let data_files = write_batches_to_files(table.clone(), batches, schema).await?;

        // Create and commit an iceberg transaction to append created DataFile(s) to the table
        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let append_action = tx.fast_append().add_data_files(data_files);
            let tx = append_action.apply(tx)?;
            tx.commit(catalog.as_ref()).await?;
        }
    }

    Ok(())
}

/// Write batches of data to an Iceberg table and return data files
async fn write_batches_to_files(
    table: Arc<Table>,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Vec<DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        table.identifier().name.clone(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let iceberg_schema = arrow_schema_to_schema(schema)?;

    // dbg!(&iceberg_schema);

    let schema_ref = Arc::new(iceberg_schema);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        //TODO optimize/parameterize writer properties
        // set compression?
        WriterProperties::default(),
        //TODO undertsand why schema from table is incorrect
        schema_ref,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
    let mut data_file_writer = data_file_writer_builder.build().await?;

    for batch in batches {
        data_file_writer.write(batch).await?;
    }

    let data_files = data_file_writer.close().await?;
    Ok(data_files)
}
