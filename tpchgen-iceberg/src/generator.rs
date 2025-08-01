use crate::catalog::IcebergCatalog;
use crate::{IcebergConfig, IcebergError, Result};
use arrow::array::RecordBatch;
use futures::StreamExt;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::DataFile;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::TableIdent;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tpchgen_arrow::RecordBatchIterator;

pub struct IcebergGenerator {
    catalog: IcebergCatalog,
    batch_size: usize,
    max_concurrent_parts: usize,
}

impl IcebergGenerator {
    /// Create a new Iceberg generator
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        let catalog = IcebergCatalog::new(&config).await?;

        Ok(IcebergGenerator {
            catalog,
            batch_size: 8192,
            max_concurrent_parts: 4,
        })
    }

    /// Set the batch size for Arrow record batches
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of concurrent table generations
    pub fn with_max_concurrent_parts(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent_parts = max_concurrent;
        self
    }

    /// Generate specified table
    pub async fn generate_table<I>(
        &self,
        table_name: &str,
        sources: Box<dyn Iterator<Item = I>>,
    ) -> Result<()>
    where
        I: RecordBatchIterator + Send + 'static,
    {
        log::info!("generate iceberg table {table_name}");

        let mut sources_iter = sources.peekable();

        // get schema from the first iterator
        let Some(first_iter) = sources_iter.peek() else {
            return Ok(()); // no data shrug
        };

        // Ensure namespace exists
        self.catalog.ensure_namespace().await?;

        let arrow_schema = Arc::clone(first_iter.schema());
        let iceberg_schema = arrow_schema_to_schema(arrow_schema.as_ref())?;

        self.catalog
            .create_table(table_name, iceberg_schema.clone(), vec![])
            .await?;

        let catalog = self.catalog.catalog().clone();
        let namespace = self.catalog.namespace().clone();
        let table_ident = TableIdent::new(namespace.name().clone(), table_name.to_string());
        let table = Arc::new(catalog.load_table(&table_ident).await?);

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            table.identifier().name.clone(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );

        let writer_properties = WriterProperties::builder()
            //TODO make configurable
            .set_compression(Compression::SNAPPY)
            .build();

        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_properties,
            Arc::new(iceberg_schema),
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);

        // Process parts in parallel and collect all data files
        let mut part_stream = futures::stream::iter(sources_iter)
            .map(|mut batch_iter| {
                let data_file_writer_builder = data_file_writer_builder.clone();
                let table_name = table_name.to_string();

                // Run each part generation on a separate thread
                tokio::task::spawn(async move {
                    let mut data_file_writer = data_file_writer_builder.build().await?;
                    generate_table_part(&mut batch_iter, &table_name, &mut data_file_writer).await
                })
            })
            .buffered(self.max_concurrent_parts); // Limit concurrency

        // Collect all data files from all parts
        let mut all_data_files = Vec::new();
        while let Some(result) = part_stream.next().await {
            // Handle the result from the spawned task
            let data_files =
                result.map_err(|e| IcebergError::Runtime(format!("Task join error: {}", e)))??;
            all_data_files.extend(data_files);
        }

        // Commit all data files in a single transaction
        if !all_data_files.is_empty() {
            let tx = Transaction::new(&table);
            let append_action = tx.fast_append().add_data_files(all_data_files.clone());
            let tx = append_action.apply(tx)?;
            tx.commit(catalog.as_ref()).await?;
            log::info!(
                "Committed {} data files to table {}",
                all_data_files.len(),
                table_name
            );
        }

        log::debug!("table {table_name} generated");

        Ok(())
    }
}

async fn generate_table_part(
    source_data_iter: &mut dyn RecordBatchIterator,
    table_name: &str,
    data_file_writer: &mut DataFileWriter<
        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
    >,
) -> Result<Vec<DataFile>> {
    log::info!("Generated data part for table {}", table_name);

    let mut batches = Vec::new();
    while let Some(batch) = source_data_iter.next() {
        batches.push(batch);
    }

    if !batches.is_empty() {
        let data_files = write_batches_to_files(batches, data_file_writer).await?;
        log::info!(
            "Generated {} data files for table {}",
            data_files.len(),
            table_name
        );
        Ok(data_files)
    } else {
        Ok(Vec::new())
    }
}

async fn write_batches_to_files(
    batches: Vec<RecordBatch>,
    data_file_writer: &mut DataFileWriter<
        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
    >,
) -> Result<Vec<DataFile>> {
    for batch in batches {
        data_file_writer.write(batch).await?;
    }

    let data_files = data_file_writer.close().await?;
    Ok(data_files)
}
