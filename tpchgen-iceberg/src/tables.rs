use crate::Result;
use arrow::array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriter;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::Catalog;
use std::sync::Arc;
use tpchgen_arrow::*;

pub(crate) async fn generate_table_part(
    source_data_iter: &mut dyn RecordBatchIterator,
    table: Arc<Table>,
    catalog: Arc<dyn Catalog>,
    data_file_writer: &mut DataFileWriter<
        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
    >,
) -> Result<()> {
    log::info!("Generated data part for table {}", table.identifier());

    let mut batches = Vec::new();
    while let Some(batch) = source_data_iter.next() {
        batches.push(batch);
    }

    if !batches.is_empty() {
        let data_files = write_batches_to_files(batches, data_file_writer).await?;
        log::info!("Generated {} data files", data_files.len());

        // Create and commit an iceberg transaction to append created DataFile(s) to the table
        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let append_action = tx.fast_append().add_data_files(data_files);
            let tx = append_action.apply(tx)?;
            tx.commit(catalog.as_ref()).await?;
            log::info!("Transaction commited to table {}", table.identifier());
        }
    }

    Ok(())
}

/// Write batches of data to an Iceberg table and return data files
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
