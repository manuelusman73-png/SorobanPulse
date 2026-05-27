//! Issue #373: Streaming Parquet export with bounded memory usage.
//! Writes Parquet row groups incrementally as batches are fetched from the database.

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::io::Cursor;
use std::sync::Arc;
use uuid::Uuid;

pub struct EventRow {
    pub id: Uuid,
    pub contract_id: String,
    pub event_type: String,
    pub tx_hash: String,
    pub ledger: i64,
    pub timestamp: DateTime<Utc>,
    pub event_data: Value,
    pub created_at: DateTime<Utc>,
}

/// Create a Parquet schema for events.
pub fn create_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("ledger", DataType::Int64, false),
        Field::new("timestamp_us", DataType::Int64, false),
        Field::new("event_data", DataType::Utf8, false),
        Field::new("created_at_us", DataType::Int64, false),
    ]))
}

/// Convert a batch of EventRows to a RecordBatch.
pub fn events_to_batch(schema: &Arc<Schema>, events: &[EventRow]) -> Result<RecordBatch, parquet::errors::ParquetError> {
    let ids: ArrayRef = Arc::new(StringArray::from(
        events.iter().map(|e| e.id.to_string()).collect::<Vec<_>>(),
    ));
    let contract_ids: ArrayRef = Arc::new(StringArray::from(
        events.iter().map(|e| e.contract_id.clone()).collect::<Vec<_>>(),
    ));
    let event_types: ArrayRef = Arc::new(StringArray::from(
        events.iter().map(|e| e.event_type.clone()).collect::<Vec<_>>(),
    ));
    let tx_hashes: ArrayRef = Arc::new(StringArray::from(
        events.iter().map(|e| e.tx_hash.clone()).collect::<Vec<_>>(),
    ));
    let ledgers: ArrayRef = Arc::new(Int64Array::from(
        events.iter().map(|e| e.ledger).collect::<Vec<_>>(),
    ));
    let timestamps: ArrayRef = Arc::new(Int64Array::from(
        events
            .iter()
            .map(|e| e.timestamp.timestamp_micros())
            .collect::<Vec<_>>(),
    ));
    let event_datas: ArrayRef = Arc::new(StringArray::from(
        events
            .iter()
            .map(|e| e.event_data.to_string())
            .collect::<Vec<_>>(),
    ));
    let created_ats: ArrayRef = Arc::new(Int64Array::from(
        events
            .iter()
            .map(|e| e.created_at.timestamp_micros())
            .collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(
        schema.clone(),
        vec![
            ids, contract_ids, event_types, tx_hashes, ledgers, timestamps, event_datas,
            created_ats,
        ],
    )
    .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
}

/// Serialize a slice of `EventRow` to Parquet bytes (legacy, for backward compatibility).
pub fn write_events_parquet(events: &[EventRow]) -> Result<Vec<u8>, parquet::errors::ParquetError> {
    let schema = create_schema();
    let batch = events_to_batch(&schema, events)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

/// Create a streaming Parquet writer for incremental writes.
pub fn create_streaming_writer(batch_size: usize) -> Result<StreamingParquetWriter, parquet::errors::ParquetError> {
    let schema = create_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let buf = Cursor::new(Vec::new());
    let writer = ArrowWriter::try_new(buf, schema.clone(), Some(props))?;

    Ok(StreamingParquetWriter {
        writer,
        schema,
        batch_size,
    })
}

/// Streaming Parquet writer that writes row groups incrementally.
pub struct StreamingParquetWriter {
    writer: ArrowWriter<Cursor<Vec<u8>>>,
    schema: Arc<Schema>,
    batch_size: usize,
}

impl StreamingParquetWriter {
    /// Write a batch of events as a row group.
    pub fn write_batch(&mut self, events: &[EventRow]) -> Result<(), parquet::errors::ParquetError> {
        if events.is_empty() {
            return Ok(());
        }
        let batch = events_to_batch(&self.schema, events)?;
        self.writer.write(&batch)?;
        Ok(())
    }

    /// Finalize the writer and return the complete Parquet bytes.
    pub fn finish(mut self) -> Result<Vec<u8>, parquet::errors::ParquetError> {
        self.writer.close()?;
        Ok(self.writer.into_inner().into_inner())
    }
}
