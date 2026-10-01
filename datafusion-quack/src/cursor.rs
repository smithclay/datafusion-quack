//! Result cursors: a running query's stream, cut into numbered FETCH batches.
//!
//! The batches a PREPARE response carries inline come first. FETCH batches are
//! numbered from 1 after them. A batch stays until the client acknowledges it, so a
//! retried FETCH gets the same bytes. A FETCH past the last batch gets an empty
//! response carrying the batch total.
//!
//! DuckDB fetches ahead: it asks for several batch indices at once. A FETCH for a
//! later index produces the batches before it and keeps them for their own FETCH,
//! at most `max_inflight_batches` past the last acknowledged one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use arrow_quack::{EncodedChunk, encode_record_batch};
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use quack_protocol::server::{MessageHeader, MessageType, encode_fetch_response};
use tokio::sync::watch;

use crate::error::ClientError;

/// Cancels a statement. Clones share the signal.
#[derive(Clone, Debug)]
pub(crate) struct CancelHandle(Arc<watch::Sender<bool>>);

impl CancelHandle {
    pub(crate) fn new() -> Self {
        Self(Arc::new(watch::Sender::new(false)))
    }

    pub(crate) fn cancel(&self) {
        self.0.send_replace(true);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once the statement is cancelled.
    pub(crate) async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        // only fails when the sender is gone, and we hold it
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

/// One FETCH batch: the encoded chunks of one or more record batches.
pub(crate) struct Batch {
    pub(crate) chunks: Vec<EncodedChunk>,
    pub(crate) rows: usize,
}

/// Pulls record batches from a stream and encodes them into [`Batch`]es.
pub(crate) struct BatchProducer {
    stream: SendableRecordBatchStream,
    cancel: CancelHandle,
    target_bytes: usize,
}

impl BatchProducer {
    pub(crate) fn new(
        stream: SendableRecordBatchStream,
        cancel: CancelHandle,
        target_bytes: usize,
    ) -> Self {
        Self {
            stream,
            cancel,
            target_bytes,
        }
    }

    /// The next batch, `None` at the end of the stream.
    pub(crate) async fn next(&mut self) -> Result<Option<Batch>, ClientError> {
        let mut batch = Batch {
            chunks: Vec::new(),
            rows: 0,
        };
        let mut bytes = 0;
        while bytes < self.target_bytes {
            if self.cancel.is_cancelled() {
                return Err(ClientError::cancelled());
            }
            let next = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return Err(ClientError::cancelled()),
                next = self.stream.next() => next,
            };
            let Some(record_batch) = next else { break };
            let record_batch = record_batch?;
            for chunk in encode_record_batch(&record_batch)? {
                bytes += chunk.bytes.len();
                batch.rows += chunk.rows;
                batch.chunks.push(chunk);
            }
        }
        Ok((!batch.chunks.is_empty()).then_some(batch))
    }
}

/// Where the rest of the result comes from.
enum Source {
    Running(BatchProducer),
    Finished,
    Failed(ClientError),
}

struct CursorState {
    source: Source,
    /// Batches produced so far, inline ones included (dense indices 1..=produced).
    produced: u64,
    /// The highest dense index the client has acknowledged.
    acked: u64,
    /// Produced batches no FETCH has asked for yet.
    ready: BTreeMap<u64, Batch>,
    /// Served FETCH responses, kept for a retry until acknowledged.
    served: BTreeMap<u64, Bytes>,
}

/// A result being fetched.
pub(crate) struct Cursor {
    /// Batches the PREPARE response carried.
    prepare_batches: u64,
    max_inflight_batches: u64,
    cancel: CancelHandle,
    last_activity: Mutex<Instant>,
    state: tokio::sync::Mutex<CursorState>,
}

impl Cursor {
    /// A cursor over what remains of `producer` after `prepare_batches` inline batches.
    /// `producer` is `None` when the stream already ended.
    pub(crate) fn new(
        producer: Option<BatchProducer>,
        prepare_batches: u64,
        max_inflight_batches: u64,
        cancel: CancelHandle,
    ) -> Self {
        Self {
            prepare_batches,
            max_inflight_batches,
            cancel,
            last_activity: Mutex::new(Instant::now()),
            state: tokio::sync::Mutex::new(CursorState {
                source: producer.map_or(Source::Finished, Source::Running),
                produced: prepare_batches,
                acked: prepare_batches,
                ready: BTreeMap::new(),
                served: BTreeMap::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
    }

    pub(crate) fn idle_for(&self, now: Instant) -> std::time::Duration {
        let last = *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        now.saturating_duration_since(last)
    }

    fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    /// Answers a FETCH for client batch `batch_index`, acknowledging everything up to
    /// `ack_index`.
    pub(crate) async fn fetch(
        &self,
        batch_index: u64,
        ack_index: u64,
    ) -> Result<Bytes, ClientError> {
        if batch_index == 0 {
            return Err(ClientError::invalid_input(
                "FETCH_REQUEST is missing its batch index",
            ));
        }
        self.touch();
        let dense = batch_index.saturating_add(self.prepare_batches);
        let dense_ack = ack_index.saturating_add(self.prepare_batches);

        let mut state = self.state.lock().await;
        if dense_ack > state.acked {
            state.acked = dense_ack;
            state.served = state.served.split_off(&(dense_ack + 1));
            state.ready = state.ready.split_off(&(dense_ack + 1));
        }
        if let Some(response) = state.served.get(&dense) {
            return Ok(response.clone());
        }
        if let Some(batch) = state.ready.remove(&dense) {
            return serve(&mut state, dense, batch_index, batch);
        }
        if dense <= state.produced {
            return Err(ClientError::invalid_input(format!(
                "Batch {batch_index} was already acknowledged"
            )));
        }
        let running = matches!(state.source, Source::Running(_));
        if running && dense - state.acked > self.max_inflight_batches {
            return Err(ClientError::invalid_input(format!(
                "FETCH of batch {batch_index} is more than {} batches past the last acknowledged one",
                self.max_inflight_batches
            )));
        }

        while state.produced < dense {
            let Source::Running(producer) = &mut state.source else {
                break;
            };
            match producer.next().await {
                Ok(Some(batch)) => {
                    state.produced += 1;
                    let index = state.produced;
                    if index == dense {
                        self.touch();
                        return serve(&mut state, dense, batch_index, batch);
                    }
                    state.ready.insert(index, batch);
                }
                Ok(None) => state.source = Source::Finished,
                Err(error) => state.source = Source::Failed(error),
            }
        }
        self.touch();
        match &state.source {
            Source::Failed(error) => Err(error.clone()),
            _ => {
                // the stream ended below this index
                let total = state.produced - self.prepare_batches;
                Ok(Bytes::from(encode_fetch_response(
                    &MessageHeader::new(MessageType::FetchResponse),
                    &[] as &[EncodedChunk],
                    Some(total),
                    None,
                )?))
            }
        }
    }
}

fn serve(
    state: &mut CursorState,
    dense: u64,
    batch_index: u64,
    batch: Batch,
) -> Result<Bytes, ClientError> {
    let response = Bytes::from(encode_fetch_response(
        &MessageHeader::new(MessageType::FetchResponse),
        &batch.chunks,
        None,
        Some(batch_index),
    )?);
    state.served.insert(dense, response.clone());
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use quack_protocol::server::{QuackMessage, decode_request};

    use super::*;

    fn producer(batches: usize, cancel: &CancelHandle) -> BatchProducer {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let items: Vec<_> = (0..batches)
            .map(|b| {
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![b as i64; 10]))],
                )
                .unwrap())
            })
            .collect();
        let stream = RecordBatchStreamAdapter::new(schema, futures::stream::iter(items));
        // target 1 byte: one record batch per FETCH batch
        BatchProducer::new(Box::pin(stream), cancel.clone(), 1)
    }

    fn cursor(batches: usize, inline: u64) -> Cursor {
        let cancel = CancelHandle::new();
        let producer = producer(batches, &cancel);
        Cursor::new(Some(producer), inline, 4, cancel)
    }

    fn decoded(bytes: &Bytes) -> (usize, Option<u64>, Option<u64>) {
        match decode_request(bytes).unwrap() {
            QuackMessage::FetchResponse {
                results,
                total_batches,
                batch_index,
                ..
            } => (results.len(), total_batches, batch_index),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn batches_are_numbered_from_one_and_end_with_the_total() {
        let cursor = cursor(3, 0);
        for index in 1..=3 {
            let (chunks, total, batch) = decoded(&cursor.fetch(index, index - 1).await.unwrap());
            assert_eq!((chunks, total, batch), (1, None, Some(index)));
        }
        assert_eq!(
            decoded(&cursor.fetch(4, 3).await.unwrap()),
            (0, Some(3), None)
        );
        // and again, past the end
        assert_eq!(
            decoded(&cursor.fetch(9, 3).await.unwrap()),
            (0, Some(3), None)
        );
    }

    #[tokio::test]
    async fn retries_get_the_same_bytes_until_acknowledged() {
        let cursor = cursor(3, 0);
        let first = cursor.fetch(1, 0).await.unwrap();
        assert_eq!(cursor.fetch(1, 0).await.unwrap(), first);
        cursor.fetch(2, 1).await.unwrap();
        assert!(cursor.fetch(1, 1).await.is_err());
    }

    #[tokio::test]
    async fn fetching_ahead_keeps_the_skipped_batches() {
        let cursor = cursor(5, 0);
        assert_eq!(decoded(&cursor.fetch(3, 0).await.unwrap()).2, Some(3));
        assert_eq!(decoded(&cursor.fetch(1, 0).await.unwrap()).2, Some(1));
        assert_eq!(decoded(&cursor.fetch(2, 0).await.unwrap()).2, Some(2));
        // more than max_inflight_batches (4) past the ack
        assert!(cursor.fetch(9, 0).await.is_err());
    }

    #[tokio::test]
    async fn inline_batches_shift_the_numbering() {
        // two batches went inline: FETCH 1 is the third
        let cancel = CancelHandle::new();
        let mut producer = producer(4, &cancel);
        producer.next().await.unwrap();
        producer.next().await.unwrap();
        let cursor = Cursor::new(Some(producer), 2, 4, cancel);
        assert_eq!(decoded(&cursor.fetch(1, 0).await.unwrap()).2, Some(1));
        assert_eq!(decoded(&cursor.fetch(2, 1).await.unwrap()).2, Some(2));
        assert_eq!(
            decoded(&cursor.fetch(3, 2).await.unwrap()),
            (0, Some(2), None)
        );
    }

    #[tokio::test]
    async fn cancelled_cursors_fail_with_an_interrupt() {
        let cursor = cursor(3, 0);
        cursor.cancel();
        let error = cursor.fetch(1, 0).await.unwrap_err();
        assert!(error.message.contains("Interrupted"), "{error}");
    }
}
