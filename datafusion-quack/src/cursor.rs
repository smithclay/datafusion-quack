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
//!
//! Two locks: the buffers, held only to look up or file a batch, and the producer,
//! held while the next batch is computed. A FETCH of a batch already produced never
//! waits for the stream.
//!
//! Batches held for the client count against the session's DataFusion memory pool,
//! from when they are produced until they are acknowledged. When the pool is full,
//! the result fails with an `Out of Memory` error instead of growing.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use arrow_quack::{EncodedChunk, encode_record_batch};
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::MemoryReservation;
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

    /// Whether `other` is a clone of this handle.
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
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

impl Batch {
    fn bytes(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }
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

/// The produced batches, by dense index (inline ones included, from 1).
struct Buffers {
    /// Batches produced so far.
    produced: u64,
    /// The highest index the client has acknowledged.
    acked: u64,
    /// Produced batches no FETCH has asked for yet.
    ready: BTreeMap<u64, Batch>,
    /// Served FETCH responses, kept for a retry until acknowledged.
    served: BTreeMap<u64, Bytes>,
    /// How the stream ended; `None` while it runs.
    end: Option<Result<(), ClientError>>,
}

/// A result being fetched.
pub(crate) struct Cursor {
    /// Batches the PREPARE response carried.
    prepare_batches: u64,
    max_inflight_batches: u64,
    cancel: CancelHandle,
    last_activity: Mutex<Instant>,
    /// FETCHes in progress.
    fetching: AtomicUsize,
    buffers: Mutex<Buffers>,
    /// Held while the next batch is produced; `None` once the stream ended.
    producer: tokio::sync::Mutex<Option<BatchProducer>>,
    /// The memory of the batches in `buffers`.
    reservation: MemoryReservation,
}

impl Cursor {
    /// A cursor over what remains of `producer` after `prepare_batches` inline batches.
    /// `producer` is `None` when the stream already ended. The batches it holds are
    /// accounted in `reservation`.
    pub(crate) fn new(
        producer: Option<BatchProducer>,
        prepare_batches: u64,
        max_inflight_batches: u64,
        cancel: CancelHandle,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            prepare_batches,
            max_inflight_batches,
            cancel,
            last_activity: Mutex::new(Instant::now()),
            fetching: AtomicUsize::new(0),
            buffers: Mutex::new(Buffers {
                produced: prepare_batches,
                acked: prepare_batches,
                ready: BTreeMap::new(),
                served: BTreeMap::new(),
                end: producer.is_none().then_some(Ok(())),
            }),
            producer: tokio::sync::Mutex::new(producer),
            reservation,
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
    }

    /// How long since the last FETCH; zero while one is running.
    pub(crate) fn idle_for(&self, now: Instant) -> std::time::Duration {
        if self.fetching.load(Ordering::Acquire) > 0 {
            return std::time::Duration::ZERO;
        }
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

    fn buffers(&self) -> MutexGuard<'_, Buffers> {
        self.buffers.lock().unwrap_or_else(PoisonError::into_inner)
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
        // a FETCH that takes a long time to produce its batch isn't idle
        self.fetching.fetch_add(1, Ordering::AcqRel);
        let _fetching = scopeguard(|| {
            self.fetching.fetch_sub(1, Ordering::AcqRel);
            self.touch();
        });
        let request = Request {
            dense: batch_index.saturating_add(self.prepare_batches),
            dense_ack: ack_index.saturating_add(self.prepare_batches),
            batch_index,
            ack_index,
        };
        if let Some(response) = self.answer(&request)? {
            return Ok(response);
        }
        // produce up to the batch; another FETCH may produce it first
        let mut producer = self.producer.lock().await;
        loop {
            if let Some(response) = self.answer(&request)? {
                return Ok(response);
            }
            let Some(running) = producer.as_mut() else {
                // the stream ended (answer reports it), so this can't be reached
                return Err(ClientError::invalid_input("Result has been closed"));
            };
            let next = running.next().await.and_then(|batch| match batch {
                Some(batch) => {
                    self.reservation.try_grow(batch.bytes()).map_err(|e| {
                        ClientError::from(e).with_context("holding result batches for the client")
                    })?;
                    Ok(Some(batch))
                }
                None => Ok(None),
            });
            let mut buffers = self.buffers();
            match next {
                Ok(Some(batch)) => {
                    buffers.produced += 1;
                    let index = buffers.produced;
                    buffers.ready.insert(index, batch);
                }
                Ok(None) => buffers.end = Some(Ok(())),
                Err(error) => buffers.end = Some(Err(error)),
            }
            if buffers.end.is_some() {
                *producer = None;
            }
        }
    }

    /// Answers `request` from the buffers, or `None` when its batch is yet to be
    /// produced.
    fn answer(&self, request: &Request) -> Result<Option<Bytes>, ClientError> {
        let mut buffers = self.buffers();
        let buffers = &mut *buffers;
        if request.dense_ack > buffers.produced {
            // an ack of batches never sent would let the read-ahead limit run unbounded
            return Err(ClientError::invalid_input(format!(
                "FETCH acknowledges batch {}, which hasn't been sent",
                request.ack_index
            )));
        }
        if request.dense_ack > buffers.acked {
            buffers.acked = request.dense_ack;
            let first_kept = request.dense_ack + 1;
            let served = buffers.served.split_off(&first_kept);
            let ready = buffers.ready.split_off(&first_kept);
            let served = std::mem::replace(&mut buffers.served, served);
            let ready = std::mem::replace(&mut buffers.ready, ready);
            self.reservation.shrink(
                served.values().map(Bytes::len).sum::<usize>()
                    + ready.values().map(Batch::bytes).sum::<usize>(),
            );
        }
        if let Some(response) = buffers.served.get(&request.dense) {
            return Ok(Some(response.clone()));
        }
        if let Some(batch) = buffers.ready.remove(&request.dense) {
            let response = Bytes::from(encode_fetch_response(
                &MessageHeader::new(MessageType::FetchResponse),
                &batch.chunks,
                None,
                Some(request.batch_index),
            )?);
            // the response replaces the batch: a header more
            self.reservation.grow(response.len());
            self.reservation.shrink(batch.bytes());
            buffers.served.insert(request.dense, response.clone());
            return Ok(Some(response));
        }
        if request.dense <= buffers.produced {
            return Err(ClientError::invalid_input(format!(
                "Batch {} was already acknowledged",
                request.batch_index
            )));
        }
        match &buffers.end {
            None if request.dense.saturating_sub(buffers.acked) > self.max_inflight_batches => {
                Err(ClientError::invalid_input(format!(
                    "FETCH of batch {} is more than {} batches past the last acknowledged one",
                    request.batch_index, self.max_inflight_batches
                )))
            }
            None => Ok(None),
            Some(Err(error)) => Err(error.clone()),
            // the stream ended below this index
            Some(Ok(())) => Ok(Some(Bytes::from(encode_fetch_response(
                &MessageHeader::new(MessageType::FetchResponse),
                &[] as &[EncodedChunk],
                Some(buffers.produced - self.prepare_batches),
                None,
            )?))),
        }
    }
}

/// A FETCH, with its indices in the client's numbering and the dense one.
struct Request {
    dense: u64,
    dense_ack: u64,
    batch_index: u64,
    ack_index: u64,
}

/// Runs `f` when dropped, so it runs however the FETCH ends (an early return, an
/// error, or a dropped request).
fn scopeguard(f: impl FnOnce()) -> impl Drop {
    struct Guard<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for Guard<F> {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                f();
            }
        }
    }
    Guard(Some(f))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
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

    fn unbounded() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn cursor(batches: usize, inline: u64) -> Cursor {
        let cancel = CancelHandle::new();
        let producer = producer(batches, &cancel);
        Cursor::new(Some(producer), inline, 4, cancel, unbounded())
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
        let cursor = Cursor::new(Some(producer), 2, 4, cancel, unbounded());
        assert_eq!(decoded(&cursor.fetch(1, 0).await.unwrap()).2, Some(1));
        assert_eq!(decoded(&cursor.fetch(2, 1).await.unwrap()).2, Some(2));
        assert_eq!(
            decoded(&cursor.fetch(3, 2).await.unwrap()),
            (0, Some(2), None)
        );
    }

    #[tokio::test]
    async fn acks_of_unsent_batches_are_refused() {
        let cursor = cursor(5, 0);
        // would otherwise lift the read-ahead limit to a million batches
        let error = cursor.fetch(1_000_001, 1_000_000).await.unwrap_err();
        assert!(error.message.contains("hasn't been sent"), "{error}");
        assert!(cursor.fetch(1, u64::MAX).await.is_err());
        assert!(cursor.fetch(u64::MAX, 0).await.is_err());
        // the cursor still serves in order
        assert_eq!(decoded(&cursor.fetch(1, 0).await.unwrap()).2, Some(1));
    }

    #[tokio::test]
    async fn a_running_fetch_is_not_idle() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let stream = RecordBatchStreamAdapter::new(schema, futures::stream::pending());
        let cancel = CancelHandle::new();
        let producer = BatchProducer::new(Box::pin(stream), cancel.clone(), 1);
        let cursor = Arc::new(Cursor::new(Some(producer), 0, 4, cancel, unbounded()));
        let fetching = tokio::spawn({
            let cursor = Arc::clone(&cursor);
            async move { cursor.fetch(1, 0).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let later = Instant::now() + std::time::Duration::from_secs(3600);
        assert_eq!(cursor.idle_for(later), std::time::Duration::ZERO);
        fetching.abort();
        let _ = fetching.await;
        assert!(cursor.idle_for(later) > std::time::Duration::from_secs(3000));
    }

    #[tokio::test]
    async fn a_retry_answers_while_the_next_batch_is_produced() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64; 10]))],
        )
        .unwrap();
        // one batch, then a stream that never yields another
        let stream = futures::stream::iter([Ok(first)]).chain(futures::stream::pending());
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        let cancel = CancelHandle::new();
        let producer = BatchProducer::new(Box::pin(stream), cancel.clone(), 1);
        let cursor = Arc::new(Cursor::new(Some(producer), 0, 4, cancel, unbounded()));
        let first = cursor.fetch(1, 0).await.unwrap();
        let producing = tokio::spawn({
            let cursor = Arc::clone(&cursor);
            async move { cursor.fetch(2, 0).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let retried = tokio::time::timeout(std::time::Duration::from_secs(1), cursor.fetch(1, 0))
            .await
            .expect("a retry doesn't wait for the next batch");
        assert_eq!(retried.unwrap(), first);
        producing.abort();
    }

    #[tokio::test]
    async fn held_batches_count_against_the_memory_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100_000));
        let cancel = CancelHandle::new();
        let cursor = Cursor::new(
            Some(producer(5, &cancel)),
            0,
            4,
            cancel,
            MemoryConsumer::new("test").register(&pool),
        );
        cursor.fetch(3, 0).await.unwrap();
        let held = pool.reserved();
        assert!(held > 0);
        cursor.fetch(4, 3).await.unwrap();
        assert!(pool.reserved() < held, "acknowledged batches are released");
        drop(cursor);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn a_full_memory_pool_fails_the_result() {
        // room for one batch, not three
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(150));
        let cancel = CancelHandle::new();
        let cursor = Cursor::new(
            Some(producer(5, &cancel)),
            0,
            4,
            cancel,
            MemoryConsumer::new("test").register(&pool),
        );
        let error = cursor.fetch(3, 0).await.unwrap_err();
        assert_eq!(
            error.exception_type,
            crate::ExceptionType::OutOfMemory,
            "{error}"
        );
        assert!(
            cursor.fetch(1, 0).await.is_ok(),
            "batches already held are served"
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
