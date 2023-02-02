//! We keep all CubeStore metrics in one place for discoverability.
//! The convention is to prefix all metrics with `cs.` (short for CubeStore).

use crate::util::metrics;
use crate::util::metrics::{Counter, Gauge, Histogram};

/// The number of process startups.
pub static STARTUPS: Counter = metrics::counter("cs.startup");

/// Incoming SQL queries that do data reads.
pub static DATA_QUERIES: Counter = metrics::counter("cs.sql.query.data");
pub static DATA_QUERIES_CACHE_HIT: Counter = metrics::counter("cs.sql.query.data.cache.hit");
pub static DATA_QUERIES_CACHE_SIZE: Gauge = metrics::gauge("cs.sql.query.data.cache.size");
pub static DATA_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.data.ms");
/// Incoming SQL queries that only read metadata or do trivial computations.
pub static META_QUERIES: Counter = metrics::counter("cs.sql.query.meta");
pub static META_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.meta.ms");
/// Incoming cache queries.
pub static CACHE_QUERIES: Counter = metrics::counter("cs.sql.query.cache");
pub static CACHE_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.cache.ms");
/// Incoming queue queries.
pub static QUEUE_QUERIES: Counter = metrics::counter("cs.sql.query.queue");
pub static QUEUE_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.queue.ms");

/// Temporary for APM analysis
pub static STREAMING_ROWS_READ: Counter = metrics::counter("cs.streaming.rows");
pub static STREAMING_CHUNKS_READ: Counter = metrics::counter("cs.streaming.chunks");
pub static IN_MEMORY_CHUNKS_COUNT: Gauge = metrics::gauge("cs.workers.in_memory_chunks)");
pub static IN_MEMORY_CHUNKS_ROWS: Gauge = metrics::gauge("cs.workers.in_memory_chunks.rows)");
