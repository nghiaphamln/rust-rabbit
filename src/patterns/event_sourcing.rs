use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};
use uuid::Uuid;

use crate::error::RustRabbitError;

/// Unique identifier for aggregate roots
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateId(String);

impl AggregateId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for AggregateId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AggregateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Event sequence number for ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventSequence(u64);

impl EventSequence {
    pub fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for EventSequence {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Domain event with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub event_id: String,
    pub aggregate_id: AggregateId,
    pub aggregate_type: String,
    pub event_type: String,
    pub event_data: Vec<u8>,
    pub metadata: HashMap<String, String>,
    pub sequence: EventSequence,
    pub timestamp: DateTime<Utc>,
    pub version: u32,
}

impl DomainEvent {
    pub fn new(
        aggregate_id: AggregateId,
        aggregate_type: String,
        event_type: String,
        event_data: Vec<u8>,
        sequence: EventSequence,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            aggregate_id,
            aggregate_type,
            event_type,
            event_data,
            metadata: HashMap::new(),
            sequence,
            timestamp: Utc::now(),
            version: 1,
        }
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }
}

/// Snapshot of aggregate state at a specific point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSnapshot {
    pub aggregate_id: AggregateId,
    pub aggregate_type: String,
    pub sequence: EventSequence,
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub version: u32,
}

impl AggregateSnapshot {
    pub fn new(
        aggregate_id: AggregateId,
        aggregate_type: String,
        sequence: EventSequence,
        data: Vec<u8>,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type,
            sequence,
            data,
            timestamp: Utc::now(),
            version: 1,
        }
    }
}

/// Event stream query parameters
#[derive(Debug, Clone)]
pub struct EventStreamQuery {
    pub aggregate_id: AggregateId,
    pub from_sequence: Option<EventSequence>,
    pub to_sequence: Option<EventSequence>,
    pub event_types: Option<Vec<String>>,
    pub limit: Option<usize>,
}

impl EventStreamQuery {
    pub fn for_aggregate(aggregate_id: AggregateId) -> Self {
        Self {
            aggregate_id,
            from_sequence: None,
            to_sequence: None,
            event_types: None,
            limit: None,
        }
    }

    pub fn from_sequence(mut self, sequence: EventSequence) -> Self {
        self.from_sequence = Some(sequence);
        self
    }

    pub fn to_sequence(mut self, sequence: EventSequence) -> Self {
        self.to_sequence = Some(sequence);
        self
    }

    pub fn with_event_types(mut self, event_types: Vec<String>) -> Self {
        self.event_types = Some(event_types);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Trait for event store implementations
#[async_trait::async_trait]
pub trait EventStore {
    /// Append events to the store
    async fn append_events(&self, events: Vec<DomainEvent>) -> Result<()>;

    /// Read events from the store
    async fn read_events(&self, query: EventStreamQuery) -> Result<Vec<DomainEvent>>;

    /// Get the latest sequence number for an aggregate
    async fn get_latest_sequence(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Option<EventSequence>>;

    /// Save a snapshot
    async fn save_snapshot(&self, snapshot: AggregateSnapshot) -> Result<()>;

    /// Load the latest snapshot for an aggregate
    async fn load_snapshot(&self, aggregate_id: &AggregateId) -> Result<Option<AggregateSnapshot>>;

    /// Check if aggregate exists
    async fn aggregate_exists(&self, aggregate_id: &AggregateId) -> Result<bool>;
}

/// In-memory event store implementation (for testing/development)
#[derive(Debug)]
pub struct InMemoryEventStore {
    events: Arc<Mutex<HashMap<AggregateId, Vec<DomainEvent>>>>,
    snapshots: Arc<Mutex<HashMap<AggregateId, AggregateSnapshot>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(HashMap::new())),
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn event_count(&self) -> usize {
        self.events
            .lock()
            .unwrap()
            .values()
            .map(|events| events.len())
            .sum()
    }

    pub fn snapshot_count(&self) -> usize {
        self.snapshots.lock().unwrap().len()
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl EventStore for InMemoryEventStore {
    async fn append_events(&self, events: Vec<DomainEvent>) -> Result<()> {
        let mut store = self.events.lock().unwrap();

        for event in events {
            let aggregate_id = event.aggregate_id.clone();

            debug!(
                aggregate_id = %aggregate_id,
                event_type = %event.event_type,
                sequence = event.sequence.value(),
                "Appending event to store"
            );

            let aggregate_events = store.entry(aggregate_id).or_default();

            // Ensure sequence ordering
            if let Some(last_event) = aggregate_events.last() {
                if event.sequence.value() <= last_event.sequence.value() {
                    return Err(RustRabbitError::EventSequenceError.into());
                }
            }

            aggregate_events.push(event);
        }

        Ok(())
    }

    async fn read_events(&self, query: EventStreamQuery) -> Result<Vec<DomainEvent>> {
        let store = self.events.lock().unwrap();

        let aggregate_events = store
            .get(&query.aggregate_id)
            .map(|events| events.as_slice())
            .unwrap_or(&[]);

        let mut filtered_events: Vec<DomainEvent> = aggregate_events
            .iter()
            .filter(|event| {
                // Filter by sequence range
                if let Some(from_seq) = query.from_sequence {
                    if event.sequence < from_seq {
                        return false;
                    }
                }
                if let Some(to_seq) = query.to_sequence {
                    if event.sequence > to_seq {
                        return false;
                    }
                }

                // Filter by event types
                if let Some(ref event_types) = query.event_types {
                    if !event_types.contains(&event.event_type) {
                        return false;
                    }
                }

                true
            })
            .cloned()
            .collect();

        // Apply limit
        if let Some(limit) = query.limit {
            filtered_events.truncate(limit);
        }

        debug!(
            aggregate_id = %query.aggregate_id,
            event_count = filtered_events.len(),
            "Read events from store"
        );

        Ok(filtered_events)
    }

    async fn get_latest_sequence(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Option<EventSequence>> {
        let store = self.events.lock().unwrap();

        let latest_sequence = store
            .get(aggregate_id)
            .and_then(|events| events.last())
            .map(|event| event.sequence);

        Ok(latest_sequence)
    }

    async fn save_snapshot(&self, snapshot: AggregateSnapshot) -> Result<()> {
        let mut store = self.snapshots.lock().unwrap();

        debug!(
            aggregate_id = %snapshot.aggregate_id,
            sequence = snapshot.sequence.value(),
            "Saving snapshot"
        );

        store.insert(snapshot.aggregate_id.clone(), snapshot);
        Ok(())
    }

    async fn load_snapshot(&self, aggregate_id: &AggregateId) -> Result<Option<AggregateSnapshot>> {
        let store = self.snapshots.lock().unwrap();
        Ok(store.get(aggregate_id).cloned())
    }

    async fn aggregate_exists(&self, aggregate_id: &AggregateId) -> Result<bool> {
        let store = self.events.lock().unwrap();
        Ok(store.contains_key(aggregate_id))
    }
}

/// Event sourcing repository for managing aggregates
pub struct EventSourcingRepository<T> {
    event_store: Arc<dyn EventStore + Send + Sync>,
    snapshot_frequency: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> EventSourcingRepository<T>
where
    T: AggregateRoot + Send + Sync,
{
    pub fn new(event_store: Arc<dyn EventStore + Send + Sync>) -> Self {
        Self {
            event_store,
            snapshot_frequency: 100, // Take snapshot every 100 events
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_snapshot_frequency(mut self, frequency: u64) -> Self {
        self.snapshot_frequency = frequency;
        self
    }

    /// Load aggregate from event store
    pub async fn load(&self, aggregate_id: &AggregateId) -> Result<Option<T>> {
        info!(aggregate_id = %aggregate_id, "Loading aggregate");

        // Try to load from snapshot first
        let (aggregate, from_sequence) =
            if let Some(snapshot) = self.event_store.load_snapshot(aggregate_id).await? {
                debug!(
                    aggregate_id = %aggregate_id,
                    sequence = snapshot.sequence.value(),
                    "Loaded from snapshot"
                );

                let aggregate = T::from_snapshot(snapshot)?;
                let from_sequence = aggregate.sequence().next();
                (Some(aggregate), Some(from_sequence))
            } else {
                (None, None)
            };

        // Load events since snapshot (or all events if no snapshot)
        let query = EventStreamQuery::for_aggregate(aggregate_id.clone());
        let query = if let Some(from_seq) = from_sequence {
            query.from_sequence(from_seq)
        } else {
            query
        };

        let events = self.event_store.read_events(query).await?;

        if events.is_empty() && aggregate.is_none() {
            return Ok(None);
        }

        // Apply events to aggregate
        let mut final_aggregate = aggregate.unwrap_or_else(|| T::new(aggregate_id.clone()));

        for event in events {
            final_aggregate.apply_event(&event)?;
        }

        debug!(
            aggregate_id = %aggregate_id,
            sequence = final_aggregate.sequence().value(),
            "Aggregate loaded successfully"
        );

        Ok(Some(final_aggregate))
    }

    /// Save aggregate to event store
    pub async fn save(&self, aggregate: &mut T) -> Result<()> {
        let uncommitted_events = aggregate.uncommitted_events();

        if uncommitted_events.is_empty() {
            debug!(aggregate_id = %aggregate.id(), "No uncommitted events to save");
            return Ok(());
        }

        info!(
            aggregate_id = %aggregate.id(),
            event_count = uncommitted_events.len(),
            "Saving aggregate"
        );

        // Append events to store
        self.event_store
            .append_events(uncommitted_events.clone())
            .await?;

        // Take snapshot if needed
        if aggregate.sequence().value().is_multiple_of(self.snapshot_frequency) {
            let snapshot = aggregate.create_snapshot()?;
            self.event_store.save_snapshot(snapshot).await?;

            debug!(
                aggregate_id = %aggregate.id(),
                sequence = aggregate.sequence().value(),
                "Snapshot created"
            );
        }

        // Mark events as committed
        aggregate.mark_events_committed();

        debug!(
            aggregate_id = %aggregate.id(),
            sequence = aggregate.sequence().value(),
            "Aggregate saved successfully"
        );

        Ok(())
    }

    /// Check if aggregate exists
    pub async fn exists(&self, aggregate_id: &AggregateId) -> Result<bool> {
        self.event_store.aggregate_exists(aggregate_id).await
    }
}

/// Trait for aggregate roots in event sourcing
pub trait AggregateRoot {
    /// Create new aggregate with given ID
    fn new(id: AggregateId) -> Self;

    /// Get aggregate ID
    fn id(&self) -> &AggregateId;

    /// Get current sequence number
    fn sequence(&self) -> EventSequence;

    /// Apply an event to the aggregate
    fn apply_event(&mut self, event: &DomainEvent) -> Result<()>;

    /// Get uncommitted events
    fn uncommitted_events(&self) -> Vec<DomainEvent>;

    /// Mark events as committed
    fn mark_events_committed(&mut self);

    /// Create snapshot of current state
    fn create_snapshot(&self) -> Result<AggregateSnapshot>;

    /// Restore from snapshot
    fn from_snapshot(snapshot: AggregateSnapshot) -> Result<Self>
    where
        Self: Sized;
}

/// Event replay service for rebuilding projections
pub struct EventReplayService {
    event_store: Arc<dyn EventStore + Send + Sync>,
}

impl EventReplayService {
    pub fn new(event_store: Arc<dyn EventStore + Send + Sync>) -> Self {
        Self { event_store }
    }

    /// Replay all events for an aggregate
    pub async fn replay_aggregate(&self, aggregate_id: &AggregateId) -> Result<Vec<DomainEvent>> {
        info!(aggregate_id = %aggregate_id, "Starting event replay");

        let query = EventStreamQuery::for_aggregate(aggregate_id.clone());
        let events = self.event_store.read_events(query).await?;

        info!(
            aggregate_id = %aggregate_id,
            event_count = events.len(),
            "Event replay completed"
        );

        Ok(events)
    }

    /// Replay events within a sequence range
    pub async fn replay_range(
        &self,
        aggregate_id: &AggregateId,
        from_sequence: EventSequence,
        to_sequence: EventSequence,
    ) -> Result<Vec<DomainEvent>> {
        info!(
            aggregate_id = %aggregate_id,
            from_sequence = from_sequence.value(),
            to_sequence = to_sequence.value(),
            "Starting event replay for range"
        );

        let query = EventStreamQuery::for_aggregate(aggregate_id.clone())
            .from_sequence(from_sequence)
            .to_sequence(to_sequence);

        let events = self.event_store.read_events(query).await?;

        info!(
            aggregate_id = %aggregate_id,
            event_count = events.len(),
            "Event replay range completed"
        );

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestAggregate {
        id: AggregateId,
        sequence: EventSequence,
        value: String,
        uncommitted_events: Vec<DomainEvent>,
    }

    impl AggregateRoot for TestAggregate {
        fn new(id: AggregateId) -> Self {
            Self {
                id,
                sequence: EventSequence::new(0),
                value: String::new(),
                uncommitted_events: Vec::new(),
            }
        }

        fn id(&self) -> &AggregateId {
            &self.id
        }

        fn sequence(&self) -> EventSequence {
            self.sequence
        }

        fn apply_event(&mut self, event: &DomainEvent) -> Result<()> {
            match event.event_type.as_str() {
                "ValueChanged" => {
                    self.value = String::from_utf8_lossy(&event.event_data).to_string();
                    self.sequence = event.sequence;
                }
                _ => return Err(RustRabbitError::UnknownEventType(event.event_type.clone()).into()),
            }
            Ok(())
        }

        fn uncommitted_events(&self) -> Vec<DomainEvent> {
            self.uncommitted_events.clone()
        }

        fn mark_events_committed(&mut self) {
            self.uncommitted_events.clear();
        }

        fn create_snapshot(&self) -> Result<AggregateSnapshot> {
            Ok(AggregateSnapshot::new(
                self.id.clone(),
                "TestAggregate".to_string(),
                self.sequence,
                self.value.as_bytes().to_vec(),
            ))
        }

        fn from_snapshot(snapshot: AggregateSnapshot) -> Result<Self> {
            Ok(Self {
                id: snapshot.aggregate_id,
                sequence: snapshot.sequence,
                value: String::from_utf8_lossy(&snapshot.data).to_string(),
                uncommitted_events: Vec::new(),
            })
        }
    }

    impl TestAggregate {
        fn change_value(&mut self, new_value: String) {
            let event = DomainEvent::new(
                self.id.clone(),
                "TestAggregate".to_string(),
                "ValueChanged".to_string(),
                new_value.as_bytes().to_vec(),
                self.sequence.next(),
            );

            self.apply_event(&event).unwrap();
            self.uncommitted_events.push(event);
        }
    }

    #[tokio::test]
    async fn test_aggregate_id_generation() {
        let id1 = AggregateId::new();
        let id2 = AggregateId::new();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn test_event_sequence() {
        let seq1 = EventSequence::new(1);
        let seq2 = seq1.next();
        assert_eq!(seq2.value(), 2);
        assert!(seq2 > seq1);
    }

    #[tokio::test]
    async fn test_in_memory_event_store() {
        let store = InMemoryEventStore::new();
        let aggregate_id = AggregateId::new();

        let event = DomainEvent::new(
            aggregate_id.clone(),
            "TestAggregate".to_string(),
            "TestEvent".to_string(),
            b"test data".to_vec(),
            EventSequence::new(1),
        );

        // Append event
        store.append_events(vec![event.clone()]).await.unwrap();

        // Read events
        let query = EventStreamQuery::for_aggregate(aggregate_id.clone());
        let events = store.read_events(query).await.unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "TestEvent");

        // Check latest sequence
        let latest_seq = store.get_latest_sequence(&aggregate_id).await.unwrap();
        assert_eq!(latest_seq, Some(EventSequence::new(1)));
    }

    #[tokio::test]
    async fn test_event_sourcing_repository() {
        let store = Arc::new(InMemoryEventStore::new());
        let repo = EventSourcingRepository::<TestAggregate>::new(store.clone());

        let aggregate_id = AggregateId::new();
        let mut aggregate = TestAggregate::new(aggregate_id.clone());

        // Modify aggregate
        aggregate.change_value("Hello".to_string());
        aggregate.change_value("World".to_string());

        // Save aggregate
        repo.save(&mut aggregate).await.unwrap();

        // Load aggregate
        let loaded_aggregate = repo.load(&aggregate_id).await.unwrap().unwrap();
        assert_eq!(loaded_aggregate.value, "World");
        assert_eq!(loaded_aggregate.sequence.value(), 2);
    }

    #[tokio::test]
    async fn test_snapshot_functionality() {
        let store = Arc::new(InMemoryEventStore::new());
        let repo =
            EventSourcingRepository::<TestAggregate>::new(store.clone()).with_snapshot_frequency(2); // Snapshot every 2 events

        let aggregate_id = AggregateId::new();
        let mut aggregate = TestAggregate::new(aggregate_id.clone());

        // Generate events to trigger snapshot
        aggregate.change_value("First".to_string());
        aggregate.change_value("Second".to_string());

        repo.save(&mut aggregate).await.unwrap();

        // Should have created a snapshot
        assert_eq!(store.snapshot_count(), 1);

        // Load aggregate (should use snapshot)
        let loaded_aggregate = repo.load(&aggregate_id).await.unwrap().unwrap();
        assert_eq!(loaded_aggregate.value, "Second");
        assert_eq!(loaded_aggregate.sequence.value(), 2);
    }

    #[tokio::test]
    async fn test_event_replay_service() {
        let store = Arc::new(InMemoryEventStore::new());
        let repo = EventSourcingRepository::<TestAggregate>::new(store.clone());
        let replay_service = EventReplayService::new(store);

        let aggregate_id = AggregateId::new();
        let mut aggregate = TestAggregate::new(aggregate_id.clone());

        // Create some events
        aggregate.change_value("Event1".to_string());
        aggregate.change_value("Event2".to_string());
        aggregate.change_value("Event3".to_string());

        repo.save(&mut aggregate).await.unwrap();

        // Replay all events
        let replayed_events = replay_service
            .replay_aggregate(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(replayed_events.len(), 3);

        // Replay range
        let range_events = replay_service
            .replay_range(&aggregate_id, EventSequence::new(2), EventSequence::new(3))
            .await
            .unwrap();
        assert_eq!(range_events.len(), 2);
    }
}
