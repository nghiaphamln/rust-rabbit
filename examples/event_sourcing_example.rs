use anyhow::Result;
use rust_rabbit::patterns::event_sourcing::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Example business domain: Bank Account
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BankAccount {
    id: AggregateId,
    sequence: EventSequence,
    balance: f64,
    holder_name: String,
    is_frozen: bool,
    uncommitted_events: Vec<DomainEvent>,
}

// Account events
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountOpened {
    holder_name: String,
    initial_balance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MoneyDeposited {
    amount: f64,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MoneyWithdrawn {
    amount: f64,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountFrozen {
    reason: String,
}

impl AggregateRoot for BankAccount {
    fn new(id: AggregateId) -> Self {
        Self {
            id,
            sequence: EventSequence::new(0),
            balance: 0.0,
            holder_name: String::new(),
            is_frozen: false,
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
            "AccountOpened" => {
                let event_data: AccountOpened = serde_json::from_slice(&event.event_data)?;
                self.holder_name = event_data.holder_name;
                self.balance = event_data.initial_balance;
                self.sequence = event.sequence;
            }
            "MoneyDeposited" => {
                let event_data: MoneyDeposited = serde_json::from_slice(&event.event_data)?;
                self.balance += event_data.amount;
                self.sequence = event.sequence;
            }
            "MoneyWithdrawn" => {
                let event_data: MoneyWithdrawn = serde_json::from_slice(&event.event_data)?;
                self.balance -= event_data.amount;
                self.sequence = event.sequence;
            }
            "AccountFrozen" => {
                self.is_frozen = true;
                self.sequence = event.sequence;
            }
            _ => return Err(anyhow::anyhow!("Unknown event type: {}", event.event_type)),
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
        let snapshot_data = serde_json::to_vec(&self)?;
        Ok(AggregateSnapshot::new(
            self.id.clone(),
            "BankAccount".to_string(),
            self.sequence,
            snapshot_data,
        ))
    }

    fn from_snapshot(snapshot: AggregateSnapshot) -> Result<Self> {
        let account: BankAccount = serde_json::from_slice(&snapshot.data)?;
        Ok(account)
    }
}

impl BankAccount {
    // Business operations that generate events
    fn open_account(&mut self, holder_name: String, initial_balance: f64) -> Result<()> {
        if self.sequence.value() > 0 {
            return Err(anyhow::anyhow!("Account already opened"));
        }

        let event_data = AccountOpened {
            holder_name: holder_name.clone(),
            initial_balance,
        };

        let event = DomainEvent::new(
            self.id.clone(),
            "BankAccount".to_string(),
            "AccountOpened".to_string(),
            serde_json::to_vec(&event_data)?,
            self.sequence.next(),
        );

        self.apply_event(&event)?;
        self.uncommitted_events.push(event);
        Ok(())
    }

    fn deposit(&mut self, amount: f64, description: String) -> Result<()> {
        if amount <= 0.0 {
            return Err(anyhow::anyhow!("Deposit amount must be positive"));
        }
        if self.is_frozen {
            return Err(anyhow::anyhow!("Account is frozen"));
        }

        let event_data = MoneyDeposited {
            amount,
            description,
        };

        let event = DomainEvent::new(
            self.id.clone(),
            "BankAccount".to_string(),
            "MoneyDeposited".to_string(),
            serde_json::to_vec(&event_data)?,
            self.sequence.next(),
        );

        self.apply_event(&event)?;
        self.uncommitted_events.push(event);
        Ok(())
    }

    fn withdraw(&mut self, amount: f64, description: String) -> Result<()> {
        if amount <= 0.0 {
            return Err(anyhow::anyhow!("Withdrawal amount must be positive"));
        }
        if self.is_frozen {
            return Err(anyhow::anyhow!("Account is frozen"));
        }
        if self.balance < amount {
            return Err(anyhow::anyhow!("Insufficient funds"));
        }

        let event_data = MoneyWithdrawn {
            amount,
            description,
        };

        let event = DomainEvent::new(
            self.id.clone(),
            "BankAccount".to_string(),
            "MoneyWithdrawn".to_string(),
            serde_json::to_vec(&event_data)?,
            self.sequence.next(),
        );

        self.apply_event(&event)?;
        self.uncommitted_events.push(event);
        Ok(())
    }

    fn freeze_account(&mut self, reason: String) -> Result<()> {
        if self.is_frozen {
            return Err(anyhow::anyhow!("Account is already frozen"));
        }

        let event_data = AccountFrozen { reason };

        let event = DomainEvent::new(
            self.id.clone(),
            "BankAccount".to_string(),
            "AccountFrozen".to_string(),
            serde_json::to_vec(&event_data)?,
            self.sequence.next(),
        );

        self.apply_event(&event)?;
        self.uncommitted_events.push(event);
        Ok(())
    }

    // Getters
    pub fn balance(&self) -> f64 {
        self.balance
    }

    pub fn holder_name(&self) -> &str {
        &self.holder_name
    }

    pub fn is_frozen(&self) -> bool {
        self.is_frozen
    }
}

/// Example demonstrating event sourcing pattern
#[tokio::main]
async fn main() -> Result<()> {
    println!("📚 RustRabbit Event Sourcing Demo");
    println!("=================================");
    println!("Scenario: Bank Account Management");
    println!();

    // Demo 1: Basic event sourcing operations
    println!("📋 Demo 1: Basic Event Sourcing Operations");
    demo_basic_operations().await?;

    println!("\n{}", "=".repeat(50));

    // Demo 2: Event replay and reconstruction
    println!("📋 Demo 2: Event Replay and Reconstruction");
    demo_event_replay().await?;

    println!("\n{}", "=".repeat(50));

    // Demo 3: Snapshots
    println!("📋 Demo 3: Snapshots and Performance");
    demo_snapshots().await?;

    println!("\n✅ Event sourcing demos completed!");
    Ok(())
}

async fn demo_basic_operations() -> Result<()> {
    // Create event store and repository
    let event_store = Arc::new(InMemoryEventStore::new());
    let repository = EventSourcingRepository::<BankAccount>::new(event_store.clone());

    // Create new account
    let account_id = AggregateId::new();
    let mut account = BankAccount::new(account_id.clone());

    println!("🏦 Opening new bank account...");
    account.open_account("John Doe".to_string(), 1000.0)?;

    println!("   Account holder: {}", account.holder_name());
    println!("   Initial balance: ${:.2}", account.balance());

    // Save the account (this persists the AccountOpened event)
    repository.save(&mut account).await?;

    // Perform some transactions
    println!("\n💰 Performing transactions...");

    account.deposit(250.0, "Salary deposit".to_string())?;
    println!("   ✅ Deposited $250.00");

    account.withdraw(100.0, "ATM withdrawal".to_string())?;
    println!("   ✅ Withdrew $100.00");

    account.deposit(75.0, "Refund".to_string())?;
    println!("   ✅ Deposited $75.00");

    // Save all transactions
    repository.save(&mut account).await?;

    println!("\n📊 Final account state:");
    println!("   Balance: ${:.2}", account.balance());
    println!("   Event sequence: {}", account.sequence().value());
    println!("   Total events in store: {}", event_store.event_count());

    Ok(())
}

async fn demo_event_replay() -> Result<()> {
    // Create event store and add some events
    let event_store = Arc::new(InMemoryEventStore::new());
    let repository = EventSourcingRepository::<BankAccount>::new(event_store.clone());

    // Create and populate account
    let account_id = AggregateId::new();
    let mut account = BankAccount::new(account_id.clone());

    println!("🏦 Creating account with transaction history...");
    account.open_account("Alice Smith".to_string(), 500.0)?;
    account.deposit(300.0, "Initial deposit".to_string())?;
    account.withdraw(150.0, "Rent payment".to_string())?;
    account.deposit(200.0, "Freelance payment".to_string())?;
    account.freeze_account("Suspicious activity detected".to_string())?;

    // Save the account
    repository.save(&mut account).await?;

    println!(
        "   Created account with {} events",
        account.sequence().value()
    );
    println!("   Account frozen: {}", account.is_frozen());

    // Now demonstrate replay
    println!("\n🔄 Replaying events to reconstruct account state...");

    // Load account from event store (this replays all events)
    let reconstructed_account = repository.load(&account_id).await?.unwrap();

    println!("   ✅ Account reconstructed from events!");
    println!("   Holder: {}", reconstructed_account.holder_name());
    println!("   Balance: ${:.2}", reconstructed_account.balance());
    println!("   Frozen: {}", reconstructed_account.is_frozen());
    println!("   Sequence: {}", reconstructed_account.sequence().value());

    // Demonstrate event replay service
    println!("\n📖 Using event replay service...");
    let replay_service = EventReplayService::new(event_store);
    let events = replay_service.replay_aggregate(&account_id).await?;

    println!("   Event history:");
    for (i, event) in events.iter().enumerate() {
        println!(
            "   {}. {} at sequence {}",
            i + 1,
            event.event_type,
            event.sequence.value()
        );
    }

    Ok(())
}

async fn demo_snapshots() -> Result<()> {
    // Create event store with snapshot frequency of 3 events
    let event_store = Arc::new(InMemoryEventStore::new());
    let repository =
        EventSourcingRepository::<BankAccount>::new(event_store.clone()).with_snapshot_frequency(3);

    let account_id = AggregateId::new();
    let mut account = BankAccount::new(account_id.clone());

    println!("🏦 Creating account and generating events to trigger snapshots...");

    // Generate enough events to trigger snapshots
    account.open_account("Bob Johnson".to_string(), 1000.0)?;
    repository.save(&mut account).await?;

    account.deposit(100.0, "Deposit 1".to_string())?;
    repository.save(&mut account).await?;

    account.deposit(200.0, "Deposit 2".to_string())?;
    repository.save(&mut account).await?; // This should trigger a snapshot

    account.withdraw(50.0, "Withdrawal 1".to_string())?;
    repository.save(&mut account).await?;

    account.deposit(300.0, "Deposit 3".to_string())?;
    repository.save(&mut account).await?;

    println!("   Events generated: {}", account.sequence().value());
    println!("   Total events in store: {}", event_store.event_count());
    println!("   Snapshots in store: {}", event_store.snapshot_count());

    // Load account - should use snapshot + remaining events
    println!("\n📸 Loading account using snapshots...");
    let loaded_account = repository.load(&account_id).await?.unwrap();

    println!("   ✅ Account loaded efficiently using snapshots!");
    println!("   Balance: ${:.2}", loaded_account.balance());
    println!("   Sequence: {}", loaded_account.sequence().value());

    Ok(())
}

// Utility function to print event details
fn print_event_details(events: &[DomainEvent]) {
    for (i, event) in events.iter().enumerate() {
        println!(
            "   {}. {} (seq: {})",
            i + 1,
            event.event_type,
            event.sequence.value()
        );
    }
}
