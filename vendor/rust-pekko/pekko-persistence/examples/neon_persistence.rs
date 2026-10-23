//! Neon PostgreSQL Persistence Example
//!
//! This example demonstrates using Pekko Persistence with Neon (serverless PostgreSQL).
//!
//! # Setup
//! 1. Create a Neon account at https://neon.tech
//! 2. Create a new project and get the connection string
//! 3. Set the DATABASE_URL environment variable:
//!    export DATABASE_URL="postgres://user:pass@ep-xxx.region.aws.neon.tech/neondb?sslmode=require"
//!
//! # Run
//! cargo run --example neon_persistence --features postgres

use pekko_persistence::{
    postgres::{PostgresJournal, PostgresJournalConfig, PostgresSnapshotStore, PostgresSnapshotConfig},
    Journal, SnapshotStore, PersistentRepr, SnapshotMetadata,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Bank account events
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BankEvent {
    AccountOpened { owner: String, initial_balance: u64 },
    MoneyDeposited { amount: u64 },
    MoneyWithdrawn { amount: u64 },
}

/// Bank account state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BankAccountState {
    owner: String,
    balance: u64,
    is_open: bool,
    transaction_count: u64,
}

impl BankAccountState {
    fn apply(&mut self, event: &BankEvent) {
        match event {
            BankEvent::AccountOpened { owner, initial_balance } => {
                self.owner = owner.clone();
                self.balance = *initial_balance;
                self.is_open = true;
                self.transaction_count += 1;
            }
            BankEvent::MoneyDeposited { amount } => {
                self.balance += amount;
                self.transaction_count += 1;
            }
            BankEvent::MoneyWithdrawn { amount } => {
                self.balance = self.balance.saturating_sub(*amount);
                self.transaction_count += 1;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set.\n\
                 Example: postgres://user:pass@ep-xxx.region.aws.neon.tech/neondb?sslmode=require");

    println!("🚀 Connecting to Neon PostgreSQL...");

    // Create Journal
    let journal_config = PostgresJournalConfig::new(&database_url)
        .with_table_name("pekko_events");
    
    let journal = Arc::new(
        PostgresJournal::new(journal_config).await?
    );
    println!("✅ Journal initialized");

    // Create Snapshot Store (sharing the same pool)
    let snapshot_config = PostgresSnapshotConfig::new(&database_url)
        .with_table_name("pekko_snapshots");
    
    let snapshot_store = Arc::new(
        PostgresSnapshotStore::from_pool(journal.pool().clone(), snapshot_config).await?
    );
    println!("✅ Snapshot store initialized");

    // Demo: Bank account persistence
    let persistence_id = format!("bank-account-{}", uuid::Uuid::new_v4());
    println!("\n📝 Creating bank account: {}", persistence_id);

    // Event 1: Open account
    let event1 = BankEvent::AccountOpened {
        owner: "Paul".to_string(),
        initial_balance: 1000,
    };
    persist_event(&journal, &persistence_id, 1, &event1).await?;
    println!("   → Account opened with $1000");

    // Event 2: Deposit
    let event2 = BankEvent::MoneyDeposited { amount: 500 };
    persist_event(&journal, &persistence_id, 2, &event2).await?;
    println!("   → Deposited $500");

    // Event 3: Withdraw
    let event3 = BankEvent::MoneyWithdrawn { amount: 200 };
    persist_event(&journal, &persistence_id, 3, &event3).await?;
    println!("   → Withdrew $200");

    // Recover state from events
    println!("\n🔄 Recovering state from events...");
    let mut state = BankAccountState::default();
    
    let events = journal.replay_messages(&persistence_id, 1, u64::MAX, None).await?;
    println!("   Found {} events", events.len());
    
    for event_repr in &events {
        let event: BankEvent = serde_json::from_slice(&event_repr.payload)?;
        state.apply(&event);
    }

    println!("\n💰 Current state:");
    println!("   Owner: {}", state.owner);
    println!("   Balance: ${}", state.balance);
    println!("   Transactions: {}", state.transaction_count);

    // Save a snapshot
    println!("\n📸 Saving snapshot at sequence {}...", events.len());
    let snapshot_data = serde_json::to_vec(&state)?;
    let metadata = SnapshotMetadata {
        persistence_id: persistence_id.clone(),
        sequence_nr: events.len() as u64,
        timestamp: Utc::now(),
    };
    snapshot_store.save_snapshot(&metadata, snapshot_data).await?;
    println!("   ✅ Snapshot saved");

    // Demonstrate recovery from snapshot
    println!("\n🔄 Simulating restart - recovering from snapshot...");
    let loaded = snapshot_store.load_snapshot(&persistence_id, None, None).await?;
    
    if let Some((snap_meta, snap_data)) = loaded {
        let recovered: BankAccountState = serde_json::from_slice(&snap_data)?;
        println!("   ✅ Recovered from snapshot at sequence {}", snap_meta.sequence_nr);
        println!("   Balance: ${}", recovered.balance);
    }

    // Cleanup (optional - comment out to keep data)
    println!("\n🧹 Cleaning up test data...");
    journal.delete_messages_to(&persistence_id, u64::MAX).await?;
    snapshot_store.delete_snapshot(&metadata).await?;
    println!("   ✅ Cleaned up");

    println!("\n✨ Done!");
    
    Ok(())
}

async fn persist_event(
    journal: &PostgresJournal,
    persistence_id: &str,
    seq: u64,
    event: &BankEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(event)?;
    let repr = PersistentRepr::new(
        persistence_id.to_string(),
        seq,
        "BankEvent".to_string(),
        payload,
    );
    journal.write_messages(vec![repr]).await?;
    Ok(())
}
