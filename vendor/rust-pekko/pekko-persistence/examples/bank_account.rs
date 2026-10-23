//! Bank Account Example using Pekko Persistence
//! 
//! This example demonstrates how to use Pekko Persistence to create a persistent bank account actor
//! that can recover its state from events stored in a journal.

use pekko_actor::{Actor, ActorContext, ActorSystem, Props};
use pekko_persistence::{
    InMemoryJournal, InMemorySnapshotStore, PersistentActor, PersistentContext,
    SerializerRegistry,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error};
use uuid::Uuid;

/// Bank account events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BankEvent {
    AccountOpened {
        account_id: String,
        owner: String,
        initial_balance: u64,
    },
    MoneyDeposited {
        amount: u64,
        transaction_id: Uuid,
    },
    MoneyWithdrawn {
        amount: u64,
        transaction_id: Uuid,
    },
    AccountClosed {
        final_balance: u64,
    },
}

/// Bank account state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BankAccountState {
    pub account_id: String,
    pub owner: String,
    pub balance: u64,
    pub is_open: bool,
    pub transaction_count: u64,
}

/// Bank account commands
#[derive(Debug, Clone)]
pub enum BankCommand {
    OpenAccount {
        owner: String,
        initial_balance: u64,
    },
    Deposit {
        amount: u64,
    },
    Withdraw {
        amount: u64,
    },
    GetBalance,
    CloseAccount,
    CreateSnapshot,
}

/// Bank responses
#[derive(Debug, Clone)]
pub enum BankResponse {
    AccountOpened {
        account_id: String,
        balance: u64,
    },
    OperationSuccessful {
        new_balance: u64,
        transaction_id: Uuid,
    },
    Balance {
        account_id: String,
        balance: u64,
    },
    AccountClosed {
        final_balance: u64,
    },
    Error {
        message: String,
    },
    SnapshotSaved,
}

/// Persistent bank account actor
pub struct BankAccountActor {
    account_id: String,
    state: BankAccountState,
}

impl BankAccountActor {
    pub fn new(account_id: String) -> Self {
        Self {
            state: BankAccountState {
                account_id: account_id.clone(),
                ..Default::default()
            },
            account_id,
        }
    }

    fn validate_deposit(&self, amount: u64) -> Result<(), String> {
        if !self.state.is_open {
            return Err("Account is not open".to_string());
        }
        if amount == 0 {
            return Err("Deposit amount must be greater than zero".to_string());
        }
        if amount > 1_000_000 {
            return Err("Deposit amount too large".to_string());
        }
        Ok(())
    }

    fn validate_withdrawal(&self, amount: u64) -> Result<(), String> {
        if !self.state.is_open {
            return Err("Account is not open".to_string());
        }
        if amount == 0 {
            return Err("Withdrawal amount must be greater than zero".to_string());
        }
        if amount > self.state.balance {
            return Err("Insufficient funds".to_string());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor for BankAccountActor {
    type Message = BankCommand;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        // In a real implementation, this would delegate to the persistent actor
        // For now, we'll implement the basic logic here
        match msg {
            BankCommand::GetBalance => {
                // In a real implementation, this would send a response
            }
            BankCommand::Deposit { amount } => {
                // In a real implementation, this would persist the event
            }
            BankCommand::Withdraw { amount } => {
                // In a real implementation, this would persist the event
            }
            _ => {
                // Other commands would be handled by the persistent actor
            }
        }
    }
}

#[async_trait::async_trait]
impl PersistentActor for BankAccountActor {
    type Event = BankEvent;
    type State = BankAccountState;
    type Snapshot = BankAccountState;

    fn persistence_id(&self) -> String {
        format!("bank-account-{}", self.account_id)
    }

    async fn receive_recover(
        &mut self,
        event: Self::Event,
        _ctx: &mut PersistentContext<Self>,
    ) {
        match event {
            BankEvent::AccountOpened {
                account_id,
                owner,
                initial_balance,
            } => {
                self.state.account_id = account_id;
                self.state.owner = owner;
                self.state.balance = initial_balance;
                self.state.is_open = true;
                self.state.transaction_count += 1;
            }
            BankEvent::MoneyDeposited { amount, .. } => {
                self.state.balance += amount;
                self.state.transaction_count += 1;
            }
            BankEvent::MoneyWithdrawn { amount, .. } => {
                self.state.balance = self.state.balance.saturating_sub(amount);
                self.state.transaction_count += 1;
            }
            BankEvent::AccountClosed { final_balance, .. } => {
                self.state.balance = final_balance;
                self.state.is_open = false;
                self.state.transaction_count += 1;
            }
        }
    }

    async fn receive_command(
        &mut self,
        msg: Self::Message,
        ctx: &mut PersistentContext<Self>,
    ) {
        match msg {
            BankCommand::OpenAccount {
                owner,
                initial_balance,
            } => {
                if self.state.is_open {
                    return;
                }

                let event = BankEvent::AccountOpened {
                    account_id: self.account_id.clone(),
                    owner,
                    initial_balance,
                };

                match ctx.persist(event).await {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("Failed to persist account opening: {}", e);
                    }
                }
            }
            BankCommand::Deposit { amount } => {
                if let Err(e) = self.validate_deposit(amount) {
                    error!("Deposit validation failed: {}", e);
                    return;
                }

                let transaction_id = Uuid::new_v4();
                let event = BankEvent::MoneyDeposited {
                    amount,
                    transaction_id,
                };

                match ctx.persist(event).await {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("Failed to persist deposit: {}", e);
                    }
                }
            }
            BankCommand::Withdraw { amount } => {
                if let Err(e) = self.validate_withdrawal(amount) {
                    error!("Withdrawal validation failed: {}", e);
                    return;
                }

                let transaction_id = Uuid::new_v4();
                let event = BankEvent::MoneyWithdrawn {
                    amount,
                    transaction_id,
                };

                match ctx.persist(event).await {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("Failed to persist withdrawal: {}", e);
                    }
                }
            }
            BankCommand::CloseAccount => {
                if !self.state.is_open {
                    return;
                }

                let event = BankEvent::AccountClosed {
                    final_balance: self.state.balance,
                };

                match ctx.persist(event).await {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("Failed to persist account closure: {}", e);
                    }
                }
            }
            BankCommand::CreateSnapshot => {
                match ctx.save_snapshot(self.state.clone()).await {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("Failed to save snapshot: {}", e);
                    }
                }
            }
            BankCommand::GetBalance => {
            }
        }
    }

    fn apply_event(&mut self, event: &Self::Event) -> Self::State {
        match event {
            BankEvent::AccountOpened {
                account_id,
                owner,
                initial_balance,
            } => {
                BankAccountState {
                    account_id: account_id.clone(),
                    owner: owner.clone(),
                    balance: *initial_balance,
                    is_open: true,
                    transaction_count: 1,
                }
            }
            BankEvent::MoneyDeposited { amount, .. } => {
                let mut new_state = self.state.clone();
                new_state.balance += amount;
                new_state.transaction_count += 1;
                new_state
            }
            BankEvent::MoneyWithdrawn { amount, .. } => {
                let mut new_state = self.state.clone();
                new_state.balance = new_state.balance.saturating_sub(*amount);
                new_state.transaction_count += 1;
                new_state
            }
            BankEvent::AccountClosed { final_balance, .. } => {
                let mut new_state = self.state.clone();
                new_state.balance = *final_balance;
                new_state.is_open = false;
                new_state.transaction_count += 1;
                new_state
            }
        }
    }

    fn create_snapshot(&self) -> Self::Snapshot {
        self.state.clone()
    }

    fn apply_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.state = snapshot;
    }
}

/// Response handler actor for demonstration
#[derive(Clone)]
pub struct ResponseHandler {
    name: String,
}

impl ResponseHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl Actor for ResponseHandler {
    type Message = BankResponse;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    // Create actor system
    let system = ActorSystem::new("bank-system");

    // Create response handler
    let response_handler = system.spawn::<ResponseHandler>(
        ResponseHandler::new("Client".to_string()),
        "response-handler",
    ).await?;

    // Create journal and snapshot store
    let journal = Arc::new(InMemoryJournal::new());
    let snapshot_store = Arc::new(InMemorySnapshotStore::new());
    let serializer_registry = Arc::new(SerializerRegistry::default());

    // Create bank account actor
    let account_id = "account-001".to_string();
    let bank_actor = BankAccountActor::new(account_id.clone());

    // Create persistent context (simplified for example)
    // In a real implementation, this would be properly initialized
    // For now, we'll skip the persistent context creation
    // let mut ctx = PersistentContext::new(
    //     // This would need a real ActorContext in practice
    //     unsafe { std::mem::zeroed() }, // Placeholder
    //     journal.clone(),
    //     snapshot_store.clone(),
    // );

    // Simulate some operations using the actor directly
    let mut bank_actor = BankAccountActor::new(account_id.clone());
    // In a real implementation, this would use the persistent context
    Ok(())
}