use anyhow::Result;
use rust_rabbit::patterns::saga::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// Example business domain: E-commerce order processing saga
#[derive(Debug)]
struct OrderProcessingSaga;

// Payment service executor
#[derive(Debug)]
struct PaymentExecutor;

#[async_trait::async_trait]
impl SagaStepExecutor for PaymentExecutor {
    async fn execute_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("💳 Processing payment...");
        let default_order = "unknown".to_string();
        let default_amount = "0".to_string();
        let order_id = context.get("order_id").unwrap_or(&default_order);
        let amount = context.get("amount").unwrap_or(&default_amount);

        println!("   Order ID: {}", order_id);
        println!("   Amount: ${}", amount);

        // Simulate payment processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut result_context = HashMap::new();
        result_context.insert("payment_id".to_string(), "PAY_12345".to_string());
        result_context.insert("payment_status".to_string(), "completed".to_string());

        println!("   ✅ Payment processed successfully");
        Ok(StepResult::Success(result_context))
    }

    async fn compensate_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("💳 🔄 Refunding payment...");
        let default_payment = "unknown".to_string();
        let payment_id = context.get("payment_id").unwrap_or(&default_payment);

        println!("   Refunding payment: {}", payment_id);

        // Simulate refund processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("   ✅ Payment refunded successfully");
        Ok(StepResult::Success(HashMap::new()))
    }
}

// Inventory service executor
#[derive(Debug)]
struct InventoryExecutor {
    should_fail: bool,
}

impl InventoryExecutor {
    fn new(should_fail: bool) -> Self {
        Self { should_fail }
    }
}

#[async_trait::async_trait]
impl SagaStepExecutor for InventoryExecutor {
    async fn execute_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("📦 Reserving inventory...");
        let default_order = "unknown".to_string();
        let default_item = "unknown".to_string();
        let default_quantity = "1".to_string();
        let order_id = context.get("order_id").unwrap_or(&default_order);
        let item_id = context.get("item_id").unwrap_or(&default_item);
        let quantity = context.get("quantity").unwrap_or(&default_quantity);

        println!("   Order ID: {}", order_id);
        println!("   Item: {}", item_id);
        println!("   Quantity: {}", quantity);

        // Simulate inventory check
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.should_fail {
            println!("   ❌ Insufficient inventory!");
            return Ok(StepResult::Failure("Insufficient inventory".to_string()));
        }

        let mut result_context = HashMap::new();
        result_context.insert("reservation_id".to_string(), "RES_67890".to_string());
        result_context.insert("inventory_status".to_string(), "reserved".to_string());

        println!("   ✅ Inventory reserved successfully");
        Ok(StepResult::Success(result_context))
    }

    async fn compensate_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("📦 🔄 Releasing inventory...");
        let default_reservation = "unknown".to_string();
        let reservation_id = context
            .get("reservation_id")
            .unwrap_or(&default_reservation);

        println!("   Releasing reservation: {}", reservation_id);

        // Simulate inventory release
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("   ✅ Inventory released successfully");
        Ok(StepResult::Success(HashMap::new()))
    }
}

// Shipping service executor
#[derive(Debug)]
struct ShippingExecutor;

#[async_trait::async_trait]
impl SagaStepExecutor for ShippingExecutor {
    async fn execute_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("🚚 Arranging shipping...");
        let default_order = "unknown".to_string();
        let default_address = "unknown".to_string();
        let order_id = context.get("order_id").unwrap_or(&default_order);
        let address = context.get("address").unwrap_or(&default_address);

        println!("   Order ID: {}", order_id);
        println!("   Address: {}", address);

        // Simulate shipping arrangement
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut result_context = HashMap::new();
        result_context.insert("tracking_id".to_string(), "TRACK_ABCDE".to_string());
        result_context.insert("shipping_status".to_string(), "scheduled".to_string());

        println!("   ✅ Shipping arranged successfully");
        Ok(StepResult::Success(result_context))
    }

    async fn compensate_step(
        &self,
        _action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        println!("🚚 🔄 Canceling shipping...");
        let default_tracking = "unknown".to_string();
        let tracking_id = context.get("tracking_id").unwrap_or(&default_tracking);

        println!("   Canceling shipment: {}", tracking_id);

        // Simulate shipping cancellation
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("   ✅ Shipping canceled successfully");
        Ok(StepResult::Success(HashMap::new()))
    }
}

/// Example demonstrating saga pattern for distributed transactions
#[tokio::main]
async fn main() -> Result<()> {
    println!("🔄 RustRabbit Saga Pattern Demo");
    println!("===============================");
    println!("Scenario: E-commerce Order Processing");
    println!();

    // Demo 1: Successful order processing
    println!("📋 Demo 1: Successful Order Processing");
    demo_successful_order().await?;

    println!("\n{}", "=".repeat(50));

    // Demo 2: Failed order with compensation
    println!("📋 Demo 2: Failed Order with Compensation");
    demo_failed_order().await?;

    println!("\n✅ Saga pattern demos completed!");
    Ok(())
}

async fn demo_successful_order() -> Result<()> {
    let mut coordinator = SagaCoordinator::new();

    // Register step executors
    coordinator.register_executor("payment".to_string(), Arc::new(PaymentExecutor));
    coordinator.register_executor(
        "inventory".to_string(),
        Arc::new(InventoryExecutor::new(false)),
    );
    coordinator.register_executor("shipping".to_string(), Arc::new(ShippingExecutor));

    // Create saga steps for order processing
    let steps = vec![
        SagaStep {
            step_id: "payment".to_string(),
            action: SagaAction::new("payment".to_string(), b"process_payment".to_vec()),
            compensation: Some(SagaAction::new(
                "payment".to_string(),
                b"refund_payment".to_vec(),
            )),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        },
        SagaStep {
            step_id: "inventory".to_string(),
            action: SagaAction::new("inventory".to_string(), b"reserve_inventory".to_vec()),
            compensation: Some(SagaAction::new(
                "inventory".to_string(),
                b"release_inventory".to_vec(),
            )),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        },
        SagaStep {
            step_id: "shipping".to_string(),
            action: SagaAction::new("shipping".to_string(), b"arrange_shipping".to_vec()),
            compensation: Some(SagaAction::new(
                "shipping".to_string(),
                b"cancel_shipping".to_vec(),
            )),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        },
    ];

    // Create saga instance
    let mut saga = SagaInstance::new("order_processing".to_string(), steps);
    saga.add_context("order_id".to_string(), "ORD_123456".to_string());
    saga.add_context("amount".to_string(), "99.99".to_string());
    saga.add_context("item_id".to_string(), "ITEM_789".to_string());
    saga.add_context("quantity".to_string(), "2".to_string());
    saga.add_context(
        "address".to_string(),
        "123 Main St, City, State".to_string(),
    );

    let saga_id = saga.saga_id.clone();
    println!("🚀 Starting order processing saga: {}", saga_id);

    // Start saga execution - this will process all steps automatically
    coordinator.start_saga(saga).await?;

    // Give some time for the saga to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check final status
    if let Some(status) = coordinator.get_saga_status(&saga_id) {
        println!("📊 Final saga status: {:?}", status);
    }

    Ok(())
}

async fn demo_failed_order() -> Result<()> {
    let mut coordinator = SagaCoordinator::new();

    // Register step executors (inventory will fail)
    coordinator.register_executor("payment".to_string(), Arc::new(PaymentExecutor));
    coordinator.register_executor(
        "inventory".to_string(),
        Arc::new(InventoryExecutor::new(true)),
    ); // Will fail
    coordinator.register_executor("shipping".to_string(), Arc::new(ShippingExecutor));

    // Create saga steps
    let steps = vec![
        SagaStep {
            step_id: "payment".to_string(),
            action: SagaAction::new("payment".to_string(), b"process_payment".to_vec()),
            compensation: Some(SagaAction::new(
                "payment".to_string(),
                b"refund_payment".to_vec(),
            )),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        },
        SagaStep {
            step_id: "inventory".to_string(),
            action: SagaAction::new("inventory".to_string(), b"reserve_inventory".to_vec()),
            compensation: Some(SagaAction::new(
                "inventory".to_string(),
                b"release_inventory".to_vec(),
            )),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        },
    ];

    // Create saga instance
    let mut saga = SagaInstance::new("failed_order_processing".to_string(), steps);
    saga.add_context("order_id".to_string(), "ORD_FAIL123".to_string());
    saga.add_context("amount".to_string(), "149.99".to_string());
    saga.add_context("item_id".to_string(), "ITEM_OUT_OF_STOCK".to_string());
    saga.add_context("quantity".to_string(), "5".to_string());

    let saga_id = saga.saga_id.clone();
    println!("🚀 Starting order processing saga: {}", saga_id);

    // Start saga execution - this will process all steps and handle failures automatically
    coordinator.start_saga(saga).await?;

    // Give some time for the saga to complete (including compensation)
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Check final status
    if let Some(status) = coordinator.get_saga_status(&saga_id) {
        println!("📊 Final saga status: {:?}", status);
    }

    Ok(())
}
