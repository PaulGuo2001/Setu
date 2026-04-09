#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

// 演示 Setu Runtime 基本功能的示例程序

use setu_runtime::{
    RuntimeExecutor, ExecutionContext, Transaction, InMemoryStateStore, StateStore,
};
use setu_types::Address;
use sui_example_utils::{ExampleArgs, ExampleState};

fn setup_state() -> anyhow::Result<(ExampleState<InMemoryStateStore>, ExampleArgs)> {
    tracing_subscriber::fmt::init();

    println!("\n=== Setu Runtime 演示 ===\n");

    let mut store = InMemoryStateStore::new();

    let alice = Address::from_hex("0xc0a6c424ac7157ae408398df7e5f4552091a69125d5dfcb7b8c2659029395bdf").unwrap();
    let bob = Address::from_hex("0xeedf1a9c68b3f4a8b1a1032b2b5ad5c4795c026514f8317c7a215e218dccd6cf").unwrap();

    println!("👤 Alice: {}", alice);
    println!("👤 Bob: {}", bob);
    println!();

    let alice_coin = setu_types::create_coin(alice.clone(), 1000);
    let alice_coin_id = *alice_coin.id();
    println!("💰 创建 Coin for Alice: {} (余额: 1000 SETU)", alice_coin_id);
    store.set_object(alice_coin_id, alice_coin)?;

    let bob_coin = setu_types::create_coin(bob.clone(), 500);
    let bob_coin_id = *bob_coin.id();
    println!("💰 创建 Coin for Bob: {} (余额: 500 SETU)", bob_coin_id);
    store.set_object(bob_coin_id, bob_coin)?;

    println!("\n✅ Runtime 执行器已创建\n");

    let mut args = ExampleArgs::new("basic_transfer");
    args.insert_address("alice", alice);
    args.insert_address("bob", bob);
    args.insert_object_id("alice_coin_id", alice_coin_id);
    args.insert_object_id("bob_coin_id", bob_coin_id);

    Ok((ExampleState::new(RuntimeExecutor::new(store)), args))
}

fn execute_scenario(
    mut state: ExampleState<InMemoryStateStore>,
    args: &ExampleArgs,
) -> anyhow::Result<ExampleState<InMemoryStateStore>> {
    println!("=== 测试 1: 部分转账 ===");
    println!("📤 Alice 转账 300 SETU 给 Bob");

    let tx1 = Transaction::new_transfer(
        args.address("alice")?,
        args.object_id("alice_coin_id")?,
        args.address("bob")?,
        Some(300),
    );

    let ctx = ExecutionContext {
        executor_id: "solver1".to_string(),
        timestamp: 1000,
        in_tee: false,
    };

    let output1 = state.executor.execute_transaction(&tx1, &ctx)?;
    println!("✅ 交易成功: {}", output1.message.unwrap());
    println!("   - 状态变更: {} 条", output1.state_changes.len());
    println!("   - 创建新对象: {} 个", output1.created_objects.len());

    let transferred_coin_id = output1
        .created_objects
        .first()
        .copied()
        .expect("partial transfer creates coin");

    println!();
    println!("=== 测试 2: 完整转账 ===");
    println!("📤 Bob 完全转账新 Coin 给 Alice");

    let tx2 = Transaction::new_transfer(
        args.address("bob")?,
        transferred_coin_id,
        args.address("alice")?,
        None,
    );

    let ctx2 = ExecutionContext {
        executor_id: "solver2".to_string(),
        timestamp: 2000,
        in_tee: false,
    };

    let output2 = state.executor.execute_transaction(&tx2, &ctx2)?;
    println!("✅ 交易成功: {}", output2.message.unwrap());
    println!("   - 状态变更: {} 条", output2.state_changes.len());
    println!("   - 创建新对象: {} 个", output2.created_objects.len());

    println!();
    println!("=== 测试 3: 查询余额 ===");

    let query_tx = Transaction::new_balance_query(args.address("alice")?);
    let ctx3 = ExecutionContext {
        executor_id: "solver3".to_string(),
        timestamp: 3000,
        in_tee: false,
    };

    let output3 = state.executor.execute_transaction(&query_tx, &ctx3)?;
    println!("✅ 查询成功");
    println!("   - Alice 总余额: {:?}", output3.query_result.unwrap());
    println!();

    Ok(state)
}

fn assert_state(state: &ExampleState<InMemoryStateStore>, args: &ExampleArgs) -> anyhow::Result<()> {
    let alice_coin = state
        .executor
        .state()
        .get_object(&args.object_id("alice_coin_id")?)?
        .unwrap();
    println!("   - Alice 剩余余额: {} SETU", alice_coin.data.balance.value());

    let bob = args.address("bob")?;
    let alice = args.address("alice")?;
    let bob_coin_id = args.object_id("bob_coin_id")?;
    let transferred_coin = state
        .executor
        .state()
        .get_owned_objects(&alice)?
        .into_iter()
        .find(|object_id| *object_id != args.object_id("alice_coin_id").unwrap())
        .and_then(|object_id| state.executor.state().get_object(&object_id).ok().flatten())
        .expect("transferred coin owned by Alice");

    println!(
        "   - Coin 新所有者: {}",
        transferred_coin.metadata.owner.as_ref().unwrap()
    );

    let alice_total = state.executor.state().get_total_balance(&alice);
    println!("   - Alice 所有 Coin 总额: {} SETU", alice_total);

    let bob_total = state.executor.state().get_total_balance(&bob);
    println!("   - Bob 所有 Coin 总额: {} SETU", bob_total);
    println!();

    if alice_total != 1000 {
        anyhow::bail!("expected Alice total 1000 SETU, got {}", alice_total);
    }
    if bob_total != 500 {
        anyhow::bail!("expected Bob total 500 SETU, got {}", bob_total);
    }
    if transferred_coin.metadata.owner.as_ref() != Some(&alice) {
        anyhow::bail!("expected transferred coin owner to be Alice");
    }
    if state.executor.state().get_object(&bob_coin_id)?.is_none() {
        anyhow::bail!("expected Bob's original coin to remain present");
    }

    println!("=== 演示完成 ===\n");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (state, args) = setup_state()?;
    let state = execute_scenario(state, &args)?;
    assert_state(&state, &args)
}
