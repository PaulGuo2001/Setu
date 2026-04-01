use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, ExecutionContext, InMemoryStateStore, RuntimeExecutor,
    StateStore, SuiVmArg, Transaction,
};
use setu_types::{deterministic_coin_id, Address};

const CONTRACT: &str = r#"module my_coin_pkg::my_coin {
    use sui::coin::{Self, Coin, TreasuryCap};
    use sui::tx_context::{Self, TxContext};
    use sui::transfer;
    use std::option;

    public struct MY_COIN has drop {}

    fun init(witness: MY_COIN, ctx: &mut TxContext) {
        let (treasury_cap, metadata) = coin::create_currency(
            witness,
            9,
            b"MYC",
            b"My Coin",
            b"An example Sui coin",
            option::none(),
            ctx,
        );

        transfer::public_transfer(treasury_cap, tx_context::sender(ctx));
        transfer::public_freeze_object(metadata);
    }

    fun mint_to(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        amount: u64,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        let coin = coin::mint(treasury_cap, amount, ctx);
        transfer::public_transfer(coin, recipient);
    }

    fun maybe_mint_to(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        amount: u64,
        recipient: address,
        should_transfer: bool,
        ctx: &mut TxContext,
    ) {
        if (should_transfer) {
            mint_to(treasury_cap, amount, recipient, ctx);
        };
    }

    fun reward_pair(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        primary_amount: u64,
        primary_recipient: address,
        secondary_amount: u64,
        secondary_recipient: address,
        mint_secondary: bool,
        ctx: &mut TxContext,
    ) {
        mint_to(treasury_cap, primary_amount, primary_recipient, ctx);
        maybe_mint_to(
            treasury_cap,
            secondary_amount,
            secondary_recipient,
            mint_secondary,
            ctx,
        );
    }

    public entry fun orchestrate_rewards(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        primary_amount: u64,
        primary_recipient: address,
        secondary_amount: u64,
        secondary_recipient: address,
        mint_secondary: bool,
        ctx: &mut TxContext,
    ) {
        reward_pair(
            treasury_cap,
            primary_amount,
            primary_recipient,
            secondary_amount,
            secondary_recipient,
            mint_secondary,
            ctx,
        );
    }

    fun burn_inner(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        coin: Coin<MY_COIN>,
    ) {
        coin::burn(treasury_cap, coin);
    }

    public entry fun burn_via_helper(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        coin: Coin<MY_COIN>,
    ) {
        burn_inner(treasury_cap, coin);
    }
}"#;

fn main() -> Result<()> {
    let pkg = create_temp_package_with_contract()?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "my_coin")
        .context("Failed to compile and disassemble Sui contract")?;
    println!("Compiled + disassembled module: my_coin");

    let state = InMemoryStateStore::new();
    let mut executor = RuntimeExecutor::new(state);
    let sender = Address::from_str_id("alice");
    let recipient = Address::from_str_id("bob");
    let third_recipient = Address::from_str_id("carol");
    let coin_type = "MY_COIN";

    // 1) Execute a nested helper flow that mints to alice and conditionally mints to bob.
    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "orchestrate_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::U64(100),
            SuiVmArg::Address(sender),
            SuiVmArg::U64(40),
            SuiVmArg::Address(recipient),
            SuiVmArg::Bool(true),
            SuiVmArg::Opaque,
        ],
        2,
    )?;

    let alice_coin_id = deterministic_coin_id(&sender, coin_type);
    let alice_coin = executor
        .state()
        .get_object(&alice_coin_id)?
        .context("alice coin missing after orchestrate_rewards")?;
    let bob_coin_id = deterministic_coin_id(&recipient, coin_type);
    let bob_coin = executor
        .state()
        .get_object(&bob_coin_id)?
        .context("bob coin missing after nested helper flow")?;
    println!(
        "After orchestrate_rewards(true), alice balance = {}, bob balance = {}",
        alice_coin.data.balance.value(),
        bob_coin.data.balance.value()
    );

    // 2) Run the same nested flow with the secondary branch disabled.
    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "orchestrate_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::U64(10),
            SuiVmArg::Address(sender),
            SuiVmArg::U64(55),
            SuiVmArg::Address(recipient),
            SuiVmArg::Bool(false),
            SuiVmArg::Opaque,
        ],
        3,
    )?;

    let alice_after_second = executor
        .state()
        .get_object(&alice_coin_id)?
        .context("alice coin missing after orchestrate_rewards(false)")?;
    let bob_after_false = executor
        .state()
        .get_object(&bob_coin_id)?
        .context("bob coin missing after orchestrate_rewards(false)")?;
    if alice_after_second.data.balance.value() != 110 {
        bail!(
            "expected alice balance 110 after second orchestrate_rewards, got {}",
            alice_after_second.data.balance.value()
        );
    }
    if bob_after_false.data.balance.value() != 40 {
        bail!(
            "orchestrate_rewards(false) should skip the secondary helper call, got bob balance {}",
            bob_after_false.data.balance.value()
        );
    }
    println!(
        "After orchestrate_rewards(false), alice balance = {}, bob balance remains {}",
        alice_after_second.data.balance.value(),
        bob_after_false.data.balance.value()
    );

    // 3) Exercise a second nested flow that deposits into bob's existing coin and creates carol's coin.
    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "orchestrate_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::U64(15),
            SuiVmArg::Address(recipient),
            SuiVmArg::U64(5),
            SuiVmArg::Address(third_recipient),
            SuiVmArg::Bool(true),
            SuiVmArg::Opaque,
        ],
        4,
    )?;

    let bob_after_top_up = executor
        .state()
        .get_object(&bob_coin_id)?
        .context("bob coin missing after top-up flow")?;
    let carol_coin_id = deterministic_coin_id(&third_recipient, coin_type);
    let carol_coin = executor
        .state()
        .get_object(&carol_coin_id)?
        .context("carol coin missing after top-up flow")?;
    if bob_after_top_up.data.balance.value() != 55 {
        bail!(
            "expected bob balance 55 after top-up flow, got {}",
            bob_after_top_up.data.balance.value()
        );
    }
    if carol_coin.data.balance.value() != 5 {
        bail!(
            "expected carol balance 5 after top-up flow, got {}",
            carol_coin.data.balance.value()
        );
    }
    println!(
        "After second helper flow, bob balance = {}, carol balance = {}",
        bob_after_top_up.data.balance.value(),
        carol_coin.data.balance.value()
    );

    // 4) Burn bob's coin through a helper-mediated entry function.
    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "burn_via_helper",
        vec![SuiVmArg::Opaque, SuiVmArg::ObjectId(bob_coin_id)],
        5,
    )?;

    let post_burn = executor.state().get_object(&bob_coin_id)?;
    if post_burn.is_some() {
        bail!("Burn failed: bob coin still exists");
    }
    println!("After burn_via_helper, bob coin deleted while carol's coin remains");

    println!("\nE2E compile -> disassemble -> RuntimeExecutor execution completed.");
    Ok(())
}

fn execute_program_tx(
    executor: &mut RuntimeExecutor<InMemoryStateStore>,
    sender: &Address,
    disassembly: &str,
    function_name: &str,
    args: Vec<SuiVmArg>,
    timestamp: u64,
) -> Result<()> {
    let tx = Transaction::new_program_deterministic(
        sender.clone(),
        disassembly.to_owned(),
        function_name,
        args,
        timestamp,
    );
    let ctx = ExecutionContext {
        executor_id: "sui_contract_e2e".to_string(),
        timestamp,
        in_tee: false,
    };

    executor
        .execute_transaction(&tx, &ctx)
        .with_context(|| format!("Failed to execute '{}' via RuntimeExecutor", function_name))?;

    Ok(())
}

fn create_temp_package_with_contract() -> Result<PathBuf> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let root = std::env::temp_dir().join(format!("setu_sui_e2e_{}", ts));
    fs::create_dir_all(&root)?;

    let status = Command::new("sui")
        .arg("move")
        .arg("new")
        .arg("my_coin_pkg")
        .current_dir(&root)
        .status()
        .context("Failed to execute `sui move new`")?;
    if !status.success() {
        bail!("`sui move new` failed with status {}", status);
    }

    let pkg = root.join("my_coin_pkg");
    let src = pkg.join("sources");
    let default_module = src.join("my_coin_pkg.move");
    if default_module.exists() {
        fs::remove_file(default_module)?;
    }
    fs::write(src.join("my_coin.move"), CONTRACT)?;

    Ok(pkg)
}
