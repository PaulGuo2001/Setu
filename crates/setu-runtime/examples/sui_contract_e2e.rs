use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, translate_burn_from_disassembly,
    translate_mint_from_disassembly, ExecutionContext, InMemoryStateStore, RuntimeExecutor,
    StateStore, SuiBurnCall, SuiMintCall, Transaction,
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

    public entry fun mint(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        amount: u64,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        let coin = coin::mint(treasury_cap, amount, ctx);
        transfer::public_transfer(coin, recipient);
    }

    public entry fun burn(
        treasury_cap: &mut TreasuryCap<MY_COIN>,
        coin: Coin<MY_COIN>,
    ) {
        coin::burn(treasury_cap, coin);
    }
}"#;

fn main() -> Result<()> {
    let pkg = create_temp_package_with_contract()?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "my_coin")
        .context("Failed to compile and disassemble Sui contract")?;
    println!("Compiled + disassembled module: my_coin");

    let mut executor = RuntimeExecutor::new(InMemoryStateStore::new());
    let sender = Address::from_str_id("alice");
    let recipient = Address::from_str_id("bob");
    let coin_type = "MY_COIN".to_string();

    let ctx = ExecutionContext {
        executor_id: "sui-e2e".to_string(),
        timestamp: 1,
        in_tee: false,
    };

    // 1) Translate Sui mint entry -> Setu VM Program, then execute
    let mint_program = translate_mint_from_disassembly(
        &disassembly,
        &SuiMintCall {
            amount: 100,
            recipient,
            coin_type: coin_type.clone(),
        },
    )?;
    let mint_tx = Transaction::new_program(sender, mint_program);
    let mint_out = executor.execute_transaction(&mint_tx, &ctx)?;
    println!(
        "Mint: success={}, state_changes={}",
        mint_out.success,
        mint_out.state_changes.len()
    );

    let bob_coin_id = deterministic_coin_id(&recipient, &coin_type);
    let bob_coin = executor
        .state()
        .get_object(&bob_coin_id)?
        .context("Recipient coin missing after mint")?;
    println!(
        "After mint, bob balance = {}",
        bob_coin.data.balance.value()
    );

    // 2) Translate Sui burn entry -> Setu VM Program, then execute
    let burn_program = translate_burn_from_disassembly(
        &disassembly,
        &SuiBurnCall {
            coin_id: bob_coin_id,
        },
    )?;
    let burn_tx = Transaction::new_program(sender, burn_program);
    let burn_out = executor.execute_transaction(&burn_tx, &ctx)?;
    println!(
        "Burn: success={}, state_changes={}",
        burn_out.success,
        burn_out.state_changes.len()
    );

    let post_burn = executor.state().get_object(&bob_coin_id)?;
    if post_burn.is_some() {
        bail!("Burn failed: recipient coin still exists");
    }
    println!("After burn, bob coin deleted");

    println!("\nE2E compile -> translate -> VM execute completed.");
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
