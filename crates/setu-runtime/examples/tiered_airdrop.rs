#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, InMemoryStateStore, RuntimeExecutor, StateStore, SuiVmArg,
};
use setu_types::{deterministic_coin_id, Address};
use sui_example_utils::{create_temp_package_with_contract, execute_program_tx};

const CONTRACT: &str = r#"module tiered_airdrop::tiered_airdrop {
    use sui::coin::{Self, TreasuryCap};
    use sui::tx_context::{Self, TxContext};
    use sui::transfer;
    use std::option;

    public struct TIERED_AIRDROP has drop {}

    fun init(witness: TIERED_AIRDROP, ctx: &mut TxContext) {
        let (treasury_cap, metadata) = coin::create_currency(
            witness,
            9,
            b"AIR",
            b"Tiered Airdrop",
            b"Campaign rewards",
            option::none(),
            ctx,
        );

        transfer::public_transfer(treasury_cap, tx_context::sender(ctx));
        transfer::public_freeze_object(metadata);
    }

    fun mint_to(
        treasury_cap: &mut TreasuryCap<TIERED_AIRDROP>,
        amount: u64,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        let coin = coin::mint(treasury_cap, amount, ctx);
        transfer::public_transfer(coin, recipient);
    }

    fun base_reward(
        treasury_cap: &mut TreasuryCap<TIERED_AIRDROP>,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        mint_to(treasury_cap, 50, recipient, ctx);
    }

    fun vip_bonus(
        treasury_cap: &mut TreasuryCap<TIERED_AIRDROP>,
        recipient: address,
        is_vip: bool,
        ctx: &mut TxContext,
    ) {
        if (is_vip) {
            mint_to(treasury_cap, 25, recipient, ctx);
        };
    }

    fun streak_bonus(
        treasury_cap: &mut TreasuryCap<TIERED_AIRDROP>,
        recipient: address,
        has_streak: bool,
        ctx: &mut TxContext,
    ) {
        if (has_streak) {
            mint_to(treasury_cap, 10, recipient, ctx);
        };
    }

    public entry fun distribute_campaign_rewards(
        treasury_cap: &mut TreasuryCap<TIERED_AIRDROP>,
        recipient: address,
        is_vip: bool,
        has_streak: bool,
        ctx: &mut TxContext,
    ) {
        base_reward(treasury_cap, recipient, ctx);
        vip_bonus(treasury_cap, recipient, is_vip, ctx);
        streak_bonus(treasury_cap, recipient, has_streak, ctx);
    }
}"#;

fn main() -> Result<()> {
    let pkg = create_temp_package_with_contract("tiered_airdrop", "tiered_airdrop.move", CONTRACT)?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "tiered_airdrop")
        .context("Failed to compile tiered_airdrop package")?;
    println!("Compiled + disassembled module: tiered_airdrop");

    let state = InMemoryStateStore::new();
    let mut executor = RuntimeExecutor::new(state);
    let sender = Address::from_str_id("campaign_owner");
    let bob = Address::from_str_id("bob");
    let carol = Address::from_str_id("carol");
    let dave = Address::from_str_id("dave");
    let coin_type = "TIERED_AIRDROP";

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "distribute_campaign_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::Address(bob),
            SuiVmArg::Bool(true),
            SuiVmArg::Bool(true),
            SuiVmArg::Opaque,
        ],
        1,
        "tiered_airdrop",
    )?;

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "distribute_campaign_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::Address(carol),
            SuiVmArg::Bool(false),
            SuiVmArg::Bool(true),
            SuiVmArg::Opaque,
        ],
        2,
        "tiered_airdrop",
    )?;

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "distribute_campaign_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::Address(dave),
            SuiVmArg::Bool(false),
            SuiVmArg::Bool(false),
            SuiVmArg::Opaque,
        ],
        3,
        "tiered_airdrop",
    )?;

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "distribute_campaign_rewards",
        vec![
            SuiVmArg::Opaque,
            SuiVmArg::Address(bob),
            SuiVmArg::Bool(false),
            SuiVmArg::Bool(true),
            SuiVmArg::Opaque,
        ],
        4,
        "tiered_airdrop",
    )?;

    let bob_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&bob, coin_type))?
        .context("bob reward coin missing")?;
    let carol_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&carol, coin_type))?
        .context("carol reward coin missing")?;
    let dave_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&dave, coin_type))?
        .context("dave reward coin missing")?;

    if bob_coin.data.balance.value() != 145 {
        bail!(
            "expected bob reward total 145, got {}",
            bob_coin.data.balance.value()
        );
    }
    if carol_coin.data.balance.value() != 60 {
        bail!(
            "expected carol reward total 60, got {}",
            carol_coin.data.balance.value()
        );
    }
    if dave_coin.data.balance.value() != 50 {
        bail!(
            "expected dave reward total 50, got {}",
            dave_coin.data.balance.value()
        );
    }

    println!("Bob reward total   = {}", bob_coin.data.balance.value());
    println!("Carol reward total = {}", carol_coin.data.balance.value());
    println!("Dave reward total  = {}", dave_coin.data.balance.value());
    println!("\nTiered airdrop example completed.");
    Ok(())
}
