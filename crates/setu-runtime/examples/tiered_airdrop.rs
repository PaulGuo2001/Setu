#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

use anyhow::{Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, InMemoryStateStore, RuntimeExecutor, SuiVmArg,
};
use sui_example_utils::{
    create_temp_package_with_contract, execute_program_scenario, expect_coin_balance, ExampleState,
    ProgramCallSpec,
};

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

fn setup_state() -> Result<(ExampleState<InMemoryStateStore>, Vec<ProgramCallSpec>)> {
    let pkg = create_temp_package_with_contract("tiered_airdrop", "tiered_airdrop.move", CONTRACT)?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "tiered_airdrop")
        .context("Failed to compile tiered_airdrop package")?;
    println!("Compiled + disassembled module: tiered_airdrop");

    let sender = setu_types::Address::from_str_id("campaign_owner");
    let bob = setu_types::Address::from_str_id("bob");
    let carol = setu_types::Address::from_str_id("carol");
    let dave = setu_types::Address::from_str_id("dave");
    let calls = vec![
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "distribute_campaign_rewards".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::Address(bob),
                SuiVmArg::Bool(true),
                SuiVmArg::Bool(true),
                SuiVmArg::Opaque,
            ],
            timestamp: 1,
            executor_id: "tiered_airdrop".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "distribute_campaign_rewards".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::Address(carol),
                SuiVmArg::Bool(false),
                SuiVmArg::Bool(true),
                SuiVmArg::Opaque,
            ],
            timestamp: 2,
            executor_id: "tiered_airdrop".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "distribute_campaign_rewards".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::Address(dave),
                SuiVmArg::Bool(false),
                SuiVmArg::Bool(false),
                SuiVmArg::Opaque,
            ],
            timestamp: 3,
            executor_id: "tiered_airdrop".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly,
            function_name: "distribute_campaign_rewards".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::Address(bob),
                SuiVmArg::Bool(false),
                SuiVmArg::Bool(true),
                SuiVmArg::Opaque,
            ],
            timestamp: 4,
            executor_id: "tiered_airdrop".to_string(),
        },
    ];

    Ok((
        ExampleState::new(RuntimeExecutor::new(InMemoryStateStore::new())),
        calls,
    ))
}

fn assert_state(state: &ExampleState<InMemoryStateStore>) -> Result<()> {
    let coin_type = "TIERED_AIRDROP";
    let bob_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("bob"),
        coin_type,
        145,
        "bob reward",
    )?;
    let carol_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("carol"),
        coin_type,
        60,
        "carol reward",
    )?;
    let dave_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("dave"),
        coin_type,
        50,
        "dave reward",
    )?;

    println!("Bob reward total   = {}", bob_coin.data.balance.value());
    println!("Carol reward total = {}", carol_coin.data.balance.value());
    println!("Dave reward total  = {}", dave_coin.data.balance.value());
    println!("\nTiered airdrop example completed.");
    Ok(())
}

fn main() -> Result<()> {
    let (state, calls) = setup_state()?;
    let state = execute_program_scenario(state, &calls)?;
    assert_state(&state)
}
