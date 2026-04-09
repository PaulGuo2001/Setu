#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, InMemoryStateStore, RuntimeExecutor, StateStore, SuiVmArg,
};
use setu_types::deterministic_coin_id;
use sui_example_utils::{
    create_temp_package_with_contract, execute_program_scenario, ExampleState, ProgramCallSpec,
};

const CONTRACT: &str = r#"module burn_or_redirect::burn_or_redirect {
    use sui::coin::{Self, Coin, TreasuryCap};
    use sui::tx_context::{Self, TxContext};
    use sui::transfer;
    use std::option;

    public struct BURN_OR_REDIRECT has drop {}

    fun init(witness: BURN_OR_REDIRECT, ctx: &mut TxContext) {
        let (treasury_cap, metadata) = coin::create_currency(
            witness,
            9,
            b"CLM",
            b"Claim Token",
            b"Claim resolution flow",
            option::none(),
            ctx,
        );

        transfer::public_transfer(treasury_cap, tx_context::sender(ctx));
        transfer::public_freeze_object(metadata);
    }

    fun mint_to(
        treasury_cap: &mut TreasuryCap<BURN_OR_REDIRECT>,
        amount: u64,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        let coin = coin::mint(treasury_cap, amount, ctx);
        transfer::public_transfer(coin, recipient);
    }

    public entry fun issue_claimable(
        treasury_cap: &mut TreasuryCap<BURN_OR_REDIRECT>,
        amount: u64,
        claimant: address,
        ctx: &mut TxContext,
    ) {
        mint_to(treasury_cap, amount, claimant, ctx);
    }

    fun redirect_coin(
        coin: Coin<BURN_OR_REDIRECT>,
        fallback_recipient: address,
    ) {
        transfer::public_transfer(coin, fallback_recipient);
    }

    fun destroy_coin(
        treasury_cap: &mut TreasuryCap<BURN_OR_REDIRECT>,
        coin: Coin<BURN_OR_REDIRECT>,
    ) {
        coin::burn(treasury_cap, coin);
    }

    fun resolve_exit(
        treasury_cap: &mut TreasuryCap<BURN_OR_REDIRECT>,
        coin: Coin<BURN_OR_REDIRECT>,
        fallback_recipient: address,
        should_burn: bool,
    ) {
        if (should_burn) {
            destroy_coin(treasury_cap, coin);
        } else {
            redirect_coin(coin, fallback_recipient);
        };
    }

    public entry fun resolve_failed_claim(
        treasury_cap: &mut TreasuryCap<BURN_OR_REDIRECT>,
        coin: Coin<BURN_OR_REDIRECT>,
        fallback_recipient: address,
        should_burn: bool,
    ) {
        resolve_exit(treasury_cap, coin, fallback_recipient, should_burn);
    }
}"#;

fn setup_state() -> Result<(ExampleState<InMemoryStateStore>, Vec<ProgramCallSpec>)> {
    let pkg =
        create_temp_package_with_contract("burn_or_redirect", "burn_or_redirect.move", CONTRACT)?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "burn_or_redirect")
        .context("Failed to compile burn_or_redirect package")?;
    println!("Compiled + disassembled module: burn_or_redirect");

    let sender = setu_types::Address::from_str_id("claims_operator");
    let bob = setu_types::Address::from_str_id("bob");
    let carol = setu_types::Address::from_str_id("carol");
    let dave = setu_types::Address::from_str_id("dave");
    let eve = setu_types::Address::from_str_id("eve");
    let calls = vec![
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "issue_claimable".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::U64(30),
                SuiVmArg::Address(bob),
                SuiVmArg::Opaque,
            ],
            timestamp: 1,
            executor_id: "burn_or_redirect".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "issue_claimable".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::U64(12),
                SuiVmArg::Address(carol),
                SuiVmArg::Opaque,
            ],
            timestamp: 2,
            executor_id: "burn_or_redirect".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "resolve_failed_claim".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::ObjectId(deterministic_coin_id(&bob, "BURN_OR_REDIRECT")),
                SuiVmArg::Address(dave),
                SuiVmArg::Bool(false),
            ],
            timestamp: 3,
            executor_id: "burn_or_redirect".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly,
            function_name: "resolve_failed_claim".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::ObjectId(deterministic_coin_id(&carol, "BURN_OR_REDIRECT")),
                SuiVmArg::Address(eve),
                SuiVmArg::Bool(true),
            ],
            timestamp: 4,
            executor_id: "burn_or_redirect".to_string(),
        },
    ];

    Ok((
        ExampleState::new(RuntimeExecutor::new(InMemoryStateStore::new())),
        calls,
    ))
}

fn assert_state(state: &ExampleState<InMemoryStateStore>) -> Result<()> {
    let coin_type = "BURN_OR_REDIRECT";
    let bob_coin_id =
        deterministic_coin_id(&setu_types::Address::from_str_id("bob"), coin_type);
    let carol_coin_id =
        deterministic_coin_id(&setu_types::Address::from_str_id("carol"), coin_type);

    if state.executor.state().get_object(&bob_coin_id)?.is_some() {
        bail!("bob claim coin should have been redirected away");
    }
    if state.executor.state().get_object(&carol_coin_id)?.is_some() {
        bail!("carol claim coin should have been burned");
    }

    let redirected_coin = state
        .executor
        .state()
        .get_object(&deterministic_coin_id(
            &setu_types::Address::from_str_id("dave"),
            coin_type,
        ))?
        .context("redirected coin missing for dave")?;
    if redirected_coin.data.balance.value() != 30 {
        bail!(
            "expected redirected coin balance 30, got {}",
            redirected_coin.data.balance.value()
        );
    }

    if state
        .executor
        .state()
        .get_object(&deterministic_coin_id(
            &setu_types::Address::from_str_id("eve"),
            coin_type,
        ))?
        .is_some()
    {
        bail!("eve should not receive a coin when the claim is burned");
    }

    println!(
        "Redirected claim balance for dave = {}",
        redirected_coin.data.balance.value()
    );
    println!("Carol claim coin was burned and eve received nothing");
    println!("\nBurn-or-redirect example completed.");
    Ok(())
}

fn main() -> Result<()> {
    let (state, calls) = setup_state()?;
    let state = execute_program_scenario(state, &calls)?;
    assert_state(&state)
}
