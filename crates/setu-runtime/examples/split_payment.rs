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

const CONTRACT: &str = r#"module split_payment::split_payment {
    use sui::coin::{Self, TreasuryCap};
    use sui::tx_context::{Self, TxContext};
    use sui::transfer;
    use std::option;

    public struct SPLIT_PAYMENT has drop {}

    fun init(witness: SPLIT_PAYMENT, ctx: &mut TxContext) {
        let (treasury_cap, metadata) = coin::create_currency(
            witness,
            9,
            b"PAY",
            b"Split Payment",
            b"Marketplace settlement",
            option::none(),
            ctx,
        );

        transfer::public_transfer(treasury_cap, tx_context::sender(ctx));
        transfer::public_freeze_object(metadata);
    }

    fun mint_to(
        treasury_cap: &mut TreasuryCap<SPLIT_PAYMENT>,
        amount: u64,
        recipient: address,
        ctx: &mut TxContext,
    ) {
        let coin = coin::mint(treasury_cap, amount, ctx);
        transfer::public_transfer(coin, recipient);
    }

    fun settle_primary_payouts(
        treasury_cap: &mut TreasuryCap<SPLIT_PAYMENT>,
        merchant_amount: u64,
        merchant: address,
        logistics_amount: u64,
        logistics: address,
        ctx: &mut TxContext,
    ) {
        mint_to(treasury_cap, merchant_amount, merchant, ctx);
        mint_to(treasury_cap, logistics_amount, logistics, ctx);
    }

    fun settle_secondary_payouts(
        treasury_cap: &mut TreasuryCap<SPLIT_PAYMENT>,
        affiliate_amount: u64,
        affiliate: address,
        platform_amount: u64,
        platform: address,
        pay_affiliate: bool,
        ctx: &mut TxContext,
    ) {
        if (pay_affiliate) {
            mint_to(treasury_cap, affiliate_amount, affiliate, ctx);
        };
        mint_to(treasury_cap, platform_amount, platform, ctx);
    }

    public entry fun settle_order(
        treasury_cap: &mut TreasuryCap<SPLIT_PAYMENT>,
        merchant_amount: u64,
        merchant: address,
        logistics_amount: u64,
        logistics: address,
        affiliate_amount: u64,
        affiliate: address,
        platform_amount: u64,
        platform: address,
        pay_affiliate: bool,
        ctx: &mut TxContext,
    ) {
        settle_primary_payouts(
            treasury_cap,
            merchant_amount,
            merchant,
            logistics_amount,
            logistics,
            ctx,
        );
        settle_secondary_payouts(
            treasury_cap,
            affiliate_amount,
            affiliate,
            platform_amount,
            platform,
            pay_affiliate,
            ctx,
        );
    }
}"#;

fn setup_state() -> Result<(ExampleState<InMemoryStateStore>, Vec<ProgramCallSpec>)> {
    let pkg = create_temp_package_with_contract("split_payment", "split_payment.move", CONTRACT)?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "split_payment")
        .context("Failed to compile split_payment package")?;
    println!("Compiled + disassembled module: split_payment");

    let sender = setu_types::Address::from_str_id("market_operator");
    let merchant = setu_types::Address::from_str_id("merchant");
    let logistics = setu_types::Address::from_str_id("logistics");
    let affiliate = setu_types::Address::from_str_id("affiliate");
    let platform = setu_types::Address::from_str_id("platform");
    let calls = vec![
        ProgramCallSpec {
            sender,
            disassembly: disassembly.clone(),
            function_name: "settle_order".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::U64(70),
                SuiVmArg::Address(merchant),
                SuiVmArg::U64(20),
                SuiVmArg::Address(logistics),
                SuiVmArg::U64(5),
                SuiVmArg::Address(affiliate),
                SuiVmArg::U64(5),
                SuiVmArg::Address(platform),
                SuiVmArg::Bool(true),
                SuiVmArg::Opaque,
            ],
            timestamp: 1,
            executor_id: "split_payment".to_string(),
        },
        ProgramCallSpec {
            sender,
            disassembly,
            function_name: "settle_order".to_string(),
            args: vec![
                SuiVmArg::Opaque,
                SuiVmArg::U64(40),
                SuiVmArg::Address(merchant),
                SuiVmArg::U64(10),
                SuiVmArg::Address(logistics),
                SuiVmArg::U64(7),
                SuiVmArg::Address(affiliate),
                SuiVmArg::U64(8),
                SuiVmArg::Address(platform),
                SuiVmArg::Bool(false),
                SuiVmArg::Opaque,
            ],
            timestamp: 2,
            executor_id: "split_payment".to_string(),
        },
    ];

    Ok((
        ExampleState::new(RuntimeExecutor::new(InMemoryStateStore::new())),
        calls,
    ))
}

fn assert_state(state: &ExampleState<InMemoryStateStore>) -> Result<()> {
    let coin_type = "SPLIT_PAYMENT";
    let merchant_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("merchant"),
        coin_type,
        110,
        "merchant settlement",
    )?;
    let logistics_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("logistics"),
        coin_type,
        30,
        "logistics settlement",
    )?;
    let affiliate_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("affiliate"),
        coin_type,
        5,
        "affiliate settlement",
    )?;
    let platform_coin = expect_coin_balance(
        state.executor.state(),
        &setu_types::Address::from_str_id("platform"),
        coin_type,
        13,
        "platform settlement",
    )?;

    println!("Merchant total  = {}", merchant_coin.data.balance.value());
    println!("Logistics total = {}", logistics_coin.data.balance.value());
    println!("Affiliate total = {}", affiliate_coin.data.balance.value());
    println!("Platform total  = {}", platform_coin.data.balance.value());
    println!("\nSplit payment example completed.");
    Ok(())
}

fn main() -> Result<()> {
    let (state, calls) = setup_state()?;
    let state = execute_program_scenario(state, &calls)?;
    assert_state(&state)
}
