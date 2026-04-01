#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, InMemoryStateStore, RuntimeExecutor, StateStore, SuiVmArg,
};
use setu_types::{deterministic_coin_id, Address};
use sui_example_utils::{create_temp_package_with_contract, execute_program_tx};

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

fn main() -> Result<()> {
    let pkg = create_temp_package_with_contract("split_payment", "split_payment.move", CONTRACT)?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "split_payment")
        .context("Failed to compile split_payment package")?;
    println!("Compiled + disassembled module: split_payment");

    let state = InMemoryStateStore::new();
    let mut executor = RuntimeExecutor::new(state);
    let sender = Address::from_str_id("market_operator");
    let merchant = Address::from_str_id("merchant");
    let logistics = Address::from_str_id("logistics");
    let affiliate = Address::from_str_id("affiliate");
    let platform = Address::from_str_id("platform");
    let coin_type = "SPLIT_PAYMENT";

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "settle_order",
        vec![
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
        1,
        "split_payment",
    )?;

    execute_program_tx(
        &mut executor,
        &sender,
        &disassembly,
        "settle_order",
        vec![
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
        2,
        "split_payment",
    )?;

    let merchant_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&merchant, coin_type))?
        .context("merchant settlement coin missing")?;
    let logistics_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&logistics, coin_type))?
        .context("logistics settlement coin missing")?;
    let affiliate_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&affiliate, coin_type))?
        .context("affiliate settlement coin missing")?;
    let platform_coin = executor
        .state()
        .get_object(&deterministic_coin_id(&platform, coin_type))?
        .context("platform settlement coin missing")?;

    if merchant_coin.data.balance.value() != 110 {
        bail!(
            "expected merchant settlement total 110, got {}",
            merchant_coin.data.balance.value()
        );
    }
    if logistics_coin.data.balance.value() != 30 {
        bail!(
            "expected logistics settlement total 30, got {}",
            logistics_coin.data.balance.value()
        );
    }
    if affiliate_coin.data.balance.value() != 5 {
        bail!(
            "expected affiliate settlement total 5, got {}",
            affiliate_coin.data.balance.value()
        );
    }
    if platform_coin.data.balance.value() != 13 {
        bail!(
            "expected platform settlement total 13, got {}",
            platform_coin.data.balance.value()
        );
    }

    println!("Merchant total  = {}", merchant_coin.data.balance.value());
    println!("Logistics total = {}", logistics_coin.data.balance.value());
    println!("Affiliate total = {}", affiliate_coin.data.balance.value());
    println!("Platform total  = {}", platform_coin.data.balance.value());
    println!("\nSplit payment example completed.");
    Ok(())
}
