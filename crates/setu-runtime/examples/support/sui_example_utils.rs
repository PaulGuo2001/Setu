use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use setu_runtime::{ExecutionContext, InMemoryStateStore, RuntimeExecutor, SuiVmArg, Transaction};
use setu_types::Address;

pub fn execute_program_tx(
    executor: &mut RuntimeExecutor<InMemoryStateStore>,
    sender: &Address,
    disassembly: &str,
    function_name: &str,
    args: Vec<SuiVmArg>,
    timestamp: u64,
    executor_id: &str,
) -> Result<()> {
    let tx = Transaction::new_program_deterministic(
        *sender,
        disassembly.to_owned(),
        function_name,
        args,
        timestamp,
    );
    let ctx = ExecutionContext {
        executor_id: executor_id.to_string(),
        timestamp,
        in_tee: false,
    };

    executor
        .execute_transaction(&tx, &ctx)
        .with_context(|| format!("Failed to execute '{}' via RuntimeExecutor", function_name))?;

    Ok(())
}

pub fn create_temp_package_with_contract(
    package_prefix: &str,
    module_filename: &str,
    contract: &str,
) -> Result<PathBuf> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let root = std::env::temp_dir().join(format!("{}_{}", package_prefix, ts));
    fs::create_dir_all(&root)?;

    let status = Command::new("sui")
        .arg("move")
        .arg("new")
        .arg(package_prefix)
        .current_dir(&root)
        .status()
        .context("Failed to execute `sui move new`")?;
    if !status.success() {
        bail!("`sui move new` failed with status {}", status);
    }

    let pkg = root.join(package_prefix);
    let src = pkg.join("sources");
    let default_module = src.join(format!("{}.move", package_prefix));
    if default_module.exists() {
        fs::remove_file(default_module)?;
    }
    fs::write(src.join(module_filename), contract)?;

    Ok(pkg)
}
