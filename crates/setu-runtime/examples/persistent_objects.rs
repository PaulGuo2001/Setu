#[path = "support/sui_example_utils.rs"]
mod sui_example_utils;

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use setu_runtime::{
    compile_package_to_disassembly, RuntimeExecutor, SetuMerkleStateStore, StateStore, SuiVmArg,
    SuiVmStoredObject, SuiVmStoredValue,
};
use setu_types::ObjectId;
use tempfile::TempDir;
use sui_example_utils::{ExampleState, ProgramCallSpec, execute_program_scenario};

const CONTRACT: &str = r#"module persistent_counter_pkg::counter {
    public struct Counter has key, store {
        id: UID,
        value: u64,
    }

    entry fun increment(counter: &mut Counter) {
        let current = counter.value;
        counter.value = current + 1;
    }
}"#;

fn setup_state() -> Result<(ExampleState<SetuMerkleStateStore>, Vec<ProgramCallSpec>)> {
    let pkg = create_temp_package_with_contract()?;
    println!("Created package: {}", pkg.display());

    let disassembly = compile_package_to_disassembly(&pkg, "counter")
        .context("Failed to compile and disassemble counter contract")?;
    println!("Compiled + disassembled module: counter");

    let db_dir = TempDir::new().context("Failed to create temp directory")?;
    let db_path = db_dir.path().join("setu_merkle_db");
    println!("Setu storage path: {}", db_path.display());

    let owner = setu_types::Address::from_str_id("alice");
    let counter_id = ObjectId::new([0x31; 32]);

    let mut store =
        SetuMerkleStateStore::open_root(&db_path).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    store
        .set_vm_object(
            counter_id,
            SuiVmStoredObject::new_owned(
                counter_id,
                "Counter",
                owner,
                std::collections::BTreeMap::from([(
                    "value".to_string(),
                    SuiVmStoredValue::U64(41),
                )]),
            ),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let anchor_id = store
        .commit_pending()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    println!(
        "Seeded ROOT subnet counter {} with value 41 at anchor {}",
        counter_id, anchor_id
    );
    println!("Initial state root: 0x{}", to_hex(&store.state_root()));

    let calls = vec![ProgramCallSpec {
        sender: owner,
        disassembly,
        function_name: "increment".to_string(),
        args: vec![SuiVmArg::ObjectId(counter_id)],
        timestamp: 10,
        executor_id: "persistent_objects".to_string(),
    }];

    Ok((
        ExampleState::with_persistent_dir(RuntimeExecutor::new(store), db_dir, db_path),
        calls,
    ))
}

fn assert_state(mut state: ExampleState<SetuMerkleStateStore>) -> Result<()> {
    let counter_id = ObjectId::new([0x31; 32]);
    let counter = state
        .executor
        .state()
        .get_vm_object(&counter_id)?
        .context("counter missing after increment")?;
    let value = counter
        .get_u64_field("value")
        .context("counter missing 'value' field after increment")?;
    if value != 42 {
        bail!("expected counter value 42 after increment, got {}", value);
    }

    println!("Increment executed: counter {} is now {}", counter_id, value);
    println!(
        "Committed state root: 0x{}",
        to_hex(&state.executor.state().state_root())
    );

    let db_path = state
        .persistent_path
        .clone()
        .context("missing persistent storage path")?;
    let persistent_dir = state.persistent_dir.take();
    drop(state);

    let reopened = SetuMerkleStateStore::open_root(&db_path)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let counter = reopened
        .get_vm_object(&counter_id)?
        .context("counter missing after reopening persisted Setu state")?;
    let value = counter
        .get_u64_field("value")
        .context("counter missing 'value' field after reopening")?;
    if value != 42 {
        bail!("expected reopened counter value 42, got {}", value);
    }
    if reopened.get_object_bytes(&counter_id).is_none() {
        bail!("MerkleStateProvider should return raw object bytes for the counter");
    }

    println!(
        "Reopened state: counter {} recovered with value {}",
        counter_id, value
    );
    println!("Recovered state root: 0x{}", to_hex(&reopened.state_root()));
    println!("\nPersistent counter example completed.");
    drop(persistent_dir);

    Ok(())
}

fn main() -> Result<()> {
    let (state, calls) = setup_state()?;
    let state = execute_program_scenario(state, &calls)?;
    assert_state(state)
}

fn create_temp_package_with_contract() -> Result<PathBuf> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let package_prefix = format!("persistent_counter_pkg_{}", ts);
    let root = std::env::temp_dir().join(format!("persistent_counter_example_{}", ts));
    fs::create_dir_all(&root)?;

    let status = Command::new("sui")
        .arg("move")
        .arg("new")
        .arg(&package_prefix)
        .current_dir(&root)
        .status()
        .context("Failed to execute `sui move new`")?;
    if !status.success() {
        bail!("`sui move new` failed with status {}", status);
    }

    let pkg = root.join(&package_prefix);
    let src = pkg.join("sources");
    let default_module = src.join(format!("{}.move", package_prefix));
    if default_module.exists() {
        fs::remove_file(default_module)?;
    }

    let contract = CONTRACT.replace(
        "persistent_counter_pkg::counter",
        &format!("{}::counter", package_prefix),
    );
    fs::write(src.join("counter.move"), contract)?;

    Ok(pkg)
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
