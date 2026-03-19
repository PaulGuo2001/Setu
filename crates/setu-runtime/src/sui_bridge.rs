//! Experimental bridge: Sui Move disassembly -> Setu VM Program.
//!
//! This is a compatibility layer for a narrow subset of Sui contracts.
//! It does NOT execute native Sui bytecode directly. Instead, it:
//! 1) Uses `sui move build --disassemble` to produce `.mvb` text,
//! 2) Validates known instruction patterns,
//! 3) Translates supported entry functions into Setu VM `Program`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use setu_types::{Address, ObjectId};

use crate::error::{RuntimeError, RuntimeResult};
use crate::program_vm::{BuiltinFunction, Instruction, Program, VmConstant};

#[derive(Debug, Clone)]
pub struct SuiMintCall {
    pub amount: u64,
    pub recipient: Address,
    /// Coin type namespace used by Setu deterministic coin IDs.
    pub coin_type: String,
}

#[derive(Debug, Clone)]
pub struct SuiBurnCall {
    /// Coin object to burn (consumed/delete in Setu VM model).
    pub coin_id: ObjectId,
}

/// Compile a Sui Move package and return disassembly text for `module_name`.
pub fn compile_package_to_disassembly(
    package_path: &Path,
    module_name: &str,
) -> RuntimeResult<String> {
    let status = Command::new("sui")
        .arg("move")
        .arg("build")
        .arg("--disassemble")
        .arg("--path")
        .arg(package_path)
        .status()
        .map_err(|e| RuntimeError::ProgramExecution(format!("Failed to run sui build: {}", e)))?;

    if !status.success() {
        return Err(RuntimeError::ProgramExecution(format!(
            "sui move build failed with status {}",
            status
        )));
    }

    let disassembly_file = find_disassembly_file(package_path, module_name)?;
    fs::read_to_string(&disassembly_file).map_err(|e| {
        RuntimeError::ProgramExecution(format!(
            "Failed reading disassembly file {}: {}",
            disassembly_file.display(),
            e
        ))
    })
}

fn find_disassembly_file(package_path: &Path, module_name: &str) -> RuntimeResult<PathBuf> {
    let build_dir = package_path.join("build");
    let entries = fs::read_dir(&build_dir).map_err(|e| {
        RuntimeError::ProgramExecution(format!(
            "Failed reading build dir {}: {}",
            build_dir.display(),
            e
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            RuntimeError::ProgramExecution(format!("Failed reading build entry: {}", e))
        })?;
        let candidate = entry
            .path()
            .join("disassembly")
            .join(format!("{}.mvb", module_name));
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(RuntimeError::ProgramExecution(format!(
        "Disassembly file for module '{}' not found under {}",
        module_name,
        build_dir.display()
    )))
}

/// Translate Sui disassembly `mint` entry into Setu VM Program.
///
/// Supported pattern (from the sample contract):
/// - call `coin::mint<T>(...)`
/// - call `transfer::public_transfer<Coin<T>>(...)`
pub fn translate_mint_from_disassembly(
    disassembly: &str,
    call: &SuiMintCall,
) -> RuntimeResult<Program> {
    require_contains(disassembly, "entry public mint(")?;
    require_contains(disassembly, "Call coin::mint<")?;
    require_contains(disassembly, "Call transfer::public_transfer<Coin<")?;

    Ok(build_setu_mint_program(call))
}

/// Translate Sui disassembly `burn` entry into Setu VM Program.
///
/// Supported pattern (from the sample contract):
/// - call `coin::burn<T>(...)`
/// - `Pop`
pub fn translate_burn_from_disassembly(
    disassembly: &str,
    call: &SuiBurnCall,
) -> RuntimeResult<Program> {
    require_contains(disassembly, "entry public burn(")?;
    require_contains(disassembly, "Call coin::burn<")?;
    require_contains(disassembly, "Pop")?;

    Ok(build_setu_burn_program(call))
}

fn require_contains(text: &str, needle: &str) -> RuntimeResult<()> {
    if !text.contains(needle) {
        return Err(RuntimeError::ProgramExecution(format!(
            "Unsupported disassembly pattern: missing '{}'",
            needle
        )));
    }
    Ok(())
}

fn build_setu_mint_program(call: &SuiMintCall) -> Program {
    // locals:
    // 0 recipient
    // 1 coin_type
    // 2 recipient_coin_id
    // 3 updated_recipient_coin
    Program {
        locals_count: 4,
        instructions: vec![
            Instruction::LoadConst(VmConstant::Address(call.recipient)), // 0
            Instruction::StLoc(0),
            Instruction::LoadConst(VmConstant::String(call.coin_type.clone())), // 2
            Instruction::StLoc(1),
            Instruction::CopyLoc(0), // 4
            Instruction::CopyLoc(1),
            Instruction::CallGeneric {
                function: BuiltinFunction::DeterministicCoinId,
                type_args: vec!["coin".to_string()],
                arg_count: 2,
            },
            Instruction::StLoc(2),
            Instruction::CopyLoc(2), // 8
            Instruction::Exists,
            Instruction::BrFalse(20),
            // exists path: deposit into recipient coin
            Instruction::CopyLoc(2), // 11
            Instruction::BorrowGlobal,
            Instruction::LoadConst(VmConstant::U64(call.amount)),
            Instruction::Call {
                function: BuiltinFunction::CoinDeposit,
                arg_count: 2,
            },
            Instruction::StLoc(3),
            Instruction::CopyLoc(2),
            Instruction::CopyLoc(3),
            Instruction::MoveTo,
            Instruction::Branch(27),
            // create path
            Instruction::CopyLoc(2), // 20: keep one id for MoveTo
            Instruction::CopyLoc(2), // id consumed by PackCoin
            Instruction::CopyLoc(0),
            Instruction::LoadConst(VmConstant::U64(call.amount)),
            Instruction::CopyLoc(1),
            Instruction::PackCoin,
            Instruction::MoveTo,
            Instruction::Ret, // 27
        ],
    }
}

fn build_setu_burn_program(call: &SuiBurnCall) -> Program {
    // Burning a coin in current Setu VM model is represented as consuming/deleting
    // the provided coin object.
    Program {
        locals_count: 0,
        instructions: vec![
            Instruction::LoadConst(VmConstant::ObjectId(call.coin_id)),
            Instruction::MoveFrom,
            Instruction::Ret,
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use setu_types::Address;

    #[test]
    fn test_translate_mint_pattern() {
        let disassembly = r#"
entry public mint(treasury_cap#0#0: &mut TreasuryCap<MY_COIN>, amount#0#0: u64, recipient#0#0: address, ctx#0#0: &mut TxContext) {
B0:
    3: Call coin::mint<MY_COIN>(&mut TreasuryCap<MY_COIN>, u64, &mut TxContext): Coin<MY_COIN>
    5: Call transfer::public_transfer<Coin<MY_COIN>>(Coin<MY_COIN>, address)
    6: Ret
}
"#;
        let call = SuiMintCall {
            amount: 42,
            recipient: Address::from_str_id("bob"),
            coin_type: "MY_COIN".to_string(),
        };
        let program = translate_mint_from_disassembly(disassembly, &call).unwrap();
        assert!(!program.instructions.is_empty());
        assert_eq!(program.locals_count, 4);
    }

    #[test]
    fn test_translate_burn_pattern() {
        let disassembly = r#"
entry public burn(treasury_cap#0#0: &mut TreasuryCap<MY_COIN>, coin#0#0: Coin<MY_COIN>) {
B0:
    2: Call coin::burn<MY_COIN>(&mut TreasuryCap<MY_COIN>, Coin<MY_COIN>): u64
    3: Pop
    4: Ret
}
"#;
        let call = SuiBurnCall {
            coin_id: ObjectId::new([7u8; 32]),
        };
        let program = translate_burn_from_disassembly(disassembly, &call).unwrap();
        assert_eq!(program.instructions.len(), 3);
    }
}
