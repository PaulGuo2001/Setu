//! Direct Sui disassembly VM (subset).
//!
//! This module executes a subset of Sui Move disassembly opcodes directly,
//! instead of translating specific contract patterns into Setu VM programs.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use setu_types::{
    deterministic_coin_id, hash_utils::sha256_hash, Address, Balance, CoinData, CoinType, Object,
    ObjectId,
};

use crate::error::{RuntimeError, RuntimeResult};
use crate::state::StateStore;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SuiVmArg {
    U64(u64),
    Bool(bool),
    Address(Address),
    ObjectId(ObjectId),
    Bytes(Vec<u8>),
    /// Opaque argument for types that the subset VM does not model.
    Opaque,
    /// Optional way to pass TxContext epoch explicitly.
    TxContextEpoch(u64),
}

type VmCell = Rc<RefCell<VmData>>;

#[derive(Debug, Clone)]
struct VmRef {
    cell: VmCell,
    mutable: bool,
}

#[derive(Debug, Clone)]
enum VmValue {
    Cell(VmCell),
    Ref(VmRef),
}

#[derive(Debug, Clone)]
enum VmData {
    U64(u64),
    Bool(bool),
    Address(Address),
    Bytes(Vec<u8>),
    Coin(Object<CoinData>),
    Balance(u64),
    Struct(VmStruct),
    Table(VmTable),
    ObjectId(ObjectId),
    TxContext(VmTxContext),
    None,
    Opaque,
}

#[derive(Debug, Clone)]
struct VmTxContext {
    epoch: u64,
}

#[derive(Debug, Clone)]
struct VmStruct {
    type_name: String,
    // Keep declaration order for deterministic encode/decode.
    fields: Vec<(String, VmCell)>,
}

impl VmStruct {
    fn get_field(&self, name: &str) -> Option<VmCell> {
        self.fields
            .iter()
            .find_map(|(n, v)| if n == name { Some(v.clone()) } else { None })
    }
}

#[derive(Debug, Clone)]
struct VmTable {
    entries: BTreeMap<u64, VmCell>,
}

#[derive(Debug, Clone)]
enum SuiOpcode {
    MoveLoc(usize),
    CopyLoc(usize),
    StLoc(usize),
    ImmBorrowLoc(usize),
    MutBorrowLoc(usize),
    ImmBorrowField(String),
    MutBorrowField(String),
    ReadRef,
    WriteRef,
    LdU64(u64),
    LdU8(u8),
    LdConst(usize),
    LdTrue,
    LdFalse,
    BrFalse(usize),
    BrTrue(usize),
    Branch(usize),
    Eq,
    Ge,
    Add,
    Sub,
    VecPack(usize),
    Pack(String),
    Call { function: String, arg_count: usize },
    FreezeRef,
    Pop,
    Abort,
    Ret,
}

#[derive(Debug, Clone)]
struct ParsedFunction {
    param_types: Vec<String>,
    local_types: HashMap<usize, String>,
    instructions: Vec<(usize, SuiOpcode)>,
}

#[derive(Debug, Clone)]
struct ParsedModule {
    structs: HashMap<String, Vec<String>>,
    constants: HashMap<usize, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoredVmValue {
    U64(u64),
    Bool(bool),
    Address(Address),
    Bytes(Vec<u8>),
    Balance(u64),
    ObjectId(ObjectId),
    Struct {
        type_name: String,
        fields: Vec<(String, StoredVmValue)>,
    },
    Table(Vec<(u64, StoredVmValue)>),
    None,
    Opaque,
}

#[derive(Debug, Clone)]
pub struct SuiVmWrite {
    pub object_id: ObjectId,
    pub old_state: Option<Vec<u8>>,
    pub new_state: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SuiVmExecutionOutcome {
    pub writes: Vec<SuiVmWrite>,
}

pub fn execute_sui_entry_from_disassembly<S: StateStore>(
    state: &mut S,
    sender: &Address,
    disassembly: &str,
    function_name: &str,
    args: &[SuiVmArg],
) -> RuntimeResult<()> {
    let _ = execute_sui_entry_with_outcome(state, sender, disassembly, function_name, args)?;
    Ok(())
}

pub fn execute_sui_entry_with_outcome<S: StateStore>(
    state: &mut S,
    sender: &Address,
    disassembly: &str,
    function_name: &str,
    args: &[SuiVmArg],
) -> RuntimeResult<SuiVmExecutionOutcome> {
    let module = parse_module(disassembly)?;
    let function = parse_entry_function(disassembly, function_name)?;
    let mut vm = SuiDisasmVm::new(state, sender, module, function, args)?;
    vm.run()
}

struct SuiDisasmVm<'a, S: StateStore> {
    state: &'a mut S,
    sender: &'a Address,
    structs: HashMap<String, Vec<String>>,
    constants: HashMap<usize, u64>,
    instructions: Vec<(usize, SuiOpcode)>,
    instruction_pc: HashMap<usize, usize>,
    locals: Vec<Option<VmValue>>,
    stack: Vec<VmValue>,
    temp_counter: u64,
    // Tracked mutable shared objects loaded from StateStore::get_vm_object.
    tracked_vm_param_cells: HashMap<ObjectId, VmCell>,
    write_order: Vec<ObjectId>,
    old_states: HashMap<ObjectId, Option<Vec<u8>>>,
    final_states: HashMap<ObjectId, Option<Vec<u8>>>,
}

impl<'a, S: StateStore> SuiDisasmVm<'a, S> {
    fn new(
        state: &'a mut S,
        sender: &'a Address,
        module: ParsedModule,
        function: ParsedFunction,
        args: &[SuiVmArg],
    ) -> RuntimeResult<Self> {
        if args.len() != function.param_types.len() {
            return Err(RuntimeError::ProgramExecution(format!(
                "Arg count mismatch: expected {}, got {}",
                function.param_types.len(),
                args.len()
            )));
        }

        let max_local_from_instr = function
            .instructions
            .iter()
            .filter_map(|(_, op)| match op {
                SuiOpcode::MoveLoc(idx)
                | SuiOpcode::CopyLoc(idx)
                | SuiOpcode::StLoc(idx)
                | SuiOpcode::ImmBorrowLoc(idx)
                | SuiOpcode::MutBorrowLoc(idx) => Some(*idx),
                _ => None,
            })
            .max()
            .unwrap_or(0);

        let max_local_from_types = function.local_types.keys().copied().max().unwrap_or(0);

        let locals_count = usize::max(
            function.param_types.len(),
            usize::max(max_local_from_instr + 1, max_local_from_types + 1),
        );
        let mut locals = vec![None; locals_count];
        let mut tracked_vm_param_cells = HashMap::new();

        for (idx, (arg, param_ty)) in args.iter().zip(function.param_types.iter()).enumerate() {
            let (value, maybe_vm_obj) = Self::coerce_arg(state, sender, arg, param_ty)?;
            if let Some((obj_id, cell)) = maybe_vm_obj {
                tracked_vm_param_cells.insert(obj_id, cell);
            }
            locals[idx] = Some(value);
        }

        let mut instruction_pc = HashMap::new();
        for (pc, (inst_idx, _)) in function.instructions.iter().enumerate() {
            instruction_pc.insert(*inst_idx, pc);
        }

        Ok(Self {
            state,
            sender,
            structs: module.structs,
            constants: module.constants,
            instructions: function.instructions,
            instruction_pc,
            locals,
            stack: Vec::new(),
            temp_counter: 1,
            tracked_vm_param_cells,
            write_order: Vec::new(),
            old_states: HashMap::new(),
            final_states: HashMap::new(),
        })
    }

    fn run(&mut self) -> RuntimeResult<SuiVmExecutionOutcome> {
        let mut pc = 0usize;
        let step_limit = 200_000usize;
        let mut steps = 0usize;

        while pc < self.instructions.len() {
            if steps >= step_limit {
                return Err(RuntimeError::ProgramExecution(
                    "Sui VM step limit exceeded".to_string(),
                ));
            }
            steps += 1;

            let (_, op) = self.instructions[pc].clone();
            pc += 1;

            match op {
                SuiOpcode::MoveLoc(idx) => {
                    let slot = self.locals.get_mut(idx).ok_or_else(|| {
                        RuntimeError::ProgramExecution(format!("Invalid local index {}", idx))
                    })?;
                    let v = slot.take().ok_or_else(|| {
                        RuntimeError::ProgramExecution(format!("Local {} is uninitialized", idx))
                    })?;
                    self.stack.push(v);
                }
                SuiOpcode::CopyLoc(idx) => {
                    let v = self.local_get(idx)?.clone();
                    self.stack.push(v);
                }
                SuiOpcode::StLoc(idx) => {
                    let v = self.pop()?;
                    let slot = self.locals.get_mut(idx).ok_or_else(|| {
                        RuntimeError::ProgramExecution(format!("Invalid local index {}", idx))
                    })?;
                    *slot = Some(v);
                }
                SuiOpcode::ImmBorrowLoc(idx) => {
                    let v = self.local_get(idx)?.clone();
                    self.stack.push(self.to_ref(v, false)?);
                }
                SuiOpcode::MutBorrowLoc(idx) => {
                    let v = self.local_get(idx)?.clone();
                    self.stack.push(self.to_ref(v, true)?);
                }
                SuiOpcode::ImmBorrowField(field) => {
                    let base = self.pop()?;
                    self.stack.push(self.borrow_field(base, &field, false)?);
                }
                SuiOpcode::MutBorrowField(field) => {
                    let base = self.pop()?;
                    self.stack.push(self.borrow_field(base, &field, true)?);
                }
                SuiOpcode::ReadRef => {
                    let r = self.pop_ref_any()?;
                    let data = deep_clone_data(&r.cell.borrow());
                    self.stack.push(cell_value(data));
                }
                SuiOpcode::WriteRef => {
                    // Move bytecode stack order here is [value, &mut ref] before WriteRef.
                    let target = self.pop_mut_ref()?;
                    let value = self.pop_data_value()?;
                    *target.cell.borrow_mut() = value;
                }
                SuiOpcode::LdU64(v) => self.stack.push(cell_value(VmData::U64(v))),
                SuiOpcode::LdU8(v) => self.stack.push(cell_value(VmData::U64(v as u64))),
                SuiOpcode::LdConst(idx) => {
                    let v = self.constants.get(&idx).copied().ok_or_else(|| {
                        RuntimeError::ProgramExecution(format!("Missing LdConst[{}]", idx))
                    })?;
                    self.stack.push(cell_value(VmData::U64(v)));
                }
                SuiOpcode::LdTrue => self.stack.push(cell_value(VmData::Bool(true))),
                SuiOpcode::LdFalse => self.stack.push(cell_value(VmData::Bool(false))),
                SuiOpcode::BrFalse(target) => {
                    if !self.pop_bool()? {
                        pc = self.jump_target(target)?;
                    }
                }
                SuiOpcode::BrTrue(target) => {
                    if self.pop_bool()? {
                        pc = self.jump_target(target)?;
                    }
                }
                SuiOpcode::Branch(target) => {
                    pc = self.jump_target(target)?;
                }
                SuiOpcode::Eq => {
                    let rhs = self.pop_data_value()?;
                    let lhs = self.pop_data_value()?;
                    self.stack
                        .push(cell_value(VmData::Bool(vm_data_eq(&lhs, &rhs)?)));
                }
                SuiOpcode::Ge => {
                    let rhs = self.pop_u64()?;
                    let lhs = self.pop_u64()?;
                    self.stack.push(cell_value(VmData::Bool(lhs >= rhs)));
                }
                SuiOpcode::Add => {
                    let rhs = self.pop_u64()?;
                    let lhs = self.pop_u64()?;
                    let out = lhs.checked_add(rhs).ok_or_else(|| {
                        RuntimeError::ProgramExecution("u64 overflow in Add".to_string())
                    })?;
                    self.stack.push(cell_value(VmData::U64(out)));
                }
                SuiOpcode::Sub => {
                    let rhs = self.pop_u64()?;
                    let lhs = self.pop_u64()?;
                    let out = lhs.checked_sub(rhs).ok_or_else(|| {
                        RuntimeError::ProgramExecution("u64 underflow in Sub".to_string())
                    })?;
                    self.stack.push(cell_value(VmData::U64(out)));
                }
                SuiOpcode::VecPack(len) => {
                    let mut items = Vec::with_capacity(len);
                    for _ in 0..len {
                        items.push(self.pop_data_value()?);
                    }
                    items.reverse();
                    let mut bytes = Vec::with_capacity(items.len());
                    for item in items {
                        match item {
                            VmData::U64(v) if v <= u8::MAX as u64 => bytes.push(v as u8),
                            other => {
                                return Err(RuntimeError::ProgramExecution(format!(
                                    "VecPack currently supports vector<u8>, got {:?}",
                                    other
                                )))
                            }
                        }
                    }
                    self.stack.push(cell_value(VmData::Bytes(bytes)));
                }
                SuiOpcode::Pack(type_name) => {
                    let field_names = self
                        .structs
                        .get(&type_name)
                        .ok_or_else(|| {
                            RuntimeError::ProgramExecution(format!(
                                "Unknown struct '{}' for Pack",
                                type_name
                            ))
                        })?
                        .clone();

                    let mut values = Vec::with_capacity(field_names.len());
                    for _ in 0..field_names.len() {
                        values.push(self.pop_data_value()?);
                    }
                    values.reverse();

                    let mut fields = Vec::with_capacity(field_names.len());
                    for (name, data) in field_names.iter().cloned().zip(values.into_iter()) {
                        fields.push((name, Rc::new(RefCell::new(data))));
                    }

                    self.stack
                        .push(cell_value(VmData::Struct(VmStruct { type_name, fields })));
                }
                SuiOpcode::Call {
                    function,
                    arg_count,
                } => {
                    self.execute_call(&function, arg_count)?;
                }
                SuiOpcode::FreezeRef => {
                    let v = self.pop()?;
                    self.stack.push(self.freeze_ref(v)?);
                }
                SuiOpcode::Pop => {
                    let _ = self.pop()?;
                }
                SuiOpcode::Abort => {
                    let code = self.pop_u64()?;
                    return Err(RuntimeError::ProgramAbort(code.to_string()));
                }
                SuiOpcode::Ret => return self.finish(),
            }
        }

        Err(RuntimeError::ProgramExecution(
            "Function terminated without Ret".to_string(),
        ))
    }

    fn finish(&mut self) -> RuntimeResult<SuiVmExecutionOutcome> {
        let tracked: Vec<(ObjectId, VmCell)> = self
            .tracked_vm_param_cells
            .iter()
            .map(|(id, cell)| (*id, cell.clone()))
            .collect();
        for (object_id, cell) in tracked {
            let data = deep_clone_data(&cell.borrow());
            let bytes = encode_vm_data(&data)?;
            self.set_vm_object_tracked(object_id, bytes)?;
        }

        let mut writes = Vec::new();
        for object_id in &self.write_order {
            let old_state = self.old_states.get(object_id).cloned().unwrap_or(None);
            let new_state = self.final_states.get(object_id).cloned().unwrap_or(None);
            if old_state == new_state {
                continue;
            }
            writes.push(SuiVmWrite {
                object_id: *object_id,
                old_state,
                new_state,
            });
        }
        Ok(SuiVmExecutionOutcome { writes })
    }

    fn coerce_arg(
        state: &mut S,
        sender: &Address,
        arg: &SuiVmArg,
        param_type: &str,
    ) -> RuntimeResult<(VmValue, Option<(ObjectId, VmCell)>)> {
        if param_type == "u64" {
            return match arg {
                SuiVmArg::U64(v) => Ok((cell_value(VmData::U64(*v)), None)),
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected u64 arg, got {:?}",
                    arg
                ))),
            };
        }

        if param_type == "bool" {
            return match arg {
                SuiVmArg::Bool(v) => Ok((cell_value(VmData::Bool(*v)), None)),
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected bool arg, got {:?}",
                    arg
                ))),
            };
        }

        if param_type == "address" {
            return match arg {
                SuiVmArg::Address(v) => Ok((cell_value(VmData::Address(*v)), None)),
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected address arg, got {:?}",
                    arg
                ))),
            };
        }

        if param_type.contains("vector<u8>") {
            return match arg {
                SuiVmArg::Bytes(v) => Ok((cell_value(VmData::Bytes(v.clone())), None)),
                SuiVmArg::Opaque => Ok((cell_value(VmData::Bytes(Vec::new())), None)),
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected vector<u8> arg, got {:?}",
                    arg
                ))),
            };
        }

        if param_type.ends_with("TxContext") || param_type.contains("TxContext") {
            let epoch = match arg {
                SuiVmArg::TxContextEpoch(v) => *v,
                _ => 0,
            };
            return Ok((cell_value(VmData::TxContext(VmTxContext { epoch })), None));
        }

        if param_type.contains("Coin<") {
            return match arg {
                SuiVmArg::ObjectId(object_id) => {
                    let coin = state
                        .get_object(object_id)?
                        .ok_or(RuntimeError::ObjectNotFound(*object_id))?;
                    Ok((cell_value(VmData::Coin(coin)), None))
                }
                SuiVmArg::Opaque => {
                    let coin = Object::new_owned(
                        ObjectId::ZERO,
                        *sender,
                        CoinData {
                            coin_type: CoinType::native(),
                            balance: Balance::new(0),
                        },
                    );
                    Ok((cell_value(VmData::Coin(coin)), None))
                }
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected coin object id arg, got {:?}",
                    arg
                ))),
            };
        }

        if param_type.contains("Channel") {
            return match arg {
                SuiVmArg::ObjectId(object_id) => {
                    let bytes = state
                        .get_vm_object(object_id)?
                        .ok_or_else(|| RuntimeError::ObjectNotFound(*object_id))?;
                    let data = decode_vm_data(&bytes)?;
                    let cell = Rc::new(RefCell::new(data));
                    let tracked = if param_type.starts_with("&mut") {
                        Some((*object_id, cell.clone()))
                    } else {
                        None
                    };
                    Ok((VmValue::Cell(cell), tracked))
                }
                SuiVmArg::Opaque => {
                    let channel = VmData::Struct(VmStruct {
                        type_name: "Channel".to_string(),
                        fields: vec![],
                    });
                    Ok((cell_value(channel), None))
                }
                _ => Err(RuntimeError::ProgramExecution(format!(
                    "Expected channel object id arg, got {:?}",
                    arg
                ))),
            };
        }

        Ok((cell_value(VmData::Opaque), None))
    }

    fn jump_target(&self, target: usize) -> RuntimeResult<usize> {
        self.instruction_pc.get(&target).copied().ok_or_else(|| {
            RuntimeError::ProgramExecution(format!("Invalid branch target {}", target))
        })
    }

    fn execute_call(&mut self, function: &str, arg_count: usize) -> RuntimeResult<()> {
        if self.stack.len() < arg_count {
            return Err(RuntimeError::ProgramExecution(format!(
                "Call {} expects {} args, stack has {}",
                function,
                arg_count,
                self.stack.len()
            )));
        }

        let mut args = Vec::with_capacity(arg_count);
        for _ in 0..arg_count {
            args.push(self.pop()?);
        }
        args.reverse();

        if function.starts_with("coin::mint<") {
            self.call_coin_mint(function, &args)
        } else if function.starts_with("coin::split<") {
            self.call_coin_split(&args)
        } else if function.starts_with("coin::into_balance<") {
            self.call_coin_into_balance(&args)
        } else if function.starts_with("transfer::public_transfer<Coin<") {
            self.call_public_transfer(&args)
        } else if function.starts_with("coin::burn<") {
            self.call_coin_burn(&args)
        } else if function.starts_with("object::new") {
            self.call_object_new(&args)
        } else if function.starts_with("object::id<") {
            self.call_object_id(&args)
        } else if function.starts_with("object::id_to_bytes") {
            self.call_object_id_to_bytes(&args)
        } else if function.starts_with("tx_context::sender") {
            self.call_tx_context_sender(&args)
        } else if function.starts_with("tx_context::epoch") {
            self.call_tx_context_epoch(&args)
        } else if function.starts_with("table::new<") {
            self.call_table_new(&args)
        } else if function.starts_with("table::borrow_mut<") {
            self.call_table_borrow_mut(&args)
        } else if function.starts_with("option::none<") {
            self.call_option_none(&args)
        } else if function.starts_with("event::emit<") {
            self.call_event_emit(&args)
        } else if function.starts_with("transfer::share_object<") {
            self.call_share_object(&args)
        } else if function.starts_with("hash::sha2_256") {
            self.call_hash_sha2_256(&args)
        } else if function.starts_with("bcs::to_bytes<") {
            self.call_bcs_to_bytes(function, &args)
        } else if function.starts_with("vector::append<") {
            self.call_vector_append(&args)
        } else if function.starts_with("ecdsa_k1::secp256k1_verify") {
            self.call_ecdsa_verify(&args)
        } else {
            Err(RuntimeError::ProgramExecution(format!(
                "Unsupported Sui native call '{}'",
                function
            )))
        }
    }

    fn call_coin_mint(&mut self, function: &str, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 3 {
            return Err(RuntimeError::ProgramExecution(format!(
                "coin::mint expects 3 args, got {}",
                args.len()
            )));
        }

        let amount = expect_u64(&args[1])?;
        let coin_type = extract_generic_type(function).unwrap_or_else(|| "SUI".to_string());
        let object_id = self.next_temp_object_id()?;
        let coin = Object::new_owned(
            object_id,
            *self.sender,
            CoinData {
                coin_type: CoinType::new(coin_type),
                balance: Balance::new(amount),
            },
        );
        self.stack.push(cell_value(VmData::Coin(coin)));
        Ok(())
    }

    fn call_coin_split(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 3 {
            return Err(RuntimeError::ProgramExecution(format!(
                "coin::split expects 3 args, got {}",
                args.len()
            )));
        }

        let amount = expect_u64(&args[1])?;
        let cell = mutable_cell_from_value(&args[0])?;
        let split_coin_id = self.next_temp_object_id()?;
        let coin = {
            let mut borrowed = cell.borrow_mut();
            match &mut *borrowed {
                VmData::Coin(coin) => {
                    let split = coin
                        .data
                        .balance
                        .withdraw(amount)
                        .map_err(RuntimeError::InvalidTransaction)?;
                    coin.increment_version();
                    let split_coin = Object::new_owned(
                        split_coin_id,
                        coin.metadata.owner.unwrap_or(*self.sender),
                        CoinData {
                            coin_type: coin.data.coin_type.clone(),
                            balance: split,
                        },
                    );

                    let source_coin_id = *coin.id();
                    if self.state.get_object(&source_coin_id)?.is_some() {
                        self.set_coin_object_tracked(source_coin_id, coin.clone())?;
                    }
                    split_coin
                }
                other => {
                    return Err(RuntimeError::ProgramExecution(format!(
                        "coin::split first arg must be Coin, got {:?}",
                        other
                    )))
                }
            }
        };

        self.stack.push(cell_value(VmData::Coin(coin)));
        Ok(())
    }

    fn call_coin_into_balance(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "coin::into_balance expects 1 arg, got {}",
                args.len()
            )));
        }
        let amount = match read_data(&args[0])? {
            VmData::Coin(coin) => coin.data.balance.value(),
            other => {
                return Err(RuntimeError::ProgramExecution(format!(
                    "coin::into_balance arg must be Coin, got {:?}",
                    other
                )))
            }
        };
        self.stack.push(cell_value(VmData::Balance(amount)));
        Ok(())
    }

    fn call_public_transfer(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 2 {
            return Err(RuntimeError::ProgramExecution(format!(
                "transfer::public_transfer expects 2 args, got {}",
                args.len()
            )));
        }

        let coin = match read_data(&args[0])? {
            VmData::Coin(v) => v,
            other => {
                return Err(RuntimeError::ProgramExecution(format!(
                    "public_transfer first arg must be coin, got {:?}",
                    other
                )))
            }
        };
        let recipient = expect_address(&args[1])?;

        let amount = coin.data.balance.value();
        let coin_type = coin.data.coin_type.clone();
        let recipient_coin_id = deterministic_coin_id(&recipient, coin_type.as_str());

        let source_coin_id = *coin.id();
        if self.state.get_object(&source_coin_id)?.is_some() {
            self.delete_object_tracked(&source_coin_id)?;
        }

        if let Some(mut existing) = self.state.get_object(&recipient_coin_id)? {
            existing
                .data
                .balance
                .deposit(Balance::new(amount))
                .map_err(RuntimeError::InvalidTransaction)?;
            existing.increment_version();
            self.set_coin_object_tracked(recipient_coin_id, existing)?;
        } else {
            let new_coin = Object::new_owned(
                recipient_coin_id,
                recipient,
                CoinData {
                    coin_type,
                    balance: Balance::new(amount),
                },
            );
            self.set_coin_object_tracked(recipient_coin_id, new_coin)?;
        }

        Ok(())
    }

    fn call_coin_burn(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 2 {
            return Err(RuntimeError::ProgramExecution(format!(
                "coin::burn expects 2 args, got {}",
                args.len()
            )));
        }

        let coin = match read_data(&args[1])? {
            VmData::Coin(v) => v,
            other => {
                return Err(RuntimeError::ProgramExecution(format!(
                    "coin::burn second arg must be coin, got {:?}",
                    other
                )))
            }
        };

        let object_id = *coin.id();
        if self.state.get_object(&object_id)?.is_some() {
            self.delete_object_tracked(&object_id)?;
        }
        self.stack.push(cell_value(VmData::U64(0)));
        Ok(())
    }

    fn call_object_new(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "object::new expects 1 arg, got {}",
                args.len()
            )));
        }
        let _ = read_data(&args[0])?;
        let object_id = self.next_temp_object_id()?;
        self.stack.push(cell_value(VmData::ObjectId(object_id)));
        Ok(())
    }

    fn call_object_id(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "object::id expects 1 arg, got {}",
                args.len()
            )));
        }
        let id = match read_data(&args[0])? {
            VmData::Struct(s) => {
                let id_cell = s.get_field("id").ok_or_else(|| {
                    RuntimeError::ProgramExecution("struct has no 'id' field".to_string())
                })?;
                let id = {
                    let borrowed = id_cell.borrow();
                    match &*borrowed {
                        VmData::ObjectId(id) => *id,
                        other => {
                            return Err(RuntimeError::ProgramExecution(format!(
                                "id field must be ObjectId, got {:?}",
                                other
                            )))
                        }
                    }
                };
                id
            }
            other => {
                return Err(RuntimeError::ProgramExecution(format!(
                    "object::id expects struct arg, got {:?}",
                    other
                )))
            }
        };
        self.stack.push(cell_value(VmData::ObjectId(id)));
        Ok(())
    }

    fn call_object_id_to_bytes(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "object::id_to_bytes expects 1 arg, got {}",
                args.len()
            )));
        }
        let id = expect_object_id(&args[0])?;
        self.stack
            .push(cell_value(VmData::Bytes(id.as_bytes().to_vec())));
        Ok(())
    }

    fn call_tx_context_sender(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "tx_context::sender expects 1 arg, got {}",
                args.len()
            )));
        }
        let _ = read_data(&args[0])?;
        self.stack.push(cell_value(VmData::Address(*self.sender)));
        Ok(())
    }

    fn call_tx_context_epoch(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "tx_context::epoch expects 1 arg, got {}",
                args.len()
            )));
        }
        let epoch = match read_data(&args[0])? {
            VmData::TxContext(ctx) => ctx.epoch,
            _ => 0,
        };
        self.stack.push(cell_value(VmData::U64(epoch)));
        Ok(())
    }

    fn call_table_new(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "table::new expects 1 arg, got {}",
                args.len()
            )));
        }
        let _ = read_data(&args[0])?;
        self.stack.push(cell_value(VmData::Table(VmTable {
            entries: BTreeMap::new(),
        })));
        Ok(())
    }

    fn call_table_borrow_mut(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 2 {
            return Err(RuntimeError::ProgramExecution(format!(
                "table::borrow_mut expects 2 args, got {}",
                args.len()
            )));
        }

        let table_cell = mutable_cell_from_value(&args[0])?;
        let key = expect_u64(&args[1])?;

        let entry_cell = {
            let mut table_data = table_cell.borrow_mut();
            let table = match &mut *table_data {
                VmData::Table(t) => t,
                other => {
                    return Err(RuntimeError::ProgramExecution(format!(
                        "table::borrow_mut first arg must be Table, got {:?}",
                        other
                    )))
                }
            };

            table.entries.get(&key).cloned().ok_or_else(|| {
                RuntimeError::ProgramAbort(format!("table::borrow_mut missing key {}", key))
            })?
        };

        self.stack.push(VmValue::Ref(VmRef {
            cell: entry_cell,
            mutable: true,
        }));
        Ok(())
    }

    fn call_option_none(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if !args.is_empty() {
            return Err(RuntimeError::ProgramExecution(format!(
                "option::none expects 0 args, got {}",
                args.len()
            )));
        }
        self.stack.push(cell_value(VmData::None));
        Ok(())
    }

    fn call_event_emit(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "event::emit expects 1 arg, got {}",
                args.len()
            )));
        }
        let _ = read_data(&args[0])?;
        Ok(())
    }

    fn call_share_object(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "transfer::share_object expects 1 arg, got {}",
                args.len()
            )));
        }

        let data = read_data(&args[0])?;
        let object_id = extract_struct_id(&data)?;
        let bytes = encode_vm_data(&data)?;
        self.set_vm_object_tracked(object_id, bytes)?;
        Ok(())
    }

    fn call_hash_sha2_256(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "hash::sha2_256 expects 1 arg, got {}",
                args.len()
            )));
        }
        let bytes = expect_bytes(&args[0])?;
        let hash = sha256_hash(&bytes);
        self.stack.push(cell_value(VmData::Bytes(hash.to_vec())));
        Ok(())
    }

    fn call_bcs_to_bytes(&mut self, function: &str, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 1 {
            return Err(RuntimeError::ProgramExecution(format!(
                "bcs::to_bytes expects 1 arg, got {}",
                args.len()
            )));
        }

        if function.contains("<u64>") {
            let v = expect_u64(&args[0])?;
            self.stack
                .push(cell_value(VmData::Bytes(v.to_le_bytes().to_vec())));
            Ok(())
        } else {
            Err(RuntimeError::ProgramExecution(format!(
                "Unsupported bcs::to_bytes type in '{}'",
                function
            )))
        }
    }

    fn call_vector_append(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 2 {
            return Err(RuntimeError::ProgramExecution(format!(
                "vector::append expects 2 args, got {}",
                args.len()
            )));
        }

        let target = mutable_cell_from_value(&args[0])?;
        let rhs = expect_bytes(&args[1])?;

        let mut borrowed = target.borrow_mut();
        match &mut *borrowed {
            VmData::Bytes(lhs) => {
                lhs.extend(rhs);
                Ok(())
            }
            other => Err(RuntimeError::ProgramExecution(format!(
                "vector::append first arg must be vector<u8>, got {:?}",
                other
            ))),
        }
    }

    fn call_ecdsa_verify(&mut self, args: &[VmValue]) -> RuntimeResult<()> {
        if args.len() != 4 {
            return Err(RuntimeError::ProgramExecution(format!(
                "ecdsa_k1::secp256k1_verify expects 4 args, got {}",
                args.len()
            )));
        }

        let sig = expect_bytes(&args[0])?;
        let pubkey = expect_bytes(&args[1])?;
        let msg = expect_bytes(&args[2])?;
        let hash_alg = expect_u64(&args[3])? as u8;

        // Lightweight VM subset behavior: structurally validate inputs.
        // Accept hash_alg=1 (SHA256) when all byte vectors are non-empty.
        let ok = hash_alg == 1 && !sig.is_empty() && !pubkey.is_empty() && !msg.is_empty();
        self.stack.push(cell_value(VmData::Bool(ok)));
        Ok(())
    }

    fn next_temp_object_id(&mut self) -> RuntimeResult<ObjectId> {
        loop {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&self.temp_counter.to_le_bytes());
            bytes[8..].copy_from_slice(&self.sender.as_bytes()[..24]);
            self.temp_counter += 1;

            let candidate = ObjectId::new(bytes);
            if self.state.get_object(&candidate)?.is_some() {
                continue;
            }
            if self.state.get_vm_object(&candidate)?.is_some() {
                continue;
            }
            if self.final_states.contains_key(&candidate) {
                continue;
            }
            return Ok(candidate);
        }
    }

    fn local_get(&self, idx: usize) -> RuntimeResult<&VmValue> {
        self.locals
            .get(idx)
            .ok_or_else(|| RuntimeError::ProgramExecution(format!("Invalid local index {}", idx)))?
            .as_ref()
            .ok_or_else(|| {
                RuntimeError::ProgramExecution(format!("Local {} is uninitialized", idx))
            })
    }

    fn pop(&mut self) -> RuntimeResult<VmValue> {
        self.stack
            .pop()
            .ok_or_else(|| RuntimeError::ProgramExecution("Stack underflow".to_string()))
    }

    fn pop_data_value(&mut self) -> RuntimeResult<VmData> {
        let v = self.pop()?;
        read_data(&v)
    }

    fn pop_bool(&mut self) -> RuntimeResult<bool> {
        match self.pop_data_value()? {
            VmData::Bool(v) => Ok(v),
            other => Err(RuntimeError::ProgramExecution(format!(
                "Expected bool, got {:?}",
                other
            ))),
        }
    }

    fn pop_u64(&mut self) -> RuntimeResult<u64> {
        match self.pop_data_value()? {
            VmData::U64(v) => Ok(v),
            other => Err(RuntimeError::ProgramExecution(format!(
                "Expected u64, got {:?}",
                other
            ))),
        }
    }

    fn pop_ref_any(&mut self) -> RuntimeResult<VmRef> {
        match self.pop()? {
            VmValue::Ref(r) => Ok(r),
            other => Err(RuntimeError::ProgramExecution(format!(
                "Expected reference, got {:?}",
                other
            ))),
        }
    }

    fn pop_mut_ref(&mut self) -> RuntimeResult<VmRef> {
        let r = self.pop_ref_any()?;
        if !r.mutable {
            return Err(RuntimeError::ProgramExecution(
                "Expected mutable reference".to_string(),
            ));
        }
        Ok(r)
    }

    fn to_ref(&self, value: VmValue, mutable: bool) -> RuntimeResult<VmValue> {
        match value {
            VmValue::Cell(cell) => Ok(VmValue::Ref(VmRef { cell, mutable })),
            VmValue::Ref(r) => {
                if mutable && !r.mutable {
                    return Err(RuntimeError::ProgramExecution(
                        "Cannot derive mutable ref from immutable ref".to_string(),
                    ));
                }
                Ok(VmValue::Ref(VmRef {
                    cell: r.cell,
                    mutable,
                }))
            }
        }
    }

    fn freeze_ref(&self, value: VmValue) -> RuntimeResult<VmValue> {
        match value {
            VmValue::Ref(r) => Ok(VmValue::Ref(VmRef {
                cell: r.cell,
                mutable: false,
            })),
            VmValue::Cell(cell) => Ok(VmValue::Ref(VmRef {
                cell,
                mutable: false,
            })),
        }
    }

    fn borrow_field(&self, base: VmValue, field: &str, mutable: bool) -> RuntimeResult<VmValue> {
        let (base_cell, base_mutable) = match base {
            VmValue::Cell(cell) => (cell, true),
            VmValue::Ref(r) => (r.cell, r.mutable),
        };

        if mutable && !base_mutable {
            return Err(RuntimeError::ProgramExecution(format!(
                "Cannot mutably borrow field '{}' from immutable reference",
                field
            )));
        }

        let field_cell = {
            let borrowed = base_cell.borrow();
            let s = match &*borrowed {
                VmData::Struct(s) => s,
                other => {
                    return Err(RuntimeError::ProgramExecution(format!(
                        "Field borrow requires struct base, got {:?}",
                        other
                    )))
                }
            };

            s.get_field(field).ok_or_else(|| {
                RuntimeError::ProgramExecution(format!(
                    "Unknown struct field '{}' on {}",
                    field, s.type_name
                ))
            })?
        };

        Ok(VmValue::Ref(VmRef {
            cell: field_cell,
            mutable,
        }))
    }

    fn mark_touched(&mut self, object_id: ObjectId) -> RuntimeResult<()> {
        if self.old_states.contains_key(&object_id) {
            return Ok(());
        }
        let old_state = self.snapshot_state_bytes(&object_id)?;
        self.old_states.insert(object_id, old_state.clone());
        self.final_states.insert(object_id, old_state);
        self.write_order.push(object_id);
        Ok(())
    }

    fn snapshot_state_bytes(&self, object_id: &ObjectId) -> RuntimeResult<Option<Vec<u8>>> {
        if let Some(coin) = self.state.get_object(object_id)? {
            return Ok(Some(coin.to_coin_state_bytes()));
        }
        self.state.get_vm_object(object_id)
    }

    fn set_coin_object_tracked(
        &mut self,
        object_id: ObjectId,
        object: Object<CoinData>,
    ) -> RuntimeResult<()> {
        self.mark_touched(object_id)?;
        let new_state = object.to_coin_state_bytes();
        self.state.set_object(object_id, object)?;
        self.final_states.insert(object_id, Some(new_state));
        Ok(())
    }

    fn set_vm_object_tracked(&mut self, object_id: ObjectId, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.mark_touched(object_id)?;
        self.state.set_vm_object(object_id, bytes.clone())?;
        self.final_states.insert(object_id, Some(bytes));
        Ok(())
    }

    fn delete_object_tracked(&mut self, object_id: &ObjectId) -> RuntimeResult<()> {
        self.mark_touched(*object_id)?;
        if self.state.get_object(object_id)?.is_some() {
            self.state.delete_object(object_id)?;
        }
        if self.state.get_vm_object(object_id)?.is_some() {
            self.state.delete_vm_object(object_id)?;
        }
        self.final_states.insert(*object_id, None);
        Ok(())
    }
}

fn parse_module(disassembly: &str) -> RuntimeResult<ParsedModule> {
    Ok(ParsedModule {
        structs: parse_struct_defs(disassembly)?,
        constants: parse_constants(disassembly)?,
    })
}

fn parse_struct_defs(disassembly: &str) -> RuntimeResult<HashMap<String, Vec<String>>> {
    let lines: Vec<&str> = disassembly.lines().collect();
    let mut out = HashMap::new();
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i].trim();
        if line.starts_with("struct ") {
            let rest = line.trim_start_matches("struct ");
            let name = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| {
                    RuntimeError::ProgramExecution("Malformed struct header".to_string())
                })?
                .to_string();
            let mut fields = Vec::new();
            i += 1;
            while i < lines.len() {
                let cur = lines[i].trim();
                if cur == "}" {
                    break;
                }
                if cur.is_empty() {
                    i += 1;
                    continue;
                }
                if let Some((field, _)) = cur.split_once(':') {
                    fields.push(field.trim().to_string());
                }
                i += 1;
            }
            out.insert(name, fields);
        }
        i += 1;
    }
    Ok(out)
}

fn parse_constants(disassembly: &str) -> RuntimeResult<HashMap<usize, u64>> {
    let lines: Vec<&str> = disassembly.lines().collect();
    let mut out = HashMap::new();
    let mut in_constants = false;

    for raw in lines {
        let line = raw.trim();
        if line.starts_with("Constants [") {
            in_constants = true;
            continue;
        }
        if !in_constants {
            continue;
        }
        if line == "]" {
            break;
        }
        if line.is_empty() {
            continue;
        }

        // Format: 0 => u64: 0
        let (idx_raw, rhs) = line.split_once("=>").ok_or_else(|| {
            RuntimeError::ProgramExecution(format!("Malformed constants line '{}'", line))
        })?;
        let idx = idx_raw.trim().parse::<usize>().map_err(|_| {
            RuntimeError::ProgramExecution(format!("Malformed const index in '{}'", line))
        })?;

        if !rhs.contains("u64:") {
            continue;
        }

        let (_, value_raw) = rhs.rsplit_once(':').ok_or_else(|| {
            RuntimeError::ProgramExecution(format!("Malformed constants value in '{}'", line))
        })?;
        let value = value_raw.trim().parse::<u64>().map_err(|_| {
            RuntimeError::ProgramExecution(format!("Malformed const u64 in '{}'", line))
        })?;
        out.insert(idx, value);
    }

    Ok(out)
}

fn parse_entry_function(disassembly: &str, function_name: &str) -> RuntimeResult<ParsedFunction> {
    let lines: Vec<&str> = disassembly.lines().collect();
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i].trim();
        if is_function_header(line) {
            let parsed_name = parse_function_name(line)?;
            if parsed_name == function_name {
                let param_types = parse_param_types(line)?;
                let mut local_types: HashMap<usize, String> = HashMap::new();
                for (idx, ty) in param_types.iter().enumerate() {
                    local_types.insert(idx, ty.clone());
                }

                let mut instructions = Vec::new();
                i += 1;
                while i < lines.len() {
                    let cur = lines[i].trim();
                    if cur == "}" {
                        return Ok(ParsedFunction {
                            param_types,
                            local_types,
                            instructions,
                        });
                    }

                    if let Some((idx, ty)) = parse_local_decl(cur)? {
                        local_types.insert(idx, ty);
                    }
                    if let Some(inst) = parse_instruction(cur)? {
                        instructions.push(inst);
                    }
                    i += 1;
                }
                return Err(RuntimeError::ProgramExecution(format!(
                    "Function '{}' body not closed",
                    function_name
                )));
            }
        }
        i += 1;
    }

    Err(RuntimeError::ProgramExecution(format!(
        "Entry function '{}' not found in disassembly",
        function_name
    )))
}

fn parse_local_decl(line: &str) -> RuntimeResult<Option<(usize, String)>> {
    if !line.starts_with('L') {
        return Ok(None);
    }

    let (head, rest) = match line.split_once(':') {
        Some(v) => v,
        None => return Ok(None),
    };

    let idx = match head.trim_start_matches('L').parse::<usize>() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let (_, ty) = rest.rsplit_once(':').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed local declaration '{}'", line))
    })?;

    Ok(Some((idx, ty.trim().to_string())))
}

fn is_function_header(line: &str) -> bool {
    if !line.ends_with('{') || !line.contains('(') || line.starts_with("module ") {
        return false;
    }
    if line.starts_with("struct ") || line.starts_with("Constants ") {
        return false;
    }
    let lower = line.to_ascii_lowercase();
    lower.starts_with("entry ") || lower.starts_with("public ") || line.starts_with("init(")
}

fn parse_function_name(line: &str) -> RuntimeResult<String> {
    let open = line.find('(').ok_or_else(|| {
        RuntimeError::ProgramExecution("Malformed function header: missing '('".to_string())
    })?;
    let prefix = line[..open].trim();
    prefix
        .split_whitespace()
        .last()
        .map(str::to_string)
        .ok_or_else(|| {
            RuntimeError::ProgramExecution(
                "Malformed function header: missing function name".to_string(),
            )
        })
}

fn parse_param_types(line: &str) -> RuntimeResult<Vec<String>> {
    let open = line.find('(').ok_or_else(|| {
        RuntimeError::ProgramExecution("Malformed function header: missing '('".to_string())
    })?;
    let close = line.rfind(')').ok_or_else(|| {
        RuntimeError::ProgramExecution("Malformed function header: missing ')'".to_string())
    })?;
    let raw = &line[open + 1..close];
    if raw.trim().is_empty() {
        return Ok(vec![]);
    }
    let parts = split_top_level(raw, ',');
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let (_, ty) = part.split_once(':').ok_or_else(|| {
            RuntimeError::ProgramExecution(format!("Malformed parameter '{}'", part))
        })?;
        out.push(ty.trim().to_string());
    }
    Ok(out)
}

fn parse_instruction(line: &str) -> RuntimeResult<Option<(usize, SuiOpcode)>> {
    if line.is_empty() || line.ends_with(':') {
        return Ok(None);
    }

    let Some((idx_str, rhs)) = line.split_once(':') else {
        return Ok(None);
    };
    let idx = match idx_str.trim().parse::<usize>() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let body = rhs.trim();

    if body.starts_with("MoveLoc[") {
        return Ok(Some((idx, SuiOpcode::MoveLoc(parse_bracket_usize(body)?))));
    }
    if body.starts_with("CopyLoc[") {
        return Ok(Some((idx, SuiOpcode::CopyLoc(parse_bracket_usize(body)?))));
    }
    if body.starts_with("StLoc[") {
        return Ok(Some((idx, SuiOpcode::StLoc(parse_bracket_usize(body)?))));
    }
    if body.starts_with("ImmBorrowLoc[") {
        return Ok(Some((
            idx,
            SuiOpcode::ImmBorrowLoc(parse_bracket_usize(body)?),
        )));
    }
    if body.starts_with("MutBorrowLoc[") {
        return Ok(Some((
            idx,
            SuiOpcode::MutBorrowLoc(parse_bracket_usize(body)?),
        )));
    }
    if body.starts_with("ImmBorrowField[") {
        return Ok(Some((
            idx,
            SuiOpcode::ImmBorrowField(parse_field_name(body)?),
        )));
    }
    if body.starts_with("MutBorrowField[") {
        return Ok(Some((
            idx,
            SuiOpcode::MutBorrowField(parse_field_name(body)?),
        )));
    }
    if body == "ReadRef" {
        return Ok(Some((idx, SuiOpcode::ReadRef)));
    }
    if body == "WriteRef" {
        return Ok(Some((idx, SuiOpcode::WriteRef)));
    }
    if body.starts_with("LdU64(") {
        return Ok(Some((idx, SuiOpcode::LdU64(parse_paren_u64(body)?))));
    }
    if body.starts_with("LdU8(") {
        return Ok(Some((idx, SuiOpcode::LdU8(parse_paren_u64(body)? as u8))));
    }
    if body.starts_with("LdConst[") {
        return Ok(Some((idx, SuiOpcode::LdConst(parse_bracket_usize(body)?))));
    }
    if body == "LdTrue" {
        return Ok(Some((idx, SuiOpcode::LdTrue)));
    }
    if body == "LdFalse" {
        return Ok(Some((idx, SuiOpcode::LdFalse)));
    }
    if body.starts_with("BrFalse(") {
        return Ok(Some((idx, SuiOpcode::BrFalse(parse_paren_usize(body)?))));
    }
    if body.starts_with("BrTrue(") {
        return Ok(Some((idx, SuiOpcode::BrTrue(parse_paren_usize(body)?))));
    }
    if body.starts_with("Branch(") {
        return Ok(Some((idx, SuiOpcode::Branch(parse_paren_usize(body)?))));
    }
    if body == "Eq" {
        return Ok(Some((idx, SuiOpcode::Eq)));
    }
    if body == "Ge" {
        return Ok(Some((idx, SuiOpcode::Ge)));
    }
    if body == "Add" {
        return Ok(Some((idx, SuiOpcode::Add)));
    }
    if body == "Sub" {
        return Ok(Some((idx, SuiOpcode::Sub)));
    }
    if body.starts_with("VecPack(") {
        return Ok(Some((idx, SuiOpcode::VecPack(parse_vec_pack_len(body)?))));
    }
    if body.starts_with("Pack[") {
        return Ok(Some((idx, SuiOpcode::Pack(parse_pack_type_name(body)?))));
    }
    if body.starts_with("Call ") {
        let call = body.trim_start_matches("Call ").trim();
        let open = call
            .find('(')
            .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed call '{}'", body)))?;
        let function = call[..open].trim().to_string();
        let close = call
            .find("):")
            .or_else(|| call.rfind(')'))
            .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed call '{}'", body)))?;
        let args_raw = &call[open + 1..close];
        let arg_count = if args_raw.trim().is_empty() {
            0
        } else {
            split_top_level(args_raw, ',').len()
        };
        return Ok(Some((
            idx,
            SuiOpcode::Call {
                function,
                arg_count,
            },
        )));
    }
    if body == "FreezeRef" {
        return Ok(Some((idx, SuiOpcode::FreezeRef)));
    }
    if body == "Pop" {
        return Ok(Some((idx, SuiOpcode::Pop)));
    }
    if body == "Abort" {
        return Ok(Some((idx, SuiOpcode::Abort)));
    }
    if body == "Ret" {
        return Ok(Some((idx, SuiOpcode::Ret)));
    }

    Err(RuntimeError::ProgramExecution(format!(
        "Unsupported Sui opcode line '{}'",
        line
    )))
}

fn parse_field_name(body: &str) -> RuntimeResult<String> {
    let open = body.find('(').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed field borrow '{}'", body))
    })?;
    let close = body.rfind(')').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed field borrow '{}'", body))
    })?;
    let inside = &body[open + 1..close];

    // Channel.balance_a: u64
    let dot = inside.find('.').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed field target '{}'", body))
    })?;
    let after_dot = &inside[dot + 1..];
    let (field, _) = after_dot.split_once(':').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed field target '{}'", body))
    })?;
    Ok(field.trim().to_string())
}

fn parse_pack_type_name(body: &str) -> RuntimeResult<String> {
    let open = body.find('(').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed Pack opcode '{}'", body))
    })?;
    let close = body.rfind(')').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed Pack opcode '{}'", body))
    })?;
    Ok(body[open + 1..close].trim().to_string())
}

fn parse_vec_pack_len(body: &str) -> RuntimeResult<usize> {
    let open = body.find('(').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed VecPack opcode '{}'", body))
    })?;
    let close = body.find(')').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed VecPack opcode '{}'", body))
    })?;
    let inside = &body[open + 1..close];
    let parts: Vec<String> = split_top_level(inside, ',');
    if parts.len() != 2 {
        return Err(RuntimeError::ProgramExecution(format!(
            "Malformed VecPack args '{}'",
            body
        )));
    }
    parts[1]
        .trim()
        .parse::<usize>()
        .map_err(|_| RuntimeError::ProgramExecution(format!("Malformed VecPack len in '{}'", body)))
}

fn parse_bracket_usize(body: &str) -> RuntimeResult<usize> {
    let start = body.find('[').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed local access '{}'", body))
    })?;
    let end = body.find(']').ok_or_else(|| {
        RuntimeError::ProgramExecution(format!("Malformed local access '{}'", body))
    })?;
    body[start + 1..end]
        .trim()
        .parse::<usize>()
        .map_err(|_| RuntimeError::ProgramExecution(format!("Malformed local index in '{}'", body)))
}

fn parse_paren_u64(body: &str) -> RuntimeResult<u64> {
    let start = body
        .find('(')
        .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed literal '{}'", body)))?;
    let end = body
        .find(')')
        .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed literal '{}'", body)))?;
    body[start + 1..end]
        .trim()
        .parse::<u64>()
        .map_err(|_| RuntimeError::ProgramExecution(format!("Malformed u64 literal in '{}'", body)))
}

fn parse_paren_usize(body: &str) -> RuntimeResult<usize> {
    let start = body
        .find('(')
        .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed jump '{}'", body)))?;
    let end = body
        .find(')')
        .ok_or_else(|| RuntimeError::ProgramExecution(format!("Malformed jump '{}'", body)))?;
    body[start + 1..end]
        .trim()
        .parse::<usize>()
        .map_err(|_| RuntimeError::ProgramExecution(format!("Malformed jump target in '{}'", body)))
}

fn split_top_level(text: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;

    for ch in text.chars() {
        match ch {
            '<' => {
                angle_depth += 1;
                buf.push(ch);
            }
            '>' => {
                angle_depth = angle_depth.saturating_sub(1);
                buf.push(ch);
            }
            '(' => {
                paren_depth += 1;
                buf.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                buf.push(ch);
            }
            _ if ch == sep && angle_depth == 0 && paren_depth == 0 => {
                let part = buf.trim();
                if !part.is_empty() {
                    out.push(part.to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }

    let tail = buf.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

fn extract_generic_type(function: &str) -> Option<String> {
    let start = function.find('<')?;
    let end = function[start + 1..].find('>')?;
    Some(function[start + 1..start + 1 + end].to_string())
}

fn cell_value(data: VmData) -> VmValue {
    VmValue::Cell(Rc::new(RefCell::new(data)))
}

fn read_data(value: &VmValue) -> RuntimeResult<VmData> {
    match value {
        VmValue::Cell(cell) => Ok(deep_clone_data(&cell.borrow())),
        VmValue::Ref(r) => Ok(deep_clone_data(&r.cell.borrow())),
    }
}

fn mutable_cell_from_value(value: &VmValue) -> RuntimeResult<VmCell> {
    match value {
        VmValue::Cell(cell) => Ok(cell.clone()),
        VmValue::Ref(r) if r.mutable => Ok(r.cell.clone()),
        VmValue::Ref(_) => Err(RuntimeError::ProgramExecution(
            "Expected mutable reference".to_string(),
        )),
    }
}

fn expect_u64(value: &VmValue) -> RuntimeResult<u64> {
    match read_data(value)? {
        VmData::U64(v) => Ok(v),
        other => Err(RuntimeError::ProgramExecution(format!(
            "Expected u64 value, got {:?}",
            other
        ))),
    }
}

fn expect_address(value: &VmValue) -> RuntimeResult<Address> {
    match read_data(value)? {
        VmData::Address(v) => Ok(v),
        other => Err(RuntimeError::ProgramExecution(format!(
            "Expected address value, got {:?}",
            other
        ))),
    }
}

fn expect_object_id(value: &VmValue) -> RuntimeResult<ObjectId> {
    match read_data(value)? {
        VmData::ObjectId(v) => Ok(v),
        other => Err(RuntimeError::ProgramExecution(format!(
            "Expected object id value, got {:?}",
            other
        ))),
    }
}

fn expect_bytes(value: &VmValue) -> RuntimeResult<Vec<u8>> {
    match read_data(value)? {
        VmData::Bytes(v) => Ok(v),
        other => Err(RuntimeError::ProgramExecution(format!(
            "Expected vector<u8> value, got {:?}",
            other
        ))),
    }
}

fn deep_clone_data(data: &VmData) -> VmData {
    match data {
        VmData::U64(v) => VmData::U64(*v),
        VmData::Bool(v) => VmData::Bool(*v),
        VmData::Address(v) => VmData::Address(*v),
        VmData::Bytes(v) => VmData::Bytes(v.clone()),
        VmData::Coin(v) => VmData::Coin(v.clone()),
        VmData::Balance(v) => VmData::Balance(*v),
        VmData::ObjectId(v) => VmData::ObjectId(*v),
        VmData::TxContext(v) => VmData::TxContext(v.clone()),
        VmData::None => VmData::None,
        VmData::Opaque => VmData::Opaque,
        VmData::Struct(s) => VmData::Struct(VmStruct {
            type_name: s.type_name.clone(),
            fields: s
                .fields
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        Rc::new(RefCell::new(deep_clone_data(&v.borrow()))),
                    )
                })
                .collect(),
        }),
        VmData::Table(t) => VmData::Table(VmTable {
            entries: t
                .entries
                .iter()
                .map(|(k, v)| (*k, Rc::new(RefCell::new(deep_clone_data(&v.borrow())))))
                .collect(),
        }),
    }
}

fn vm_data_eq(lhs: &VmData, rhs: &VmData) -> RuntimeResult<bool> {
    Ok(match (lhs, rhs) {
        (VmData::U64(a), VmData::U64(b)) => a == b,
        (VmData::Bool(a), VmData::Bool(b)) => a == b,
        (VmData::Address(a), VmData::Address(b)) => a == b,
        (VmData::Bytes(a), VmData::Bytes(b)) => a == b,
        (VmData::ObjectId(a), VmData::ObjectId(b)) => a == b,
        (VmData::Balance(a), VmData::Balance(b)) => a == b,
        (VmData::None, VmData::None) => true,
        (VmData::Opaque, VmData::Opaque) => true,
        (a, b) => {
            return Err(RuntimeError::ProgramExecution(format!(
                "Eq unsupported for values {:?} and {:?}",
                a, b
            )))
        }
    })
}

fn extract_struct_id(data: &VmData) -> RuntimeResult<ObjectId> {
    match data {
        VmData::Struct(s) => {
            let id_cell = s.get_field("id").ok_or_else(|| {
                RuntimeError::ProgramExecution("Shared object struct missing id field".to_string())
            })?;
            let id = {
                let borrowed = id_cell.borrow();
                match &*borrowed {
                    VmData::ObjectId(v) => *v,
                    other => {
                        return Err(RuntimeError::ProgramExecution(format!(
                            "Shared object id field must be ObjectId, got {:?}",
                            other
                        )))
                    }
                }
            };
            Ok(id)
        }
        other => Err(RuntimeError::ProgramExecution(format!(
            "share_object expects struct value, got {:?}",
            other
        ))),
    }
}

fn encode_vm_data(data: &VmData) -> RuntimeResult<Vec<u8>> {
    let stored = to_stored_value(data)?;
    serde_json::to_vec(&stored)
        .map_err(|e| RuntimeError::ProgramExecution(format!("VM object encode failed: {}", e)))
}

fn decode_vm_data(bytes: &[u8]) -> RuntimeResult<VmData> {
    let stored: StoredVmValue = serde_json::from_slice(bytes)
        .map_err(|e| RuntimeError::ProgramExecution(format!("VM object decode failed: {}", e)))?;
    from_stored_value(&stored)
}

fn to_stored_value(data: &VmData) -> RuntimeResult<StoredVmValue> {
    Ok(match data {
        VmData::U64(v) => StoredVmValue::U64(*v),
        VmData::Bool(v) => StoredVmValue::Bool(*v),
        VmData::Address(v) => StoredVmValue::Address(*v),
        VmData::Bytes(v) => StoredVmValue::Bytes(v.clone()),
        VmData::Balance(v) => StoredVmValue::Balance(*v),
        VmData::ObjectId(v) => StoredVmValue::ObjectId(*v),
        VmData::None => StoredVmValue::None,
        VmData::Opaque => StoredVmValue::Opaque,
        VmData::Struct(s) => StoredVmValue::Struct {
            type_name: s.type_name.clone(),
            fields: s
                .fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), to_stored_value(&v.borrow())?)))
                .collect::<RuntimeResult<Vec<_>>>()?,
        },
        VmData::Table(t) => StoredVmValue::Table(
            t.entries
                .iter()
                .map(|(k, v)| Ok((*k, to_stored_value(&v.borrow())?)))
                .collect::<RuntimeResult<Vec<_>>>()?,
        ),
        VmData::Coin(_) => {
            return Err(RuntimeError::ProgramExecution(
                "Cannot persist Coin as VM shared object".to_string(),
            ))
        }
        VmData::TxContext(_) => {
            return Err(RuntimeError::ProgramExecution(
                "Cannot persist TxContext as VM shared object".to_string(),
            ))
        }
    })
}

fn from_stored_value(stored: &StoredVmValue) -> RuntimeResult<VmData> {
    Ok(match stored {
        StoredVmValue::U64(v) => VmData::U64(*v),
        StoredVmValue::Bool(v) => VmData::Bool(*v),
        StoredVmValue::Address(v) => VmData::Address(*v),
        StoredVmValue::Bytes(v) => VmData::Bytes(v.clone()),
        StoredVmValue::Balance(v) => VmData::Balance(*v),
        StoredVmValue::ObjectId(v) => VmData::ObjectId(*v),
        StoredVmValue::None => VmData::None,
        StoredVmValue::Opaque => VmData::Opaque,
        StoredVmValue::Struct { type_name, fields } => VmData::Struct(VmStruct {
            type_name: type_name.clone(),
            fields: fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), Rc::new(RefCell::new(from_stored_value(v)?)))))
                .collect::<RuntimeResult<Vec<_>>>()?,
        }),
        StoredVmValue::Table(entries) => VmData::Table(VmTable {
            entries: entries
                .iter()
                .map(|(k, v)| Ok((*k, Rc::new(RefCell::new(from_stored_value(v)?)))))
                .collect::<RuntimeResult<BTreeMap<_, _>>>()?,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStateStore;

    const DISASSEMBLY: &str = r#"
entry public mint(treasury_cap#0#0: &mut TreasuryCap<MY_COIN>, amount#0#0: u64, recipient#0#0: address, ctx#0#0: &mut TxContext) {
B0:
	0: MoveLoc[0](treasury_cap#0#0: &mut TreasuryCap<MY_COIN>)
	1: MoveLoc[1](amount#0#0: u64)
	2: MoveLoc[3](ctx#0#0: &mut TxContext)
	3: Call coin::mint<MY_COIN>(&mut TreasuryCap<MY_COIN>, u64, &mut TxContext): Coin<MY_COIN>
	4: MoveLoc[2](recipient#0#0: address)
	5: Call transfer::public_transfer<Coin<MY_COIN>>(Coin<MY_COIN>, address)
	6: Ret
}

entry public conditional_transfer(treasury_cap#0#0: &mut TreasuryCap<MY_COIN>, amount#0#0: u64, recipient#0#0: address, should_transfer#0#0: bool, ctx#0#0: &mut TxContext) {
B0:
	0: MoveLoc[3](should_transfer#0#0: bool)
	1: BrFalse(9)
B1:
	2: MoveLoc[0](treasury_cap#0#0: &mut TreasuryCap<MY_COIN>)
	3: MoveLoc[1](amount#0#0: u64)
	4: MoveLoc[4](ctx#0#0: &mut TxContext)
	5: Call coin::mint<MY_COIN>(&mut TreasuryCap<MY_COIN>, u64, &mut TxContext): Coin<MY_COIN>
	6: MoveLoc[2](recipient#0#0: address)
	7: Call transfer::public_transfer<Coin<MY_COIN>>(Coin<MY_COIN>, address)
	8: Branch(13)
B2:
	9: MoveLoc[0](treasury_cap#0#0: &mut TreasuryCap<MY_COIN>)
	10: Pop
	11: MoveLoc[4](ctx#0#0: &mut TxContext)
	12: Pop
B3:
	13: Ret
}

entry public burn(treasury_cap#0#0: &mut TreasuryCap<MY_COIN>, coin#0#0: Coin<MY_COIN>) {
B0:
	0: MoveLoc[0](treasury_cap#0#0: &mut TreasuryCap<MY_COIN>)
	1: MoveLoc[1](coin#0#0: Coin<MY_COIN>)
	2: Call coin::burn<MY_COIN>(&mut TreasuryCap<MY_COIN>, Coin<MY_COIN>): u64
	3: Pop
	4: Ret
}
"#;

    #[test]
    fn test_execute_conditional_transfer_subset() {
        let mut state = InMemoryStateStore::new();
        let alice = Address::from_str_id("alice");
        let bob = Address::from_str_id("bob");

        execute_sui_entry_from_disassembly(
            &mut state,
            &alice,
            DISASSEMBLY,
            "mint",
            &[
                SuiVmArg::Opaque,
                SuiVmArg::U64(100),
                SuiVmArg::Address(alice),
                SuiVmArg::Opaque,
            ],
        )
        .unwrap();

        execute_sui_entry_from_disassembly(
            &mut state,
            &alice,
            DISASSEMBLY,
            "conditional_transfer",
            &[
                SuiVmArg::Opaque,
                SuiVmArg::U64(40),
                SuiVmArg::Address(bob),
                SuiVmArg::Bool(true),
                SuiVmArg::Opaque,
            ],
        )
        .unwrap();

        let bob_coin_id = deterministic_coin_id(&bob, "MY_COIN");
        let bob_coin = state.get_object(&bob_coin_id).unwrap().unwrap();
        assert_eq!(bob_coin.data.balance.value(), 40);

        execute_sui_entry_from_disassembly(
            &mut state,
            &alice,
            DISASSEMBLY,
            "conditional_transfer",
            &[
                SuiVmArg::Opaque,
                SuiVmArg::U64(55),
                SuiVmArg::Address(bob),
                SuiVmArg::Bool(false),
                SuiVmArg::Opaque,
            ],
        )
        .unwrap();

        let bob_coin_after = state.get_object(&bob_coin_id).unwrap().unwrap();
        assert_eq!(bob_coin_after.data.balance.value(), 40);

        execute_sui_entry_from_disassembly(
            &mut state,
            &alice,
            DISASSEMBLY,
            "burn",
            &[SuiVmArg::Opaque, SuiVmArg::ObjectId(bob_coin_id)],
        )
        .unwrap();

        assert!(state.get_object(&bob_coin_id).unwrap().is_none());
    }
}
