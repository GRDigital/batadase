use super::{Transaction, RwTxn};
use std::convert::AsMut;
use fehler::throws;
pub use self::error::Error;
pub mod error;

#[enumflags2::bitflags]
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbFlags {
	ReverseKey = lmdb_sys::MDB_REVERSEKEY, // keys compared in reverse order
	IntegerKey = lmdb_sys::MDB_INTEGERKEY, // keys are binary integers in native byte order (u32 [C unsigned int] or usize [C size_t]), all must be same size
	Create = lmdb_sys::MDB_CREATE,         // create db if it doesnt' exist, only write tx

	DupSort = lmdb_sys::MDB_DUPSORT, // allow duplicate keys, stored in sorted order
		DupFixed = lmdb_sys::MDB_DUPFIXED,     // all values are same size
		IntegerDup = lmdb_sys::MDB_INTEGERDUP, // duplicate data items are binary integers
		ReverseDup = lmdb_sys::MDB_REVERSEDUP, // duplicate data items should be compared in reverse order
}

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorOpFlags {
	GetCurrent = lmdb_sys::MDB_GET_CURRENT, // Return key/data at current cursor position

	First = lmdb_sys::MDB_FIRST, // Position at first key/data item
	Last = lmdb_sys::MDB_LAST,   // Position at last key/data item

	Next = lmdb_sys::MDB_NEXT, // Position at next data item
	Prev = lmdb_sys::MDB_PREV, // Position at previous data item

	Set = lmdb_sys::MDB_SET,            // Position at specified key
	SetKey = lmdb_sys::MDB_SET_KEY,     // Position at specified key, return key + data
	SetRange = lmdb_sys::MDB_SET_RANGE, // Position at first key greater than or equal to specified key.

	// ONLY DbFlags::DupFixed
		GetMultiple = lmdb_sys::MDB_GET_MULTIPLE,   // Return key and up to a page of duplicate data items from current cursor position. Move cursor to prepare for CursorOpFlags::NextMultiple
		NextMultiple = lmdb_sys::MDB_NEXT_MULTIPLE, // Return key and up to a page of duplicate data items from next cursor position. Move cursor to prepare for CursorOpFlags::NextMultiple

	// ONLY DbFlags::DupSort
		FirstDup = lmdb_sys::MDB_FIRST_DUP, // Position at first data item of current key
		LastDup = lmdb_sys::MDB_LAST_DUP,   // Position at last data item of current key

		NextDup = lmdb_sys::MDB_NEXT_DUP,     // Position at next data item of current key
		NextNodup = lmdb_sys::MDB_NEXT_NODUP, // Position at first data item of next key
		PrevDup = lmdb_sys::MDB_PREV_DUP,     // Position at previous data item of current key
		PrevNodup = lmdb_sys::MDB_PREV_NODUP, // Position at last data item of previous key

		GetBoth = lmdb_sys::MDB_GET_BOTH,            // Position at key/data pair
		GetBothRange = lmdb_sys::MDB_GET_BOTH_RANGE, // position at key, nearest data
}

#[repr(transparent)]
struct Val<'a>(lmdb_sys::MDB_val, std::marker::PhantomData<&'a ()>);

impl<'a> Val<'a> {
	fn from_buf(mut buf: impl AsMut<[u8]> + 'a) -> Self {
		let buf = buf.as_mut();
		Self(lmdb_sys::MDB_val { mv_size: buf.len(), mv_data: buf.as_mut_ptr().cast() }, std::marker::PhantomData)
	}

	fn new_outparam<'tx: 'a>(_tx: &'tx impl Transaction) -> Self {
		Self(lmdb_sys::MDB_val { mv_size: 0, mv_data: std::ptr::null_mut() }, std::marker::PhantomData)
	}

	fn as_slice(&self) -> &'a [u8] {
		unsafe { std::slice::from_raw_parts(self.mv_data.cast::<u8>(), self.mv_size) }
	}
}

impl std::ops::Deref for Val<'_> {
	type Target = lmdb_sys::MDB_val;

	fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::DerefMut for Val<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

pub(super) struct Cursor<'tx, TX: Transaction>(*mut lmdb_sys::MDB_cursor, &'tx TX);
unsafe impl<'tx, TX: Transaction> Send for Cursor<'tx, TX> {}
unsafe impl<'tx, TX: Transaction> Sync for Cursor<'tx, TX> {}

impl<'tx, TX: Transaction> Cursor<'tx, TX> {
	#[throws]
	pub(super) fn open(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		let mut cursor = std::ptr::null_mut();
		error::handle_cursor_open_code(unsafe { lmdb_sys::mdb_cursor_open(tx.raw(), dbi, &mut cursor) })?;
		Self(cursor, tx)
	}

	pub(super) fn get(&mut self, flags: CursorOpFlags) -> Option<(&'tx [u8], &'tx [u8])> {
		let mut key = Val::new_outparam(self.1);
		let mut value = Val::new_outparam(self.1);
		if !error::handle_cursor_get_code(unsafe { lmdb_sys::mdb_cursor_get(self.0, &mut *key, &mut *value, flags as _) }) { return None }
		Some((
			key.as_slice(),
			value.as_slice(),
		))
	}

	pub(super) fn get_with_u64_key(&mut self, flags: CursorOpFlags) -> Option<(u64, &'tx [u8])> {
		let mut key = Val::new_outparam(self.1);
		let mut value = Val::new_outparam(self.1);
		if !error::handle_cursor_get_code(unsafe { lmdb_sys::mdb_cursor_get(self.0, &mut *key, &mut *value, flags as _) }) { return None }
		debug_assert!(key.mv_size == std::mem::size_of::<u64>());
		Some((
			u64::from_ne_bytes(unsafe { *key.mv_data.cast::<[u8; std::mem::size_of::<u64>()]>() }),
			value.as_slice(),
		))
	}
}

impl<TX: Transaction> Drop for Cursor<'_, TX> {
	fn drop(&mut self) {
		unsafe { lmdb_sys::mdb_cursor_close(self.0) };
	}
}

#[throws]
pub(super) fn put(tx: &RwTxn, dbi: lmdb_sys::MDB_dbi, key: impl AsMut<[u8]>, val: impl AsMut<[u8]>) {
	error::handle_put_code(unsafe { lmdb_sys::mdb_put(tx.raw(), dbi, &mut *Val::from_buf(key), &mut *Val::from_buf(val), 0) })?;
}

#[throws]
pub(super) fn del(tx: &RwTxn, dbi: lmdb_sys::MDB_dbi, key: impl AsMut<[u8]>) -> bool {
	error::handle_del_code(unsafe { lmdb_sys::mdb_del(tx.raw(), dbi, &mut *Val::from_buf(key), std::ptr::null_mut()) })?
}

#[throws]
pub(super) fn drop(tx: &RwTxn, dbi: lmdb_sys::MDB_dbi) {
	error::handle_drop_code(unsafe { lmdb_sys::mdb_drop(tx.raw(), dbi, 0) })?
}

#[throws]
pub(super) fn get(tx: &impl Transaction, dbi: lmdb_sys::MDB_dbi, key: impl AsMut<[u8]>) -> Option<&[u8]> {
	let mut value = Val::new_outparam(tx);
	if !error::handle_get_code(unsafe { lmdb_sys::mdb_get(tx.raw(), dbi, &mut *Val::from_buf(key), &mut *value) })? { return None; }
	Some(value.as_slice())
}

#[throws]
pub(super) fn txn_begin(env: *mut lmdb_sys::MDB_env, flags: u32) -> *mut lmdb_sys::MDB_txn {
	let mut tx: *mut lmdb_sys::MDB_txn = std::ptr::null_mut();
	error::handle_txn_begin_code(unsafe { lmdb_sys::mdb_txn_begin(env, std::ptr::null_mut(), flags, &mut tx) })?;
	tx
}

#[throws]
pub(super) fn txn_commit(tx: *mut lmdb_sys::MDB_txn) {
	error::handle_txn_commit_code(unsafe { lmdb_sys::mdb_txn_commit(tx) })?;
}

#[throws]
pub(super) fn env_create() -> *mut lmdb_sys::MDB_env {
	let mut env: *mut lmdb_sys::MDB_env = std::ptr::null_mut();
	error::handle_env_create_code(unsafe { lmdb_sys::mdb_env_create(&mut env) })?;
	env
}

#[throws]
pub(super) fn env_set_maxdbs(env: *mut lmdb_sys::MDB_env, maxdbs: u32) {
	error::handle_env_set_maxdbs_code(unsafe { lmdb_sys::mdb_env_set_maxdbs(env, maxdbs) })?;
}

#[throws]
pub(super) fn env_set_mapsize(env: *mut lmdb_sys::MDB_env, mapsize: usize) {
	error::handle_env_set_mapsize_code(unsafe { lmdb_sys::mdb_env_set_mapsize(env, mapsize) })?;
}

#[allow(unused_variables)]
#[throws]
pub(super) fn env_open(env: *mut lmdb_sys::MDB_env, path: &[u8], flags: u32, mode: u32) {
	#[cfg(unix)] let mode = mode;
	#[cfg(windows)] let mode = 0;
	error::handle_env_open(unsafe { lmdb_sys::mdb_env_open(env, path.as_ptr().cast(), flags, mode) })?;
}

pub(super) fn dbi_open(tx: *mut lmdb_sys::MDB_txn, name: &[u8], flags: enumflags2::BitFlags<DbFlags>) -> lmdb_sys::MDB_dbi {
	let mut dbi: lmdb_sys::MDB_dbi = 0;
	error::handle_dbi_open_code(unsafe { lmdb_sys::mdb_dbi_open(tx, name.as_ptr().cast(), flags.bits(), &mut dbi) });
	dbi
}
