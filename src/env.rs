#![allow(dead_code)]

use crate::prelude::*;
use std::collections::HashMap;

use super::{lmdb::{self, DbFlags}, DbName, Error, RoTxn, RwTxn, Transaction};

pub struct Env {
	raw_env: *mut lmdb_sys::MDB_env,
	pub dbs: HashMap<&'static [u8], lmdb_sys::MDB_dbi>,
	pub(super) write_lock: tokio::sync::Mutex<()>,
}

pub struct EnvBuilder {
	raw_env: *mut lmdb_sys::MDB_env,
	dbs: HashMap<&'static [u8], lmdb_sys::MDB_dbi>,
	db_create_tx: RwTxn,
}

unsafe impl Send for Env {}
unsafe impl Sync for Env {}
unsafe impl Send for EnvBuilder {}
unsafe impl Sync for EnvBuilder {}

impl Env {
	#[throws]
	pub fn builder() -> EnvBuilder {
		const MAX_DBS: u32 = 128;

		#[cfg(not(test))] const PATH: &[u8] = b"db\0";
		#[cfg(test)]      const PATH: &[u8] = b"../db\0";

		let flags =
			lmdb_sys::MDB_NOMETASYNC | // maybe lose last transaction in case of a crash
			lmdb_sys::MDB_NOTLS |      // don't use thread-local storage - read and write transactions can be on any thread, still at most 1 write tx
			lmdb_sys::MDB_NORDAHEAD;   // don't readahead - useful when datasets are bigger than ram (does nothing on Windows)

		let raw_env = lmdb::env_create()?;
		lmdb::env_set_maxdbs(raw_env, MAX_DBS)?;
		lmdb::env_set_mapsize(raw_env, 1 << 36)?; // 64 gb
		// 0664 is permissions for db folder on Unix - read/write/not execute
		lmdb::env_open(raw_env, PATH, flags, 664)?;

		EnvBuilder { raw_env, dbs: HashMap::with_capacity(MAX_DBS as _), db_create_tx: RwTxn(lmdb::txn_begin(raw_env, 0)?) }
	}

	#[throws] pub fn read_tx(&self) -> RoTxn { RoTxn(lmdb::txn_begin(self.raw_env, lmdb_sys::MDB_RDONLY)?) }
	#[throws] pub fn write_tx(&self) -> RwTxn { RwTxn(lmdb::txn_begin(self.raw_env, 0)?) }
}

impl EnvBuilder {
	pub fn with<N: DbName>(mut self, mod_path: &str) -> Self {
		let name = &N::NAME[mod_path.len()+2..];
		log::trace!("creating {}", unsafe { std::str::from_utf8_unchecked(name) });
		let dbi = lmdb::dbi_open(self.db_create_tx.raw(), name, N::flags() | DbFlags::Create);
		self.dbs.insert(N::NAME, dbi);
		self
	}

	#[throws]
	pub fn build(self) -> Env {
		self.db_create_tx.commit()?;
		Env { raw_env: self.raw_env, dbs: self.dbs, write_lock: tokio::sync::Mutex::default() }
	}
}
