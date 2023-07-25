#![allow(dead_code)]

use futures::prelude::*;
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
	#[throws] pub(super) fn write_tx(&self) -> RwTxn { RwTxn(lmdb::txn_begin(self.raw_env, 0)?) }

	#[throws]
	pub async fn write<Res, Job>(&'static self, job: Job) -> Res where
		Res: Send + 'static,
		Job: (FnOnce(&RwTxn) -> Res) + Send + 'static,
		{
			let _lock = self.write_lock.lock().await;
			let now = std::time::Instant::now();

			let res = tokio::task::spawn_blocking(move || {
				let tx = self.write_tx()?;
				let res = job(&tx);
				tx.commit()?;
				Ok(res)
			}).await.expect("tokio spawn_blocking failed");
			drop(_lock);

			let lock_held = now.elapsed().as_secs_f32();
			if lock_held > 10. {
				log::error!("write lock held for {lock_held:.2} secs");
			} else if lock_held > 2.5 {
				log::warn!("write lock held for {lock_held:.2} secs");
			} else if lock_held > 0.25 {
				log::info!("write lock held for {lock_held:.2} secs");
			} else {
				log::trace!("write lock held for {lock_held:.2} secs");
			}

			res?
		}

	// outer result is whether DB ops failed or not,
	// inner result is whether the job failed or not
	// the tx isn't committed if the job fails
	//
	// should probably just be a Result<Res, Err> but
	// it requires all jobs to specify return type
	#[throws]
	pub async fn try_write<Res, Job>(&'static self, job: Job) -> anyhow::Result<Res> where
		Res: Send + 'static,
		Job: (FnOnce(&RwTxn) -> anyhow::Result<Res>) + Send + 'static,
		{
			let _lock = self.write_lock.lock().await;
			let now = std::time::Instant::now();

			let res = tokio::task::spawn_blocking(move || {
				let tx = self.write_tx()?;
				let res = job(&tx);
				if res.is_ok() {
					tx.commit()?;
				} else {
					tx.abort();
				}
				Ok(res)
			}).await.expect("tokio spawn_blocking failed");
			drop(_lock);

			let lock_held = now.elapsed().as_secs_f32();
			if lock_held > 10. {
				log::error!("write lock held for {lock_held:.2} secs");
			} else if lock_held > 2.5 {
				log::warn!("write lock held for {lock_held:.2} secs");
			} else if lock_held > 0.25 {
				log::info!("write lock held for {lock_held:.2} secs");
			} else {
				log::trace!("write lock held for {lock_held:.2} secs");
			}

			res?
		}

	/// discouraged
	///
	/// returning RwTxn is necessary because of lifetime issues,
	/// we can use the for<'a> syntax to make it work but
	/// it forbids type inference in usage sites
	#[throws]
	pub async fn write_async<Res, Job, Fut>(&'static self, job: Job) -> Res where
		Job: FnOnce(RwTxn) -> Fut,
		Fut: Future<Output = (RwTxn, Res)>,
	{
		let _lock = self.write_lock.lock().await;
		let now = std::time::Instant::now();

		let tx = self.write_tx()?;
		let (tx, res) = job(tx).await;
		tx.commit()?;
		drop(_lock);

		let lock_held = now.elapsed().as_secs_f32();
		if lock_held > 10. {
			log::error!("async write lock held for {lock_held:.2} secs");
		} else if lock_held > 2.5 {
			log::warn!("async write lock held for {lock_held:.2} secs");
		} else if lock_held > 0.25 {
			log::info!("async write lock held for {lock_held:.2} secs");
		} else {
			log::trace!("async write lock held for {lock_held:.2} secs");
		}

		res
	}

	// unclear if useful?
	/*
	#[throws]
	pub fn blocking_write<Res, F: (FnOnce(&RwTxn) -> Res)>(job: F) -> Res {
		let _lock = tokio::task::block_in_place(|| ENV.write_lock.lock());
		let now = std::time::Instant::now();
		let tx = ENV.write_tx()?;
		let res = job(&tx);
		tx.commit()?;
		drop(_lock);
		log::debug!("write lock held for {:.2} secs", now.elapsed().as_secs_f32());
		res
	}

	#[throws]
	pub fn blocking_try_write<Res, F: (FnOnce(&RwTxn) -> anyhow::Result<Res>)>(job: F) -> anyhow::Result<Res> {
		let _lock = tokio::task::block_in_place(|| ENV.write_lock.lock());
		let now = std::time::Instant::now();
		let tx = ENV.write_tx()?;
		let res = job(&tx);
		if res.is_ok() {
			tx.commit()?;
		} else {
			tx.abort();
		}
		drop(_lock);
		log::debug!("write lock held for {:.2} secs", now.elapsed().as_secs_f32());
		res
	}
	*/
}

impl EnvBuilder {
	#[must_use]
	pub fn with<N: DbName>(mut self) -> Self {
		log::trace!("creating {}", unsafe { std::str::from_utf8_unchecked(N::NAME) });
		let dbi = lmdb::dbi_open(self.db_create_tx.raw(), N::NAME, N::flags() | DbFlags::Create);
		self.dbs.insert(N::NAME, dbi);
		self
	}

	#[throws]
	pub fn build(self) -> Env {
		self.db_create_tx.commit()?;
		Env { raw_env: self.raw_env, dbs: self.dbs, write_lock: tokio::sync::Mutex::default() }
	}
}
