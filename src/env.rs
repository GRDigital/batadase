#![allow(dead_code)]

use futures::prelude::*;
use crate::prelude::*;
use std::collections::HashMap;

use super::{lmdb::{self, DbFlags}, DbName, Error, RoTxn, RwTxn, Transaction};

pub struct Env {
	raw_env: *mut lmdb_sys::MDB_env,
	pub dbs: HashMap<&'static [u8], lmdb_sys::MDB_dbi>,
	// TODO: mutex consists of a semaphore and unsafecell, so we should replace this with a semaphore
	pub(super) write_lock: tokio::sync::Mutex<()>,
}

pub struct EnvBuilder {
	raw_env: *mut lmdb_sys::MDB_env,
	dbs: Vec<(&'static [u8], enumflags2::BitFlags<lmdb::DbFlags>)>,
}

unsafe impl Send for Env {}
unsafe impl Sync for Env {}
unsafe impl Send for EnvBuilder {}
unsafe impl Sync for EnvBuilder {}

// TODO: maxdbs, mapsize, maxreaders, path should be configurable
impl Env {
	#[throws]
	pub fn builder() -> EnvBuilder {
		EnvBuilder { raw_env: lmdb::env_create()?, dbs: Vec::new() }
	}

	pub fn reader_list(&self) {
		unsafe extern "C" fn msg(msg: *const libc::c_char, _: *mut libc::c_void) -> i32 {
			let cstr = std::ffi::CStr::from_ptr(msg);
			println!("{}", cstr.to_string_lossy());
			0
		}
		unsafe { lmdb_sys::mdb_reader_list(self.raw_env, Some(msg), std::ptr::null_mut()) };
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
	pub async fn try_write<Res, Err, Job>(&'static self, job: Job) -> Result<Res, Err> where
		Res: Send + 'static,
		Job: (FnOnce(&RwTxn) -> Result<Res, Err>) + Send + 'static,
		Err: Send + 'static,
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
	#[throws]
	pub fn mapsize(self, size: usize) -> Self {
		// lmdb::env_set_mapsize(raw_env, 1 << 32)?; // 4 gb
		lmdb::env_set_mapsize(self.raw_env, size)?;
		self
	}

	#[throws]
	pub fn maxreaders(self, readers: u32) -> Self {
		// lmdb::env_set_maxreaders(raw_env, 1 << 10)?; // 1024
		lmdb::env_set_maxreaders(self.raw_env, readers)?;
		self
	}

	#[must_use]
	pub fn with<N: DbName>(mut self) -> Self {
		self.dbs.push((N::NAME, N::flags()));
		self
	}

	#[throws]
	pub fn build(self) -> Env {
		#[cfg(not(test))] const PATH: &[u8] = b"db\0";
		#[cfg(test)]      const PATH: &[u8] = b"../db\0";

		let flags =
			lmdb_sys::MDB_NOMETASYNC | // maybe lose last transaction in case of a crash
			lmdb_sys::MDB_NOTLS |      // don't use thread-local storage - read and write transactions can be on any thread, still at most 1 write tx
			lmdb_sys::MDB_NORDAHEAD;   // don't readahead - useful when datasets are bigger than ram (does nothing on Windows)

		lmdb::env_set_maxdbs(self.raw_env, self.dbs.len() as u32)?;
		
		// 0664 is permissions for db folder on Unix - read/write/not execute
		lmdb::env_open(self.raw_env, PATH, flags, 664)?;

		let db_create_tx = RwTxn(lmdb::txn_begin(self.raw_env, 0)?);
		let mut dbs = HashMap::with_capacity(self.dbs.len());
		for (name, flags) in self.dbs {
			log::trace!("creating {}", unsafe { std::str::from_utf8_unchecked(name) });
			dbs.insert(name, lmdb::dbi_open(db_create_tx.raw(), name, flags | DbFlags::Create));
		}
		db_create_tx.commit()?;

		Env { raw_env: self.raw_env, dbs, write_lock: tokio::sync::Mutex::default() }
	}
}
