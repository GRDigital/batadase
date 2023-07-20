use fehler::throws;
use futures::prelude::*;
use crate::prelude::log;
use super::{DbName, Error, lmdb, env::Env};

pub struct RoTxn(pub(super) *mut lmdb_sys::MDB_txn);
pub struct RwTxn(pub(super) *mut lmdb_sys::MDB_txn);

/// it is Sync + Send since you can't close a db after you open it
unsafe impl Sync for RoTxn {}
unsafe impl Send for RoTxn {}
unsafe impl Sync for RwTxn {}
unsafe impl Send for RwTxn {}

pub trait Transaction: Sized + 'static {
	fn raw(&self) -> *mut lmdb_sys::MDB_txn;
	#[throws] fn commit(self) {
		lmdb::txn_commit(self.raw())?;
		// internally it is literally `let _ = ManuallyDrop::new(x);`
		std::mem::forget(self);
	}
	fn abort(self) {}
	fn get<Name: DbName>(&self) -> Name::Table<'_, Self> { Name::get(self) }
}

impl Transaction for RoTxn {
	fn raw(&self) -> *mut lmdb_sys::MDB_txn { self.0 }
}
impl Transaction for RwTxn {
	fn raw(&self) -> *mut lmdb_sys::MDB_txn { self.0 }
}

impl Drop for RoTxn { fn drop(&mut self) { unsafe { lmdb_sys::mdb_txn_abort(self.0); } } }
impl Drop for RwTxn { fn drop(&mut self) { unsafe { lmdb_sys::mdb_txn_abort(self.0); } } }

pub fn read_tx(env: &'static Env) -> Result<RoTxn, Error> { env.read_tx() }

#[throws]
pub async fn write<Res, Job>(job: Job, env: &'static Env) -> Res where
	Res: Send + 'static,
	Job: (FnOnce(&RwTxn) -> Res) + Send + 'static,
{
	let _lock = env.write_lock.lock().await;
	let now = std::time::Instant::now();

	let res = tokio::task::spawn_blocking(move || {
		let tx = env.write_tx()?;
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
//
// should probably just be a Result<Res, Err> but
// it requires all jobs to specify return type
#[throws]
pub async fn try_write<Res, Job>(job: Job, env: &'static Env) -> anyhow::Result<Res> where
	Res: Send + 'static,
	Job: (FnOnce(&RwTxn) -> anyhow::Result<Res>) + Send + 'static,
{
	let _lock = env.write_lock.lock().await;
	let now = std::time::Instant::now();

	let res = tokio::task::spawn_blocking(move || {
		let tx = env.write_tx()?;
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
pub async fn write_async<Res, Job, Fut>(job: Job, env: &'static Env) -> Res where
	Job: FnOnce(RwTxn) -> Fut,
	Fut: Future<Output = (RwTxn, Res)>,
{
	let _lock = env.write_lock.lock().await;
	let now = std::time::Instant::now();

	let tx = env.write_tx()?;
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
