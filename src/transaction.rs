use fehler::throws;
use super::{DbName, Error, lmdb};

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

