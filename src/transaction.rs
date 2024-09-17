use fehler::throws;
use super::{DbName, Error, lmdb};

// The third Triagon was born of Death. It saw that the world was radiating excess energy.
// It wanted to put great things into motion. But greatness wasn't possible without value. The first transaction.
//
// It took its blade and cut a large hole into the boundary, creating a sudden flash of high volume transactional power.
// And just for a moment things seeped value into themselves, assuming souls. The second transaction.
//
// The hole was quickly mended, and the overpowering transmission of value was cut short.
// But in that moment the seed of primordial financial might was planted, and the world took on its transactional form.
// Conflict and discord emerged, and the third Triagon was ecstatic. The third transaction.

#[repr(transparent)] pub struct RoTxn(pub(super) *mut lmdb_sys::MDB_txn);
#[repr(transparent)] pub struct RwTxn(pub(super) *mut lmdb_sys::MDB_txn);

/// it is Sync + Send since you can't close a db after you open it
unsafe impl Sync for RoTxn {}
unsafe impl Send for RoTxn {}
unsafe impl Sync for RwTxn {}
unsafe impl Send for RwTxn {}

// potential avenue:
// then TX: Transaction turns into smth like TX: Deref<Target = RoTxn>
/*
impl std::ops::Deref for RwTxn {
	type Target = RoTxn;

	fn deref(&self) -> &Self::Target {
		let ptr = std::ptr::from_ref::<RwTxn>(self).cast::<RoTxn>();
		unsafe { &*ptr }
	}
}

impl RoTxn {
	// discouraged
	pub fn raw(&self) -> *mut lmdb_sys::MDB_txn { self.0 }
	pub fn get<Name: DbName>(&self) -> Name::Table<'_, Self> { Name::get(self) }
	#[throws]
	pub fn commit(self) {
		lmdb::txn_commit(self.raw())?;
		// internally it is literally `let _ = ManuallyDrop::new(x);`
		std::mem::forget(self);
	}
	pub fn abort(self) {
		// runs destructor, which does mdb_txn_abort
	}
}
*/

pub trait Transaction: Sized + 'static {
	fn raw(&self) -> *mut lmdb_sys::MDB_txn;
	#[throws]
	fn commit(self) {
		lmdb::txn_commit(self.raw())?;
		// internally it is literally `let _ = ManuallyDrop::new(x);`
		// basically we're just avoiding Drop
		std::mem::forget(self);
	}
	fn abort(self) {
		// runs Drop, which does mdb_txn_abort
	}
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
