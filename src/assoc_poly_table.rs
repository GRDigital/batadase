use crate::{Transaction, RwTxn, Table, RkyvSer, RkyvVal, RkyvDe, Error, lmdb};
use culpa::throws;
use std::marker::PhantomData;

pub struct AssocPolyTable<'tx, TX, K> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<K>,
}

impl<TX: Transaction, K> Table<TX> for AssocPolyTable<'_, TX, K> {
	fn dbi(&self) -> lmdb_sys::MDB_dbi { self.dbi }
	fn txn(&self) -> &TX { self.tx }
}

// RwTxn only, so all methods mutate
impl<K> AssocPolyTable<'_, RwTxn, K> where
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
{
	#[throws]
	pub fn put<V>(&self, key: &K, value: &V) where
		V: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	{
		let mut key_bytes = rkyv::to_bytes(key)?;
		let mut value_bytes = rkyv::to_bytes(value)?;
		lmdb::put(self.tx, self.dbi, &mut key_bytes, &mut value_bytes)?;
	}

	#[throws]
	pub fn delete(&self, key: &K) -> bool {
		let mut key_bytes = rkyv::to_bytes(key)?;
		lmdb::del(self.tx, self.dbi, &mut key_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)?; }
}

// both RoTxn and RwTxn, so all methods are read-only
impl<'tx, TX, K> AssocPolyTable<'tx, TX, K> where
	TX: Transaction,
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	rkyv::Archived<K>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		Self { tx, dbi, _pd: PhantomData }
	}

	#[throws]
	pub fn get<V>(&self, key: &K) -> Option<&'tx rkyv::Archived<V>> where
		V: rkyv::Archive,
		rkyv::Archived<V>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>> + rkyv::Deserialize<V, RkyvDe>,
	{
		let mut key_bytes = rkyv::to_bytes(key)?;
		let Some(value_bytes) = lmdb::get(self.tx, self.dbi, &mut key_bytes)? else { return None; };
		Some(rkyv::access::<rkyv::Archived<V>, _>(value_bytes)?)
	}

	#[throws]
	pub fn get_unrkyv<V>(&self, key: &K) -> Option<V> where
		V: rkyv::Archive,
		rkyv::Archived<V>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>> + rkyv::Deserialize<V, RkyvDe>,
	{
		let Some(archived) = self.get::<V>(key)? else { return None; };
		Some(rkyv::deserialize::<V, rkyv::rancor::Error>(archived)?)
	}
}
