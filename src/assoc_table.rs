use super::*;
use prelude::throws;
use std::marker::PhantomData;

pub struct AssocTable<'tx, TX, K, V> {
	tx: &'tx TX,
	pub dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<(K, V)>,
}

// RwTxn only, so all methods mutate
impl<'tx, K, V> AssocTable<'tx, RwTxn, K, V> where
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	V: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
{
	#[throws]
	pub fn put(&self, key: &K, value: &V) {
		let mut key_bytes = rkyv::to_bytes(key).unwrap();
		let mut value_bytes = rkyv::to_bytes(value).unwrap();
		lmdb::put(self.tx, self.dbi, &mut key_bytes, &mut value_bytes)?
	}

	#[throws]
	pub fn delete(&self, key: &K) -> bool {
		let mut key_bytes = rkyv::to_bytes(key).unwrap();
		lmdb::del(self.tx, self.dbi, &mut key_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)? }
}

// both RoTxn and RwTxn, so all methods are read-only
impl<'tx, TX, K, V> AssocTable<'tx, TX, K, V> where
	TX: Transaction,
	K: rkyv::Archive + for <'a> rkyv::Serialize<RkyvSer<'a>>,
	V: rkyv::Archive,
	rkyv::Archived<K>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
	rkyv::Archived<V>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		Self { tx, dbi, _pd: PhantomData }
	}

	#[throws]
	pub fn get(&self, key: &K) -> Option<&'tx rkyv::Archived<V>> {
		let mut key_bytes = rkyv::to_bytes(key).unwrap();
		lmdb::get(self.tx, self.dbi, &mut key_bytes)?
			.map(|value| rkyv::access::<rkyv::Archived<V>, _>(value).unwrap())
	}

	#[throws]
	pub fn last(&self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
		lmdb::Cursor::open(self.tx, self.dbi)?
			.get(lmdb::CursorOpFlags::Last)
			.map(|(key, value)| (
				rkyv::access::<rkyv::Archived<K>, _>(key).unwrap(),
				rkyv::access::<rkyv::Archived<V>, _>(value).unwrap(),
			))
	}

	#[expect(clippy::iter_not_returning_iterator)]
	#[throws]
	pub fn iter(&self) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		struct Cursor<'tx, TX: Transaction, K, V>(lmdb::Cursor<'tx, TX>, PhantomData<(K, V)>);

		impl<'tx, TX, K, V> Iterator for Cursor<'tx, TX, K, V> where
			TX: Transaction,
			K: rkyv::Archive,
			V: rkyv::Archive,
			rkyv::Archived<K>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
			rkyv::Archived<V>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
		{
			type Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>);

			fn next(&mut self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
				self.0.get(lmdb::CursorOpFlags::Next).map(|(key, value)| (
					rkyv::access::<rkyv::Archived<K>, _>(key).unwrap(),
					rkyv::access::<rkyv::Archived<V>, _>(value).unwrap(),
				))
			}
		}

		Cursor::<TX, K, V>(lmdb::Cursor::open(self.tx, self.dbi)?, PhantomData)
	}

	#[throws]
	pub fn iter_rev(&self) -> impl Iterator<Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> where
		rkyv::Archived<K>: 'tx,
		rkyv::Archived<V>: 'tx,
	{
		struct Cursor<'tx, TX: Transaction, K, V>(lmdb::Cursor<'tx, TX>, PhantomData<(K, V)>);

		impl<'tx, TX, K, V> Iterator for Cursor<'tx, TX, K, V> where
			TX: Transaction,
			K: rkyv::Archive,
			V: rkyv::Archive,
			rkyv::Archived<K>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
			rkyv::Archived<V>: 'tx + for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>>,
		{
			type Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>);

			fn next(&mut self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
				self.0.get(lmdb::CursorOpFlags::Prev).map(|(key, value)| (
					rkyv::access::<rkyv::Archived<K>, _>(key).unwrap(),
					rkyv::access::<rkyv::Archived<V>, _>(value).unwrap(),
				))
			}
		}

		Cursor::<TX, K, V>(lmdb::Cursor::open(self.tx, self.dbi)?, PhantomData)
	}
}
