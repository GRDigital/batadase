use super::*;
use std::marker::PhantomData;

pub struct AssocTable<'tx, TX, K, V> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<(K, V)>,
}

impl<'tx, K, V> AssocTable<'tx, RwTxn, K, V> where
	K: rkyv::Archive + rkyv::Serialize<RkyvSmallSer>,
	V: rkyv::Archive + rkyv::Serialize<RkyvSmallSer>,
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

impl<'tx, TX, K, V> AssocTable<'tx, TX, K, V> where
	TX: Transaction,
	K: rkyv::Archive + rkyv::Serialize<RkyvSmallSer>,
	V: rkyv::Archive,
{
	pub(super) fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self { Self { tx, dbi, _pd: PhantomData } }

	#[throws]
	pub fn get(&self, key: &K) -> Option<&'tx rkyv::Archived<V>> {
		let mut key_bytes = rkyv::to_bytes(key).unwrap();
		lmdb::get(self.tx, self.dbi, &mut key_bytes)?
			.map(|value| unsafe { rkyv::archived_root::<V>(value) })
	}

	#[throws]
	pub fn last(&self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
		lmdb::Cursor::open(self.tx, self.dbi)?
			.get(lmdb::CursorOpFlags::Last)
			.map(|(key, value)| (
				unsafe { rkyv::archived_root::<K>(key) },
				unsafe { rkyv::archived_root::<V>(value) },
			))
	}

	#[allow(clippy::iter_not_returning_iterator)]
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
			rkyv::Archived<K>: 'tx,
			rkyv::Archived<V>: 'tx,
		{
			type Item = (&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>);

			fn next(&mut self) -> Option<(&'tx rkyv::Archived<K>, &'tx rkyv::Archived<V>)> {
				self.0.get(lmdb::CursorOpFlags::Next).map(|(key, value)| (
					unsafe { rkyv::archived_root::<K>(key) },
					unsafe { rkyv::archived_root::<V>(value) },
				))
			}
		}

		Cursor::<TX, K, V>(lmdb::Cursor::open(self.tx, self.dbi)?, PhantomData)
	}
}
