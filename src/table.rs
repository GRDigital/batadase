use super::*;
use prelude::{Index, throws};
use std::marker::PhantomData;

pub struct Table<'tx, TX, T> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
	_pd: PhantomData<T>,
}

impl<'tx, T> Table<'tx, RwTxn, T> where
	T: rkyv::Serialize<RkyvSmallSer>,
{
	#[throws]
	pub fn put(&self, index: Index<T>, t: &T) {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t).unwrap();
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes)?
	}

	#[throws]
	pub fn put_last(&self, t: &T) -> Index<T> {
		let index = Index::from(self.last()?.map_or(0, |(x, _)| u64::from(x) + 1));
		self.put(index, t)?;
		index
	}

	#[throws]
	pub fn delete_index(&self, index: Index<T>) -> bool {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::del(self.tx, self.dbi, &mut index_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)? }
}

impl<'tx, TX, T> Table<'tx, TX, T> where
	TX: Transaction,
	T: rkyv::Archive,
{
	pub fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self {
		Self { tx, dbi, _pd: PhantomData }
	}

	#[throws]
	pub fn get(&self, index: Index<T>) -> Option<&'tx rkyv::Archived<T>> {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::get(self.tx, self.dbi, &mut index_bytes)?
			.map(|value| unsafe { rkyv::archived_root::<T>(value) })
	}

	#[throws]
	pub fn last(&self) -> Option<(Index<T>, &'tx rkyv::Archived<T>)> {
		lmdb::Cursor::open(self.tx, self.dbi)?
			.get_with_u64_key(lmdb::CursorOpFlags::Last)
			.map(|(key, value)| (
				Index::from(key),
				unsafe { rkyv::archived_root::<T>(value) },
			))
	}

	#[allow(clippy::iter_not_returning_iterator)]
	#[throws]
	pub fn iter(&self) -> impl Iterator<Item = (Index<T>, &'tx rkyv::Archived<T>)> where
		rkyv::Archived<T>: 'tx,
	{
		struct Cursor<'tx, TX: Transaction, T>(lmdb::Cursor<'tx, TX>, PhantomData<T>);

		impl<'tx, TX, T> Iterator for Cursor<'tx, TX, T> where
			TX: Transaction,
			T: rkyv::Archive,
			rkyv::Archived<T>: 'tx,
		{
			type Item = (Index<T>, &'tx rkyv::Archived<T>);

			fn next(&mut self) -> Option<(Index<T>, &'tx rkyv::Archived<T>)> {
				self.0.get_with_u64_key(lmdb::CursorOpFlags::Next).map(|(key, value)| (
					Index::from(key),
					unsafe { rkyv::archived_root::<T>(value) },
				))
			}
		}

		Cursor::<TX, T>(lmdb::Cursor::open(self.tx, self.dbi)?, PhantomData)
	}
}
