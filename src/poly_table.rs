use super::*;
use prelude::Index;

pub struct PolyTable<'tx, TX> {
	tx: &'tx TX,
	dbi: lmdb_sys::MDB_dbi,
}

impl<'tx> PolyTable<'tx, RwTxn> {
	#[throws]
	pub fn put<T>(&self, index: Index<T>, t: &T) where
		T: rkyv::Archive + rkyv::Serialize<RkyvSmallSer>,
	{
		let mut index_bytes = u64::from(index).to_ne_bytes();
		let mut value_bytes = rkyv::to_bytes(t).unwrap();
		lmdb::put(self.tx, self.dbi, &mut index_bytes, &mut value_bytes)?
	}

	#[throws]
	pub fn put_last<T>(&self, t: &T) -> Index<T> where
		T: rkyv::Archive + rkyv::Serialize<RkyvSmallSer>,
	{
		let index = Index::from(self.last_numeric_index()?.map_or(0, |x| x + 1));
		self.put(index, t)?;
		index
	}

	#[throws]
	pub fn delete_index<T>(&self, index: Index<T>) -> bool {
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::del(self.tx, self.dbi, &mut index_bytes)?
	}

	#[throws]
	pub fn clear(&self) { lmdb::drop(self.tx, self.dbi)? }
}

impl<'tx, TX> PolyTable<'tx, TX> where
	TX: Transaction,
{
	pub(super) fn build(tx: &'tx TX, dbi: lmdb_sys::MDB_dbi) -> Self { Self { tx, dbi } }

	#[throws]
	pub fn get<T>(&self, index: Index<T>) -> Option<&'tx rkyv::Archived<T>> where
		T: rkyv::Archive,
	{
		let mut index_bytes = u64::from(index).to_ne_bytes();
		lmdb::get(self.tx, self.dbi, &mut index_bytes)?
			.map(|value| unsafe { rkyv::archived_root::<T>(value) })
	}

	#[throws]
	fn last_numeric_index(&self) -> Option<u64> {
		lmdb::Cursor::open(self.tx, self.dbi)?
			.get_with_u64_key(lmdb::CursorOpFlags::Last)
			.map(|(key, _)| key)
	}

	// Maybe you just can't iterate poly tables as you're really not meant to
	// if you want diagnostics or whatever then restore indices from the sources
	// #[throws]
	// pub fn iter(&self) -> impl Iterator<Item = (Index<T>, &rkyv::Archived<T>)> { ... }
}
