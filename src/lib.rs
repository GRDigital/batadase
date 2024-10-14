//! Batadase: An incredible LMDB wrapper.
//!
//!
//! Note that this crate does not compile on wasm, but batadase-index does.
//!
//!
//! You can use the batadase-macros crate to do make a table easier, e.g.
//! ```ignore
//! #[derive(batadase_macros::DbName)]
//! #[flags(lmdb::DbFlags::IntegerKey)]
//! #[table(Table<'tx, TX, MyActualDataStruct>)]
//! struct MyTable
//! ```
//! then use def_tx_ops below to init the db.

pub use batadase_index::Index;
pub use batadase_macros::DbName;
pub use env::Env;
pub use lmdb::{DbFlags, CursorOpFlags};
pub use transaction::{Transaction, RoTxn, RwTxn};
pub use enumflags2;
pub use error::Error;
pub use rkyv;

pub mod env;
pub mod lmdb;
pub mod transaction;
pub mod error;

pub mod index_table;
pub mod assoc_table;
pub mod index_poly_table;
pub mod assoc_poly_table;

pub trait Table<TX: Transaction> {
	fn dbi(&self) -> lmdb_sys::MDB_dbi;
	fn txn(&self) -> &TX;
	fn flags() -> enumflags2::BitFlags<DbFlags> { enumflags2::BitFlags::empty() }

	#[culpa::throws]
	fn entries(&self) -> usize {
		let stat = lmdb::stat(self.txn().raw(), self.dbi())?;
		stat.ms_entries
	}
}

// potentially useful relation table flavours:
// * one to many via Indices
// * two-way one-to-one via Indices
// * two-way many-to-many via Indices

type RkyvSer<'a> = rkyv::api::high::HighSerializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::rancor::Error>;
type RkyvDe = rkyv::api::high::HighDeserializer<rkyv::rancor::Error>;
type RkyvVal<'a> = rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>;

pub trait DbName {
	type Table<'tx, TX: Transaction>: Table<TX>;
	const NAME: &'static [u8];

	fn get<TX: Transaction>(tx: &TX) -> Self::Table<'_, TX>;
	fn flags() -> enumflags2::BitFlags<lmdb::DbFlags> { enumflags2::BitFlags::empty() }
}

pub fn unrkyv<T>(archive: &rkyv::Archived<T>) -> Result<T, rkyv::rancor::Error> where
	T: rkyv::Archive,
	rkyv::Archived<T>: rkyv::Deserialize<T, RkyvDe>,
{ rkyv::deserialize::<T, rkyv::rancor::Error>(archive) }

pub fn unrkyv_from_bytes<T>(bytes: &[u8]) -> Result<T, rkyv::rancor::Error> where
	T: rkyv::Archive,
	rkyv::Archived<T>: for <'a> rkyv::bytecheck::CheckBytes<RkyvVal<'a>> + rkyv::Deserialize<T, RkyvDe>,
{ rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes) }

/// If you use a single static Env, e.g.
/// ```ignore
/// static ENV: Lazy<Env> = Lazy::new(||
///     Env::builder().unwrap()
///         .mapsize(1 << 30).unwrap() // 1 gb
///         .with::<MyTable>()
///         ...
///         ...
///         .build("db").unwrap()
/// );
/// ```
/// you can use this macro for more convenient (?) global read/write fns, e.g.
/// ```ignore
/// def_tx_ops!(ENV)
///
/// db::try_write(move |tx| { ... }).await??
/// ```
#[macro_export]
macro_rules! def_tx_ops {
	// name of your &'static Env
	($env_name:ident) => {
		pub fn read_tx() -> ::std::result::Result<::batadase::transaction::RoTxn, ::batadase::Error>  { $env_name.read_tx() }

		pub async fn write<Res, Job>(job: Job) -> ::std::result::Result<Res, ::batadase::Error> where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> Res) + ::std::marker::Send + 'static,
		{ $env_name.write(job).await }

		pub async fn try_write<Res, Err, Job>(job: Job) -> ::std::result::Result<::std::result::Result<Res, Err>, ::batadase::Error> where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> ::std::result::Result<Res, Err>) + ::std::marker::Send + 'static,
			Err: ::std::marker::Send + 'static,
		{ $env_name.try_write(job).await }

		pub async fn write_async<Res>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, Res>) -> ::std::result::Result<Res, ::batadase::Error>
		{ $env_name.write_async(job).await }

		pub async fn try_write_async<Res, Error>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, ::std::result::Result<Res, Error>>) -> ::std::result::Result<::std::result::Result<Res, Error>, ::batadase::Error>
		{ $env_name.try_write_async(job).await }
	};

	($env_name:ident, $err:ty) => {
		pub fn read_tx() -> ::std::result::Result<::batadase::transaction::RoTxn, ::batadase::Error>  { $env_name.read_tx() }

		pub async fn write<Res, Job>(job: Job) -> ::std::result::Result<Res, ::batadase::Error> where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> Res) + ::std::marker::Send + 'static,
		{ $env_name.write(job).await }

		pub async fn try_write<Res, Job>(job: Job) -> ::std::result::Result<::std::result::Result<Res, $err>, ::batadase::Error> where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> ::std::result::Result<Res, $err>) + ::std::marker::Send + 'static,
		{ $env_name.try_write(job).await }

		pub async fn write_async<Res>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, Res>) -> ::std::result::Result<Res, ::batadase::Error>
		{ $env_name.write_async(job).await }

		pub async fn try_write_async<Res>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, ::std::result::Result<Res, $err>>) -> ::std::result::Result<::std::result::Result<Res, $err>, ::batadase::Error>
		{ $env_name.try_write_async(job).await }
	};

	// the only error most people should really care about is read_tx's ReadersFull
	(unwrapped $env_name:ident, $err:ty) => {
		pub fn read_tx() -> ::batadase::transaction::RoTxn { $env_name.read_tx().unwrap() }

		pub async fn write<Res, Job>(job: Job) -> Res where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> Res) + ::std::marker::Send + 'static,
		{ $env_name.write(job).await.unwrap() }

		pub async fn try_write<Res, Job>(job: Job) -> ::std::result::Result<Res, $err> where
			Res: ::std::marker::Send + 'static,
			Job: (::std::ops::FnOnce(&::batadase::transaction::RwTxn) -> ::std::result::Result<Res, $err>) + ::std::marker::Send + 'static,
		{ $env_name.try_write(job).await.unwrap() }

		pub async fn write_async<Res>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, Res>) -> Res
		{ $env_name.write_async(job).await.unwrap() }

		pub async fn try_write_async<Res>(job: impl for <'tx> ::batadase::env::WriteCallback<'tx, ::std::result::Result<Res, $err>>) -> ::std::result::Result<Res, $err>
		{ $env_name.try_write_async(job).await.unwrap() }
	};
}
