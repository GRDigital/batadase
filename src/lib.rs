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
pub use lmdb::{Error, DbFlags, CursorOpFlags};
pub use once_cell::sync::Lazy;
pub use transaction::*;
pub use enumflags2;

pub mod assoc_table;
pub mod env;
pub mod lmdb;
pub mod poly_table;
pub mod table;
pub mod transaction;

pub mod prelude;

// potentially useful relation table flavours:
// * one to many via Indices
// * two-way one-to-one via Indices
// * two-way many-to-many via Indices

type RkyvSer<'a> = rkyv::api::high::HighSerializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::rancor::Error>;
type RkyvVal<'a> = rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>;

pub trait DbName {
	type Table<'tx, TX: Transaction>;
	const NAME: &'static [u8];

	fn get<TX: Transaction>(tx: &TX) -> Self::Table<'_, TX>;
	fn flags() -> enumflags2::BitFlags<lmdb::DbFlags> { enumflags2::BitFlags::empty() }
}

pub fn version() -> semver::Version {
	static VERSION: Lazy<String> = Lazy::new(|| std::fs::read_to_string("db/version")
		.expect("Failed to get DB version from file. Please ensure there is a 'version' file in the db directory with a valid semver version.")
	);

	semver::Version::parse(VERSION.trim()).expect("Can't parse version")
}

pub fn set_version(version: &semver::Version) -> std::io::Result<()> {
	std::fs::write("db/version", version.to_string())
}

pub fn verify(expected: &semver::Version) {
	let version = version();
	assert!(version == *expected, "DB version error: expected {expected}, but DB was found at {version}.");
}

/// If you use a single static Env, e.g.
/// ```ignore
/// static ENV: Lazy<Env> = Lazy::new(||
/// Env::builder(MODULE_PATH).unwrap()
///        .with::<MyTable>()
///        ...
///        ...
///        .build().unwrap()
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

		/// discouraged
		///
		/// returning RwTxn is necessary because of lifetime issues,
		/// we can use the for<'a> syntax to make it work but
		/// it forbids type inference in usage sites
		pub async fn write_async<Res, Job, Fut>(job: Job) -> ::std::result::Result<Res, ::batadase::Error> where
			Job: ::std::ops::FnOnce(::batadase::transaction::RwTxn) -> Fut,
			Fut: ::std::future::Future<Output = (::batadase::transaction::RwTxn, Res)>,
		{ $env_name.write_async(job).await }
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

		/// discouraged
		///
		/// returning RwTxn is necessary because of lifetime issues,
		/// we can use the for<'a> syntax to make it work but
		/// it forbids type inference in usage sites
		pub async fn write_async<Res, Job, Fut>(job: Job) -> ::std::result::Result<Res, ::batadase::Error> where
			Job: ::std::ops::FnOnce(::batadase::transaction::RwTxn) -> Fut,
			Fut: ::std::future::Future<Output = (::batadase::transaction::RwTxn, Res)>,
		{ $env_name.write_async(job).await }
	};

	// the only error most people should really care about is read_tx's ReadersFull
	// atm it's 4096 max read transactions at once, which should be enough
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

		/// discouraged
		///
		/// returning RwTxn is necessary because of lifetime issues,
		/// we can use the for<'a> syntax to make it work but
		/// it forbids type inference in usage sites
		pub async fn write_async<Res, Job, Fut>(job: Job) -> Res where
			Job: ::std::ops::FnOnce(::batadase::transaction::RwTxn) -> Fut,
			Fut: ::std::future::Future<Output = (::batadase::transaction::RwTxn, Res)>,
		{ $env_name.write_async(job).await.unwrap() }
	};
}
