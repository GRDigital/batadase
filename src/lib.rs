// pub use assoc_table::AssocTable;
// pub use poly_table::PolyTable;
// pub use table::Table;

pub use grd_utils::common_prelude::*;

pub use env::Env;
pub use lmdb::Error;
pub use index::Index;
pub use transaction::*;

pub mod assoc_table;
pub mod poly_table;
pub mod table;

pub mod env;
pub mod transaction;
pub mod lmdb;

pub mod index;
pub mod prelude;

// potentially useful relation table flavours:
// * one to many via Indices
// * two-way one-to-one via Indices
// * two-way many-to-many via Indices

type RkyvSmallSer = rkyv::ser::serializers::AllocSerializer<512>;

pub trait DbName {
	type Table<'tx, TX: Transaction>;
	const NAME: &'static [u8];

	fn get<TX: Transaction>(tx: &TX) -> Self::Table<'_, TX>;
	fn flags() -> enumflags2::BitFlags<lmdb::DbFlags> { enumflags2::BitFlags::empty() }
}

pub fn get_version() -> anyhow::Result<semver::Version> {
	let file = std::fs::read_to_string("db/version")?;
	Ok(semver::Version::parse(file.trim())?)
}

pub fn verify(expected: semver::Version) {
	let version = get_version()
		.expect("Failed to get DB version from file. Please ensure there is a 'version' file in the db directory with a valid semver version.");
	assert!(version != expected, "DB version error: expected {expected}, but DB was found at {version}.");
}

/// If you use a single static Env, e.g.
/// ```
/// static ENV: Lazy<Env> = Lazy::new(||
///    Env::builder().unwrap()
///        .with::<names::TableName>(&names::MODULE_PATH)
///        ...
///        ...
///        .build().unwrap()
/// );
/// ```
/// you can call it'ENV' and use this macro for more convenient read/write fn without having to pass it in, e.g. just
/// `db::try_write(move |tx| { ... }).await??`
#[macro_export]
macro_rules! def_tx_ops {
	() => {
		pub fn read_tx() -> Result<::batadase::transaction::RoTxn, ::batadase::Error>  { ::batadase::transaction::read_tx(&ENV) }

		pub async fn write<Res, Job>(job: Job) -> Result<Res, ::batadase::Error> where
			Res: Send + 'static,
			Job: (FnOnce(&::batadase::transaction::RwTxn) -> Res) + Send + 'static,
			{
				::batadase::transaction::write(job, &ENV).await
			}

		pub async fn try_write<Res, Job>(job: Job) -> Result<anyhow::Result<Res>, ::batadase::Error> where
			Res: Send + 'static,
			Job: (FnOnce(&::batadase::transaction::RwTxn) -> anyhow::Result<Res>) + Send + 'static,
			{
				::batadase::transaction::try_write(job, &ENV).await
			}

		/// discouraged
		///
		/// returning RwTxn is necessary because of lifetime issues,
		/// we can use the for<'a> syntax to make it work but
		/// it forbids type inference in usage sites
		pub async fn write_async<Res, Job, Fut>(job: Job) -> Result<Res, ::batadase::Error> where
			Job: FnOnce(::batadase::transaction::RwTxn) -> Fut,
			Fut: Future<Output = (::batadase::transaction::RwTxn, Res)>,
			{
				::batadase::transaction::write_async(job, &ENV).await
			}
	}
}
