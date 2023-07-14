// pub use assoc_table::AssocTable;
// pub use poly_table::PolyTable;
// pub use table::Table;

pub use grd_utils::common_prelude::*;

pub use env::Env;
pub use lmdb::Error;
pub use transaction::*;

mod assoc_table;
mod poly_table;
mod table;

mod env;
mod transaction;
mod lmdb;

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
	if version != expected {
		panic!("DB version error: expected {expected}, but DB was found at {version}.")
	}
}
