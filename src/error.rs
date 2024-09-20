#[derive(thiserror::Error, Debug)]
pub enum Error {
	#[error(transparent)] Lmdb(#[from] crate::lmdb::Error),
	#[error(transparent)] Rkyv(#[from] rkyv::rancor::Error),
}
