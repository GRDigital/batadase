use fehler::{throw, throws};
use error as shared_error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
	#[error("the version of the LMDB library doesn't match the version that created the database environment")] VersionMismatch,
	#[error("the environment file headers are corrupted")] Corrupted,
	#[error("the directory specified by the path parameter doesn't exist")] DirDoesntExist,
	#[error("the user didn't have permission to access the environment files")] NoAccess,
	#[error("the environment was locked by another process")] EnvLocked,
	#[error("database create error")] CreateError(i32),
	#[error("an invalid parameter was specified")] InvalidParameter,
	#[error("a fatal error occurred earlier and the environment must be shut down")] Panic,
	#[error("another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well")] MapResized,
	#[error("a read-only transaction was requested and the reader lock table is full")] ReadersFull,
	#[error("no more disk space")] NoDiskSpace,
	#[error("database is full")] MapFull,
	#[error("too many tx dirty pags")] TxnFull,
	#[error("trying to write a readonly transaction")] TxnPerm,
	#[error("a low-level I/O error occured while writing")] Io,
	#[error("out of memory")] Oom,
	#[error("key already exists and overwrite isn't requested")] KeyExists,
	#[error("misc error")] Misc,
}

impl From<Error> for shared_error::Error {
    fn from(value: Error) -> Self {
       shared_error::Error::DbError(value.to_string())
    }
}

#[throws]
pub fn handle_del_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EACCES => throw!(Error::TxnPerm),
		libc::EINVAL => throw!(Error::InvalidParameter),
		_ => throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_put_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_MAP_FULL => throw!(Error::MapFull),
		lmdb_sys::MDB_TXN_FULL => throw!(Error::TxnFull),
		lmdb_sys::MDB_KEYEXIST => throw!(Error::KeyExists),
		libc::EACCES => throw!(Error::TxnPerm),
		libc::EINVAL => throw!(Error::InvalidParameter),
		_ => throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_drop_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_get_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EINVAL => throw!(Error::InvalidParameter),
		_ => throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_cursor_open_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		libc::EINVAL => throw!(Error::InvalidParameter),
		_ => throw!(Error::Misc),
	}
}

pub fn handle_cursor_get_code(code: i32) -> bool {
	match code {
		lmdb_sys::MDB_SUCCESS => true,
		lmdb_sys::MDB_NOTFOUND => false,
		libc::EINVAL => panic!("cursor invalid parameter"),
		e => panic!("cursor misc error {e}"),
	}
}

#[throws]
pub fn handle_txn_begin_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_PANIC => fehler::throw!(Error::Panic),
		lmdb_sys::MDB_MAP_RESIZED => fehler::throw!(Error::MapResized),
		lmdb_sys::MDB_READERS_FULL => fehler::throw!(Error::ReadersFull),
		libc::ENOMEM => fehler::throw!(Error::Oom),
		_ => fehler::throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_txn_commit_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		libc::EINVAL => fehler::throw!(Error::InvalidParameter),
		libc::ENOSPC => fehler::throw!(Error::NoDiskSpace),
		libc::EIO => fehler::throw!(Error::Io),
		libc::ENOMEM => fehler::throw!(Error::Oom),
		_ => fehler::throw!(Error::Misc),
	}
}

#[throws]
pub fn handle_env_create_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		e => fehler::throw!(Error::CreateError(e)),
	}
}

#[throws]
pub fn handle_env_set_maxdbs_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => fehler::throw!(Error::InvalidParameter),
	}
}

#[throws]
pub fn handle_env_set_mapsize_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		_ => fehler::throw!(Error::InvalidParameter),
	}
}

#[throws]
pub fn handle_env_open(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_VERSION_MISMATCH => fehler::throw!(Error::VersionMismatch),
		lmdb_sys::MDB_INVALID => fehler::throw!(Error::Corrupted),
		libc::ENOENT | libc::ESRCH => fehler::throw!(Error::DirDoesntExist),
		libc::EACCES => fehler::throw!(Error::NoAccess),
		libc::EAGAIN => fehler::throw!(Error::EnvLocked),
		_ => fehler::throw!(Error::Misc),
	}
}

pub fn handle_dbi_open_code(code: i32) {
	match code {
		lmdb_sys::MDB_SUCCESS => {},
		lmdb_sys::MDB_NOTFOUND => panic!("not found"), // should be unreachable
		lmdb_sys::MDB_DBS_FULL => panic!("too many dbs"),
		e => panic!("misc error {e}"),
	}
}
