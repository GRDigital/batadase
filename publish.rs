//# command-macros = { version = "*", features = ["nightly"] }

#![feature(proc_macro_hygiene)]
use command_macros::command as cmd;
use std::thread::sleep;
use std::time::Duration;

fn main() {
	assert!(cmd!(cargo publish).current_dir("batadase-index").status().unwrap().success());
	sleep(Duration::from_secs(60));
	assert!(cmd!(cargo publish).current_dir("batadase-macros").status().unwrap().success());
	sleep(Duration::from_secs(60));
	assert!(cmd!(cargo publish).current_dir("batadase").status().unwrap().success());
}
