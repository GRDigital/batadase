use std::thread::sleep;
use std::time::Duration;
use std::process::Command;

fn main() {
	assert!(Command::new("cargo").arg("publish").current_dir("index").status().unwrap().success());
	sleep(Duration::from_secs(60));
	assert!(Command::new("cargo").arg("publish").current_dir("macros").status().unwrap().success());
	sleep(Duration::from_secs(60));
	assert!(Command::new("cargo").arg("publish").status().unwrap().success());
}
