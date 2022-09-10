test:
	RUSTFLAGS="--cfg madsim" CARGO_TARGET_DIR="target/sim" cargo test
