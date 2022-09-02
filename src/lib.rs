#[allow(unused_imports)]
#[macro_use]
extern crate log;

// After you finish the implementation, `#[allow(unused)]` should be removed.
#[allow(dead_code, unused)]
mod client;
mod msg;
#[allow(unused)]
mod server;
#[cfg(all(test, madsim))]
mod tests;
