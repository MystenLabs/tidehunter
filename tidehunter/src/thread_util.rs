use std::thread::JoinHandle;
use std::time::Duration;

/// Joins a thread with a timeout. If the thread doesn't complete within the timeout,
/// panics in test mode or prints a warning in production mode.
///
/// # Arguments
/// * `jh` - The thread's JoinHandle to wait for
/// * `thread_name` - Name of the thread for error messages
/// * `timeout_secs` - Timeout in seconds
pub fn join_thread_with_timeout(jh: JoinHandle<()>, thread_name: &str, timeout_secs: u64) {
    // Create a channel to signal join completion
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = jh.join();
        tx.send(result).ok();
    });

    // Wait up to timeout for the thread to complete
    match rx.recv_timeout(Duration::from_secs(timeout_secs)) {
        Ok(Ok(_result)) => {
            // Thread completed successfully
        }
        Ok(Err(e)) => {
            panic!("{thread_name} thread panicked: {e:?}");
        }
        Err(_timeout) => {
            let msg = format!(
                "{thread_name} thread did not stop in {timeout_secs} seconds (this should not happen)"
            );

            #[cfg(test)]
            panic!("{msg}");

            #[cfg(not(test))]
            eprintln!("WARNING: {msg}");
        }
    }
}
