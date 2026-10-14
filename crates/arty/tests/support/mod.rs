// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use arty::task::{JoinError, JoinHandle};
use testing_aids::TEST_TIMEOUT;

pub(crate) trait JoinHandleExt {
    type Output;

    fn join(self) -> Result<Self::Output, JoinError>;
}

impl<R> JoinHandleExt for JoinHandle<R>
where
    R: Send + 'static,
{
    type Output = R;

    fn join(self) -> Result<Self::Output, JoinError> {
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            _ = result_tx.send(futures::executor::block_on(self));
        });
        match result_rx.recv_timeout(TEST_TIMEOUT) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("task join did not complete before the test deadline: {error}");
                std::process::abort();
            }
        }
    }
}
