// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// An executor-local identity that is never reused, even when task storage is recycled.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct TaskId(u64);

impl TaskId {
    pub(crate) fn next(&mut self) -> Self {
        self.0 = self.0.checked_add(1).expect("an executor cannot register more than u64::MAX tasks");
        *self
    }

    #[cfg(test)]
    pub(crate) const fn fake() -> Self {
        Self(0)
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_impl_all;

    use super::*;

    #[test]
    fn identities_are_thread_safe() {
        assert_impl_all!(TaskId: Send, Sync);
    }

    #[test]
    fn identities_are_never_reused() {
        let mut id = TaskId::default();
        let first = id.next();
        let second = id.next();
        assert_ne!(first, second);
        assert_ne!(first, TaskId::default());
    }

    #[test]
    fn exhausted_identity_never_wraps() {
        let mut id = TaskId(u64::MAX);
        testing_aids::assert_panic!(id.next());
        assert_eq!(id, TaskId(u64::MAX));
    }
}
