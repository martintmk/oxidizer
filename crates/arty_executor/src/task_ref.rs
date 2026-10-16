// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::ptr::NonNull;

use plurality::{Box as PoolBox, coerce};

use crate::TypeErasedTask;

/// Executor-local task identity.
///
/// Values are never reused, so notifications remain harmless after their task storage is
/// released or its plurality slot is reused.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TaskId {
    index: u32,
    generation: u32,
}

#[derive(Debug, Default)]
pub(crate) struct TaskIdSource {
    next_index: u32,
    generation: u32,
}

impl TaskIdSource {
    pub(crate) fn take_next(&mut self) -> TaskId {
        let id = TaskId {
            index: self.next_index,
            generation: self.generation,
        };
        let (next_index, wrapped) = self.next_index.overflowing_add(1);
        self.next_index = next_index;
        if wrapped {
            self.generation = self
                .generation
                .checked_add(1)
                .expect("task ID generations must not wrap because stale IDs must never identify a new task");
        }
        id
    }
}

impl TaskId {
    #[cfg(test)]
    pub(crate) const fn fake() -> Self {
        Self { index: 0, generation: 0 }
    }
}

/// Allows a task to be referenced by the executor and its resources released, without knowing
/// the exact type of the task, only that it implements [`TypeErasedTask`].
///
/// This acts like a super-powered pointer and enforces no ownership semantics - the caller is
/// responsible for not using it once the task has been dropped.
#[derive(Copy, Clone, Debug)]
pub(crate) struct TaskRef {
    inner: NonNull<dyn TypeErasedTask>,
}

/// A live executor task paired with its never-reused identity.
#[derive(Copy, Clone, Debug)]
pub(crate) struct RegisteredTask {
    id: TaskId,
    task_ref: TaskRef,
}

impl RegisteredTask {
    pub(crate) fn new(id: TaskId, task_ref: TaskRef) -> Self {
        Self { id, task_ref }
    }

    pub(crate) fn task_ref(self) -> TaskRef {
        self.task_ref
    }
}

impl Borrow<TaskId> for RegisteredTask {
    fn borrow(&self) -> &TaskId {
        &self.id
    }
}

impl PartialEq for RegisteredTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RegisteredTask {}

impl Hash for RegisteredTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl TaskRef {
    pub(crate) fn new<T: TypeErasedTask + 'static>(inner: PoolBox<T>) -> Self {
        let inner: PoolBox<dyn TypeErasedTask> = PoolBox::unsize(inner, coerce!(dyn TypeErasedTask));
        Self {
            inner: PoolBox::into_raw(inner),
        }
    }

    /// # Safety
    ///
    /// The caller is responsible for ensuring that Rust aliasing rules are not violated.
    ///
    /// This may only be called on the same thread as the task was created on. That is, the
    /// `TaskRef` may be passed from thread to thread but has to end up back on its original
    /// thread to actually be used.
    ///
    /// The caller must guarantee that the referenced task is still alive.
    #[must_use]
    pub(crate) unsafe fn as_task(&self) -> Pin<&dyn TypeErasedTask> {
        // SAFETY: Forwarding safety guarantees from the caller.
        let task = unsafe { self.inner.as_ref() };

        // SAFETY: Our tasks are always pinned, they just lose this metadata in their pointer form.
        unsafe { Pin::new_unchecked(task) }
    }

    /// Drops the task and returns its slot to the pool.
    ///
    /// # Safety
    ///
    /// This may only be called on the same thread as the task was created on. That is, the
    /// `TaskRef` may be passed from thread to thread but has to end up back on its original
    /// thread to actually be used.
    ///
    /// The caller must ensure this is called exactly once for the referenced task and that no
    /// references to it remain.
    pub(crate) unsafe fn release(self) {
        // SAFETY: Forwarding the caller's guarantee that this is the original raw pointer and is
        // reconstructed exactly once after all references have expired.
        drop(unsafe { PoolBox::<dyn TypeErasedTask>::from_raw(self.inner) });
    }
}

// SAFETY: It is permissible to send `TaskRef` between threads for the purpose of passing the
// reference around. However, methods must only be called on the original thread the task
// was created on.
unsafe impl Send for TaskRef {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_ids_are_monotonic() {
        let mut source = TaskIdSource::default();
        let first = source.take_next();
        let second = source.take_next();

        assert_ne!(first, second);
        assert_eq!(source.next_index, 2);
        assert_eq!(source.generation, 0);
    }

    #[test]
    fn task_id_index_wrap_advances_generation() {
        let mut source = TaskIdSource {
            next_index: u32::MAX,
            generation: 7,
        };

        assert_eq!(
            source.take_next(),
            TaskId {
                index: u32::MAX,
                generation: 7
            }
        );
        assert_eq!(source.take_next(), TaskId { index: 0, generation: 8 });
    }

    #[test]
    fn task_id_generation_wrap_is_rejected() {
        let mut source = TaskIdSource {
            next_index: u32::MAX,
            generation: u32::MAX,
        };

        testing_aids::assert_panic!(source.take_next());
    }
}
