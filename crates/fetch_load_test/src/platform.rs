// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Process controls and process resource counters.

use std::process::Child;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A point-in-time process resource reading.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Snapshot {
    /// User and kernel processor time.
    pub processor_time: Duration,
    /// Processor cycles when available.
    pub cycles: Option<u64>,
    /// Current working set.
    pub working_set_bytes: u64,
    /// Peak working set since process start.
    pub peak_working_set_bytes: u64,
    /// Current private committed bytes.
    pub private_bytes: u64,
}

/// Owns the operating-system process group for one child process.
#[derive(Debug)]
pub struct ChildGroup {
    platform: imp::ChildGroup,
}

impl ChildGroup {
    /// Assigns a child to a group that can terminate its exact process tree.
    ///
    /// # Errors
    ///
    /// Returns an error when the process group, affinity, or child assignment fails.
    pub fn assign(child: &Child, affinity: Option<u64>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            platform: imp::ChildGroup::assign(child, affinity)?,
        })
    }

    /// Terminates every process in the group.
    ///
    /// # Errors
    ///
    /// Returns an error when the operating system cannot terminate the group.
    pub fn terminate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.platform.terminate()
    }
}

/// Applies an affinity mask to the current process.
///
/// # Errors
///
/// Returns an error when the mask is invalid or the operating system rejects it.
pub fn apply_current_affinity(mask: Option<u64>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    imp::apply_current_affinity(mask)
}

/// Captures current-process resources.
#[must_use]
pub fn capture() -> Snapshot {
    imp::capture()
}

#[cfg(windows)]
fn configure_owned_resource<T, E>(resource: T, configure: impl FnOnce(&T) -> Result<(), E>) -> Result<T, E> {
    configure(&resource)?;
    Ok(resource)
}

#[cfg(windows)]
mod imp {
    use std::os::windows::io::AsRawHandle as _;
    use std::process::Child;
    use std::time::Duration;

    use windows::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
    };
    use windows::Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS_EX};
    use windows::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes, SetProcessAffinityMask};
    use windows::Win32::System::WindowsProgramming::QueryProcessCycleTime;

    use super::{Snapshot, configure_owned_resource};

    #[derive(Debug)]
    struct JobHandle(HANDLE);

    impl Drop for JobHandle {
        fn drop(&mut self) {
            // SAFETY: this value owns the handle and closes it exactly once.
            let _ = unsafe { CloseHandle(self.0) };
        }
    }

    #[derive(Debug)]
    pub(super) struct ChildGroup {
        job: JobHandle,
    }

    impl ChildGroup {
        pub(super) fn assign(child: &Child, affinity: Option<u64>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            // SAFETY: a null name requests an unnamed job object.
            let job = JobHandle(unsafe { CreateJobObjectW(None, None)? });
            let process = HANDLE(child.as_raw_handle());
            let job = configure_owned_resource(job, |job| {
                let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
                information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                // SAFETY: the pointer and byte count describe `information` for the duration of the call.
                unsafe {
                    SetInformationJobObject(
                        job.0,
                        JobObjectExtendedLimitInformation,
                        (&raw const information).cast(),
                        u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>()).expect("job information is smaller than u32::MAX"),
                    )?;
                }
                // SAFETY: the standard library keeps the child process handle valid while `child` lives.
                unsafe { AssignProcessToJobObject(job.0, process)? };
                if let Some(mask) = affinity {
                    // SAFETY: the standard library keeps the child process handle valid while `child` lives.
                    unsafe { SetProcessAffinityMask(process, usize::try_from(mask)?)? };
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            })?;
            Ok(Self { job })
        }

        pub(super) fn terminate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // SAFETY: `self.job` remains valid until `Drop`.
            unsafe { TerminateJobObject(self.job.0, 1)? };
            Ok(())
        }
    }

    pub(super) fn apply_current_affinity(mask: Option<u64>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(mask) = mask else {
            return Ok(());
        };
        // SAFETY: `GetCurrentProcess` returns a valid pseudo-handle.
        let process = unsafe { GetCurrentProcess() };
        // SAFETY: the process pseudo-handle is valid and the mask is nonzero.
        unsafe { SetProcessAffinityMask(process, usize::try_from(mask)?)? };
        Ok(())
    }

    const fn to_duration(time: FILETIME) -> Duration {
        let ticks = ((time.dwHighDateTime as u64) << 32) | (time.dwLowDateTime as u64);
        Duration::from_nanos(ticks.saturating_mul(100))
    }

    pub(super) fn capture() -> Snapshot {
        // SAFETY: `GetCurrentProcess` returns a valid pseudo-handle.
        let process = unsafe { GetCurrentProcess() };
        let mut creation = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        // SAFETY: all output pointers remain valid and aligned for the duration of the call.
        let processor_time = unsafe { GetProcessTimes(process, &raw mut creation, &raw mut exit, &raw mut kernel, &raw mut user) }
            .map(|()| to_duration(kernel).saturating_add(to_duration(user)))
            .unwrap_or_default();

        let mut cycles = 0_u64;
        // SAFETY: the output pointer remains valid for the duration of the call.
        let cycles = unsafe { QueryProcessCycleTime(process, &raw mut cycles) }.is_ok().then_some(cycles);

        let mut memory = PROCESS_MEMORY_COUNTERS_EX::default();
        // SAFETY: the API accepts the base-layout prefix and the supplied full structure size.
        let memory_ok = unsafe {
            K32GetProcessMemoryInfo(
                process,
                (&raw mut memory).cast(),
                u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS_EX>()).expect("process counters are smaller than u32::MAX"),
            )
        }
        .as_bool();
        if !memory_ok {
            memory = PROCESS_MEMORY_COUNTERS_EX::default();
        }

        Snapshot {
            processor_time,
            cycles,
            working_set_bytes: u64::try_from(memory.WorkingSetSize).unwrap_or(u64::MAX),
            peak_working_set_bytes: u64::try_from(memory.PeakWorkingSetSize).unwrap_or(u64::MAX),
            private_bytes: u64::try_from(memory.PrivateUsage).unwrap_or(u64::MAX),
        }
    }
}

#[cfg(not(windows))]
mod imp {
    use std::process::Child;

    use super::Snapshot;

    #[derive(Debug)]
    pub(super) struct ChildGroup;

    impl ChildGroup {
        pub(super) fn assign(_child: &Child, _affinity: Option<u64>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Self)
        }

        pub(super) fn terminate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    pub(super) fn apply_current_affinity(_mask: Option<u64>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    pub(super) const fn capture() -> Snapshot {
        Snapshot {
            processor_time: std::time::Duration::ZERO,
            cycles: None,
            working_set_bytes: 0,
            peak_working_set_bytes: 0,
            private_bytes: 0,
        }
    }
}
