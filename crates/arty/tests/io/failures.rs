// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Initialization failures and shutdown races.

use super::*;

#[test]
fn external_initialization_failure_is_returned_and_stops_runtime() {
    let runtime = runtime(1);
    let builtins = runtime.scheduler().block_on(async |builtins| builtins).unwrap();
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_driver_at.store(1, Ordering::SeqCst);

    let error = {
        let scenario = scenario.clone();
        thread::spawn(move || {
            builtins
                .io()
                .try_context::<TestContext<ExternalConstructionFailure>>(&scenario)
                .unwrap_err()
                .to_string()
        })
        .join()
        .unwrap()
    };

    assert_eq!(error, "driver construction failed");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 0);
}
#[test]
fn try_context_reports_driver_construction_failure_and_stops_runtime() {
    let (runtime, processor) = runtime_with_telemetry(1);
    let worker = worker_schedulers(&runtime, 1).pop().unwrap();
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_driver_at.store(1, Ordering::SeqCst);

    let error = worker
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                builtins
                    .io()
                    .try_context::<TestContext<ConstructionFailure>>(&scenario)
                    .unwrap_err()
                    .to_string()
            }
        })
        .join()
        .unwrap();
    assert_eq!(error, "driver construction failed");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 0);
    assert_eq!(event_count(&processor, "arty.rt.io.driver.initialization_failed"), 1);
}
#[test]
fn initial_cycle_failure_rolls_back_driver_and_stops_runtime() {
    let (runtime, processor) = runtime_with_telemetry(1);
    let worker = worker_schedulers(&runtime, 1).pop().unwrap();
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_initial_cycle_at.store(1, Ordering::SeqCst);
    scenario.fail_shutdown.store(true, Ordering::SeqCst);

    let error = worker
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                builtins
                    .io()
                    .try_context::<TestContext<InitialCycleFailure>>(&scenario)
                    .unwrap_err()
                    .to_string()
            }
        })
        .join()
        .unwrap();
    assert_eq!(error, "initial driver cycle failed");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "initial driver cycle failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(event_count(&processor, "arty.rt.io.driver.initialization_failed"), 1);
    assert_eq!(event_count(&processor, "arty.rt.io.driver.shutdown_failed"), 1);
}
#[test]
fn context_panics_after_requesting_shutdown_on_initialization_failure() {
    let runtime = runtime(1);
    let worker = worker_schedulers(&runtime, 1).pop().unwrap();
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_driver_at.store(1, Ordering::SeqCst);

    let panicked = worker
        .spawn({
            async move |builtins| {
                catch_unwind(AssertUnwindSafe(|| {
                    let _ = builtins.io().context::<TestContext<GetFailure>>(&scenario);
                }))
                .is_err()
            }
        })
        .join()
        .unwrap();
    assert!(panicked);
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
}
#[test]
fn late_driver_construction_failure_stops_runtime() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_driver_at.store(2, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<LateConstructionFailure>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);
    let error = workers[1]
        .spawn(async |builtins| {
            builtins
                .io()
                .try_context::<TestContext<LateConstructionFailure>>(&Scenario::options(RolePlan::PreferPrimary))
                .unwrap_err()
                .to_string()
        })
        .join()
        .unwrap();

    assert_eq!(error, "driver construction failed");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn late_initial_cycle_failure_stops_runtime_and_rolls_back_driver() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.fail_initial_cycle_at.store(2, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<LateCycleFailure>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);
    let error = workers[1]
        .spawn(async |builtins| {
            builtins
                .io()
                .try_context::<TestContext<LateCycleFailure>>(&Scenario::options(RolePlan::PreferPrimary))
                .unwrap_err()
                .to_string()
        })
        .join()
        .unwrap();

    assert_eq!(error, "initial driver cycle failed");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "initial driver cycle failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
}
#[test]
fn shutdown_during_first_registration_is_not_reported_as_driver_failure() {
    let runtime = runtime(1);
    let operations = RuntimeOperations::from(&runtime);
    let worker = worker_schedulers(&runtime, 1).pop().unwrap();
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(1, Ordering::SeqCst);
    let (outcome_tx, outcome_rx) = mpsc::channel();

    let initialization = worker.spawn({
        let scenario = scenario.clone();
        async move |builtins| {
            let outcome = builtins
                .io()
                .try_context::<TestContext<ShutdownDuringFirstRegistration>>(&scenario)
                .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
            outcome_tx.send(outcome).unwrap();
        }
    });
    scenario.wait_until_driver_blocked();
    operations.request_stop();
    scenario.release_driver();

    assert_eq!(
        outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "the runtime is shutting down"
    );
    let _ = initialization.join();
    runtime.stop().unwrap();
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn shutdown_during_late_initialization_is_observed_by_the_request() {
    let runtime = runtime(WORKERS);
    let operations = RuntimeOperations::from(&runtime);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(2, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<ShutdownDuringLateInitialization>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);

    let (outcome_tx, outcome_rx) = mpsc::channel();
    let initialization = workers[1].spawn(async move |builtins| {
        let outcome = builtins
            .io()
            .try_context::<TestContext<ShutdownDuringLateInitialization>>(&Scenario::options(RolePlan::PreferPrimary))
            .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
        outcome_tx.send(outcome).unwrap();
    });
    scenario.wait_until_driver_blocked();
    operations.request_stop();
    scenario.release_driver();

    assert_eq!(
        outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "the runtime is shutting down"
    );
    let _ = initialization.join();
    runtime.stop().unwrap();
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
}
#[test]
fn concurrent_late_success_observes_another_workers_initialization_failure() {
    const CONCURRENT_WORKERS: usize = 3;

    let runtime = runtime(CONCURRENT_WORKERS);
    let workers = worker_schedulers(&runtime, CONCURRENT_WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(2, Ordering::SeqCst);
    scenario.fail_driver_at.store(3, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<ConcurrentLateFailure>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);

    let (outcome_tx, outcome_rx) = mpsc::channel();
    let blocked_initialization = workers[1].spawn(async move |builtins| {
        let outcome = builtins
            .io()
            .try_context::<TestContext<ConcurrentLateFailure>>(&Scenario::options(RolePlan::PreferPrimary))
            .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
        outcome_tx.send(outcome).unwrap();
    });
    scenario.wait_until_driver_blocked();

    let failure = workers[2]
        .spawn(async |builtins| {
            builtins
                .io()
                .try_context::<TestContext<ConcurrentLateFailure>>(&Scenario::options(RolePlan::PreferPrimary))
                .unwrap_err()
                .to_string()
        })
        .join()
        .unwrap();
    assert_eq!(failure, "driver construction failed");
    scenario.release_driver();

    assert_eq!(
        outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "driver construction failed"
    );
    let _ = blocked_initialization.join();
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 2);
}
#[test]
fn concurrent_late_success_observes_another_workers_initial_cycle_failure() {
    const CONCURRENT_WORKERS: usize = 3;

    let runtime = runtime(CONCURRENT_WORKERS);
    let workers = worker_schedulers(&runtime, CONCURRENT_WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(2, Ordering::SeqCst);
    scenario.fail_initial_cycle_at.store(3, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<ConcurrentLateCycleFailure>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);

    let (outcome_tx, outcome_rx) = mpsc::channel();
    let blocked_initialization = workers[1].spawn(async move |builtins| {
        let outcome = builtins
            .io()
            .try_context::<TestContext<ConcurrentLateCycleFailure>>(&Scenario::options(RolePlan::PreferPrimary))
            .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
        outcome_tx.send(outcome).unwrap();
    });
    scenario.wait_until_driver_blocked();

    let failure = workers[2]
        .spawn(async |builtins| {
            builtins
                .io()
                .try_context::<TestContext<ConcurrentLateCycleFailure>>(&Scenario::options(RolePlan::PreferPrimary))
                .unwrap_err()
                .to_string()
        })
        .join()
        .unwrap();
    assert_eq!(failure, "initial driver cycle failed");
    scenario.release_driver();

    assert_eq!(
        outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "initial driver cycle failed"
    );
    let _ = blocked_initialization.join();
    assert_eq!(runtime.stop().unwrap_err().to_string(), "initial driver cycle failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), CONCURRENT_WORKERS);
}
#[test]
fn late_initialization_observes_another_workers_cycle_failure() {
    let (runtime, processor) = runtime_with_telemetry(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(2, Ordering::SeqCst);

    workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<CycleFailureDuringInitialization>>(&scenario);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);

    let (outcome_tx, outcome_rx) = mpsc::channel();
    let blocked_initialization = workers[1].spawn(async move |builtins| {
        let outcome = builtins
            .io()
            .try_context::<TestContext<CycleFailureDuringInitialization>>(&Scenario::options(RolePlan::PreferPrimary))
            .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
        outcome_tx.send(outcome).unwrap();
    });
    scenario.wait_until_driver_blocked();
    scenario.fail_normal_cycle.store(true, Ordering::SeqCst);
    scenario.wake_owners();
    wait_until(|| event_count(&processor, "arty.rt.io.driver.cycle_failed") == 1);
    scenario.release_driver();

    assert_eq!(
        outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "normal driver cycle failed"
    );
    let _ = blocked_initialization.join();
    assert_eq!(runtime.stop().unwrap_err().to_string(), "normal driver cycle failed");
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
}

#[test]
fn concurrent_failures_report_one_consistent_runtime_error() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let first = Scenario::options(RolePlan::PreferPrimary);
    let second = Scenario::options(RolePlan::Secondary);
    first.fail_driver_at.store(1, Ordering::SeqCst);
    second.fail_driver_at.store(1, Ordering::SeqCst);
    first.block_driver_at.store(1, Ordering::SeqCst);
    second.block_driver_at.store(1, Ordering::SeqCst);
    first.set_driver_failure_message("first environmental failure");
    second.set_driver_failure_message("second environmental failure");

    let first_failure = workers[0].spawn({
        let first = first.clone();
        async move |builtins| {
            builtins
                .io()
                .try_context::<TestContext<ConcurrentFailureA>>(&first)
                .unwrap_err()
                .to_string()
        }
    });
    first.wait_until_driver_blocked();
    let second_failure = workers[1].spawn({
        let second = second.clone();
        async move |builtins| {
            builtins
                .io()
                .try_context::<TestContext<ConcurrentFailureB>>(&second)
                .unwrap_err()
                .to_string()
        }
    });
    second.wait_until_driver_blocked();

    first.release_driver();
    assert_eq!(first_failure.join().unwrap(), "first environmental failure");
    second.release_driver();
    assert_eq!(second_failure.join().unwrap(), "first environmental failure");
    assert_eq!(runtime.stop().unwrap_err().to_string(), "first environmental failure");
    assert_eq!(first.driver_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(second.driver_attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn shutdown_wakes_same_registration_waiter_without_initializing_again() {
    let runtime = runtime(WORKERS);
    let operations = RuntimeOperations::from(&runtime);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(1, Ordering::SeqCst);
    let (first_outcome_tx, first_outcome_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (second_outcome_tx, second_outcome_rx) = mpsc::channel();

    let first = workers[0].spawn({
        let scenario = scenario.clone();
        async move |builtins| {
            let outcome = builtins
                .io()
                .try_context::<TestContext<ShutdownWhileWaiting>>(&scenario)
                .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
            first_outcome_tx.send(outcome).unwrap();
        }
    });
    scenario.wait_until_driver_blocked();

    let second = workers[1].spawn({
        let scenario = scenario.clone();
        async move |builtins| {
            second_started_tx.send(()).unwrap();
            let outcome = builtins
                .io()
                .try_context::<TestContext<ShutdownWhileWaiting>>(&scenario)
                .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
            second_outcome_tx.send(outcome).unwrap();
        }
    });
    second_started_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(20));
    operations.request_stop();
    scenario.release_driver();

    assert_eq!(
        first_outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "the runtime is shutting down"
    );
    assert_eq!(
        second_outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "the runtime is shutting down"
    );
    let _ = first.join();
    let _ = second.join();
    runtime.stop().unwrap();
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
}
