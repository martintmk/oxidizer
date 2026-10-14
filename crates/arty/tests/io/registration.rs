// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Registration serialization, role consistency, and nested registration.

use super::*;

#[test]
fn context_registration_initializes_every_worker_before_returning() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);

    let first = workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| builtins.io().context::<TestContext<CacheContext>>(&scenario)
        })
        .join()
        .unwrap();
    assert_eq!(
        first.thread.as_ref().unwrap().id(),
        workers[0].spawn(async |cx| cx.thread().id()).join().unwrap()
    );
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), WORKERS);
    assert_eq!(scenario.option_relocations.load(Ordering::SeqCst), 1);

    flush_workers(&workers);
    assert_eq!(scenario.context_constructions.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.relocations.load(Ordering::SeqCst), WORKERS);
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), WORKERS);

    workers[0]
        .spawn(async |builtins| {
            let _ = builtins
                .io()
                .context::<TestContext<CacheContext>>(&Scenario::options(RolePlan::PreferPrimary));
        })
        .join()
        .unwrap();
    workers[1]
        .spawn(async |builtins| {
            let _ = builtins
                .io()
                .context::<TestContext<CacheContext>>(&Scenario::options(RolePlan::PreferPrimary));
        })
        .join()
        .unwrap();

    assert_eq!(scenario.context_constructions.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), WORKERS);
    assert_eq!(scenario.driver_workers().len(), WORKERS);
    assert!(scenario.roles().iter().all(|role| *role == DriverRole::Primary));
    runtime.stop().unwrap();
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
}
#[test]
fn runtime_registries_are_isolated() {
    let first_runtime = runtime(1);
    let second_runtime = runtime(1);
    let first = Scenario::options(RolePlan::PreferPrimary);
    let second = Scenario::options(RolePlan::PreferPrimary);

    first_runtime
        .scheduler()
        .block_on({
            let first = first.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<IsolatedContext>>(&first);
            }
        })
        .unwrap();
    second_runtime
        .scheduler()
        .block_on({
            let second = second.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<IsolatedContext>>(&second);
            }
        })
        .unwrap();

    assert_eq!(first.context_constructions.load(Ordering::SeqCst), 1);
    assert_eq!(second.context_constructions.load(Ordering::SeqCst), 1);
    assert_eq!(first.roles(), vec![DriverRole::Primary]);
    assert_eq!(second.roles(), vec![DriverRole::Primary]);
    first_runtime.stop().unwrap();
    second_runtime.stop().unwrap();
}
#[test]
fn waiting_registration_observes_another_workers_failure() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(1, Ordering::SeqCst);
    scenario.fail_driver_at.store(1, Ordering::SeqCst);
    let (first_outcome_tx, first_outcome_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (second_outcome_tx, second_outcome_rx) = mpsc::channel();

    let first = workers[0].spawn({
        let scenario = scenario.clone();
        async move |builtins| {
            let outcome = builtins
                .io()
                .try_context::<TestContext<ConcurrentFirstFailure>>(&scenario)
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
                .try_context::<TestContext<ConcurrentFirstFailure>>(&scenario)
                .map_or_else(|error| error.to_string(), |_| "ok".to_owned());
            second_outcome_tx.send(outcome).unwrap();
        }
    });
    second_started_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(20));
    scenario.release_driver();

    assert_eq!(
        first_outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "driver construction failed"
    );
    assert_eq!(
        second_outcome_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap(),
        "driver construction failed"
    );
    let _ = first.join();
    let _ = second.join();
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), WORKERS);
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn concurrent_same_context_registration_constructs_options_once() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::PreferPrimary);
    scenario.block_driver_at.store(1, Ordering::SeqCst);
    let (second_started_tx, second_started_rx) = mpsc::channel();

    let first = workers[0].spawn({
        let scenario = scenario.clone();
        async move |builtins| {
            let _ = builtins.io().context::<TestContext<ConcurrentSameContext>>(&scenario);
        }
    });
    scenario.wait_until_driver_blocked();

    let second = workers[1].spawn(async move |builtins| {
        second_started_tx.send(()).unwrap();
        let _ = builtins
            .io()
            .context::<TestContext<ConcurrentSameContext>>(&Scenario::options(RolePlan::PreferPrimary));
    });
    second_started_rx.recv_timeout(testing_aids::TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(20));
    scenario.release_driver();

    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(scenario.context_constructions.load(Ordering::SeqCst), 1);
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), WORKERS);
    assert_eq!(scenario.driver_workers().len(), WORKERS);
    assert!(scenario.roles().iter().all(|role| *role == DriverRole::Primary));
    runtime.stop().unwrap();
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
}
#[test]
fn nested_context_registration_does_not_deadlock_or_duplicate_primary() {
    let (result_tx, result_rx) = mpsc::channel();
    let execution = thread::spawn(move || {
        let runtime = runtime(1);
        let outer = Scenario::options(RolePlan::PreferPrimary);
        let inner = Scenario::options(RolePlan::PreferPrimary);
        runtime
            .scheduler()
            .block_on({
                let outer = outer.clone();
                let inner = inner.clone();
                async move |builtins| {
                    outer.register_nested_from_context(builtins.io().clone(), inner);
                    let _ = builtins.io().context::<TestContext<NestedOuter>>(&outer);
                }
            })
            .unwrap();
        runtime.stop().unwrap();
        result_tx.send((outer.roles(), inner.roles())).unwrap();
    });

    let (outer_roles, inner_roles) = result_rx
        .recv_timeout(testing_aids::TEST_TIMEOUT)
        .expect("nested context registration must not deadlock");
    execution.join().unwrap();
    assert_eq!(outer_roles, vec![DriverRole::Primary]);
    assert_eq!(inner_roles, vec![DriverRole::Secondary]);
}
#[test]
fn nested_registration_is_cleaned_up_when_outer_initialization_fails() {
    let runtime = runtime(1);
    let outer = Scenario::options(RolePlan::PreferPrimary);
    let inner = Scenario::options(RolePlan::PreferPrimary);
    outer.fail_driver_at.store(1, Ordering::SeqCst);

    let error = runtime
        .scheduler()
        .block_on({
            let outer = outer.clone();
            let inner = inner.clone();
            async move |builtins| {
                outer.register_nested_from_context(builtins.io().clone(), inner);
                builtins
                    .io()
                    .try_context::<TestContext<NestedOuterFailure>>(&outer)
                    .unwrap_err()
                    .to_string()
            }
        })
        .unwrap();

    assert_eq!(error, "driver construction failed");
    assert_eq!(inner.roles(), vec![DriverRole::Secondary]);
    assert_eq!(runtime.stop().unwrap_err().to_string(), "driver construction failed");
    assert_eq!(outer.shutdowns.load(Ordering::SeqCst), 0);
    assert_eq!(inner.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn recursive_same_context_registration_panics_without_corrupting_registry() {
    let runtime = runtime(1);
    let scenario = Scenario::options(RolePlan::PreferPrimary);

    runtime
        .scheduler()
        .block_on({
            let scenario = scenario.clone();
            async move |builtins| {
                scenario.register_self_from_context(builtins.io().clone());
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = builtins.io().context::<TestContext<RecursiveContext>>(&scenario);
                }))
                .unwrap_err();
                assert!(
                    panic
                        .downcast_ref::<String>()
                        .is_some_and(|message| message.contains("recursively requested its own registration"))
                        || panic
                            .downcast_ref::<&'static str>()
                            .is_some_and(|message| message.contains("recursively requested its own registration"))
                );

                let _ = builtins.io().context::<TestContext<RecursiveContext>>(&scenario);
            }
        })
        .unwrap();

    assert_eq!(scenario.context_constructions.load(Ordering::SeqCst), 2);
    assert_eq!(scenario.driver_attempts.load(Ordering::SeqCst), 1);
    runtime.stop().unwrap();
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn inconsistent_primary_role_on_another_worker_is_rejected() {
    let (runtime, processor) = runtime_with_telemetry(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let scenario = Scenario::options(RolePlan::Inconsistent);
    scenario.fail_shutdown.store(true, Ordering::SeqCst);

    let error = workers[0]
        .spawn({
            let scenario = scenario.clone();
            async move |builtins| {
                builtins
                    .io()
                    .try_context::<TestContext<InconsistentPrimary>>(&scenario)
                    .unwrap_err()
                    .to_string()
            }
        })
        .join()
        .unwrap();

    assert!(error.contains("disallowed role Secondary"));
    assert!(runtime.stop().unwrap_err().to_string().contains("disallowed role Secondary"));
    assert_eq!(scenario.shutdowns.load(Ordering::SeqCst), WORKERS);
    assert_eq!(event_count(&processor, "arty.rt.io.driver.shutdown_failed"), WORKERS);
}
#[test]
fn racing_registrations_select_one_consistent_primary() {
    for _ in 0..16 {
        race_primary_registration_once();
    }
}
#[test]
fn additional_drivers_are_secondary_on_every_worker() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let primary = Scenario::options(RolePlan::PreferPrimary);
    let first_additional = Scenario::options(RolePlan::PreferPrimary);
    let second_additional = Scenario::options(RolePlan::PreferPrimary);

    workers[0]
        .spawn({
            let primary = primary.clone();
            let first_additional = first_additional.clone();
            let second_additional = second_additional.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<CacheContext>>(&primary);
                let _ = builtins.io().context::<TestContext<AdditionalSecondaryA>>(&first_additional);
                let _ = builtins.io().context::<TestContext<AdditionalSecondaryB>>(&second_additional);
            }
        })
        .join()
        .unwrap();
    flush_workers(&workers);

    for worker in &workers {
        worker
            .spawn(async |builtins| {
                let _ = builtins
                    .io()
                    .context::<TestContext<CacheContext>>(&Scenario::options(RolePlan::PreferPrimary));
                let _ = builtins
                    .io()
                    .context::<TestContext<AdditionalSecondaryA>>(&Scenario::options(RolePlan::PreferPrimary));
                let _ = builtins
                    .io()
                    .context::<TestContext<AdditionalSecondaryB>>(&Scenario::options(RolePlan::PreferPrimary));
            })
            .join()
            .unwrap();
    }

    assert!(primary.roles().iter().all(|role| *role == DriverRole::Primary));
    for additional in [&first_additional, &second_additional] {
        assert_eq!(additional.driver_workers().len(), WORKERS);
        assert_eq!(additional.driver_attempts.load(Ordering::SeqCst), WORKERS);
        assert!(additional.roles().iter().all(|role| *role == DriverRole::Secondary));
    }
    runtime.stop().unwrap();
}

#[test]
fn secondary_selection_releases_the_primary_reservation() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let reserved = Scenario::options(RolePlan::Secondary);
    let concurrent = Scenario::options(RolePlan::PreferPrimary);
    let later = Scenario::options(RolePlan::PreferPrimary);
    reserved.block_driver_at.store(1, Ordering::SeqCst);

    let reserved_registration = workers[0].spawn({
        let reserved = reserved.clone();
        async move |builtins| {
            let _ = builtins.io().context::<TestContext<ReservedSecondary>>(&reserved);
        }
    });
    reserved.wait_until_driver_blocked();

    let concurrent_registration = workers[1].spawn({
        let concurrent = concurrent.clone();
        async move |builtins| {
            let _ = builtins.io().context::<TestContext<ConcurrentSecondary>>(&concurrent);
        }
    });
    wait_until(|| concurrent.driver_attempts.load(Ordering::SeqCst) > 0);

    reserved.release_driver();
    reserved_registration.join().unwrap();
    concurrent_registration.join().unwrap();
    assert_eq!(reserved.roles(), vec![DriverRole::Secondary; WORKERS]);
    assert_eq!(concurrent.roles(), vec![DriverRole::Secondary; WORKERS]);

    workers[0]
        .spawn({
            let later = later.clone();
            async move |builtins| {
                let _ = builtins.io().context::<TestContext<LaterPrimary>>(&later);
            }
        })
        .join()
        .unwrap();
    assert_eq!(later.roles(), vec![DriverRole::Primary; WORKERS]);

    runtime.stop().unwrap();
    assert_eq!(reserved.shutdowns.load(Ordering::SeqCst), WORKERS);
    assert_eq!(concurrent.shutdowns.load(Ordering::SeqCst), WORKERS);
    assert_eq!(later.shutdowns.load(Ordering::SeqCst), WORKERS);
}

fn race_primary_registration_once() {
    let runtime = runtime(WORKERS);
    let workers = worker_schedulers(&runtime, WORKERS);
    let first = Scenario::options(RolePlan::PreferPrimary);
    let second = Scenario::options(RolePlan::PreferPrimary);
    let barrier = Arc::new(Barrier::new(WORKERS));

    let first_registration = workers[0].spawn({
        let first = first.clone();
        let barrier = Arc::clone(&barrier);
        async move |builtins| {
            barrier.wait();
            let _ = builtins.io().context::<TestContext<RacePrimaryA>>(&first);
        }
    });
    let second_registration = workers[1].spawn({
        let second = second.clone();
        let barrier = Arc::clone(&barrier);
        async move |builtins| {
            barrier.wait();
            let _ = builtins.io().context::<TestContext<RacePrimaryB>>(&second);
        }
    });
    first_registration.join().unwrap();
    second_registration.join().unwrap();
    flush_workers(&workers);

    for worker in &workers {
        worker
            .spawn(async |builtins| {
                let _ = builtins
                    .io()
                    .context::<TestContext<RacePrimaryA>>(&Scenario::options(RolePlan::PreferPrimary));
                let _ = builtins
                    .io()
                    .context::<TestContext<RacePrimaryB>>(&Scenario::options(RolePlan::PreferPrimary));
            })
            .join()
            .unwrap();
    }

    let first_roles = first.roles();
    let second_roles = second.roles();
    assert_eq!(first_roles.len(), WORKERS);
    assert_eq!(second_roles.len(), WORKERS);
    assert_eq!(first.driver_workers().len(), WORKERS);
    assert_eq!(second.driver_workers().len(), WORKERS);
    assert!(first_roles.iter().all(|role| *role == first_roles[0]));
    assert!(second_roles.iter().all(|role| *role == second_roles[0]));
    assert_ne!(first_roles[0], second_roles[0]);
    assert_eq!(
        [first_roles[0], second_roles[0]]
            .into_iter()
            .filter(|role| *role == DriverRole::Primary)
            .count(),
        1
    );
    runtime.stop().unwrap();
}
