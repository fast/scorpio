// Copyright 2025 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use super::*;

fn at(genesis: Instant, millis: u64) -> Instant {
    genesis
        .checked_add(Duration::from_millis(millis))
        .expect("test instant must be representable")
}

fn reset_randomized_wheel(
    wheel: &mut Wheel<u64>,
    oracle: &mut BTreeMap<u64, u64>,
    live: &mut Vec<(usize, u64)>,
    genesis: Instant,
) {
    for (wheel_id, oracle_id) in live.drain(..) {
        assert_eq!(wheel.remove(wheel_id), Some(oracle_id));
        assert!(oracle.remove(&oracle_id).is_some());
    }
    assert!(oracle.is_empty());
    *wheel = Wheel::new(genesis);
}

#[derive(Clone, Copy)]
enum RandomizedDrainMode {
    Exhaustive,
    Bounded,
}

#[derive(Default)]
struct RandomizedCoverage {
    cancellations_while_draining: usize,
    inserts_while_draining: usize,
    resumed_after_clock_advance: usize,
    suspended_drains: usize,
}

fn advance_randomized_wheel(
    wheel: &mut Wheel<u64>,
    oracle: &mut BTreeMap<u64, u64>,
    live: &mut Vec<(usize, u64)>,
    genesis: Instant,
    now: u64,
    max_steps: Option<usize>,
) -> bool {
    let mut steps = 0;
    loop {
        if max_steps.is_some_and(|max_steps| steps == max_steps) {
            return false;
        }
        let Some(step) = wheel.step(at(genesis, now)) else {
            return true;
        };
        steps += 1;

        if let Step::Fire(id) = step {
            let value = wheel.remove(id).expect("fired timer must remain live");
            let deadline = oracle
                .remove(&value)
                .expect("fired timer must exist in the oracle");
            assert!(deadline <= now, "timer {value} fired early");
            let index = live
                .iter()
                .position(|&(wheel_id, _)| wheel_id == id)
                .expect("fired timer must exist in the live set");
            live.swap_remove(index);
        }
    }
}

fn assert_randomized_wheel_matches_oracle(
    wheel: &Wheel<u64>,
    oracle: &BTreeMap<u64, u64>,
    genesis: Instant,
    now: u64,
    settled: bool,
) {
    assert_eq!(wheel.len(), oracle.len());
    let now = at(genesis, now);
    let due: Vec<_> = oracle
        .iter()
        .filter_map(|(&id, &deadline)| (at(genesis, deadline) <= now).then_some(id))
        .collect();
    if settled {
        assert!(due.is_empty(), "due timers were not fired: {due:?}");
    } else if !due.is_empty() {
        assert!(wheel.has_due(now), "due timers were not reported: {due:?}");
        assert_eq!(wheel.next_poll_at(now), Some(now));
    }

    match oracle.values().min() {
        Some(&deadline) => {
            let deadline = at(genesis, deadline);
            let latest_safe_poll = deadline.max(now);
            let next_poll_at = wheel
                .next_poll_at(now)
                .expect("a live timer must have a next poll instant");
            if settled {
                assert!(
                    next_poll_at > now,
                    "settled wheel asked for an immediate re-poll at {now:?}"
                );
            }
            assert!(
                next_poll_at <= latest_safe_poll,
                "next poll instant {next_poll_at:?} overshot {latest_safe_poll:?}; earliest \
                 deadline={deadline:?}, now={now:?}, position={}, draining={:?}, boundary={:?}",
                wheel.position,
                wheel.draining,
                wheel.next_wheel_boundary(),
            );
        }
        None => assert_eq!(wheel.next_poll_at(now), None),
    }
}

#[test]
fn levels_cover_exact_horizon() {
    assert_eq!(level_for_delta(63), 0);
    assert_eq!(level_for_delta(64), 1);
    assert_eq!(level_for_delta(4095), 1);
    assert_eq!(level_for_delta(4096), 2);
    assert_eq!(level_for_delta(HORIZON - 1), 5);
}

#[test]
fn radix_geometry_matches_six_bit_groups() {
    assert_eq!(SLOT_BITS, 6);
    assert_eq!(SLOT_MASK, 63);
    assert_eq!(level_width(0), 1);
    assert_eq!(level_width(1), 64);
    assert_eq!(level_width(2), 4096);
    assert_eq!(HORIZON, 1 << 36);

    assert_eq!(occurrence_start(100, 1, 3), Some(192));
    assert_eq!(occurrence_start(192, 1, 3), Some(192));
    assert_eq!(occurrence_start(193, 1, 3), Some(4288));
}

#[test]
fn selected_slot_stays_selected_until_it_empties() {
    let genesis = Instant::now();
    let due = at(genesis, 1);
    let selected = WheelBoundary {
        tick: 1,
        level: 0,
        slot: 1,
    };
    let mut wheel = Wheel::new(genesis);
    let due_ids: Vec<_> = (0..3)
        .map(|value| wheel.insert(Deadline::At(due), genesis, value))
        .collect();
    let background = wheel.insert(Deadline::At(at(genesis, 100)), genesis, 99);

    // Holding the boundary across the whole drain is what lets `step_non_immediate`, `has_due`,
    // and `next_poll_at` short-circuit instead of rescanning occupancy per entry. Assert that
    // observable invariant rather than instrumenting the production implementation.
    for (expected_value, expected_id) in due_ids.into_iter().enumerate() {
        let Some(Step::Fire(found)) = wheel.step_non_immediate(due) else {
            panic!("same-slot timer must fire");
        };
        assert_eq!(found, expected_id);
        assert_eq!(wheel.draining, Some(selected));
        assert!(wheel.has_due(due));
        assert_eq!(wheel.next_poll_at(due), Some(due));
        assert_eq!(wheel.remove(found), Some(expected_value));
    }

    assert_eq!(wheel.draining, None);
    assert!(wheel.step_non_immediate(due).is_none());
    assert_eq!(wheel.remove(background), Some(99));
}

#[test]
fn cascades_at_range_start_and_never_fires_early() {
    let genesis = Instant::now();
    let mut wheel = Wheel::new(genesis);
    let id = wheel.insert(Deadline::At(at(genesis, 65)), genesis, 7);

    assert!(wheel.step(at(genesis, 63)).is_none());
    assert!(matches!(wheel.step(at(genesis, 64)), Some(Step::Examined)));
    assert!(wheel.get(id).is_some());
    assert!(wheel.step(at(genesis, 64)).is_none());
    assert!(matches!(wheel.step(at(genesis, 65)), Some(Step::Fire(found)) if found == id));
    assert_eq!(wheel.remove(id), Some(7));
}

#[test]
fn promotes_at_exact_horizon_boundary() {
    let genesis = Instant::now();
    let mut wheel = Wheel::new(genesis);
    let deadline = at(genesis, HORIZON);
    let id = wheel.insert(Deadline::At(deadline), genesis, 9);

    assert_eq!(wheel.next_poll_at(genesis), Some(at(genesis, 1)));
    assert!(matches!(wheel.step(at(genesis, 1)), Some(Step::Examined)));
    assert!(wheel.get(id).is_some());
    assert!(wheel.step(at(genesis, HORIZON - 1)).is_none());
    assert!(matches!(wheel.step(deadline), Some(Step::Fire(found)) if found == id));
}

#[test]
fn submillisecond_overflow_waits_for_representable_position() {
    let genesis = Instant::now();
    let mut wheel = Wheel::new(genesis);
    let deadline = genesis + Duration::from_micros(HORIZON * 1_000 + 500);
    let id = wheel.insert(Deadline::At(deadline), genesis, 11);

    assert_eq!(wheel.next_poll_at(genesis), Some(at(genesis, 2)));
    assert!(wheel.step(genesis + Duration::from_micros(1_500)).is_none());
    assert!(matches!(wheel.step(at(genesis, 2)), Some(Step::Examined)));
    assert!(wheel.get(id).is_some());
}

#[test]
fn cancellation_unlinks_every_location() {
    let genesis = Instant::now();
    let mut wheel = Wheel::new(genesis);
    let immediate = wheel.insert(Deadline::At(genesis), genesis, 1);
    let near = wheel.insert(Deadline::At(at(genesis, 1)), genesis, 2);
    let overflow = wheel.insert(Deadline::At(at(genesis, HORIZON)), genesis, 3);
    let never = wheel.insert(Deadline::Never, genesis, 4);

    assert_eq!(wheel.remove(immediate), Some(1));
    assert_eq!(wheel.remove(near), Some(2));
    assert_eq!(wheel.remove(overflow), Some(3));
    assert_eq!(wheel.remove(never), Some(4));
    assert_eq!(wheel.len(), 0);
    assert_eq!(wheel.next_poll_at(genesis), None);
}

fn run_randomized_operations(mode: RandomizedDrainMode) -> RandomizedCoverage {
    const MAX_TEST_MILLIS: u64 = HORIZON * 8;

    let bounded = matches!(mode, RandomizedDrainMode::Bounded);
    let genesis = Instant::now();
    let mut wheel = Wheel::new(genesis);
    let mut oracle = BTreeMap::<u64, u64>::new();
    let mut live = Vec::new();
    let mut now = 0_u64;
    let mut next_id = 0_u64;
    let mut random = 0x4d59_5df4_d0f3_3173_u64;
    let mut coverage = RandomizedCoverage::default();
    let operations = if bounded { 20_000 } else { 5_000 };

    for _ in 0..operations {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let settled = match random % 5 {
            0 if !live.is_empty() => {
                if bounded && wheel.draining.is_some() {
                    coverage.cancellations_while_draining += 1;
                }
                let index = (random as usize / 5) % live.len();
                let (wheel_id, oracle_id) = live.swap_remove(index);
                assert_eq!(wheel.remove(wheel_id), Some(oracle_id));
                assert!(oracle.remove(&oracle_id).is_some());
                false
            }
            1 | 2 => {
                let jump = match random % 8 {
                    0 => 1,
                    1 => 63,
                    2 => 64,
                    3 => 65,
                    4 => 4096,
                    5 => HORIZON - 1,
                    6 => HORIZON,
                    _ => random % 10_000,
                };
                let previous_now = now;
                let was_draining = bounded && wheel.draining.is_some();
                let candidate = now.checked_add(jump).unwrap();
                if candidate > MAX_TEST_MILLIS {
                    reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
                    now = jump;
                } else {
                    now = candidate;
                }
                if was_draining && wheel.draining.is_some() && now > previous_now {
                    coverage.resumed_after_clock_advance += 1;
                }
                let max_steps = bounded.then_some(1 + ((random >> 32) as usize & 3));
                let settled = advance_randomized_wheel(
                    &mut wheel,
                    &mut oracle,
                    &mut live,
                    genesis,
                    now,
                    max_steps,
                );
                if bounded && !settled && wheel.draining.is_some() {
                    coverage.suspended_drains += 1;
                }
                settled
            }
            _ => {
                let distance = match random % 7 {
                    0 => 0,
                    1 => 1,
                    2 => 63,
                    3 => 64,
                    4 => 4096,
                    5 => HORIZON - 1,
                    _ => HORIZON,
                };
                let mut deadline = now.checked_add(distance).unwrap();
                if deadline > MAX_TEST_MILLIS {
                    reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
                    now = 0;
                    deadline = distance;
                }
                if bounded && wheel.draining.is_some() {
                    coverage.inserts_while_draining += 1;
                }
                let insertions = if bounded {
                    1 + ((random >> 16) as usize & 3)
                } else {
                    1
                };
                for _ in 0..insertions {
                    let id = wheel.insert(
                        Deadline::At(at(genesis, deadline)),
                        at(genesis, now),
                        next_id,
                    );
                    oracle.insert(next_id, deadline);
                    live.push((id, next_id));
                    next_id += 1;
                }
                false
            }
        };
        assert_randomized_wheel_matches_oracle(&wheel, &oracle, genesis, now, settled);
    }
    reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
    coverage
}

#[test]
fn randomized_operations_match_ordered_oracle() {
    run_randomized_operations(RandomizedDrainMode::Exhaustive);
}

#[test]
fn bounded_draining_survives_clock_advances_and_mutations() {
    let coverage = run_randomized_operations(RandomizedDrainMode::Bounded);

    assert!(
        coverage.suspended_drains > 0,
        "bounded steps never suspended an active-slot drain"
    );
    assert!(
        coverage.resumed_after_clock_advance > 0,
        "a suspended drain never resumed after the clock advanced"
    );
    assert!(
        coverage.inserts_while_draining > 0,
        "the oracle never inserted while a drain was suspended"
    );
    assert!(
        coverage.cancellations_while_draining > 0,
        "the oracle never cancelled while a drain was suspended"
    );
}
