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

//! Private hierarchical timing wheel used by the explicit timer service.
//!
//! Finite deadlines are stored in six levels of intrusive slot lists. Deadlines outside the moving
//! wheel horizon stay in an ordered overflow map until they can be promoted; elapsed and
//! unrepresentable deadlines use separate lists.

use std::array;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;

use slab::Slab;

#[cfg(test)]
mod tests;

const LEVELS: usize = 6;
const SLOTS: usize = u64::BITS as usize;
const SLOT_BITS: usize = SLOTS.trailing_zeros() as usize;
const SLOT_MASK: u64 = SLOTS as u64 - 1;
const TICK_MILLIS: u64 = 1;
const TICK: Duration = Duration::from_millis(TICK_MILLIS);
const _: () = assert!(SLOTS.is_power_of_two());
const _: () = assert!(SLOT_BITS * LEVELS < u64::BITS as usize);
pub(super) const HORIZON: u64 = 1 << (SLOT_BITS * LEVELS);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Deadline {
    At(Instant),
    Never,
}

impl Deadline {
    pub(super) fn checked_add(base: Instant, duration: Duration) -> Self {
        base.checked_add(duration).map_or(Self::Never, Self::At)
    }

    pub(super) fn as_instant(self) -> Option<Instant> {
        match self {
            Self::At(instant) => Some(instant),
            Self::Never => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct List {
    head: Option<usize>,
    tail: Option<usize>,
}

/// The collection currently owning a wheel entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Location {
    Immediate,
    Wheel { level: usize, slot: usize },
    Overflow(Instant),
    Never,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WheelBoundary {
    tick: u64,
    level: usize,
    slot: usize,
}

struct Node<T> {
    deadline: Deadline,
    location: Option<Location>,
    next: Option<usize>,
    prev: Option<usize>,
    value: T,
}

/// One unit of work charged against the service's timer-entry budget.
pub(super) enum Step {
    /// One entry was moved between wheel tiers without becoming ready.
    Examined,
    /// The identified entry is ready for the service to remove and fire.
    Fire(usize),
}

/// Slab-backed timer entries partitioned by deadline range.
pub(super) struct Wheel<T> {
    // Once selected, this wheel boundary stays minimal until its slot is empty. `position` must
    // not advance while it is set. Keeping it avoids a full occupancy rescan for every entry
    // in the same slot while entry-level turn budgeting remains intact.
    draining: Option<WheelBoundary>,
    entries: Slab<Node<T>>,
    genesis: Instant,
    immediate: List,
    never: List,
    // Bit `slot` is set when that level's intrusive slot list is non-empty.
    occupancy: [u64; LEVELS],
    overflow: BTreeMap<Instant, List>,
    position: u64,
    slots: [[List; SLOTS]; LEVELS],
}

impl<T> Wheel<T> {
    pub(super) fn new(genesis: Instant) -> Self {
        Self {
            draining: None,
            entries: Slab::new(),
            genesis,
            immediate: List::default(),
            never: List::default(),
            occupancy: [0; LEVELS],
            overflow: BTreeMap::new(),
            position: 0,
            slots: array::from_fn(|_| array::from_fn(|_| List::default())),
        }
    }

    pub(super) fn insert(&mut self, deadline: Deadline, now: Instant, value: T) -> usize {
        let location = self.classify(deadline, now);
        let id = self.entries.insert(Node {
            deadline,
            location: None,
            next: None,
            prev: None,
            value,
        });
        self.link_back(id, location);
        id
    }

    pub(super) fn get(&self, id: usize) -> Option<&T> {
        self.entries.get(id).map(|node| &node.value)
    }

    pub(super) fn remove(&mut self, id: usize) -> Option<T> {
        if !self.entries.contains(id) {
            return None;
        }
        self.unlink(id);
        Some(self.entries.remove(id).value)
    }

    pub(super) fn step_immediate(&self) -> Option<Step> {
        self.immediate.head.map(Step::Fire)
    }

    #[cfg(test)]
    fn step(&mut self, now: Instant) -> Option<Step> {
        self.step_immediate()
            .or_else(|| self.step_non_immediate(now))
    }

    pub(super) fn step_non_immediate(&mut self, now: Instant) -> Option<Step> {
        if self.draining.is_some() {
            return Some(self.step_draining(now));
        }

        let now_tick = self.floor_tick(now);
        if let Some(boundary) = self.next_wheel_boundary() {
            if boundary.tick <= now_tick {
                self.position = boundary.tick;
                self.draining = Some(boundary);
                return Some(self.step_draining(now));
            }
        }

        if let Some((&deadline, list)) = self.overflow.first_key_value() {
            if self.overflow_ready(deadline, now) {
                let id = list.head.expect("overflow bucket must be non-empty");
                if deadline <= now {
                    return Some(Step::Fire(id));
                }

                self.position = now_tick;
                self.unlink(id);
                let location = self.classify(self.entries[id].deadline, now);
                self.link_back(id, location);
                return Some(Step::Examined);
            }
        }

        self.position = now_tick;
        None
    }

    fn step_draining(&mut self, now: Instant) -> Step {
        let boundary = self.draining.expect("draining boundary must be selected");
        debug_assert_eq!(self.position, boundary.tick);
        debug_assert!(boundary.tick <= self.floor_tick(now));

        let location = Location::Wheel {
            level: boundary.level,
            slot: boundary.slot,
        };
        let id = self
            .list(location)
            .head
            .expect("draining slot must be non-empty");
        if boundary.level == 0 || self.deadline_elapsed(id, now) {
            return Step::Fire(id);
        }

        self.unlink(id);
        let location = self.classify(self.entries[id].deadline, now);
        self.link_back(id, location);
        Step::Examined
    }

    pub(super) fn has_due(&self, now: Instant) -> bool {
        if !self.immediate_is_empty() || self.draining.is_some() {
            return true;
        }
        let now_tick = self.floor_tick(now);
        if self
            .next_wheel_boundary()
            .is_some_and(|boundary| boundary.tick <= now_tick)
        {
            return true;
        }
        self.overflow
            .first_key_value()
            .is_some_and(|(&deadline, _)| self.overflow_ready(deadline, now))
    }

    pub(super) fn settle(&mut self, now: Instant) {
        debug_assert!(
            self.draining.is_none(),
            "settle cannot move position mid-drain"
        );
        debug_assert!(!self.has_due(now));
        self.position = self.floor_tick(now);
    }

    pub(super) fn next_poll_at(&self, now: Instant) -> Option<Instant> {
        if !self.immediate_is_empty() || self.draining.is_some() {
            return Some(now);
        }

        let now_tick = self.floor_tick(now);
        let wheel_boundary = self.next_wheel_boundary();
        let overflow_deadline = self
            .overflow
            .first_key_value()
            .map(|(&deadline, _)| deadline);
        if wheel_boundary.is_some_and(|boundary| boundary.tick <= now_tick)
            || overflow_deadline.is_some_and(|deadline| self.overflow_ready(deadline, now))
        {
            return Some(now);
        }

        let wheel = wheel_boundary.and_then(|boundary| self.instant_for_tick(boundary.tick));
        let overflow = overflow_deadline.map(|deadline| self.promotion_instant(deadline));
        match (wheel, overflow) {
            (Some(wheel), Some(overflow)) => Some(wheel.min(overflow)),
            (Some(wheel), None) => Some(wheel),
            (None, Some(overflow)) => Some(overflow),
            (None, None) => None,
        }
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.draining = None;
        self.immediate = List::default();
        self.never = List::default();
        self.occupancy = [0; LEVELS];
        self.overflow.clear();
        self.slots = array::from_fn(|_| array::from_fn(|_| List::default()));
        self.entries.drain().map(|node| node.value)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    fn classify(&self, deadline: Deadline, now: Instant) -> Location {
        let Deadline::At(deadline) = deadline else {
            return Location::Never;
        };
        if deadline <= now {
            return Location::Immediate;
        }

        let Some(tick) = self.ceil_tick(deadline) else {
            return Location::Overflow(deadline);
        };
        let Some(delta) = tick.checked_sub(self.position) else {
            return Location::Immediate;
        };
        if delta >= HORIZON {
            return Location::Overflow(deadline);
        }

        let level = level_for_delta(delta);
        let slot = ((tick >> level_shift(level)) & SLOT_MASK) as usize;
        Location::Wheel { level, slot }
    }

    fn deadline_elapsed(&self, id: usize, now: Instant) -> bool {
        match self.entries[id].deadline {
            Deadline::At(deadline) => deadline <= now,
            Deadline::Never => false,
        }
    }

    fn floor_tick(&self, instant: Instant) -> u64 {
        if instant <= self.genesis {
            return 0;
        }
        let elapsed_millis = instant.duration_since(self.genesis).as_millis();
        let ticks = elapsed_millis / u128::from(TICK_MILLIS);
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }

    fn ceil_tick(&self, instant: Instant) -> Option<u64> {
        if instant <= self.genesis {
            return Some(0);
        }
        let nanos = instant.duration_since(self.genesis).as_nanos();
        let tick_nanos = TICK.as_nanos();
        let ticks = nanos.checked_add(tick_nanos - 1)? / tick_nanos;
        u64::try_from(ticks).ok()
    }

    fn instant_for_tick(&self, tick: u64) -> Option<Instant> {
        tick.checked_mul(TICK_MILLIS)
            .and_then(|millis| self.genesis.checked_add(Duration::from_millis(millis)))
    }

    fn promotion_instant(&self, deadline: Instant) -> Instant {
        let Some(tick) = self.ceil_tick(deadline) else {
            // This deadline is outside the wheel's u64 tick domain. Keep it in overflow until the
            // deadline itself rather than repeatedly attempting an impossible promotion.
            return deadline;
        };
        tick.checked_sub(HORIZON - 1)
            .and_then(|tick| self.instant_for_tick(tick))
            .unwrap_or(self.genesis)
    }

    fn overflow_ready(&self, deadline: Instant, now: Instant) -> bool {
        self.promotion_instant(deadline) <= now
    }

    fn immediate_is_empty(&self) -> bool {
        self.immediate.head.is_none()
    }

    fn next_wheel_boundary(&self) -> Option<WheelBoundary> {
        let mut result = None;
        for (level, &occupancy) in self.occupancy.iter().enumerate() {
            let mut bits = occupancy;
            while bits != 0 {
                let slot = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let Some(boundary) = occurrence_start(self.position, level, slot) else {
                    debug_assert!(
                        false,
                        "occupied slot {slot} at level {level} overflowed the tick domain"
                    );
                    continue;
                };
                if result.is_none_or(|current: WheelBoundary| boundary < current.tick) {
                    result = Some(WheelBoundary {
                        tick: boundary,
                        level,
                        slot,
                    });
                }
            }
        }
        result
    }

    fn list(&self, location: Location) -> List {
        match location {
            Location::Immediate => self.immediate,
            Location::Wheel { level, slot } => self.slots[level][slot],
            Location::Overflow(deadline) => self.overflow[&deadline],
            Location::Never => self.never,
        }
    }

    fn set_list(&mut self, location: Location, list: List) {
        match location {
            Location::Immediate => self.immediate = list,
            Location::Wheel { level, slot } => {
                self.slots[level][slot] = list;
                if list.head.is_some() {
                    self.occupancy[level] |= 1 << slot;
                } else {
                    self.occupancy[level] &= !(1 << slot);
                    if self
                        .draining
                        .is_some_and(|boundary| boundary.level == level && boundary.slot == slot)
                    {
                        self.draining = None;
                    }
                }
            }
            Location::Overflow(deadline) => {
                if list.head.is_some() {
                    self.overflow.insert(deadline, list);
                } else {
                    self.overflow.remove(&deadline);
                }
            }
            Location::Never => self.never = list,
        }
    }

    fn link_back(&mut self, id: usize, location: Location) {
        debug_assert!(self.entries[id].location.is_none());
        let mut list = match location {
            Location::Overflow(deadline) => {
                self.overflow.get(&deadline).copied().unwrap_or_default()
            }
            _ => self.list(location),
        };
        if let Some(tail) = list.tail {
            self.entries[tail].next = Some(id);
        } else {
            list.head = Some(id);
        }
        self.entries[id].location = Some(location);
        self.entries[id].prev = list.tail;
        self.entries[id].next = None;
        list.tail = Some(id);
        self.set_list(location, list);
    }

    fn unlink(&mut self, id: usize) {
        let node = &self.entries[id];
        let location = node.location.expect("linked node must have a location");
        let prev = node.prev;
        let next = node.next;
        let mut list = self.list(location);

        if let Some(prev) = prev {
            self.entries[prev].next = next;
        } else {
            list.head = next;
        }
        if let Some(next) = next {
            self.entries[next].prev = prev;
        } else {
            list.tail = prev;
        }

        let node = &mut self.entries[id];
        node.location = None;
        node.prev = None;
        node.next = None;
        self.set_list(location, list);
    }
}

fn level_shift(level: usize) -> usize {
    debug_assert!(level < LEVELS);
    SLOT_BITS * level
}

fn level_width(level: usize) -> u64 {
    1u64 << level_shift(level)
}

fn level_for_delta(delta: u64) -> usize {
    for level in 0..LEVELS - 1 {
        if delta < level_width(level + 1) {
            return level;
        }
    }
    LEVELS - 1
}

fn occurrence_start(position: u64, level: usize, slot: usize) -> Option<u64> {
    debug_assert!(slot < SLOTS);
    let shift = level_shift(level);
    let cycle = 1u64 << (shift + SLOT_BITS);
    let base = position & !(cycle - 1);
    let mut candidate = base.checked_add((slot as u64) << shift)?;
    if candidate < position {
        candidate = candidate.checked_add(cycle)?;
    }
    Some(candidate)
}
