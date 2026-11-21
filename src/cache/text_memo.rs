//! A per-thread memo of what a statement's text alone decides.
//!
//! An extended-protocol read sends the same statement text at every
//! execution (its values travel in the `Bind`), and what the query cache
//! derives from the text (its normalized form and tables, whether it is a
//! read the cache may answer) does not depend on the values. A simple
//! query's text carries its literals and seldom repeats, so only the
//! extended path asks here.
//!
//! The memo is per thread: a lookup takes no lock and shares no cache line
//! with another worker. Each value is tagged with its owner (the cache whose
//! configuration made it, [`next_owner`]), so two caches in one process (a
//! test binary) never read each other's. It holds at most [`MAX_TEXTS`]
//! texts of at most [`MAX_TEXT_BYTES`] bytes and is emptied when full; a
//! longer text is derived at every call, as before.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Texts one thread's memo holds before it is emptied.
pub(crate) const MAX_TEXTS: usize = 256;
/// The longest text a memo keeps.
pub(crate) const MAX_TEXT_BYTES: usize = 2048;

/// A new owner tag: unique in the process, never reused.
pub(crate) fn next_owner() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Values of type `V` by statement text, each under the owner that made it.
#[derive(Debug)]
pub(crate) struct TextMemo<V> {
    values: HashMap<Box<str>, (u64, V)>,
}

impl<V> Default for TextMemo<V> {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
}

impl<V: Clone> TextMemo<V> {
    /// `owner`'s value for `text`, if it was made before.
    pub(crate) fn get(&self, owner: u64, text: &str) -> Option<V> {
        match self.values.get(text) {
            Some((made_by, value)) if *made_by == owner => Some(value.clone()),
            _ => None,
        }
    }

    /// Keep `value` as `owner`'s for `text` (replacing another owner's).
    pub(crate) fn put(&mut self, owner: u64, text: &str, value: V) {
        if text.len() > MAX_TEXT_BYTES {
            return;
        }
        if self.values.len() >= MAX_TEXTS && !self.values.contains_key(text) {
            self.values.clear();
        }
        self.values.insert(text.into(), (owner, value));
    }
}

/// `owner`'s value for `text` from the thread's `memo`, made by `make` (and
/// kept) when it is not there. `make` runs outside the memo's borrow.
pub(crate) fn of<V: Clone + 'static>(
    memo: &'static std::thread::LocalKey<std::cell::RefCell<TextMemo<V>>>,
    owner: u64,
    text: &str,
    make: impl FnOnce() -> V,
) -> V {
    if text.len() > MAX_TEXT_BYTES {
        return make();
    }
    if let Some(value) = memo.with(|m| m.borrow().get(owner, text)) {
        return value;
    }
    let value = make();
    memo.with(|m| m.borrow_mut().put(owner, text, value.clone()));
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    thread_local! {
        static LENGTHS: RefCell<TextMemo<usize>> = RefCell::default();
    }

    #[test]
    fn a_text_is_derived_once_per_owner() {
        let (a, b) = (next_owner(), next_owner());
        assert_ne!(a, b);
        let made = Cell::new(0);
        let length = |owner, text: &str| {
            of(&LENGTHS, owner, text, || {
                made.set(made.get() + 1);
                text.len()
            })
        };
        assert_eq!(length(a, "SELECT 1"), 8);
        assert_eq!(length(a, "SELECT 1"), 8);
        assert_eq!(made.get(), 1);
        // Another owner's value is never read; it replaces the first's.
        assert_eq!(length(b, "SELECT 1"), 8);
        assert_eq!(made.get(), 2);
        assert_eq!(length(a, "SELECT 1"), 8);
        assert_eq!(made.get(), 3);
    }

    #[test]
    fn the_memo_is_bounded() {
        let owner = next_owner();
        let made = Cell::new(0);
        let derive = |text: &str| {
            of(&LENGTHS, owner, text, || {
                made.set(made.get() + 1);
                text.len()
            })
        };
        // A text too long to keep is derived at every call.
        let long = "x".repeat(MAX_TEXT_BYTES + 1);
        derive(&long);
        derive(&long);
        assert_eq!(made.get(), 2);
        // A full memo starts over.
        for i in 0..=MAX_TEXTS {
            derive(&format!("SELECT {i}"));
        }
        LENGTHS.with(|m| assert!(m.borrow().values.len() <= MAX_TEXTS));
        let before = made.get();
        derive(&format!("SELECT {MAX_TEXTS}"));
        assert_eq!(made.get(), before, "the newest text is kept");
        derive("SELECT 0");
        assert_eq!(made.get(), before + 1, "the oldest went with the rest");
    }
}
