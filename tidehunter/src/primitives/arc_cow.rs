use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

/// Optimizes the case where we mostly have owned reference, but rarely need a temporary shared clone.
/// See the tests for details.
///
/// Comparing to Arc::make_mut this allows accessing owned reference without any atomic bit flips.
pub enum ArcCow<T> {
    Owned(T),
    Shared(Arc<T>),
}

impl<T: Clone + Default> ArcCow<T> {
    pub fn new_owned(t: T) -> Self {
        Self::Owned(t)
    }

    pub fn clone_shared(&mut self) -> Arc<T> {
        let owned = match self {
            Self::Shared(shared) => return shared.clone(),
            Self::Owned(owned) => owned,
        };
        let mut shared = Default::default();
        mem::swap(owned, &mut shared);
        let shared = Arc::new(shared);
        *self = Self::Shared(shared.clone());
        shared
    }

    #[allow(dead_code)]
    pub fn same_shared(&self, other: &Arc<T>) -> bool {
        match self {
            ArcCow::Owned(_) => false,
            ArcCow::Shared(shared) => Arc::ptr_eq(shared, other),
        }
    }

    pub fn make_mut(&mut self) -> &mut T {
        let shared = match self {
            Self::Owned(owned) => return owned,
            Self::Shared(shared) => shared,
        };
        let mut owned = Arc::new(Default::default());
        mem::swap(shared, &mut owned);
        let owned = Arc::try_unwrap(owned).unwrap_or_else(|shared| T::clone(&shared));
        *self = Self::Owned(owned);
        let Self::Owned(owned) = self else {
            unreachable!()
        };
        owned
        // Alternative that does not require T: Default
        // if let Self::Owned(owned) = self {
        //     return owned;
        // }
        // let mut owned = Self::default();
        // let Self::Shared(shared) = self else { unreachable!() };
        // let Self::Owned(owned_mut) = &mut owned else { unreachable!() };
        // let shared_mut = Arc::make_mut(shared);
        // mem::swap(owned_mut, shared_mut);
        // *self = owned;
        // let Self::Owned(owned_mut) = self else { unreachable!() };
        // owned_mut
    }

    #[allow(dead_code)]
    pub fn borrow(&self) -> &T {
        match self {
            ArcCow::Owned(owned) => owned,
            ArcCow::Shared(shared) => shared,
        }
    }
}

impl<T: Default> Default for ArcCow<T> {
    fn default() -> Self {
        Self::Owned(Default::default())
    }
}

impl<T: Serialize> Serialize for ArcCow<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ArcCow::Owned(o) => o.serialize(serializer),
            ArcCow::Shared(s) => s.serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ArcCow<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::Owned(T::deserialize(deserializer)?))
    }
}

impl<T> Deref for ArcCow<T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            ArcCow::Owned(o) => o,
            ArcCow::Shared(s) => s.deref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn arc_cow_test() {
        let mut arc_cow = ArcCow::<CloneCounter>::default();
        arc_cow.make_mut().1 = 15;
        assert_eq!(arc_cow.get(), (0, 15));
        arc_cow.make_mut();
        assert_eq!(arc_cow.get(), (0, 15));
        arc_cow.clone_shared(); // immediately drop the clone
        assert_eq!(arc_cow.get(), (0, 15));
        arc_cow.make_mut(); // still no clone, quietly migrate from shared to owned
        assert_eq!(arc_cow.get(), (0, 15));
        let cloned = arc_cow.clone_shared(); // keep the clone
        arc_cow.make_mut(); // make_mut now have to clone the value
        assert_eq!(arc_cow.get(), (1, 15));
        assert_eq!(cloned.get(), (1, 15));
        let clone_shared2 = arc_cow.clone_shared();
        // 'Cloned' is not getting cloned above, only the shared pointer
        assert_eq!(cloned.get(), (1, 15));
        assert_eq!(clone_shared2.get(), (1, 15));
    }

    #[derive(Default)]
    struct CloneCounter(Rc<RefCell<usize>>, usize);

    impl Clone for CloneCounter {
        fn clone(&self) -> Self {
            *self.0.borrow_mut() += 1;
            Self(self.0.clone(), self.1)
        }
    }

    impl CloneCounter {
        pub fn get(&self) -> (usize, usize) {
            (*self.0.borrow(), self.1)
        }
    }
}
