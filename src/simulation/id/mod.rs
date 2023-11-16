use std::cell::RefCell;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;

use crate::simulation::id::id_store::IdStore;
use crate::simulation::id::id_store::UntypedId;
use crate::simulation::id::serializable_type::StableTypeId;

// keep this private, as we don't want to leak how we cache ids.
mod id_store;
pub mod serializable_type;

#[derive(Debug)]
pub struct Id<T: StableTypeId> {
    _type_marker: PhantomData<T>,
    id: Rc<UntypedId>,
}

impl<T: StableTypeId + 'static> Id<T> {
    fn new(untyped_id: Rc<UntypedId>) -> Self {
        Self {
            _type_marker: PhantomData,
            id: untyped_id,
        }
    }

    /// Creates an id which is not attached to any id storage. This method is intended for test
    /// cases. The intended way of creating ids is to use IdStore::create_id(external);
    #[cfg(test)]
    pub(crate) fn new_internal(internal: u64) -> Self {
        let untyped_id = UntypedId::new(internal, String::from(""));
        Self::new(Rc::new(untyped_id))
    }

    pub fn internal(&self) -> u64 {
        self.id.internal
    }

    pub fn external(&self) -> &str {
        &self.id.external
    }

    pub fn create(id: &str) -> Self {
        ID_STORE.with(|store| store.borrow_mut().create_id(id))
    }

    pub fn get(internal: u64) -> Self {
        ID_STORE.with(|store| store.borrow().get(internal))
    }

    pub fn get_from_ext(external: &str) -> Self {
        ID_STORE.with(|store| store.borrow().get_from_ext(external))
    }
}

pub fn store_to_wire_format() -> Vec<u8> {
    ID_STORE.with(|store| store.borrow().to_wire_format())
}

pub fn store_to_file(file_path: &Path) {
    ID_STORE.with(|store| store.borrow().to_file(file_path))
}

pub fn load_from_wire_format(bytes: Vec<u8>) {
    ID_STORE.with(|store| store.borrow_mut().load_from_wire_format(bytes))
}

pub fn load_from_file(file_path: &Path) {
    ID_STORE.with(|store| store.borrow_mut().load_from_file(file_path))
}

/// Mark Id as enabled for the nohash_hasher::NoHashHasher t
impl<T: StableTypeId> nohash_hasher::IsEnabled for Id<T> {}
impl<T: StableTypeId> nohash_hasher::IsEnabled for &Id<T> {}

/// Implement PartialEq, Eq, PartialOrd, Ord, so that Ids can be used in HashMaps and Ordered collections
/// all four methods rely on the internal id.
impl<T: StableTypeId + 'static> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.internal().eq(&other.internal())
    }
}

impl<T: StableTypeId + 'static> Eq for Id<T> {}

impl<T: StableTypeId + 'static> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // use write u64 directly, so that we can use NoHashHasher with ids
        state.write_u64(self.internal());
    }
}

impl<T: StableTypeId + 'static> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal().cmp(&other.internal())
    }
}

impl<T: StableTypeId + 'static> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.internal().partial_cmp(&other.internal())
    }
}

/// This creates a new struct with a cloned Rc pointer
impl<T: StableTypeId> Clone for Id<T> {
    fn clone(&self) -> Self {
        Self {
            _type_marker: PhantomData,
            id: self.id.clone(),
        }
    }
}

thread_local! {static ID_STORE: RefCell<IdStore<'static>> = RefCell::new(IdStore::new())}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::simulation::id::{Id, UntypedId};

    #[test]
    fn test_id_eq() {
        let id: Id<()> = Id::new(Rc::new(UntypedId::new(1, String::from("external-id"))));
        assert_eq!(id, id.clone());

        let equal = Id::new(Rc::new(UntypedId::new(
            1,
            String::from("other-external-value-which-should-be-ignored"),
        )));
        assert_eq!(id, equal);

        let unequal = Id::new(Rc::new(UntypedId::new(2, String::from("external-id"))));
        assert_ne!(id, unequal)
    }

    #[test]
    fn create_id() {
        let external = String::from("external-id");

        let id: Id<()> = Id::create(&external);
        assert_eq!(external, id.external());
        assert_eq!(0, id.internal());
    }

    #[test]
    fn create_id_duplicate() {
        let external = String::from("external-id");

        let id: Id<()> = Id::create(&external);
        let duplicate: Id<()> = Id::create(&external);

        assert_eq!(id, duplicate);
    }

    #[test]
    fn create_id_multiple_types() {
        let external = String::from("external-id");

        let int_id: Id<u32> = Id::create(&external);
        assert_eq!(external, int_id.external());
        assert_eq!(0, int_id.internal());

        let float_id: Id<f32> = Id::create(&external);
        assert_eq!(external, float_id.external());
        assert_eq!(0, float_id.internal());
    }

    #[test]
    fn get_id() {
        let external_1 = String::from("id-1");
        let external_2 = String::from("id-2");
        let id_1: Id<()> = Id::create(&external_1);
        let id_2: Id<()> = Id::create(&external_2);

        let fetched_1: Id<()> = Id::get(id_1.internal());
        let fetched_2: Id<()> = Id::get(id_2.internal());
        assert_eq!(fetched_1.external(), external_1);
        assert_eq!(fetched_2.external(), external_2);
    }

    #[test]
    fn id_store_get_ext() {
        let external_1 = String::from("id-1");
        let external_2 = String::from("id-2");
        let id_1: Id<()> = Id::create(&external_1);
        let id_2: Id<()> = Id::create(&external_2);

        let fetched_1: Id<()> = Id::get_from_ext(id_1.external());
        let fetched_2: Id<()> = Id::get_from_ext(id_2.external());
        assert_eq!(fetched_1.external(), external_1);
        assert_eq!(fetched_2.external(), external_2);
    }
}
