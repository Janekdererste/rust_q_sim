use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::path::Path;
use std::rc::Rc;

use ahash::{AHashMap, RandomState};
use bytes::{Buf, BufMut};
use nohash_hasher::IntMap;
use prost::encoding::{DecodeContext, WireType};
use prost::Message;
use tracing::info;

use crate::simulation::id::serializable_type::StableTypeId;
use crate::simulation::id::Id;
use crate::simulation::io::proto::MessageIter;
use crate::simulation::wire_types::ids::ids_with_type::Data;
use crate::simulation::wire_types::ids::IdsWithType;

#[derive(Clone, Copy)]
#[allow(dead_code)] // allow dead code, because we never construct None. I still want to have it as option here.
enum IdCompression {
    LZ4,
    None,
}

fn serialize_to_file(store: &IdStore, file_path: &Path, compression: IdCompression) {
    info!("Starting writing IdStore to file {file_path:?}");
    // Create the file and all necessary directories
    let prefix = file_path.parent().unwrap();
    fs::create_dir_all(prefix).unwrap();
    let file = File::create(file_path).unwrap();

    let mut file_writer = BufWriter::new(file);
    serialize(store, &mut file_writer, compression);
    info!("Finished writing IdStore to file {file_path:?}");
}

fn serialize<W: Write>(store: &IdStore, writer: &mut W, compression: IdCompression) {
    for (type_id, ids) in &store.ids {
        let data = serialize_ids(ids, compression);
        let ids = IdsWithType {
            type_id: *type_id,
            data: Some(data),
        };
        let encoded_typed_ids = ids.encode_length_delimited_to_vec();
        writer
            .write_all(&encoded_typed_ids)
            .expect("Failed to write encoded type ids to writer.");
    }
    writer
        .flush()
        .expect("Failed to flush writer after serializing id store");
}

fn deserialize_from_file(store: &mut IdStore, file_path: &Path) {
    info!("Starting to load IdStore from file {file_path:?}");
    let file = File::open(file_path).unwrap();
    let mut file_reader = BufReader::new(file);
    deserialize(store, &mut file_reader);
}

/// This method takes a BufReader instance as we are relying on 'seek_relative' which is not part of
/// the Read trait. I think it is ok, to let callees wrap their bytes into a BufReader.
fn deserialize<R: Read + Seek>(store: &mut IdStore, reader: R) {
    info!("Starting to de-serialize Id store.");
    let delim_reader: MessageIter<IdsWithType, R> = MessageIter::new(reader);
    for message in delim_reader {
        let ids = deserialize_ids(&message);
        store.replace_ids(&ids, message.type_id);
    }

    info!("Finished de-serializing id store.");
}

fn serialize_ids(ids: &Vec<Rc<UntypedId>>, mode: IdCompression) -> Data {
    match mode {
        IdCompression::LZ4 => serialize_ids_compressed(ids),
        IdCompression::None => serialize_ids_uncompressed(ids),
    }
}

fn serialize_ids_uncompressed(ids: &Vec<Rc<UntypedId>>) -> Data {
    let mut writer = BufWriter::new(Vec::new());
    encode_ids(ids, &mut writer);

    let bytes = writer
        .into_inner()
        .expect("Failed to transform writer into_inner as Vec<u8>");
    Data::Raw(bytes)
}

fn serialize_ids_compressed(ids: &Vec<Rc<UntypedId>>) -> Data {
    let writer = BufWriter::new(Vec::new());
    let mut compressor = lz4_flex::frame::FrameEncoder::new(writer);

    encode_ids(ids, &mut compressor);

    let bytes = compressor
        .into_inner()
        .into_inner()
        .expect("Failed to transform writer into_inner as Vec<u8>");
    Data::Lz4Data(bytes)
}

fn encode_ids<W: Write>(ids: &Vec<Rc<UntypedId>>, writer: &mut W) {
    let mut id_buffer = Vec::new();

    for id in ids {
        prost::encoding::encode_varint(id.external.len() as u64, &mut id_buffer);
        id_buffer.put_slice(id.external.as_bytes());
        writer
            .write_all(&id_buffer)
            .expect("Failed to write encoded String.");
        id_buffer.clear();
    }
    writer.flush().expect("Failed to flush writer.");
}

fn deserialize_ids(ids: &IdsWithType) -> Vec<String> {
    if let Some(bytes) = &ids.data {
        match bytes {
            Data::Raw(raw_bytes) => deserialize_ids_uncompressed(raw_bytes),
            Data::Lz4Data(lz4_bytes) => deserialize_ids_compressed(lz4_bytes),
        }
    } else {
        Vec::new()
    }
}

fn deserialize_ids_compressed(bytes: &[u8]) -> Vec<String> {
    let compressed_reader = Cursor::new(bytes);
    let mut decompressor = lz4_flex::frame::FrameDecoder::new(compressed_reader);

    let mut uncompressed_bytes = Vec::new();
    decompressor
        .read_to_end(&mut uncompressed_bytes)
        .expect("Failed to de-compress bytes");

    let mut uncompressed_reader = Cursor::new(uncompressed_bytes);
    decode_ids(&mut uncompressed_reader)
}

fn deserialize_ids_uncompressed(bytes: &[u8]) -> Vec<String> {
    let mut cursor = Cursor::new(bytes);
    decode_ids(&mut cursor)
}

fn decode_ids<B: Buf>(buffer: &mut B) -> Vec<String> {
    let mut result = Vec::new();

    while buffer.has_remaining() {
        let mut external_id = String::new();
        prost::encoding::string::merge(
            WireType::LengthDelimited,
            &mut external_id,
            buffer,
            DecodeContext::default(),
        )
        .expect("Error decoding String");

        result.push(external_id);
    }
    result
}

#[derive(Debug)]
pub struct UntypedId {
    pub(crate) internal: u64,
    pub(crate) external: String,
}

impl UntypedId {
    pub(crate) fn new(internal: u64, external: String) -> Self {
        Self { internal, external }
    }
}

#[derive(Debug)]
pub struct IdStore<'ext> {
    ids: IntMap<u64, Vec<Rc<UntypedId>>>,
    mapping: IntMap<u64, AHashMap<&'ext str, u64>>,
}

/// Cache for ids. All methods are public, so that they can be used from mod.rs. The module doesn't
/// export this module, so that everything is kept package private
impl<'ext> IdStore<'ext> {
    pub fn new() -> Self {
        Self {
            ids: IntMap::default(),
            mapping: IntMap::default(),
        }
    }

    fn create_id_with_type_id(&mut self, id: &str, type_id: u64) -> Rc<UntypedId> {
        let type_mapping = self
            .mapping
            .entry(type_id)
            .or_insert_with(|| AHashMap::with_hasher(RandomState::with_seed(42)));

        if type_mapping.contains_key(id) {
            let internal = type_mapping.get(id).unwrap();
            return self
                .ids
                .get(&type_id)
                .unwrap()
                .get(*internal as usize)
                .unwrap()
                .clone();
        }

        let type_ids = self.ids.entry(type_id).or_insert_with(Vec::default);
        let next_internal = type_ids.len() as u64;
        let next_id = Rc::new(UntypedId::new(next_internal, String::from(id)));
        type_ids.push(next_id.clone());

        let ptr_external: *const String = &next_id.external;
        /*
        # Safety:

        As the external Strings are allocated by the ids, which keep a pointer to that allocation
        The allocated string will not move as long as the id exists. This means as long as the id
        is in the map, the ref to the external String which is used as a key in the map will be valid
         */
        let external_ref = unsafe { ptr_external.as_ref() }.unwrap();
        type_mapping.insert(external_ref, next_id.internal);

        next_id
    }

    fn replace_ids(&mut self, ids: &Vec<String>, type_id: u64) {
        if let Some(type_mapping) = self.mapping.get_mut(&type_id) {
            type_mapping.clear();
        }
        if let Some(type_ids) = self.ids.get_mut(&type_id) {
            type_ids.clear();
        }

        for external_id in ids {
            self.create_id_with_type_id(external_id, type_id);
        }
    }

    pub(crate) fn create_id<T: StableTypeId + 'static>(&mut self, id: &str) -> Id<T> {
        let type_id = T::stable_type_id();
        Id::new(self.create_id_with_type_id(id, type_id))
    }

    pub(crate) fn get<T: StableTypeId + 'static>(&self, internal: u64) -> Id<T> {
        let type_id = T::stable_type_id();
        let type_ids = self.ids.get(&type_id).unwrap_or_else(|| {
            panic!("No ids for type {type_id:?}. Use Id::create::<T>(...) to create ids")
        });

        let untyped_id = type_ids
            .get(internal as usize)
            .unwrap_or_else(|| panic!("No id found for internal {internal}"))
            .clone();
        Id::new(untyped_id)
    }

    pub(crate) fn get_from_ext<T: StableTypeId + 'static>(&self, external: &str) -> Id<T> {
        let type_id = T::stable_type_id();
        let type_mapping = self.mapping.get(&type_id).unwrap_or_else(|| {
            panic!("No ids for type {type_id:?}. Use Id::create::<T>(...) to create ids")
        });

        let index = type_mapping.get(external).unwrap_or_else(|| {
            panic!("Could not find id for external id: {external}");
        });

        self.get(*index)
    }

    pub(crate) fn to_file(&self, file_path: &Path) {
        serialize_to_file(self, file_path, IdCompression::LZ4);
    }

    pub(crate) fn load_from_file(&mut self, file_path: &Path) {
        deserialize_from_file(self, file_path);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, BufWriter, Cursor};
    use std::ops::Sub;
    use std::path::PathBuf;
    use std::time::Instant;

    use crate::simulation::config::PartitionMethod;
    use crate::simulation::id::id_store::{
        deserialize, deserialize_from_file, serialize, serialize_to_file, IdCompression, IdStore,
    };
    use crate::simulation::logging::init_std_out_logging;
    use crate::simulation::network::global_network::{Link, Network, Node};
    use crate::simulation::population::population::Population;
    use crate::simulation::vehicles::garage::Garage;
    use crate::simulation::wire_types::messages::Vehicle;
    use crate::simulation::wire_types::population::Person;
    use crate::simulation::wire_types::vehicles::VehicleType;
    use crate::test_utils::create_folders;

    #[test]
    fn write_read_ids_store() {
        let folder = create_folders(PathBuf::from(
            "./test_output/simulation/id/id_store/write_read_ids_store/",
        ));
        let file = folder.join("ids.pbf");
        let mut store = IdStore::new();
        store.create_id::<()>("test-1");
        store.create_id::<()>("test-2");
        store.create_id::<String>("string-id");

        serialize_to_file(&store, &file, IdCompression::LZ4);
        let mut result = IdStore::new();
        deserialize_from_file(&mut result, &file);

        println!("{result:?}");

        assert_eq!(
            store.get_from_ext::<()>("test-1"),
            result.get_from_ext::<()>("test-1")
        );
        assert_eq!(
            store.get_from_ext::<String>("string-id"),
            result.get_from_ext::<String>("string-id")
        );
    }

    #[test]
    fn write_read_ids_store_uncompressed() {
        let folder = create_folders(PathBuf::from(
            "./test_output/simulation/id/id_store/write_read_ids_store_uncompressed/",
        ));
        let file = folder.join("ids.pbf");
        let mut store = IdStore::new();
        store.create_id::<()>("test-1");
        store.create_id::<()>("test-2");
        store.create_id::<String>("string-id");

        serialize_to_file(&store, &file, IdCompression::None);
        let mut result = IdStore::new();
        deserialize_from_file(&mut result, &file);

        println!("{result:?}");

        assert_eq!(
            store.get_from_ext::<()>("test-1"),
            result.get_from_ext::<()>("test-1")
        );
        assert_eq!(
            store.get_from_ext::<String>("string-id"),
            result.get_from_ext::<String>("string-id")
        );
    }

    #[test]
    fn test_serialize_ids() {
        let mut store = IdStore::new();
        store.create_id::<()>("test-1");
        store.create_id::<()>("test-2");
        store.create_id::<String>("string-id");

        let mut serialized_bytes = Vec::new();
        let mut writer = BufWriter::new(serialized_bytes);
        serialize(&store, &mut writer, IdCompression::LZ4);

        serialized_bytes = writer
            .into_inner()
            .expect("Failed to transform into inner.");

        println!("{serialized_bytes:?}");

        let mut vec_reader = BufReader::new(Cursor::new(serialized_bytes));
        let mut result = IdStore::new();
        deserialize(&mut result, &mut vec_reader);

        println!("{result:?}");

        assert_eq!(
            store.get_from_ext::<()>("test-1"),
            result.get_from_ext::<()>("test-1")
        );
        assert_eq!(
            store.get_from_ext::<String>("string-id"),
            result.get_from_ext::<String>("string-id")
        );
    }

    #[test]
    #[ignore]
    fn compare_compression() {
        init_std_out_logging();
        let folder = create_folders(PathBuf::from(
            "./test_output/simulation/id/id_store/compare_compression/",
        ));
        let mut store = IdStore::new();

        let net = Network::from_file_path(
            &PathBuf::from("/Users/janek/Documents/rust_q_sim/input/rvr.network.xml.gz"),
            1,
            PartitionMethod::None,
        );
        for link in &net.links {
            store.create_id::<Link>(link.id.external());
        }
        for node in &net.nodes {
            store.create_id::<Node>(node.id.external());
        }

        let mut garage = Garage::from_file(&PathBuf::from(
            "/Users/janek/Documents/rust_q_sim/input/rvr.vehicles.xml",
        ));
        let pop = Population::from_file(
            &PathBuf::from("/Users/janek/Documents/rust_q_sim/input/rvr-10pct.plans.xml.gz"),
            &mut garage,
        );

        for p_id in pop.persons.keys() {
            store.create_id::<Person>(p_id.external());
        }

        for v_id in garage.vehicles.keys() {
            store.create_id::<Vehicle>(v_id.external());
        }

        for t_id in garage.vehicle_types.keys() {
            store.create_id::<VehicleType>(t_id.external());
        }

        println!("Starting to write id store raw");
        let start = Instant::now();
        serialize_to_file(&store, &folder.join("ids.raw.pbf"), IdCompression::None);
        let end = Instant::now();
        let duration = end.sub(start).as_millis();
        println!("writing uncompressed took: {duration}ms");

        println!("Starting to write id store compressed");
        let start = Instant::now();
        serialize_to_file(&store, &folder.join("ids.lz4.pbf"), IdCompression::LZ4);
        let end = Instant::now();
        let duration = end.sub(start).as_millis();
        println!("writing compressed took: {duration}ms");

        println!("Starting to read id store uncompressed");
        let start = Instant::now();
        let mut result_uncompressed = IdStore::new();
        deserialize_from_file(&mut result_uncompressed, &folder.join("ids.raw.pbf"));
        let end = Instant::now();
        let duration = end.sub(start).as_millis();
        println!("reading uncompressed took: {duration}ms");

        println!("Starting to read id store compressed");
        let start = Instant::now();
        let mut result_compressed = IdStore::new();
        deserialize_from_file(&mut result_compressed, &folder.join("ids.lz4.pbf"));
        let end = Instant::now();
        let duration = end.sub(start).as_millis();
        println!("reading compressed took: {duration}ms");
    }
}
