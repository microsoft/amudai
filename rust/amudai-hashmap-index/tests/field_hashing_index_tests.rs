use ahash::AHashSet;
use std::sync::Arc;

use amudai_format::defs::shard::{IndexArtifact, IndexDescriptor};
use amudai_format::projection::SchemaProjection;
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
use amudai_hashmap_index::FieldHashingIndexType;
use amudai_index_core::{
    IndexType,
    builder::{ArtifactData, Parameters},
    metadata::{Granularity, PositionFidelity, QueryKind, Scope},
    reader::{FieldHashingQuery, IndexQuery},
};
use amudai_io_impl::temp_file_store;
use amudai_sequence::presence::Presence;
use amudai_sequence::sequence::Sequence;
use amudai_sequence::struct_sequence::StructSequence;
use amudai_sequence::value_sequence::ValueSequence;
use amudai_shard::tests::shard_store::ShardStore;

#[test]
fn test_field_hashing_index_lookup() {
    // 1) Start from FieldHashingIndexType and create a builder via create_builder()
    let index_type = FieldHashingIndexType::new();

    // Object store + temp store for building and sealing shards
    let shard_store = ShardStore::new();
    let temp = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Minimal parameters required by the index builder
    let mut query_kind = AHashSet::new();
    query_kind.insert(QueryKind::Hash);
    let params = Parameters {
        temp_store: temp,
        object_store: Some(shard_store.object_store.clone()),
        container: None,
        scope: Scope::MultiShardField(vec![]),
        granularity: Granularity::Record,
        fidelity: PositionFidelity::Exact,
        query_kind,
        properties: Default::default(),
    };
    let mut builder = index_type.create_builder(params).unwrap();

    // 2) Prepare a nested schema with two top-level structs ("a" and "b").
    //    Each has two leaf fields: id: Int32 and name: String.
    let mut a = FieldBuilder::new_struct().with_name("a");
    a.data_type_mut().add_child(DataTypeBuilder::new(
        "id",
        BasicType::Int32,
        true,
        None,
        vec![],
    ));
    a.data_type_mut()
        .add_child(DataTypeBuilder::new_str().with_field_name("name"));

    let mut b = FieldBuilder::new_struct().with_name("b");
    b.data_type_mut().add_child(DataTypeBuilder::new(
        "id",
        BasicType::Int32,
        true,
        None,
        vec![],
    ));
    b.data_type_mut()
        .add_child(DataTypeBuilder::new_str().with_field_name("name"));

    let schema = SchemaBuilder::new(vec![a, b]).into_schema().unwrap();
    let proj = SchemaProjection::full(schema);

    // Build inner sequences for struct "a"
    let mut a_id = ValueSequence::with_capacity(
        BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            ..Default::default()
        },
        3,
    );
    a_id.extend_from_slice::<i32>(&[1, 2, 3]);
    let mut a_name = ValueSequence::with_capacity(
        BasicTypeDescriptor {
            basic_type: BasicType::String,
            ..Default::default()
        },
        3,
    );
    a_name.push_str("alice");
    a_name.push_str("bob");
    a_name.push_str("alice");
    let a_struct = StructSequence::new(
        None,
        vec![Box::new(a_id), Box::new(a_name)],
        Presence::Trivial(3),
    );

    // Build inner sequences for struct "b"
    let mut b_id = ValueSequence::with_capacity(
        BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            ..Default::default()
        },
        3,
    );
    b_id.extend_from_slice::<i32>(&[1, 2, 3]);
    let mut b_name = ValueSequence::with_capacity(
        BasicTypeDescriptor {
            basic_type: BasicType::String,
            ..Default::default()
        },
        3,
    );
    b_name.push_str("alice");
    b_name.push_str("bob");
    b_name.push_str("alice");
    let b_struct = StructSequence::new(
        None,
        vec![Box::new(b_id), Box::new(b_name)],
        Presence::Trivial(3),
    );

    // Root struct with top-level fields a and b
    let root_struct = StructSequence::new(
        None,
        vec![Box::new(a_struct), Box::new(b_struct)],
        Presence::Trivial(3),
    );

    // The projection has 4 leaf paths: a.id, a.name, b.id, b.name.
    // Duplicate the root sequence once per leaf path, and build Frame directly
    // without validation so we can provide one sequence per leaf path as expected
    // by the indexer.
    let leaf_count = proj.leaf_paths().len();
    assert_eq!(leaf_count, 4);
    let mut fields: Vec<Box<dyn Sequence>> = Vec::with_capacity(leaf_count);
    for _ in 0..leaf_count {
        fields.push(root_struct.clone_boxed());
    }

    let frame = amudai_sequence::frame::Frame {
        schema: Some(Arc::new(proj)),
        fields,
        len: 3,
    };
    let loc = amudai_sequence::frame::FrameLocation {
        stripe_ordinal: 0,
        shard_range: 0..3,
        stripe_range: 0..3,
    };

    // 3) Index entries by processing the frame
    builder.process_frame(&frame, &loc).unwrap();

    // 4) Seal the builder, then materialize artifacts into an IndexDescriptor
    let prepared_index = builder.seal().unwrap();

    // Seal PreparedShard artifacts to file URLs under the test store
    let mut artifacts = Vec::with_capacity(prepared_index.artifacts.len());
    for art in prepared_index.artifacts.into_iter() {
        match art.data {
            ArtifactData::PreparedShard(prep) => {
                // Downcast to concrete PreparedShard and seal
                let prep: amudai_shard::write::shard_builder::PreparedShard = match prep.try_into()
                {
                    Ok(p) => p,
                    Err(_) => panic!("downcast PreparedShard"),
                };
                let url = shard_store.generate_shard_url();
                let sealed = prep.seal(&url).unwrap();
                artifacts.push(IndexArtifact {
                    name: art.name,
                    properties: art.properties.finish(),
                    data_ref: Some(sealed.directory_ref),
                });
            }
            _ => panic!("unexpected artifact data kind"),
        }
    }
    let descriptor = IndexDescriptor {
        index_type: FieldHashingIndexType::NAME.to_string(),
        properties: prepared_index.properties.finish(),
        indexed_fields: vec![],
        artifacts,
        index_size: None,
    };

    // 5) Open a reader via the index type and perform lookups
    // Use the first partition's shard as the context shard for object_store
    let first_ref = descriptor
        .artifacts
        .first()
        .and_then(|a| a.data_ref.as_ref())
        .expect("artifact data_ref");
    let shard = shard_store.open_shard_ref(first_ref);
    let reader = index_type
        .open_reader(&descriptor, Some(Box::new(shard)))
        .unwrap();

    // Positive: lookup a.name == "alice" -> positions {0, 2}
    let path_a_name: Arc<Vec<Arc<str>>> = Arc::new(vec![Arc::from("a"), Arc::from("name")]);
    let q = IndexQuery::FieldHashing(FieldHashingQuery {
        path: path_a_name,
        value: Arc::<[u8]>::from(&b"alice"[..]),
        span: 3,
    });
    let res = reader.query(&q).unwrap();
    let true_positions = res.is_true().positions().collect::<Vec<_>>();
    assert_eq!(true_positions, vec![0, 2]);

    // Positive: lookup b.name == "bob" -> positions {1}
    let path_b_name: Arc<Vec<Arc<str>>> = Arc::new(vec![Arc::from("b"), Arc::from("name")]);
    let q = IndexQuery::FieldHashing(FieldHashingQuery {
        path: path_b_name,
        value: Arc::<[u8]>::from(&b"bob"[..]),
        span: 3,
    });
    let res = reader.query(&q).unwrap();
    let true_positions = res.is_true().positions().collect::<Vec<_>>();
    assert_eq!(true_positions, vec![1]);

    // Negative: lookup a.name == "charlie" -> no positions
    let q = IndexQuery::FieldHashing(FieldHashingQuery {
        path: Arc::new(vec![Arc::from("a"), Arc::from("name")]),
        value: Arc::<[u8]>::from(&b"charlie"[..]),
        span: 3,
    });
    let res = reader.query(&q).unwrap();
    assert_eq!(res.is_true().count_positions(), 0);

    // Negative: lookup a.id == 999 -> no positions
    let mut buf = Vec::new();
    buf.extend_from_slice(&999i32.to_le_bytes());
    let q = IndexQuery::FieldHashing(FieldHashingQuery {
        path: Arc::new(vec![Arc::from("a"), Arc::from("id")]),
        value: Arc::<[u8]>::from(buf.into_boxed_slice()),
        span: 3,
    });
    let res = reader.query(&q).unwrap();
    assert_eq!(res.is_true().count_positions(), 0);
}
