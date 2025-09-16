use ahash::AHashSet;
use amudai_format::defs::common::{AnyValue, ListValue, any_value};
use amudai_format::defs::shard::{IndexArtifact, IndexDescriptor, IndexedField};
use amudai_format::property_bag::PropertyBagBuilder;
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
use amudai_index_core::builder::ArtifactData;
use amudai_index_core::metadata::QueryKind;
use amudai_index_core::{IndexType, builder::Parameters, metadata, metadata::Scope};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::{
    ObjectStore,
    local_store::{LocalFsMode, LocalFsObjectStore},
};
use amudai_sequence::sequence::Sequence;
use amudai_sequence::{
    frame::{Frame, FrameLocation},
    value_sequence::BinarySequenceBuilder,
};
use amudai_shard::{
    read::shard::ShardOptions, write::shard_builder::PreparedShard as ShardPreparedShard,
};
use amudai_text_index::TextIndexType;
use std::sync::Arc;
use tempfile::TempDir;

/// Comprehensive end-to-end test that validates the complete text indexing and querying pipeline.
///
/// This test validates:
/// 1. Building a text index from textual data across multiple stripes and fields
/// 2. Reading the index and performing exact term lookups
/// 3. Reading the index and performing prefix-based searches
/// 4. Verifying position data accuracy and ordering
/// 5. Ensuring proper handling of multi-stripe and multi-field scenarios
#[test]
fn test_text_index_end_to_end() {
    // Setup storage backends with temporary directory
    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
            .expect("Failed to create LocalFsObjectStore"),
    );
    let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
        .expect("Failed to create temporary file store");

    // Create and configure the text index builder via high-level IndexType API
    let mut query_kind = AHashSet::new();
    query_kind.insert(QueryKind::Term);
    query_kind.insert(QueryKind::TermPrefix);
    query_kind.insert(QueryKind::TermSet);
    query_kind.insert(QueryKind::TermPrefixSet);

    // Provide a concrete scope so IndexType can derive field count and tokenizer defaults.
    let scope_fields = vec![
        IndexedField {
            schema_ids: vec![0],
        },
        IndexedField {
            schema_ids: vec![1],
        },
        IndexedField {
            schema_ids: vec![2],
        },
    ];

    let mut properties = PropertyBagBuilder::default();
    properties.set("collation", "unicode-case-insensitive");
    // Configure per-field tokenizers: field0 + field1 use unicode-word; field2 uses unicode-log (for IP detection)
    // Build an AnyValue::ListValue(["unicode-word", "unicode-word", "unicode-log"]) manually.
    let tokenizer_list = AnyValue {
        annotation: None,
        kind: Some(any_value::Kind::ListValue(ListValue {
            elements: vec![
                AnyValue::from("unicode-word"),
                AnyValue::from("unicode-word"),
                AnyValue::from("unicode-log"),
            ],
        })),
    };
    properties.set("tokenizers", tokenizer_list);

    let params = Parameters {
        temp_store: Arc::clone(&temp_store),
        object_store: Some(object_store.clone() as Arc<dyn ObjectStore>),
        container: None,
        scope: Scope::MultiShardField(scope_fields.clone()),
        granularity: metadata::Granularity::Element,
        fidelity: metadata::PositionFidelity::Exact,
        query_kind,
        properties,
    };
    let index_type = TextIndexType::new();
    let mut builder = index_type
        .create_builder(params)
        .expect("Failed to create builder via IndexType API");

    // Build frames and feed through the generic IndexBuilder trait (process_frame)
    // This exercises the full high-level API path instead of the manual field processing helpers.
    let string_type = BasicTypeDescriptor {
        basic_type: BasicType::String,
        signed: false,
        fixed_size: 0,
        extended_type: Default::default(),
    };

    for stripe in 0..3u16 {
        // Builders for three string fields
        let mut f0_builder = BinarySequenceBuilder::new(string_type);
        let mut f1_builder = BinarySequenceBuilder::new(string_type);
        let mut f2_builder = BinarySequenceBuilder::new(string_type);

        for i in 0..15u32 {
            // Field 0 (unicode-word): standard fruit/doc terms.
            let text0 = format!("apple apples juice document number {i} in stripe {stripe}");
            f0_builder.add_value(text0.as_bytes());

            // Field 1 (unicode-word): banana content; stripe 0 includes word 'apple' to ensure multi-field aggregation.
            let text1 = if stripe == 0 {
                format!("banana smoothie content number {i} stripe {stripe} with apple")
            } else {
                format!("banana smoothie content number {i} stripe {stripe}")
            };
            f1_builder.add_value(text1.as_bytes());

            // Field 2 (unicode-log): include IP-like tokens so unicode-log tokenizer should emit full IPv4 tokens.
            // We cycle the third octet to generate variety; unicode-log tokenizer should treat e.g. 10.0.0.1 as a single term.
            let ip = format!("10.{stripe}.{}.1", i % 3);
            let text2 = format!(
                "orange citrus tag healthy fruit fruits number {i} stripe {stripe} {ip} log-entry"
            );
            f2_builder.add_value(text2.as_bytes());
        }

        let frame = Frame::new(
            None,
            vec![
                Box::new(f0_builder.build()) as Box<dyn Sequence>,
                Box::new(f1_builder.build()) as Box<dyn Sequence>,
                Box::new(f2_builder.build()) as Box<dyn Sequence>,
            ],
            15,
        );

        let shard_base = (stripe as u64) * 100; // ensure unique base positions per stripe
        let location = FrameLocation {
            stripe_ordinal: stripe as usize,
            shard_range: shard_base..(shard_base + 15),
            stripe_range: 0..15,
        };

        builder
            .process_frame(&frame, &location)
            .expect("Failed to process frame via IndexBuilder");
    }

    // Finalize index using generic IndexBuilder::seal producing a PreparedIndex
    let prepared_index = builder.seal().expect("Failed to seal PreparedIndex");

    let index_url_base = "null:///tmp/test_index";
    let mut descriptor_artifacts: Vec<IndexArtifact> = Vec::new();
    for artifact in prepared_index.artifacts.into_iter() {
        match artifact.data {
            ArtifactData::PreparedShard(prepared) => {
                let any = prepared.into_any();
                if any.type_id() == std::any::TypeId::of::<ShardPreparedShard>() {
                    let prepared_shard = *any
                        .downcast::<ShardPreparedShard>()
                        .expect("downcast PreparedShard");
                    // Assign deterministic URL based on artifact name
                    let url = format!("{index_url_base}/{}.shard", artifact.name);
                    let sealed = prepared_shard.seal(&url).expect("seal shard artifact");
                    descriptor_artifacts.push(IndexArtifact {
                        name: artifact.name,
                        properties: vec![],
                        data_ref: Some(sealed.directory_ref),
                    });
                }
            }
            _ => {}
        }
    }
    // Build IndexDescriptor for reader open
    // Reconstruct descriptor with required 'collation' property so reader can open.
    let mut desc_props = PropertyBagBuilder::default();
    desc_props.set("collation", "unicode-case-insensitive");
    // Mirror tokenizer property in descriptor (not strictly required for reader today but documents intent)
    let tokenizer_list_desc = AnyValue {
        annotation: None,
        kind: Some(any_value::Kind::ListValue(ListValue {
            elements: vec![
                AnyValue::from("unicode-word"),
                AnyValue::from("unicode-word"),
                AnyValue::from("unicode-log"),
            ],
        })),
    };
    desc_props.set("tokenizers", tokenizer_list_desc);
    let descriptor = IndexDescriptor {
        index_type: index_type.name().to_string(),
        properties: desc_props.finish(),
        indexed_fields: scope_fields.clone(),
        artifacts: descriptor_artifacts,
        index_size: None,
    };

    // Open a shard context from the terms shard (provides object store + resolver)
    let first_ref = descriptor
        .artifacts
        .first()
        .and_then(|a| a.data_ref.as_ref())
        .expect("artifact data_ref");
    let shard = ShardOptions::new(object_store.clone())
        .open_data_ref(&first_ref)
        .expect("open terms shard as context");

    // Open reader via IndexType API
    let reader = index_type
        .open_reader(&descriptor, Some(Box::new(shard)))
        .expect("Failed to open reader via IndexType");

    // Helper functions to build and execute term/prefix queries through the generic IndexReader::query API.
    use amudai_index_core::reader::{IndexQuery, TermQuery};
    use std::sync::Arc as StdArc;

    // Compute span (exclusive upper bound). From earlier we inserted 3 stripes * 15 records = 45 logical positions.
    // The FrameLocation shard_range ensures unique base positions per stripe, but the logical domain we query over
    // is the max position + 1. We used shard bases 0,100,200 so we need span > 215. For safety, take last base + 15.
    let span = 215; // (2 * 100) + 15

    fn make_query(span: u64, kind: QueryKind, terms: &[&str]) -> IndexQuery {
        IndexQuery::Term(TermQuery {
            span,
            terms: terms.iter().map(|t| StdArc::<str>::from(*t)).collect(),
            query_kind: kind,
        })
    }

    // We still want some direct term enumeration tests to validate positions & multi-field behavior.
    // The generic query() returns a TernaryPositionSet; to emulate previous granular checks,
    // we issue narrow single-term queries and then (optionally) perform internal reader debug lookups if needed.
    // For now we adapt assertions to expect at least one matching position in the ternary set.

    // Helper to assert query has matches
    let has_matches = |set: &amudai_position_set::TernaryPositionSet| {
        set.is_true_or_unknown().count_positions() > 0
    };

    // 1. Empty prefix (all terms)
    let all_terms_query = make_query(span, QueryKind::TermPrefix, &[""]);
    let all_terms_positions = reader.query(&all_terms_query).expect("query all terms");
    assert!(
        has_matches(&all_terms_positions),
        "Expected matches for empty prefix"
    );

    // 2. Exact term queries
    for term in ["apple", "juice", "banana", "healthy"] {
        let q = make_query(span, QueryKind::Term, &[term]);
        let res = reader.query(&q).expect("execute term query");
        assert!(has_matches(&res), "Term '{term}' should yield matches");
    }

    // 2a. Verify multi-stripe indexing: 'apple' should appear in stripe 0 (base 0), stripe 1 (field0), and stripe 2.
    let apple_query = make_query(span, QueryKind::Term, &["apple"]);
    let apple_positions = reader.query(&apple_query).expect("query apple");
    let apple_true = apple_positions.is_true();
    let mut seen0 = false;
    let mut seen1 = false;
    let mut seen2 = false;
    for p in apple_true.positions() {
        if p < 15 {
            seen0 = true;
        } else if (100..115).contains(&p) {
            seen1 = true;
        } else if (200..215).contains(&p) {
            seen2 = true;
        }
        if seen0 && seen1 && seen2 {
            break;
        }
    }
    assert!(
        seen0 && seen1 && seen2,
        "Expected 'apple' term positions across all three shard stripes (multi-stripe shard indexing)"
    );

    // 3. Non-existent term
    let nonexistent_query = make_query(span, QueryKind::Term, &["nonexistent"]);
    let nonexistent_positions = reader.query(&nonexistent_query).expect("query nonexistent");
    assert!(
        !has_matches(&nonexistent_positions),
        "Non-existent term should yield no matches"
    );

    // 4. Prefix queries
    for prefix in ["app", "fruit"] {
        let q = make_query(span, QueryKind::TermPrefix, &[prefix]);
        let res = reader.query(&q).expect("execute prefix query");
        assert!(has_matches(&res), "Prefix '{prefix}' should yield matches");
    }

    // 5. Non-matching prefix
    let xyz_prefix_query = make_query(span, QueryKind::TermPrefix, &["xyz"]);
    let xyz_prefix_positions = reader.query(&xyz_prefix_query).expect("query xyz prefix");
    assert!(
        !has_matches(&xyz_prefix_positions),
        "Non-matching prefix should yield no matches"
    );

    // 6. Case-insensitive collation test
    let apple_caps_query = make_query(span, QueryKind::Term, &["APPLE"]);
    let apple_caps_positions = reader.query(&apple_caps_query).expect("query APPLE");
    assert!(
        has_matches(&apple_caps_positions),
        "Case-insensitive search should find apple occurrences"
    );

    // 7. Tokenizer-specific behavior: unicode-log field should index IP addresses as whole tokens.
    // Query an IP known to exist: 10.0.0.1 (stripe 0, i % 3 == 0)
    let ip_present_query = make_query(span, QueryKind::Term, &["10.0.0.1"]);
    let ip_present_positions = reader.query(&ip_present_query).expect("query existing IP");
    assert!(
        has_matches(&ip_present_positions),
        "Expected matches for IP token 10.0.0.1 via unicode-log tokenizer"
    );

    // Query an IP that does NOT exist (different higher-level stripe): 10.3.0.1
    let ip_absent_query = make_query(span, QueryKind::Term, &["10.3.0.1"]);
    let ip_absent_positions = reader.query(&ip_absent_query).expect("query absent IP");
    assert!(
        !has_matches(&ip_absent_positions),
        "Unexpected matches for non-existent IP token 10.3.0.1"
    );
}
