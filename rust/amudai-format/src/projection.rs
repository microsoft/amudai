//! Schema projections for selecting subsets of data types and fields.
//!
//! This module provides a projection system for working with schema subsets in the Amudai
//! format. Projections allow you to define and work with specific portions of a schema while
//! maintaining structural integrity and type safety.
//!
//! # Overview
//!
//! A projection represents a "view" into a hierarchical schema that includes only selected
//! fields and their necessary structural context. This is particularly useful for:
//!
//! - **Minimal data reading**: Only read and decode the fields you need
//! - **Memory optimization**: Reduce memory footprint by loading only required data
//! - **API contracts**: Define precise data shapes for different consumers
//!
//! # Core Concepts
//!
//! ## Structural Integrity Rule
//!
//! The fundamental rule of projections is: **any field that appears in a projection must also have
//! its parent in the projection**. This ensures:
//!
//! - No orphaned value sequences exist
//! - Lists (however deeply nested) always have their offset information
//! - The projected schema remains valid and self-contained
//!
//! ```text
//! Original Schema:          Projection:
//! root                      root
//! ├── user                  ├── user
//! │   ├── id                │   ├── id
//! │   ├── name              │   └── addresses
//! │   └── addresses         │       └── city
//! │       ├── street        └── timestamp
//! │       ├── city
//! │       └── zip
//! └── timestamp
//! ```
//!
//! ## Projection Types
//!
//! The module provides three main types for working with projections:
//!
//! 1. **`SchemaProjection`**: Top-level projection of an entire schema
//! 2. **`TypeProjection`**: Projection of a single data type node with its children
//! 3. **`ProjectedFields`**: collection of projected fields with lookup capabilities
//!
//! # Performance Considerations
//!
//! ## Lazy Lookup Table
//!
//! `ProjectedFields` uses an adaptive lookup strategy:
//! - For collections with ≤4 fields: Linear search (faster for small collections)
//! - For larger collections: Builds a hash map lazily on first name-based access
//!
//! ## Memory Efficiency
//!
//! - All projection types use `Arc` internally for cheap cloning
//! - Empty projections use a singleton pattern to avoid allocations
//! - Field names are stored as `Arc<str>` to enable sharing

use std::sync::{Arc, LazyLock, OnceLock};

use ahash::AHashMap;
use amudai_common::Result;

use crate::schema::{BasicType, BasicTypeDescriptor, DataType, FieldList, Schema, SchemaId};

/// A projection of a schema that represents a subset of the full schema tree.
///
/// `SchemaProjection` allows you to work with only the fields you need from a larger schema,
/// while maintaining references to the original `DataType` nodes from the full schema.
///
/// # Structure
///
/// A schema projection consists of:
/// - A reference to the complete `Schema` containing all type definitions
/// - A `ProjectedFields` collection specifying which top-level fields are included
///
/// The projection preserves the hierarchical structure - if a field is included, all its
/// ancestors must also be included to maintain structural integrity.
///
/// # Implementation Note
///
/// The `DataType` nodes referenced through the projection are the same instances from the
/// original schema - they are not cloned or modified. This ensures type consistency and
/// allows efficient memory usage when working with multiple projections of the same schema.
#[derive(Clone)]
pub struct SchemaProjection {
    /// The underlying schema being projected.
    schema: Schema,
    /// The fields included in this projection.
    fields: ProjectedFields,
}

impl SchemaProjection {
    /// Creates a new schema projection with the specified schema and fields.
    pub fn new(schema: Schema, fields: ProjectedFields) -> SchemaProjection {
        SchemaProjection { schema, fields }
    }

    /// Creates a full projection containing all fields from the schema.
    pub fn full(schema: Schema) -> SchemaProjection {
        let fields = ProjectedFields::from_list(schema.field_list().expect("schema fields"));
        SchemaProjection::new(schema, fields)
    }

    // Creates a filtered projection based on a predicate function.
    ///
    /// This method traverses the entire schema tree and includes only the data types
    /// that satisfy the given predicate, while maintaining structural integrity.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to create a filtered projection from
    /// * `predicate` - A function that determines which data types to include:
    ///   - Returns `Some(true)` to explicitly include the node (and any of its children
    ///     that satisfy the predicate)
    ///   - Returns `Some(false)` to explicitly exclude the node and all its children
    ///   - Returns `None` to include the node only if it has included children
    ///
    /// # Behavior
    ///
    /// The filtering process follows these rules:
    /// 1. When a node returns `Some(false)`, it and all its descendants are excluded
    /// 2. When a node returns `Some(true)`, it is included regardless of its children
    /// 3. When a node returns `None`, it is included only if at least one child is included
    /// 4. The structural integrity rule is maintained - all ancestors of included nodes are
    ///    preserved
    pub fn filtered(
        schema: Schema,
        predicate: impl Fn(&DataType) -> Option<bool>,
    ) -> SchemaProjection {
        let fields = schema
            .field_list()
            .expect("field_list")
            .into_iter()
            .filter_map(|field| TypeProjection::filtered(&field.expect("field"), &predicate))
            .collect::<Vec<_>>();
        if fields.is_empty() {
            SchemaProjection::new(schema, ProjectedFields::empty())
        } else {
            SchemaProjection::new(schema, fields.into())
        }
    }

    /// Returns a reference to the underlying schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns a reference to the projected fields.
    pub fn fields(&self) -> &ProjectedFields {
        &self.fields
    }
}

impl std::fmt::Display for SchemaProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({})", self.fields)
    }
}

/// Projection of a data type node with its name and child fields.
pub struct TypeProjection {
    /// The underlying data type being projected. This `DataType` refers back
    /// to the full schema.
    data_type: DataType,
    /// The name of this type projection. This name may differ from the inherent
    /// one define within the `data_type` to support field renaming.
    name: Arc<str>,
    /// The child fields included in this projection.
    children: ProjectedFields,
}

impl TypeProjection {
    /// Creates a new type projection with the specified data type, name, and children.
    pub fn new(
        data_type: DataType,
        name: impl Into<Arc<str>>,
        children: ProjectedFields,
    ) -> TypeProjection {
        TypeProjection {
            data_type,
            name: name.into(),
            children,
        }
    }

    /// Creates a full projection containing all child fields of the data type.
    pub fn full(data_type: DataType) -> TypeProjection {
        let name: Arc<str> = Arc::from(data_type.name().expect("name"));
        let children = ProjectedFields::from_list(data_type.field_list().expect("field_list"));
        TypeProjection::new(data_type, name, children)
    }

    /// Creates an empty projection with no child fields.
    pub fn empty(data_type: DataType) -> TypeProjection {
        let name: Arc<str> = Arc::from(data_type.name().expect("name"));
        TypeProjection::new(data_type, name, ProjectedFields::empty())
    }

    /// Creates a filtered projection based on a predicate function.
    ///
    /// This method recursively traverses a data type and its children, creating a projection
    /// that includes only the nodes that satisfy the given predicate. The method maintains
    /// the structural integrity rule by ensuring that any included node also has its ancestors
    /// included in the projection.
    ///
    /// # Arguments
    ///
    /// * `data_type` - The data type to create a filtered projection from
    /// * `predicate` - A function that determines which data types to include:
    ///   - Returns `Some(true)` to explicitly include the node (and recursively evaluate its children)
    ///   - Returns `Some(false)` to explicitly exclude the node and all its descendants
    ///   - Returns `None` to include the node only if at least one of its children is included
    ///
    /// # Returns
    ///
    /// Returns `Some(TypeProjection)` if the node or any of its descendants should be included,
    /// or `None` if the entire subtree should be excluded.
    ///
    /// # Behavior
    ///
    /// The filtering process follows these rules:
    /// 1. **Explicit exclusion (`Some(false)`)**: The node and all its descendants are excluded,
    ///    regardless of what the predicate would return for the descendants
    /// 2. **Explicit inclusion (`Some(true)`)**: The node is included, and its children are
    ///    recursively evaluated with the predicate
    /// 3. **Conditional inclusion (`None`)**: The node is included only if at least one child
    ///    passes the filter; if no children are included, the node itself is excluded
    pub fn filtered(
        data_type: &DataType,
        predicate: &impl Fn(&DataType) -> Option<bool>,
    ) -> Option<TypeProjection> {
        let self_res = predicate(data_type);
        if let Some(false) = self_res {
            return None;
        }

        let mut children = Vec::new();
        for i in 0..data_type.child_count().expect("count") {
            let child = data_type.child_at(i).expect("child_at");
            if let Some(child_projection) = TypeProjection::filtered(&child, predicate) {
                children.push(Arc::new(child_projection));
            }
        }

        if children.is_empty() && self_res.is_none() {
            return None;
        }

        let name: Arc<str> = Arc::from(data_type.name().expect("name"));
        Some(TypeProjection::new(
            data_type.clone(),
            name,
            children.into(),
        ))
    }

    /// Returns a new projection with the specified name.
    pub fn with_name(mut self, name: impl AsRef<str>) -> TypeProjection {
        self.name = Arc::from(name.as_ref());
        self
    }

    /// Returns the name of this type projection.
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// Returns a reference to the underlying data type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns a reference to the child fields.
    pub fn children(&self) -> &ProjectedFields {
        &self.children
    }

    /// Returns the basic type of the underlying data type.
    pub fn basic_type(&self) -> BasicType {
        self.data_type.basic_type().expect("basic_type")
    }

    /// Returns the type descriptor of the underlying data type.
    pub fn type_descriptor(&self) -> BasicTypeDescriptor {
        self.data_type.describe().expect("describe")
    }

    /// Returns the schema ID of the underlying data type.
    pub fn schema_id(&self) -> SchemaId {
        self.data_type.schema_id().expect("schema_id")
    }
}

/// Type alias for an `Arc<TypeProjection>`.
pub type TypeProjectionRef = Arc<TypeProjection>;

impl std::fmt::Display for TypeProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let basic_type = self.data_type().describe().unwrap_or_default();
        if basic_type.basic_type.is_composite() {
            write!(f, "{}<{}>", basic_type, self.children)
        } else {
            basic_type.fmt(f)
        }
    }
}

/// A collection of projected fields with efficient lookup capabilities.
#[derive(Clone)]
pub struct ProjectedFields {
    /// The list of field projections.
    list: Arc<[TypeProjectionRef]>,
    /// Optional lookup map for efficient field name resolution (built lazily
    /// for larger collections).
    lookup: Option<Arc<OnceLock<StrToIndexMap>>>,
}

type StrToIndexMap = AHashMap<Arc<str>, usize>;

impl ProjectedFields {
    /// Creates a new `ProjectedFields` from a list of type projections.
    pub fn new(list: Arc<[TypeProjectionRef]>) -> ProjectedFields {
        ProjectedFields {
            list,
            lookup: Some(Arc::new(OnceLock::new())),
        }
    }

    /// Creates an empty `ProjectedFields` (singleton for efficiency).
    pub fn empty() -> ProjectedFields {
        static EMPTY: LazyLock<ProjectedFields> = LazyLock::new(|| ProjectedFields {
            list: Arc::from([]),
            lookup: None,
        });
        EMPTY.clone()
    }

    /// Creates a `ProjectedFields` from a vector of type projections.
    pub fn from_vec(fields: Vec<TypeProjectionRef>) -> ProjectedFields {
        ProjectedFields::new(Arc::from(fields))
    }

    /// Creates a `ProjectedFields` from a `FieldList` by projecting all fields.
    pub fn from_list(field_list: FieldList) -> ProjectedFields {
        let list = field_list
            .into_iter()
            .map(|field| Arc::new(TypeProjection::full(field.expect("field"))))
            .collect();
        ProjectedFields::new(list)
    }

    /// Returns the number of fields in the projection.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns `true` if the projection contains no fields.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns a reference to the field at the given index, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<&TypeProjectionRef> {
        self.list.get(index)
    }

    /// Returns a reference to the first field, or `None` if empty.
    pub fn first(&self) -> Option<&TypeProjectionRef> {
        self.list.first()
    }

    /// Returns a reference to the last field, or `None` if empty.
    pub fn last(&self) -> Option<&TypeProjectionRef> {
        self.list.last()
    }

    /// Returns an iterator over the fields.
    pub fn iter(&self) -> std::slice::Iter<'_, TypeProjectionRef> {
        self.list.iter()
    }

    /// Returns `true` if the projection contains a field with the given name.
    pub fn contains(&self, name: &str) -> bool {
        self.find_index(name).is_some()
    }

    /// Finds a field by name and returns a reference to it, or `None` if not found.
    pub fn find(&self, name: &str) -> Option<&TypeProjectionRef> {
        self.find_index(name).map(|i| &self.list[i])
    }

    /// Returns the index of the field with the given name, or `None` if not found.
    pub fn find_index(&self, name: &str) -> Option<usize> {
        if let Some(lookup) = self.build_lookup() {
            lookup.get(name).cloned()
        } else {
            self.list
                .iter()
                .position(|field| field.name().as_ref() == name)
        }
    }

    /// Returns a slice of all fields.
    pub fn as_slice(&self) -> &[TypeProjectionRef] {
        self.list.as_ref()
    }

    /// Returns the underlying Arc for efficient cloning.
    pub fn as_arc(&self) -> &Arc<[TypeProjectionRef]> {
        &self.list
    }
}

impl ProjectedFields {
    /// Builds and returns the lookup map for field name resolution.
    /// Only builds for collections with more than 4 fields for performance.
    fn build_lookup(&self) -> Option<&AHashMap<Arc<str>, usize>> {
        if self.list.len() <= 4 {
            return None;
        }
        let lookup = self.lookup.as_ref()?.get_or_init(|| {
            self.list
                .iter()
                .enumerate()
                .map(|(i, field)| (field.name().clone(), i))
                .collect()
        });
        Some(lookup)
    }
}

impl std::ops::Deref for ProjectedFields {
    type Target = [TypeProjectionRef];

    fn deref(&self) -> &Self::Target {
        self.list.as_ref()
    }
}

impl std::ops::Index<usize> for ProjectedFields {
    type Output = TypeProjectionRef;

    fn index(&self, index: usize) -> &Self::Output {
        &self.list[index]
    }
}

impl From<Vec<TypeProjectionRef>> for ProjectedFields {
    fn from(fields: Vec<TypeProjectionRef>) -> Self {
        ProjectedFields::from_vec(fields)
    }
}

impl From<Vec<TypeProjection>> for ProjectedFields {
    fn from(fields: Vec<TypeProjection>) -> Self {
        ProjectedFields::from_vec(fields.into_iter().map(Arc::new).collect())
    }
}

impl<'a> IntoIterator for &'a ProjectedFields {
    type Item = &'a TypeProjectionRef;
    type IntoIter = std::slice::Iter<'a, TypeProjectionRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.iter()
    }
}

impl Default for ProjectedFields {
    fn default() -> Self {
        ProjectedFields::empty()
    }
}

impl std::fmt::Display for ProjectedFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return Ok(());
        }
        for (i, child) in self.iter().enumerate() {
            if i != 0 {
                f.write_str(", ")?;
            }
            let name = child.name();
            if name.is_empty() {
                child.fmt(f)?;
            } else {
                write!(f, "{name}: {child}")?;
            }
        }
        Ok(())
    }
}

/// Builder for constructing `SchemaProjection` instances incrementally.
///
/// `SchemaProjectionBuilder` allows you to build projections by adding field paths
/// one at a time, automatically handling the hierarchical structure and deduplication
/// of overlapping paths.
/// The builder maintains the structural integrity rule by ensuring all parent fields are
/// included when child fields are added.
///
/// # Building Process
///
/// 1. Create a builder with a target schema
/// 2. Add field paths (e.g., `["user", "addresses", "city"]`)
/// 3. Build the final projection
///
/// The builder automatically handles:
/// - Deduplication of overlapping paths
/// - Proper parent-child relationships
pub struct SchemaProjectionBuilder {
    /// The schema being projected.
    schema: Schema,
    /// Collection of top-level field builders.
    fields: Vec<TypeProjectionBuilder>,
    /// Map from field schema ids to their indices in the fields vector.
    field_map: AHashMap<SchemaId, usize>,
}

impl SchemaProjectionBuilder {
    /// Creates a new builder for the specified schema.
    ///
    /// # Arguments
    /// - `schema`: The schema to create projections from
    pub fn new(schema: Schema) -> SchemaProjectionBuilder {
        SchemaProjectionBuilder {
            schema,
            fields: Vec::new(),
            field_map: AHashMap::new(),
        }
    }

    /// Adds a field path to the projection.
    ///
    /// The path is specified as an iterable of field names(e.g., `["user", "addresses", "city"]`).
    /// The method automatically ensures that all parent fields are included in the projection
    /// to maintain structural integrity.
    ///
    /// # Arguments
    /// - `path`: An iterable of field names representing the path to include
    pub fn add_path(&mut self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        let path_nodes = self.schema.resolve_field_path(path)?;
        if !path_nodes.is_empty() {
            self.add_path_by_nodes(&path_nodes)?;
        }
        Ok(())
    }

    /// Adds a field path using pre-resolved `DataType` nodes.
    ///
    /// This method works with already resolved field paths. It recursively builds
    /// the projection tree by creating or updating child builders as needed.
    ///
    /// # Arguments
    /// - `path`: A slice of `DataType` nodes representing the resolved field path
    pub fn add_path_by_nodes(&mut self, path: &[DataType]) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        let child_node = &path[0];
        let child_id = child_node.schema_id()?;
        let child_builder = match self.field_map.get(&child_id) {
            Some(index) => &mut self.fields[*index],
            None => {
                let new_builder = TypeProjectionBuilder::new(child_node.clone());
                self.fields.push(new_builder);
                let index = self.fields.len() - 1;
                self.field_map.insert(child_id, index);
                &mut self.fields[index]
            }
        };
        child_builder.add_path_by_nodes(&path[1..])
    }

    /// Builds the final `SchemaProjection` from the accumulated field paths.
    ///
    /// This method consumes the builder and constructs the final projection by
    /// building all child type projections and assembling them into a cohesive
    /// schema projection.
    ///
    /// # Returns
    /// The constructed `SchemaProjection` containing all added field paths
    pub fn build(self) -> SchemaProjection {
        let projected_fields = if self.fields.is_empty() {
            ProjectedFields::empty()
        } else {
            ProjectedFields::from_vec(
                self.fields
                    .into_iter()
                    .map(|builder| builder.build())
                    .collect(),
            )
        };
        SchemaProjection::new(self.schema, projected_fields)
    }
}

/// Builder for constructing `TypeProjection` instances incrementally.
///
/// `TypeProjectionBuilder` manages the construction of projections for individual data types,
/// handling child field inclusion and maintaining the hierarchical structure.
pub struct TypeProjectionBuilder {
    /// The data type being projected.
    data_type: DataType,
    /// Collection of child field builders (used in selective mode).
    children: Vec<TypeProjectionBuilder>,
    /// Map from child field schema ids to their indices in the children vector.
    child_map: AHashMap<SchemaId, usize>,
}

impl TypeProjectionBuilder {
    /// Creates a new builder for the specified data type.
    ///
    /// The builder starts in selective mode with no child fields included.
    ///
    /// # Arguments
    /// - `data_type`: The data type to create projections from
    pub fn new(data_type: DataType) -> TypeProjectionBuilder {
        TypeProjectionBuilder {
            data_type,
            children: Vec::new(),
            child_map: AHashMap::new(),
        }
    }

    /// Adds a field path to this type's projection.
    ///
    /// The path is specified as an iterable of field names representing the path
    /// from this type to the target field. An empty path indicates that the full
    /// subtree should be included.
    ///
    /// # Arguments
    /// - `path`: An iterable of field names representing the path to include
    pub fn add_path(&mut self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        let path_nodes = self.data_type.resolve_field_path(path)?;
        self.add_path_by_nodes(&path_nodes)
    }

    /// Adds a field path using pre-resolved `DataType` nodes.
    ///
    /// When an empty path is provided, the builder switches to full subtree mode.
    /// Otherwise, it recursively processes the path by creating or updating child builders.
    ///
    /// # Arguments
    /// - `path`: A slice of `DataType` nodes representing the resolved field path
    pub fn add_path_by_nodes(&mut self, path: &[DataType]) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        let child_node = &path[0];
        let child_id = child_node.schema_id()?;
        let child_builder = match self.child_map.get(&child_id) {
            Some(index) => &mut self.children[*index],
            None => {
                let new_builder = TypeProjectionBuilder::new(child_node.clone());
                self.children.push(new_builder);
                let index = self.children.len() - 1;
                self.child_map.insert(child_id, index);
                &mut self.children[index]
            }
        };
        child_builder.add_path_by_nodes(&path[1..])
    }

    /// Builds the final `TypeProjection` from the accumulated field paths.
    ///
    /// This method consumes the builder and constructs the final type projection.
    ///
    /// # Returns
    /// An `Arc<TypeProjection>` representing the constructed type projection
    pub fn build(self) -> TypeProjectionRef {
        let projected_children = if self.children.is_empty() {
            ProjectedFields::empty()
        } else {
            ProjectedFields::from_vec(
                self.children
                    .into_iter()
                    .map(|builder| builder.build())
                    .collect(),
            )
        };
        let name: Arc<str> = Arc::from(self.data_type.name().expect("data type name"));
        Arc::new(TypeProjection::new(
            self.data_type,
            name,
            projected_children,
        ))
    }
}
