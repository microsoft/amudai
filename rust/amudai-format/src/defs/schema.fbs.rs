pub use root::*;

const _: () = ::planus::check_version_compatibility("planus-1.1.1");

/// The root namespace
///
/// Generated from these locations:
/// * File `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs`
#[no_implicit_prelude]
#[allow(dead_code, clippy::needless_lifetimes)]
mod root {
    /// The enum `BasicType`
    ///
    /// Generated from these locations:
    /// * Enum `BasicType` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:1`
    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        ::serde::Serialize,
        ::serde::Deserialize,
    )]
    #[repr(u8)]
    pub enum BasicType {
        /// The variant `Unit` in the enum `BasicType`
        Unit = 0,

        /// The variant `Boolean` in the enum `BasicType`
        Boolean = 1,

        /// The variant `Int8` in the enum `BasicType`
        Int8 = 2,

        /// The variant `Int16` in the enum `BasicType`
        Int16 = 3,

        /// The variant `Int32` in the enum `BasicType`
        Int32 = 4,

        /// The variant `Int64` in the enum `BasicType`
        Int64 = 5,

        /// The variant `Float32` in the enum `BasicType`
        Float32 = 6,

        /// The variant `Float64` in the enum `BasicType`
        Float64 = 7,

        /// The variant `Binary` in the enum `BasicType`
        Binary = 8,

        /// The variant `FixedSizeBinary` in the enum `BasicType`
        FixedSizeBinary = 9,

        /// The variant `String` in the enum `BasicType`
        String = 10,

        /// The variant `Guid` in the enum `BasicType`
        Guid = 11,

        /// The variant `DateTime` in the enum `BasicType`
        DateTime = 12,

        /// The variant `List` in the enum `BasicType`
        List = 13,

        /// The variant `FixedSizeList` in the enum `BasicType`
        FixedSizeList = 14,

        /// The variant `Struct` in the enum `BasicType`
        Struct = 15,

        /// The variant `Map` in the enum `BasicType`
        Map = 16,

        /// The variant `Union` in the enum `BasicType`
        Union = 17,
    }

    impl BasicType {
        /// Array containing all valid variants of BasicType
        pub const ENUM_VALUES: [Self; 18] = [
            Self::Unit,
            Self::Boolean,
            Self::Int8,
            Self::Int16,
            Self::Int32,
            Self::Int64,
            Self::Float32,
            Self::Float64,
            Self::Binary,
            Self::FixedSizeBinary,
            Self::String,
            Self::Guid,
            Self::DateTime,
            Self::List,
            Self::FixedSizeList,
            Self::Struct,
            Self::Map,
            Self::Union,
        ];
    }

    impl ::core::convert::TryFrom<u8> for BasicType {
        type Error = ::planus::errors::UnknownEnumTagKind;
        #[inline]
        fn try_from(
            value: u8,
        ) -> ::core::result::Result<Self, ::planus::errors::UnknownEnumTagKind> {
            #[allow(clippy::match_single_binding)]
            match value {
                0 => ::core::result::Result::Ok(BasicType::Unit),
                1 => ::core::result::Result::Ok(BasicType::Boolean),
                2 => ::core::result::Result::Ok(BasicType::Int8),
                3 => ::core::result::Result::Ok(BasicType::Int16),
                4 => ::core::result::Result::Ok(BasicType::Int32),
                5 => ::core::result::Result::Ok(BasicType::Int64),
                6 => ::core::result::Result::Ok(BasicType::Float32),
                7 => ::core::result::Result::Ok(BasicType::Float64),
                8 => ::core::result::Result::Ok(BasicType::Binary),
                9 => ::core::result::Result::Ok(BasicType::FixedSizeBinary),
                10 => ::core::result::Result::Ok(BasicType::String),
                11 => ::core::result::Result::Ok(BasicType::Guid),
                12 => ::core::result::Result::Ok(BasicType::DateTime),
                13 => ::core::result::Result::Ok(BasicType::List),
                14 => ::core::result::Result::Ok(BasicType::FixedSizeList),
                15 => ::core::result::Result::Ok(BasicType::Struct),
                16 => ::core::result::Result::Ok(BasicType::Map),
                17 => ::core::result::Result::Ok(BasicType::Union),

                _ => ::core::result::Result::Err(::planus::errors::UnknownEnumTagKind {
                    tag: value as i128,
                }),
            }
        }
    }

    impl ::core::convert::From<BasicType> for u8 {
        #[inline]
        fn from(value: BasicType) -> Self {
            value as u8
        }
    }

    /// # Safety
    /// The Planus compiler correctly calculates `ALIGNMENT` and `SIZE`.
    unsafe impl ::planus::Primitive for BasicType {
        const ALIGNMENT: usize = 1;
        const SIZE: usize = 1;
    }

    impl ::planus::WriteAsPrimitive<BasicType> for BasicType {
        #[inline]
        fn write<const N: usize>(&self, cursor: ::planus::Cursor<'_, N>, buffer_position: u32) {
            (*self as u8).write(cursor, buffer_position);
        }
    }

    impl ::planus::WriteAs<BasicType> for BasicType {
        type Prepared = Self;

        #[inline]
        fn prepare(&self, _builder: &mut ::planus::Builder) -> BasicType {
            *self
        }
    }

    impl ::planus::WriteAsDefault<BasicType, BasicType> for BasicType {
        type Prepared = Self;

        #[inline]
        fn prepare(
            &self,
            _builder: &mut ::planus::Builder,
            default: &BasicType,
        ) -> ::core::option::Option<BasicType> {
            if self == default {
                ::core::option::Option::None
            } else {
                ::core::option::Option::Some(*self)
            }
        }
    }

    impl ::planus::WriteAsOptional<BasicType> for BasicType {
        type Prepared = Self;

        #[inline]
        fn prepare(&self, _builder: &mut ::planus::Builder) -> ::core::option::Option<BasicType> {
            ::core::option::Option::Some(*self)
        }
    }

    impl<'buf> ::planus::TableRead<'buf> for BasicType {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'buf>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            let n: u8 = ::planus::TableRead::from_buffer(buffer, offset)?;
            ::core::result::Result::Ok(::core::convert::TryInto::try_into(n)?)
        }
    }

    impl<'buf> ::planus::VectorReadInner<'buf> for BasicType {
        type Error = ::planus::errors::UnknownEnumTag;
        const STRIDE: usize = 1;
        #[inline]
        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'buf>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::UnknownEnumTag> {
            let value = unsafe { *buffer.buffer.get_unchecked(offset) };
            let value: ::core::result::Result<Self, _> = ::core::convert::TryInto::try_into(value);
            value.map_err(|error_kind| {
                error_kind.with_error_location(
                    "BasicType",
                    "VectorRead::from_buffer",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<BasicType> for BasicType {
        const STRIDE: usize = 1;

        type Value = Self;

        #[inline]
        fn prepare(&self, _builder: &mut ::planus::Builder) -> Self {
            *self
        }

        #[inline]
        unsafe fn write_values(
            values: &[Self],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 1];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - i as u32,
                );
            }
        }
    }

    /// The table `DataType`
    ///
    /// Generated from these locations:
    /// * Table `DataType` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:23`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct DataType {
        /// The field `basic_type` in the table `DataType`
        pub basic_type: self::BasicType,
        /// The field `schema_id` in the table `DataType`
        pub schema_id: u32,
        /// The field `field_name` in the table `DataType`
        pub field_name: ::planus::alloc::string::String,
        /// The field `field_aliases` in the table `DataType`
        pub field_aliases:
            ::core::option::Option<::planus::alloc::vec::Vec<::planus::alloc::string::String>>,
        /// The field `children` in the table `DataType`
        pub children: ::planus::alloc::vec::Vec<self::DataType>,
        /// The field `signed` in the table `DataType`
        pub signed: bool,
        /// The field `fixed_size` in the table `DataType`
        pub fixed_size: u64,
        /// The field `extended_type` in the table `DataType`
        pub extended_type:
            ::core::option::Option<::planus::alloc::boxed::Box<self::ExtendedTypeAnnotation>>,
        /// The field `lookup` in the table `DataType`
        pub lookup: ::core::option::Option<::planus::alloc::boxed::Box<self::HashLookup>>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for DataType {
        fn default() -> Self {
            Self {
                basic_type: self::BasicType::Unit,
                schema_id: 0,
                field_name: ::core::default::Default::default(),
                field_aliases: ::core::default::Default::default(),
                children: ::core::default::Default::default(),
                signed: false,
                fixed_size: 0,
                extended_type: ::core::default::Default::default(),
                lookup: ::core::default::Default::default(),
            }
        }
    }

    impl DataType {
        /// Creates a [DataTypeBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> DataTypeBuilder<()> {
            DataTypeBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_basic_type: impl ::planus::WriteAsDefault<self::BasicType, self::BasicType>,
            field_schema_id: impl ::planus::WriteAsDefault<u32, u32>,
            field_field_name: impl ::planus::WriteAs<::planus::Offset<str>>,
            field_field_aliases: impl ::planus::WriteAsOptional<
                ::planus::Offset<[::planus::Offset<str>]>,
            >,
            field_children: impl ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::DataType>]>>,
            field_signed: impl ::planus::WriteAsDefault<bool, bool>,
            field_fixed_size: impl ::planus::WriteAsDefault<u64, u64>,
            field_extended_type: impl ::planus::WriteAsOptional<
                ::planus::Offset<self::ExtendedTypeAnnotation>,
            >,
            field_lookup: impl ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        ) -> ::planus::Offset<Self> {
            let prepared_basic_type = field_basic_type.prepare(builder, &self::BasicType::Unit);
            let prepared_schema_id = field_schema_id.prepare(builder, &0);
            let prepared_field_name = field_field_name.prepare(builder);
            let prepared_field_aliases = field_field_aliases.prepare(builder);
            let prepared_children = field_children.prepare(builder);
            let prepared_signed = field_signed.prepare(builder, &false);
            let prepared_fixed_size = field_fixed_size.prepare(builder, &0);
            let prepared_extended_type = field_extended_type.prepare(builder);
            let prepared_lookup = field_lookup.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<22> =
                ::core::default::Default::default();
            if prepared_fixed_size.is_some() {
                table_writer.write_entry::<u64>(6);
            }
            if prepared_schema_id.is_some() {
                table_writer.write_entry::<u32>(1);
            }
            table_writer.write_entry::<::planus::Offset<str>>(2);
            if prepared_field_aliases.is_some() {
                table_writer.write_entry::<::planus::Offset<[::planus::Offset<str>]>>(3);
            }
            table_writer.write_entry::<::planus::Offset<[::planus::Offset<self::DataType>]>>(4);
            if prepared_extended_type.is_some() {
                table_writer.write_entry::<::planus::Offset<self::ExtendedTypeAnnotation>>(7);
            }
            if prepared_lookup.is_some() {
                table_writer.write_entry::<::planus::Offset<self::HashLookup>>(8);
            }
            if prepared_basic_type.is_some() {
                table_writer.write_entry::<self::BasicType>(0);
            }
            if prepared_signed.is_some() {
                table_writer.write_entry::<bool>(5);
            }

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    if let ::core::option::Option::Some(prepared_fixed_size) = prepared_fixed_size {
                        object_writer.write::<_, _, 8>(&prepared_fixed_size);
                    }
                    if let ::core::option::Option::Some(prepared_schema_id) = prepared_schema_id {
                        object_writer.write::<_, _, 4>(&prepared_schema_id);
                    }
                    object_writer.write::<_, _, 4>(&prepared_field_name);
                    if let ::core::option::Option::Some(prepared_field_aliases) =
                        prepared_field_aliases
                    {
                        object_writer.write::<_, _, 4>(&prepared_field_aliases);
                    }
                    object_writer.write::<_, _, 4>(&prepared_children);
                    if let ::core::option::Option::Some(prepared_extended_type) =
                        prepared_extended_type
                    {
                        object_writer.write::<_, _, 4>(&prepared_extended_type);
                    }
                    if let ::core::option::Option::Some(prepared_lookup) = prepared_lookup {
                        object_writer.write::<_, _, 4>(&prepared_lookup);
                    }
                    if let ::core::option::Option::Some(prepared_basic_type) = prepared_basic_type {
                        object_writer.write::<_, _, 1>(&prepared_basic_type);
                    }
                    if let ::core::option::Option::Some(prepared_signed) = prepared_signed {
                        object_writer.write::<_, _, 1>(&prepared_signed);
                    }
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<DataType>> for DataType {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<DataType> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<DataType>> for DataType {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<DataType>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<DataType> for DataType {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<DataType> {
            DataType::create(
                builder,
                self.basic_type,
                self.schema_id,
                &self.field_name,
                &self.field_aliases,
                &self.children,
                self.signed,
                self.fixed_size,
                &self.extended_type,
                &self.lookup,
            )
        }
    }

    /// Builder for serializing an instance of the [DataType] type.
    ///
    /// Can be created using the [DataType::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct DataTypeBuilder<State>(State);

    impl DataTypeBuilder<()> {
        /// Setter for the [`basic_type` field](DataType#structfield.basic_type).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn basic_type<T0>(self, value: T0) -> DataTypeBuilder<(T0,)>
        where
            T0: ::planus::WriteAsDefault<self::BasicType, self::BasicType>,
        {
            DataTypeBuilder((value,))
        }

        /// Sets the [`basic_type` field](DataType#structfield.basic_type) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn basic_type_as_default(self) -> DataTypeBuilder<(::planus::DefaultValue,)> {
            self.basic_type(::planus::DefaultValue)
        }
    }

    impl<T0> DataTypeBuilder<(T0,)> {
        /// Setter for the [`schema_id` field](DataType#structfield.schema_id).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn schema_id<T1>(self, value: T1) -> DataTypeBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAsDefault<u32, u32>,
        {
            let (v0,) = self.0;
            DataTypeBuilder((v0, value))
        }

        /// Sets the [`schema_id` field](DataType#structfield.schema_id) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn schema_id_as_default(self) -> DataTypeBuilder<(T0, ::planus::DefaultValue)> {
            self.schema_id(::planus::DefaultValue)
        }
    }

    impl<T0, T1> DataTypeBuilder<(T0, T1)> {
        /// Setter for the [`field_name` field](DataType#structfield.field_name).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn field_name<T2>(self, value: T2) -> DataTypeBuilder<(T0, T1, T2)>
        where
            T2: ::planus::WriteAs<::planus::Offset<str>>,
        {
            let (v0, v1) = self.0;
            DataTypeBuilder((v0, v1, value))
        }
    }

    impl<T0, T1, T2> DataTypeBuilder<(T0, T1, T2)> {
        /// Setter for the [`field_aliases` field](DataType#structfield.field_aliases).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn field_aliases<T3>(self, value: T3) -> DataTypeBuilder<(T0, T1, T2, T3)>
        where
            T3: ::planus::WriteAsOptional<::planus::Offset<[::planus::Offset<str>]>>,
        {
            let (v0, v1, v2) = self.0;
            DataTypeBuilder((v0, v1, v2, value))
        }

        /// Sets the [`field_aliases` field](DataType#structfield.field_aliases) to null.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn field_aliases_as_null(self) -> DataTypeBuilder<(T0, T1, T2, ())> {
            self.field_aliases(())
        }
    }

    impl<T0, T1, T2, T3> DataTypeBuilder<(T0, T1, T2, T3)> {
        /// Setter for the [`children` field](DataType#structfield.children).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn children<T4>(self, value: T4) -> DataTypeBuilder<(T0, T1, T2, T3, T4)>
        where
            T4: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::DataType>]>>,
        {
            let (v0, v1, v2, v3) = self.0;
            DataTypeBuilder((v0, v1, v2, v3, value))
        }
    }

    impl<T0, T1, T2, T3, T4> DataTypeBuilder<(T0, T1, T2, T3, T4)> {
        /// Setter for the [`signed` field](DataType#structfield.signed).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn signed<T5>(self, value: T5) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5)>
        where
            T5: ::planus::WriteAsDefault<bool, bool>,
        {
            let (v0, v1, v2, v3, v4) = self.0;
            DataTypeBuilder((v0, v1, v2, v3, v4, value))
        }

        /// Sets the [`signed` field](DataType#structfield.signed) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn signed_as_default(
            self,
        ) -> DataTypeBuilder<(T0, T1, T2, T3, T4, ::planus::DefaultValue)> {
            self.signed(::planus::DefaultValue)
        }
    }

    impl<T0, T1, T2, T3, T4, T5> DataTypeBuilder<(T0, T1, T2, T3, T4, T5)> {
        /// Setter for the [`fixed_size` field](DataType#structfield.fixed_size).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn fixed_size<T6>(self, value: T6) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6)>
        where
            T6: ::planus::WriteAsDefault<u64, u64>,
        {
            let (v0, v1, v2, v3, v4, v5) = self.0;
            DataTypeBuilder((v0, v1, v2, v3, v4, v5, value))
        }

        /// Sets the [`fixed_size` field](DataType#structfield.fixed_size) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn fixed_size_as_default(
            self,
        ) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, ::planus::DefaultValue)> {
            self.fixed_size(::planus::DefaultValue)
        }
    }

    impl<T0, T1, T2, T3, T4, T5, T6> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6)> {
        /// Setter for the [`extended_type` field](DataType#structfield.extended_type).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn extended_type<T7>(
            self,
            value: T7,
        ) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7)>
        where
            T7: ::planus::WriteAsOptional<::planus::Offset<self::ExtendedTypeAnnotation>>,
        {
            let (v0, v1, v2, v3, v4, v5, v6) = self.0;
            DataTypeBuilder((v0, v1, v2, v3, v4, v5, v6, value))
        }

        /// Sets the [`extended_type` field](DataType#structfield.extended_type) to null.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn extended_type_as_null(self) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, ())> {
            self.extended_type(())
        }
    }

    impl<T0, T1, T2, T3, T4, T5, T6, T7> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7)> {
        /// Setter for the [`lookup` field](DataType#structfield.lookup).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn lookup<T8>(self, value: T8) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, T8)>
        where
            T8: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        {
            let (v0, v1, v2, v3, v4, v5, v6, v7) = self.0;
            DataTypeBuilder((v0, v1, v2, v3, v4, v5, v6, v7, value))
        }

        /// Sets the [`lookup` field](DataType#structfield.lookup) to null.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn lookup_as_null(self) -> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, ())> {
            self.lookup(())
        }
    }

    impl<T0, T1, T2, T3, T4, T5, T6, T7, T8> DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, T8)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [DataType].
        #[inline]
        pub fn finish(self, builder: &mut ::planus::Builder) -> ::planus::Offset<DataType>
        where
            Self: ::planus::WriteAsOffset<DataType>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::BasicType, self::BasicType>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAs<::planus::Offset<str>>,
            T3: ::planus::WriteAsOptional<::planus::Offset<[::planus::Offset<str>]>>,
            T4: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::DataType>]>>,
            T5: ::planus::WriteAsDefault<bool, bool>,
            T6: ::planus::WriteAsDefault<u64, u64>,
            T7: ::planus::WriteAsOptional<::planus::Offset<self::ExtendedTypeAnnotation>>,
            T8: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAs<::planus::Offset<DataType>>
        for DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, T8)>
    {
        type Prepared = ::planus::Offset<DataType>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<DataType> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::BasicType, self::BasicType>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAs<::planus::Offset<str>>,
            T3: ::planus::WriteAsOptional<::planus::Offset<[::planus::Offset<str>]>>,
            T4: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::DataType>]>>,
            T5: ::planus::WriteAsDefault<bool, bool>,
            T6: ::planus::WriteAsDefault<u64, u64>,
            T7: ::planus::WriteAsOptional<::planus::Offset<self::ExtendedTypeAnnotation>>,
            T8: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAsOptional<::planus::Offset<DataType>>
        for DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, T8)>
    {
        type Prepared = ::planus::Offset<DataType>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<DataType>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::BasicType, self::BasicType>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAs<::planus::Offset<str>>,
            T3: ::planus::WriteAsOptional<::planus::Offset<[::planus::Offset<str>]>>,
            T4: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::DataType>]>>,
            T5: ::planus::WriteAsDefault<bool, bool>,
            T6: ::planus::WriteAsDefault<u64, u64>,
            T7: ::planus::WriteAsOptional<::planus::Offset<self::ExtendedTypeAnnotation>>,
            T8: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAsOffset<DataType>
        for DataTypeBuilder<(T0, T1, T2, T3, T4, T5, T6, T7, T8)>
    {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<DataType> {
            let (v0, v1, v2, v3, v4, v5, v6, v7, v8) = &self.0;
            DataType::create(builder, v0, v1, v2, v3, v4, v5, v6, v7, v8)
        }
    }

    /// Reference to a deserialized [DataType].
    #[derive(Copy, Clone)]
    pub struct DataTypeRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> DataTypeRef<'a> {
        /// Getter for the [`basic_type` field](DataType#structfield.basic_type).
        #[inline]
        pub fn basic_type(&self) -> ::planus::Result<self::BasicType> {
            ::core::result::Result::Ok(
                self.0
                    .access(0, "DataType", "basic_type")?
                    .unwrap_or(self::BasicType::Unit),
            )
        }

        /// Getter for the [`schema_id` field](DataType#structfield.schema_id).
        #[inline]
        pub fn schema_id(&self) -> ::planus::Result<u32> {
            ::core::result::Result::Ok(self.0.access(1, "DataType", "schema_id")?.unwrap_or(0))
        }

        /// Getter for the [`field_name` field](DataType#structfield.field_name).
        #[inline]
        pub fn field_name(&self) -> ::planus::Result<&'a ::core::primitive::str> {
            self.0.access_required(2, "DataType", "field_name")
        }

        /// Getter for the [`field_aliases` field](DataType#structfield.field_aliases).
        #[inline]
        pub fn field_aliases(
            &self,
        ) -> ::planus::Result<
            ::core::option::Option<
                ::planus::Vector<'a, ::planus::Result<&'a ::core::primitive::str>>,
            >,
        > {
            self.0.access(3, "DataType", "field_aliases")
        }

        /// Getter for the [`children` field](DataType#structfield.children).
        #[inline]
        pub fn children(
            &self,
        ) -> ::planus::Result<::planus::Vector<'a, ::planus::Result<self::DataTypeRef<'a>>>>
        {
            self.0.access_required(4, "DataType", "children")
        }

        /// Getter for the [`signed` field](DataType#structfield.signed).
        #[inline]
        pub fn signed(&self) -> ::planus::Result<bool> {
            ::core::result::Result::Ok(self.0.access(5, "DataType", "signed")?.unwrap_or(false))
        }

        /// Getter for the [`fixed_size` field](DataType#structfield.fixed_size).
        #[inline]
        pub fn fixed_size(&self) -> ::planus::Result<u64> {
            ::core::result::Result::Ok(self.0.access(6, "DataType", "fixed_size")?.unwrap_or(0))
        }

        /// Getter for the [`extended_type` field](DataType#structfield.extended_type).
        #[inline]
        pub fn extended_type(
            &self,
        ) -> ::planus::Result<::core::option::Option<self::ExtendedTypeAnnotationRef<'a>>> {
            self.0.access(7, "DataType", "extended_type")
        }

        /// Getter for the [`lookup` field](DataType#structfield.lookup).
        #[inline]
        pub fn lookup(&self) -> ::planus::Result<::core::option::Option<self::HashLookupRef<'a>>> {
            self.0.access(8, "DataType", "lookup")
        }
    }

    impl<'a> ::core::fmt::Debug for DataTypeRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("DataTypeRef");
            f.field("basic_type", &self.basic_type());
            f.field("schema_id", &self.schema_id());
            f.field("field_name", &self.field_name());
            if let ::core::option::Option::Some(field_field_aliases) =
                self.field_aliases().transpose()
            {
                f.field("field_aliases", &field_field_aliases);
            }
            f.field("children", &self.children());
            f.field("signed", &self.signed());
            f.field("fixed_size", &self.fixed_size());
            if let ::core::option::Option::Some(field_extended_type) =
                self.extended_type().transpose()
            {
                f.field("extended_type", &field_extended_type);
            }
            if let ::core::option::Option::Some(field_lookup) = self.lookup().transpose() {
                f.field("lookup", &field_lookup);
            }
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<DataTypeRef<'a>> for DataType {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: DataTypeRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                basic_type: ::core::convert::TryInto::try_into(value.basic_type()?)?,
                schema_id: ::core::convert::TryInto::try_into(value.schema_id()?)?,
                field_name: ::core::convert::Into::into(value.field_name()?),
                field_aliases: if let ::core::option::Option::Some(field_aliases) =
                    value.field_aliases()?
                {
                    ::core::option::Option::Some(field_aliases.to_vec_result()?)
                } else {
                    ::core::option::Option::None
                },
                children: value.children()?.to_vec_result()?,
                signed: ::core::convert::TryInto::try_into(value.signed()?)?,
                fixed_size: ::core::convert::TryInto::try_into(value.fixed_size()?)?,
                extended_type: if let ::core::option::Option::Some(extended_type) =
                    value.extended_type()?
                {
                    ::core::option::Option::Some(::planus::alloc::boxed::Box::new(
                        ::core::convert::TryInto::try_into(extended_type)?,
                    ))
                } else {
                    ::core::option::Option::None
                },
                lookup: if let ::core::option::Option::Some(lookup) = value.lookup()? {
                    ::core::option::Option::Some(::planus::alloc::boxed::Box::new(
                        ::core::convert::TryInto::try_into(lookup)?,
                    ))
                } else {
                    ::core::option::Option::None
                },
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for DataTypeRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for DataTypeRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location("[DataTypeRef]", "get", buffer.offset_from_start)
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<DataType>> for DataType {
        type Value = ::planus::Offset<DataType>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<DataType>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for DataTypeRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[DataTypeRef]", "read_as_root", 0)
            })
        }
    }

    /// The table `Field`
    ///
    /// Generated from these locations:
    /// * Table `Field` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:57`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct Field {
        /// The field `data_type` in the table `Field`
        pub data_type: ::planus::alloc::boxed::Box<self::DataType>,
        /// The field `internal_field_annotation` in the table `Field`
        pub internal_field_annotation:
            ::core::option::Option<::planus::alloc::boxed::Box<self::InternalFieldAnnotation>>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for Field {
        fn default() -> Self {
            Self {
                data_type: ::core::default::Default::default(),
                internal_field_annotation: ::core::default::Default::default(),
            }
        }
    }

    impl Field {
        /// Creates a [FieldBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> FieldBuilder<()> {
            FieldBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_data_type: impl ::planus::WriteAs<::planus::Offset<self::DataType>>,
            field_internal_field_annotation: impl ::planus::WriteAsOptional<
                ::planus::Offset<self::InternalFieldAnnotation>,
            >,
        ) -> ::planus::Offset<Self> {
            let prepared_data_type = field_data_type.prepare(builder);
            let prepared_internal_field_annotation =
                field_internal_field_annotation.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<8> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<self::DataType>>(0);
            if prepared_internal_field_annotation.is_some() {
                table_writer.write_entry::<::planus::Offset<self::InternalFieldAnnotation>>(1);
            }

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_data_type);
                    if let ::core::option::Option::Some(prepared_internal_field_annotation) =
                        prepared_internal_field_annotation
                    {
                        object_writer.write::<_, _, 4>(&prepared_internal_field_annotation);
                    }
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<Field>> for Field {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Field> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<Field>> for Field {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<Field>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<Field> for Field {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Field> {
            Field::create(builder, &self.data_type, &self.internal_field_annotation)
        }
    }

    /// Builder for serializing an instance of the [Field] type.
    ///
    /// Can be created using the [Field::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct FieldBuilder<State>(State);

    impl FieldBuilder<()> {
        /// Setter for the [`data_type` field](Field#structfield.data_type).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn data_type<T0>(self, value: T0) -> FieldBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<self::DataType>>,
        {
            FieldBuilder((value,))
        }
    }

    impl<T0> FieldBuilder<(T0,)> {
        /// Setter for the [`internal_field_annotation` field](Field#structfield.internal_field_annotation).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn internal_field_annotation<T1>(self, value: T1) -> FieldBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAsOptional<::planus::Offset<self::InternalFieldAnnotation>>,
        {
            let (v0,) = self.0;
            FieldBuilder((v0, value))
        }

        /// Sets the [`internal_field_annotation` field](Field#structfield.internal_field_annotation) to null.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn internal_field_annotation_as_null(self) -> FieldBuilder<(T0, ())> {
            self.internal_field_annotation(())
        }
    }

    impl<T0, T1> FieldBuilder<(T0, T1)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [Field].
        #[inline]
        pub fn finish(self, builder: &mut ::planus::Builder) -> ::planus::Offset<Field>
        where
            Self: ::planus::WriteAsOffset<Field>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<self::DataType>>,
            T1: ::planus::WriteAsOptional<::planus::Offset<self::InternalFieldAnnotation>>,
        > ::planus::WriteAs<::planus::Offset<Field>> for FieldBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<Field>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Field> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<self::DataType>>,
            T1: ::planus::WriteAsOptional<::planus::Offset<self::InternalFieldAnnotation>>,
        > ::planus::WriteAsOptional<::planus::Offset<Field>> for FieldBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<Field>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<Field>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<self::DataType>>,
            T1: ::planus::WriteAsOptional<::planus::Offset<self::InternalFieldAnnotation>>,
        > ::planus::WriteAsOffset<Field> for FieldBuilder<(T0, T1)>
    {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Field> {
            let (v0, v1) = &self.0;
            Field::create(builder, v0, v1)
        }
    }

    /// Reference to a deserialized [Field].
    #[derive(Copy, Clone)]
    pub struct FieldRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> FieldRef<'a> {
        /// Getter for the [`data_type` field](Field#structfield.data_type).
        #[inline]
        pub fn data_type(&self) -> ::planus::Result<self::DataTypeRef<'a>> {
            self.0.access_required(0, "Field", "data_type")
        }

        /// Getter for the [`internal_field_annotation` field](Field#structfield.internal_field_annotation).
        #[inline]
        pub fn internal_field_annotation(
            &self,
        ) -> ::planus::Result<::core::option::Option<self::InternalFieldAnnotationRef<'a>>>
        {
            self.0.access(1, "Field", "internal_field_annotation")
        }
    }

    impl<'a> ::core::fmt::Debug for FieldRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("FieldRef");
            f.field("data_type", &self.data_type());
            if let ::core::option::Option::Some(field_internal_field_annotation) =
                self.internal_field_annotation().transpose()
            {
                f.field(
                    "internal_field_annotation",
                    &field_internal_field_annotation,
                );
            }
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<FieldRef<'a>> for Field {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: FieldRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                data_type: ::planus::alloc::boxed::Box::new(::core::convert::TryInto::try_into(
                    value.data_type()?,
                )?),
                internal_field_annotation: if let ::core::option::Option::Some(
                    internal_field_annotation,
                ) = value.internal_field_annotation()?
                {
                    ::core::option::Option::Some(::planus::alloc::boxed::Box::new(
                        ::core::convert::TryInto::try_into(internal_field_annotation)?,
                    ))
                } else {
                    ::core::option::Option::None
                },
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for FieldRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for FieldRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location("[FieldRef]", "get", buffer.offset_from_start)
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<Field>> for Field {
        type Value = ::planus::Offset<Field>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<Field>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for FieldRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| error_kind.with_error_location("[FieldRef]", "read_as_root", 0))
        }
    }

    /// The table `Schema`
    ///
    /// Generated from these locations:
    /// * Table `Schema` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:64`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct Schema {
        /// The field `fields` in the table `Schema`
        pub fields: ::planus::alloc::vec::Vec<self::Field>,
        /// The field `schema_id_count` in the table `Schema`
        pub schema_id_count: u32,
        /// The field `field_lookup` in the table `Schema`
        pub field_lookup: ::core::option::Option<::planus::alloc::boxed::Box<self::HashLookup>>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for Schema {
        fn default() -> Self {
            Self {
                fields: ::core::default::Default::default(),
                schema_id_count: 0,
                field_lookup: ::core::default::Default::default(),
            }
        }
    }

    impl Schema {
        /// Creates a [SchemaBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> SchemaBuilder<()> {
            SchemaBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_fields: impl ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::Field>]>>,
            field_schema_id_count: impl ::planus::WriteAsDefault<u32, u32>,
            field_field_lookup: impl ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        ) -> ::planus::Offset<Self> {
            let prepared_fields = field_fields.prepare(builder);
            let prepared_schema_id_count = field_schema_id_count.prepare(builder, &0);
            let prepared_field_lookup = field_field_lookup.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<10> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<[::planus::Offset<self::Field>]>>(0);
            if prepared_schema_id_count.is_some() {
                table_writer.write_entry::<u32>(1);
            }
            if prepared_field_lookup.is_some() {
                table_writer.write_entry::<::planus::Offset<self::HashLookup>>(2);
            }

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_fields);
                    if let ::core::option::Option::Some(prepared_schema_id_count) =
                        prepared_schema_id_count
                    {
                        object_writer.write::<_, _, 4>(&prepared_schema_id_count);
                    }
                    if let ::core::option::Option::Some(prepared_field_lookup) =
                        prepared_field_lookup
                    {
                        object_writer.write::<_, _, 4>(&prepared_field_lookup);
                    }
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<Schema>> for Schema {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Schema> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<Schema>> for Schema {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<Schema>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<Schema> for Schema {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Schema> {
            Schema::create(
                builder,
                &self.fields,
                self.schema_id_count,
                &self.field_lookup,
            )
        }
    }

    /// Builder for serializing an instance of the [Schema] type.
    ///
    /// Can be created using the [Schema::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct SchemaBuilder<State>(State);

    impl SchemaBuilder<()> {
        /// Setter for the [`fields` field](Schema#structfield.fields).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn fields<T0>(self, value: T0) -> SchemaBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::Field>]>>,
        {
            SchemaBuilder((value,))
        }
    }

    impl<T0> SchemaBuilder<(T0,)> {
        /// Setter for the [`schema_id_count` field](Schema#structfield.schema_id_count).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn schema_id_count<T1>(self, value: T1) -> SchemaBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAsDefault<u32, u32>,
        {
            let (v0,) = self.0;
            SchemaBuilder((v0, value))
        }

        /// Sets the [`schema_id_count` field](Schema#structfield.schema_id_count) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn schema_id_count_as_default(self) -> SchemaBuilder<(T0, ::planus::DefaultValue)> {
            self.schema_id_count(::planus::DefaultValue)
        }
    }

    impl<T0, T1> SchemaBuilder<(T0, T1)> {
        /// Setter for the [`field_lookup` field](Schema#structfield.field_lookup).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn field_lookup<T2>(self, value: T2) -> SchemaBuilder<(T0, T1, T2)>
        where
            T2: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        {
            let (v0, v1) = self.0;
            SchemaBuilder((v0, v1, value))
        }

        /// Sets the [`field_lookup` field](Schema#structfield.field_lookup) to null.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn field_lookup_as_null(self) -> SchemaBuilder<(T0, T1, ())> {
            self.field_lookup(())
        }
    }

    impl<T0, T1, T2> SchemaBuilder<(T0, T1, T2)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [Schema].
        #[inline]
        pub fn finish(self, builder: &mut ::planus::Builder) -> ::planus::Offset<Schema>
        where
            Self: ::planus::WriteAsOffset<Schema>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::Field>]>>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAs<::planus::Offset<Schema>> for SchemaBuilder<(T0, T1, T2)>
    {
        type Prepared = ::planus::Offset<Schema>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Schema> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::Field>]>>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAsOptional<::planus::Offset<Schema>> for SchemaBuilder<(T0, T1, T2)>
    {
        type Prepared = ::planus::Offset<Schema>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<Schema>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::Field>]>>,
            T1: ::planus::WriteAsDefault<u32, u32>,
            T2: ::planus::WriteAsOptional<::planus::Offset<self::HashLookup>>,
        > ::planus::WriteAsOffset<Schema> for SchemaBuilder<(T0, T1, T2)>
    {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<Schema> {
            let (v0, v1, v2) = &self.0;
            Schema::create(builder, v0, v1, v2)
        }
    }

    /// Reference to a deserialized [Schema].
    #[derive(Copy, Clone)]
    pub struct SchemaRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> SchemaRef<'a> {
        /// Getter for the [`fields` field](Schema#structfield.fields).
        #[inline]
        pub fn fields(
            &self,
        ) -> ::planus::Result<::planus::Vector<'a, ::planus::Result<self::FieldRef<'a>>>> {
            self.0.access_required(0, "Schema", "fields")
        }

        /// Getter for the [`schema_id_count` field](Schema#structfield.schema_id_count).
        #[inline]
        pub fn schema_id_count(&self) -> ::planus::Result<u32> {
            ::core::result::Result::Ok(self.0.access(1, "Schema", "schema_id_count")?.unwrap_or(0))
        }

        /// Getter for the [`field_lookup` field](Schema#structfield.field_lookup).
        #[inline]
        pub fn field_lookup(
            &self,
        ) -> ::planus::Result<::core::option::Option<self::HashLookupRef<'a>>> {
            self.0.access(2, "Schema", "field_lookup")
        }
    }

    impl<'a> ::core::fmt::Debug for SchemaRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("SchemaRef");
            f.field("fields", &self.fields());
            f.field("schema_id_count", &self.schema_id_count());
            if let ::core::option::Option::Some(field_field_lookup) =
                self.field_lookup().transpose()
            {
                f.field("field_lookup", &field_field_lookup);
            }
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<SchemaRef<'a>> for Schema {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: SchemaRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                fields: value.fields()?.to_vec_result()?,
                schema_id_count: ::core::convert::TryInto::try_into(value.schema_id_count()?)?,
                field_lookup: if let ::core::option::Option::Some(field_lookup) =
                    value.field_lookup()?
                {
                    ::core::option::Option::Some(::planus::alloc::boxed::Box::new(
                        ::core::convert::TryInto::try_into(field_lookup)?,
                    ))
                } else {
                    ::core::option::Option::None
                },
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for SchemaRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for SchemaRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location("[SchemaRef]", "get", buffer.offset_from_start)
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<Schema>> for Schema {
        type Value = ::planus::Offset<Schema>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<Schema>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for SchemaRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| error_kind.with_error_location("[SchemaRef]", "read_as_root", 0))
        }
    }

    /// The table `ExtendedTypeAnnotation`
    ///
    /// Generated from these locations:
    /// * Table `ExtendedTypeAnnotation` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:72`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct ExtendedTypeAnnotation {
        /// The field `label` in the table `ExtendedTypeAnnotation`
        pub label: ::planus::alloc::string::String,
        /// The field `properties` in the table `ExtendedTypeAnnotation`
        pub properties: ::planus::alloc::vec::Vec<self::ExtendedTypeProperty>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for ExtendedTypeAnnotation {
        fn default() -> Self {
            Self {
                label: ::core::default::Default::default(),
                properties: ::core::default::Default::default(),
            }
        }
    }

    impl ExtendedTypeAnnotation {
        /// Creates a [ExtendedTypeAnnotationBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> ExtendedTypeAnnotationBuilder<()> {
            ExtendedTypeAnnotationBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_label: impl ::planus::WriteAs<::planus::Offset<str>>,
            field_properties: impl ::planus::WriteAs<
                ::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>,
            >,
        ) -> ::planus::Offset<Self> {
            let prepared_label = field_label.prepare(builder);
            let prepared_properties = field_properties.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<8> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<str>>(0);
            table_writer
                .write_entry::<::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>>(1);

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_label);
                    object_writer.write::<_, _, 4>(&prepared_properties);
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<ExtendedTypeAnnotation>> for ExtendedTypeAnnotation {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeAnnotation> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<ExtendedTypeAnnotation>>
        for ExtendedTypeAnnotation
    {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<ExtendedTypeAnnotation>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<ExtendedTypeAnnotation> for ExtendedTypeAnnotation {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeAnnotation> {
            ExtendedTypeAnnotation::create(builder, &self.label, &self.properties)
        }
    }

    /// Builder for serializing an instance of the [ExtendedTypeAnnotation] type.
    ///
    /// Can be created using the [ExtendedTypeAnnotation::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct ExtendedTypeAnnotationBuilder<State>(State);

    impl ExtendedTypeAnnotationBuilder<()> {
        /// Setter for the [`label` field](ExtendedTypeAnnotation#structfield.label).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn label<T0>(self, value: T0) -> ExtendedTypeAnnotationBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<str>>,
        {
            ExtendedTypeAnnotationBuilder((value,))
        }
    }

    impl<T0> ExtendedTypeAnnotationBuilder<(T0,)> {
        /// Setter for the [`properties` field](ExtendedTypeAnnotation#structfield.properties).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn properties<T1>(self, value: T1) -> ExtendedTypeAnnotationBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>>,
        {
            let (v0,) = self.0;
            ExtendedTypeAnnotationBuilder((v0, value))
        }
    }

    impl<T0, T1> ExtendedTypeAnnotationBuilder<(T0, T1)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [ExtendedTypeAnnotation].
        #[inline]
        pub fn finish(
            self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeAnnotation>
        where
            Self: ::planus::WriteAsOffset<ExtendedTypeAnnotation>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>>,
        > ::planus::WriteAs<::planus::Offset<ExtendedTypeAnnotation>>
        for ExtendedTypeAnnotationBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<ExtendedTypeAnnotation>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeAnnotation> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>>,
        > ::planus::WriteAsOptional<::planus::Offset<ExtendedTypeAnnotation>>
        for ExtendedTypeAnnotationBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<ExtendedTypeAnnotation>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<ExtendedTypeAnnotation>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<[::planus::Offset<self::ExtendedTypeProperty>]>>,
        > ::planus::WriteAsOffset<ExtendedTypeAnnotation>
        for ExtendedTypeAnnotationBuilder<(T0, T1)>
    {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeAnnotation> {
            let (v0, v1) = &self.0;
            ExtendedTypeAnnotation::create(builder, v0, v1)
        }
    }

    /// Reference to a deserialized [ExtendedTypeAnnotation].
    #[derive(Copy, Clone)]
    pub struct ExtendedTypeAnnotationRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> ExtendedTypeAnnotationRef<'a> {
        /// Getter for the [`label` field](ExtendedTypeAnnotation#structfield.label).
        #[inline]
        pub fn label(&self) -> ::planus::Result<&'a ::core::primitive::str> {
            self.0.access_required(0, "ExtendedTypeAnnotation", "label")
        }

        /// Getter for the [`properties` field](ExtendedTypeAnnotation#structfield.properties).
        #[inline]
        pub fn properties(
            &self,
        ) -> ::planus::Result<
            ::planus::Vector<'a, ::planus::Result<self::ExtendedTypePropertyRef<'a>>>,
        > {
            self.0
                .access_required(1, "ExtendedTypeAnnotation", "properties")
        }
    }

    impl<'a> ::core::fmt::Debug for ExtendedTypeAnnotationRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("ExtendedTypeAnnotationRef");
            f.field("label", &self.label());
            f.field("properties", &self.properties());
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<ExtendedTypeAnnotationRef<'a>> for ExtendedTypeAnnotation {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: ExtendedTypeAnnotationRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                label: ::core::convert::Into::into(value.label()?),
                properties: value.properties()?.to_vec_result()?,
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for ExtendedTypeAnnotationRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for ExtendedTypeAnnotationRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location(
                    "[ExtendedTypeAnnotationRef]",
                    "get",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<ExtendedTypeAnnotation>>
        for ExtendedTypeAnnotation
    {
        type Value = ::planus::Offset<ExtendedTypeAnnotation>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<ExtendedTypeAnnotation>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for ExtendedTypeAnnotationRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[ExtendedTypeAnnotationRef]", "read_as_root", 0)
            })
        }
    }

    /// The table `ExtendedTypeProperty`
    ///
    /// Generated from these locations:
    /// * Table `ExtendedTypeProperty` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:77`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct ExtendedTypeProperty {
        /// The field `name` in the table `ExtendedTypeProperty`
        pub name: ::planus::alloc::string::String,
        /// The field `value` in the table `ExtendedTypeProperty`
        pub value: ::planus::alloc::boxed::Box<self::PropertyValue>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for ExtendedTypeProperty {
        fn default() -> Self {
            Self {
                name: ::core::default::Default::default(),
                value: ::core::default::Default::default(),
            }
        }
    }

    impl ExtendedTypeProperty {
        /// Creates a [ExtendedTypePropertyBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> ExtendedTypePropertyBuilder<()> {
            ExtendedTypePropertyBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_name: impl ::planus::WriteAs<::planus::Offset<str>>,
            field_value: impl ::planus::WriteAs<::planus::Offset<self::PropertyValue>>,
        ) -> ::planus::Offset<Self> {
            let prepared_name = field_name.prepare(builder);
            let prepared_value = field_value.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<8> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<str>>(0);
            table_writer.write_entry::<::planus::Offset<self::PropertyValue>>(1);

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_name);
                    object_writer.write::<_, _, 4>(&prepared_value);
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<ExtendedTypeProperty>> for ExtendedTypeProperty {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeProperty> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<ExtendedTypeProperty>> for ExtendedTypeProperty {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<ExtendedTypeProperty>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<ExtendedTypeProperty> for ExtendedTypeProperty {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeProperty> {
            ExtendedTypeProperty::create(builder, &self.name, &self.value)
        }
    }

    /// Builder for serializing an instance of the [ExtendedTypeProperty] type.
    ///
    /// Can be created using the [ExtendedTypeProperty::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct ExtendedTypePropertyBuilder<State>(State);

    impl ExtendedTypePropertyBuilder<()> {
        /// Setter for the [`name` field](ExtendedTypeProperty#structfield.name).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn name<T0>(self, value: T0) -> ExtendedTypePropertyBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<str>>,
        {
            ExtendedTypePropertyBuilder((value,))
        }
    }

    impl<T0> ExtendedTypePropertyBuilder<(T0,)> {
        /// Setter for the [`value` field](ExtendedTypeProperty#structfield.value).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn value<T1>(self, value: T1) -> ExtendedTypePropertyBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAs<::planus::Offset<self::PropertyValue>>,
        {
            let (v0,) = self.0;
            ExtendedTypePropertyBuilder((v0, value))
        }
    }

    impl<T0, T1> ExtendedTypePropertyBuilder<(T0, T1)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [ExtendedTypeProperty].
        #[inline]
        pub fn finish(
            self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeProperty>
        where
            Self: ::planus::WriteAsOffset<ExtendedTypeProperty>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<self::PropertyValue>>,
        > ::planus::WriteAs<::planus::Offset<ExtendedTypeProperty>>
        for ExtendedTypePropertyBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<ExtendedTypeProperty>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeProperty> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<self::PropertyValue>>,
        > ::planus::WriteAsOptional<::planus::Offset<ExtendedTypeProperty>>
        for ExtendedTypePropertyBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<ExtendedTypeProperty>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<ExtendedTypeProperty>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<str>>,
            T1: ::planus::WriteAs<::planus::Offset<self::PropertyValue>>,
        > ::planus::WriteAsOffset<ExtendedTypeProperty> for ExtendedTypePropertyBuilder<(T0, T1)>
    {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<ExtendedTypeProperty> {
            let (v0, v1) = &self.0;
            ExtendedTypeProperty::create(builder, v0, v1)
        }
    }

    /// Reference to a deserialized [ExtendedTypeProperty].
    #[derive(Copy, Clone)]
    pub struct ExtendedTypePropertyRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> ExtendedTypePropertyRef<'a> {
        /// Getter for the [`name` field](ExtendedTypeProperty#structfield.name).
        #[inline]
        pub fn name(&self) -> ::planus::Result<&'a ::core::primitive::str> {
            self.0.access_required(0, "ExtendedTypeProperty", "name")
        }

        /// Getter for the [`value` field](ExtendedTypeProperty#structfield.value).
        #[inline]
        pub fn value(&self) -> ::planus::Result<self::PropertyValueRef<'a>> {
            self.0.access_required(1, "ExtendedTypeProperty", "value")
        }
    }

    impl<'a> ::core::fmt::Debug for ExtendedTypePropertyRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("ExtendedTypePropertyRef");
            f.field("name", &self.name());
            f.field("value", &self.value());
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<ExtendedTypePropertyRef<'a>> for ExtendedTypeProperty {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: ExtendedTypePropertyRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                name: ::core::convert::Into::into(value.name()?),
                value: ::planus::alloc::boxed::Box::new(::core::convert::TryInto::try_into(
                    value.value()?,
                )?),
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for ExtendedTypePropertyRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for ExtendedTypePropertyRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location(
                    "[ExtendedTypePropertyRef]",
                    "get",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<ExtendedTypeProperty>> for ExtendedTypeProperty {
        type Value = ::planus::Offset<ExtendedTypeProperty>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<ExtendedTypeProperty>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for ExtendedTypePropertyRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[ExtendedTypePropertyRef]", "read_as_root", 0)
            })
        }
    }

    /// The enum `PropertyValueKind`
    ///
    /// Generated from these locations:
    /// * Enum `PropertyValueKind` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:82`
    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        ::serde::Serialize,
        ::serde::Deserialize,
    )]
    #[repr(u8)]
    pub enum PropertyValueKind {
        /// The variant `Null` in the enum `PropertyValueKind`
        Null = 0,

        /// The variant `String` in the enum `PropertyValueKind`
        String = 1,

        /// The variant `JsonString` in the enum `PropertyValueKind`
        JsonString = 2,

        /// The variant `AnyValueProto` in the enum `PropertyValueKind`
        AnyValueProto = 3,
    }

    impl PropertyValueKind {
        /// Array containing all valid variants of PropertyValueKind
        pub const ENUM_VALUES: [Self; 4] = [
            Self::Null,
            Self::String,
            Self::JsonString,
            Self::AnyValueProto,
        ];
    }

    impl ::core::convert::TryFrom<u8> for PropertyValueKind {
        type Error = ::planus::errors::UnknownEnumTagKind;
        #[inline]
        fn try_from(
            value: u8,
        ) -> ::core::result::Result<Self, ::planus::errors::UnknownEnumTagKind> {
            #[allow(clippy::match_single_binding)]
            match value {
                0 => ::core::result::Result::Ok(PropertyValueKind::Null),
                1 => ::core::result::Result::Ok(PropertyValueKind::String),
                2 => ::core::result::Result::Ok(PropertyValueKind::JsonString),
                3 => ::core::result::Result::Ok(PropertyValueKind::AnyValueProto),

                _ => ::core::result::Result::Err(::planus::errors::UnknownEnumTagKind {
                    tag: value as i128,
                }),
            }
        }
    }

    impl ::core::convert::From<PropertyValueKind> for u8 {
        #[inline]
        fn from(value: PropertyValueKind) -> Self {
            value as u8
        }
    }

    /// # Safety
    /// The Planus compiler correctly calculates `ALIGNMENT` and `SIZE`.
    unsafe impl ::planus::Primitive for PropertyValueKind {
        const ALIGNMENT: usize = 1;
        const SIZE: usize = 1;
    }

    impl ::planus::WriteAsPrimitive<PropertyValueKind> for PropertyValueKind {
        #[inline]
        fn write<const N: usize>(&self, cursor: ::planus::Cursor<'_, N>, buffer_position: u32) {
            (*self as u8).write(cursor, buffer_position);
        }
    }

    impl ::planus::WriteAs<PropertyValueKind> for PropertyValueKind {
        type Prepared = Self;

        #[inline]
        fn prepare(&self, _builder: &mut ::planus::Builder) -> PropertyValueKind {
            *self
        }
    }

    impl ::planus::WriteAsDefault<PropertyValueKind, PropertyValueKind> for PropertyValueKind {
        type Prepared = Self;

        #[inline]
        fn prepare(
            &self,
            _builder: &mut ::planus::Builder,
            default: &PropertyValueKind,
        ) -> ::core::option::Option<PropertyValueKind> {
            if self == default {
                ::core::option::Option::None
            } else {
                ::core::option::Option::Some(*self)
            }
        }
    }

    impl ::planus::WriteAsOptional<PropertyValueKind> for PropertyValueKind {
        type Prepared = Self;

        #[inline]
        fn prepare(
            &self,
            _builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<PropertyValueKind> {
            ::core::option::Option::Some(*self)
        }
    }

    impl<'buf> ::planus::TableRead<'buf> for PropertyValueKind {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'buf>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            let n: u8 = ::planus::TableRead::from_buffer(buffer, offset)?;
            ::core::result::Result::Ok(::core::convert::TryInto::try_into(n)?)
        }
    }

    impl<'buf> ::planus::VectorReadInner<'buf> for PropertyValueKind {
        type Error = ::planus::errors::UnknownEnumTag;
        const STRIDE: usize = 1;
        #[inline]
        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'buf>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::UnknownEnumTag> {
            let value = unsafe { *buffer.buffer.get_unchecked(offset) };
            let value: ::core::result::Result<Self, _> = ::core::convert::TryInto::try_into(value);
            value.map_err(|error_kind| {
                error_kind.with_error_location(
                    "PropertyValueKind",
                    "VectorRead::from_buffer",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<PropertyValueKind> for PropertyValueKind {
        const STRIDE: usize = 1;

        type Value = Self;

        #[inline]
        fn prepare(&self, _builder: &mut ::planus::Builder) -> Self {
            *self
        }

        #[inline]
        unsafe fn write_values(
            values: &[Self],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 1];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - i as u32,
                );
            }
        }
    }

    /// The table `PropertyValue`
    ///
    /// Generated from these locations:
    /// * Table `PropertyValue` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:93`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct PropertyValue {
        /// The field `kind` in the table `PropertyValue`
        pub kind: self::PropertyValueKind,
        /// The field `data` in the table `PropertyValue`
        pub data: ::planus::alloc::vec::Vec<u8>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for PropertyValue {
        fn default() -> Self {
            Self {
                kind: self::PropertyValueKind::Null,
                data: ::core::default::Default::default(),
            }
        }
    }

    impl PropertyValue {
        /// Creates a [PropertyValueBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> PropertyValueBuilder<()> {
            PropertyValueBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_kind: impl ::planus::WriteAsDefault<self::PropertyValueKind, self::PropertyValueKind>,
            field_data: impl ::planus::WriteAs<::planus::Offset<[u8]>>,
        ) -> ::planus::Offset<Self> {
            let prepared_kind = field_kind.prepare(builder, &self::PropertyValueKind::Null);
            let prepared_data = field_data.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<8> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<[u8]>>(1);
            if prepared_kind.is_some() {
                table_writer.write_entry::<self::PropertyValueKind>(0);
            }

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_data);
                    if let ::core::option::Option::Some(prepared_kind) = prepared_kind {
                        object_writer.write::<_, _, 1>(&prepared_kind);
                    }
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<PropertyValue>> for PropertyValue {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<PropertyValue> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<PropertyValue>> for PropertyValue {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<PropertyValue>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<PropertyValue> for PropertyValue {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<PropertyValue> {
            PropertyValue::create(builder, self.kind, &self.data)
        }
    }

    /// Builder for serializing an instance of the [PropertyValue] type.
    ///
    /// Can be created using the [PropertyValue::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct PropertyValueBuilder<State>(State);

    impl PropertyValueBuilder<()> {
        /// Setter for the [`kind` field](PropertyValue#structfield.kind).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn kind<T0>(self, value: T0) -> PropertyValueBuilder<(T0,)>
        where
            T0: ::planus::WriteAsDefault<self::PropertyValueKind, self::PropertyValueKind>,
        {
            PropertyValueBuilder((value,))
        }

        /// Sets the [`kind` field](PropertyValue#structfield.kind) to the default value.
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn kind_as_default(self) -> PropertyValueBuilder<(::planus::DefaultValue,)> {
            self.kind(::planus::DefaultValue)
        }
    }

    impl<T0> PropertyValueBuilder<(T0,)> {
        /// Setter for the [`data` field](PropertyValue#structfield.data).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn data<T1>(self, value: T1) -> PropertyValueBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAs<::planus::Offset<[u8]>>,
        {
            let (v0,) = self.0;
            PropertyValueBuilder((v0, value))
        }
    }

    impl<T0, T1> PropertyValueBuilder<(T0, T1)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [PropertyValue].
        #[inline]
        pub fn finish(self, builder: &mut ::planus::Builder) -> ::planus::Offset<PropertyValue>
        where
            Self: ::planus::WriteAsOffset<PropertyValue>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::PropertyValueKind, self::PropertyValueKind>,
            T1: ::planus::WriteAs<::planus::Offset<[u8]>>,
        > ::planus::WriteAs<::planus::Offset<PropertyValue>> for PropertyValueBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<PropertyValue>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<PropertyValue> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::PropertyValueKind, self::PropertyValueKind>,
            T1: ::planus::WriteAs<::planus::Offset<[u8]>>,
        > ::planus::WriteAsOptional<::planus::Offset<PropertyValue>>
        for PropertyValueBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<PropertyValue>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<PropertyValue>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAsDefault<self::PropertyValueKind, self::PropertyValueKind>,
            T1: ::planus::WriteAs<::planus::Offset<[u8]>>,
        > ::planus::WriteAsOffset<PropertyValue> for PropertyValueBuilder<(T0, T1)>
    {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<PropertyValue> {
            let (v0, v1) = &self.0;
            PropertyValue::create(builder, v0, v1)
        }
    }

    /// Reference to a deserialized [PropertyValue].
    #[derive(Copy, Clone)]
    pub struct PropertyValueRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> PropertyValueRef<'a> {
        /// Getter for the [`kind` field](PropertyValue#structfield.kind).
        #[inline]
        pub fn kind(&self) -> ::planus::Result<self::PropertyValueKind> {
            ::core::result::Result::Ok(
                self.0
                    .access(0, "PropertyValue", "kind")?
                    .unwrap_or(self::PropertyValueKind::Null),
            )
        }

        /// Getter for the [`data` field](PropertyValue#structfield.data).
        #[inline]
        pub fn data(&self) -> ::planus::Result<&'a [u8]> {
            self.0.access_required(1, "PropertyValue", "data")
        }
    }

    impl<'a> ::core::fmt::Debug for PropertyValueRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("PropertyValueRef");
            f.field("kind", &self.kind());
            f.field("data", &self.data());
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<PropertyValueRef<'a>> for PropertyValue {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: PropertyValueRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                kind: ::core::convert::TryInto::try_into(value.kind()?)?,
                data: value.data()?.to_vec(),
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for PropertyValueRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for PropertyValueRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location(
                    "[PropertyValueRef]",
                    "get",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<PropertyValue>> for PropertyValue {
        type Value = ::planus::Offset<PropertyValue>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<PropertyValue>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for PropertyValueRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[PropertyValueRef]", "read_as_root", 0)
            })
        }
    }

    /// The table `InternalFieldAnnotation`
    ///
    /// Generated from these locations:
    /// * Table `InternalFieldAnnotation` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:98`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct InternalFieldAnnotation {
        /// The field `kind` in the table `InternalFieldAnnotation`
        pub kind: ::planus::alloc::string::String,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for InternalFieldAnnotation {
        fn default() -> Self {
            Self {
                kind: ::core::default::Default::default(),
            }
        }
    }

    impl InternalFieldAnnotation {
        /// Creates a [InternalFieldAnnotationBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> InternalFieldAnnotationBuilder<()> {
            InternalFieldAnnotationBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_kind: impl ::planus::WriteAs<::planus::Offset<str>>,
        ) -> ::planus::Offset<Self> {
            let prepared_kind = field_kind.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<6> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<str>>(0);

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_kind);
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<InternalFieldAnnotation>> for InternalFieldAnnotation {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<InternalFieldAnnotation> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<InternalFieldAnnotation>>
        for InternalFieldAnnotation
    {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<InternalFieldAnnotation>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<InternalFieldAnnotation> for InternalFieldAnnotation {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<InternalFieldAnnotation> {
            InternalFieldAnnotation::create(builder, &self.kind)
        }
    }

    /// Builder for serializing an instance of the [InternalFieldAnnotation] type.
    ///
    /// Can be created using the [InternalFieldAnnotation::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct InternalFieldAnnotationBuilder<State>(State);

    impl InternalFieldAnnotationBuilder<()> {
        /// Setter for the [`kind` field](InternalFieldAnnotation#structfield.kind).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn kind<T0>(self, value: T0) -> InternalFieldAnnotationBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<str>>,
        {
            InternalFieldAnnotationBuilder((value,))
        }
    }

    impl<T0> InternalFieldAnnotationBuilder<(T0,)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [InternalFieldAnnotation].
        #[inline]
        pub fn finish(
            self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<InternalFieldAnnotation>
        where
            Self: ::planus::WriteAsOffset<InternalFieldAnnotation>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<T0: ::planus::WriteAs<::planus::Offset<str>>>
        ::planus::WriteAs<::planus::Offset<InternalFieldAnnotation>>
        for InternalFieldAnnotationBuilder<(T0,)>
    {
        type Prepared = ::planus::Offset<InternalFieldAnnotation>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<InternalFieldAnnotation> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<T0: ::planus::WriteAs<::planus::Offset<str>>>
        ::planus::WriteAsOptional<::planus::Offset<InternalFieldAnnotation>>
        for InternalFieldAnnotationBuilder<(T0,)>
    {
        type Prepared = ::planus::Offset<InternalFieldAnnotation>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<InternalFieldAnnotation>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<T0: ::planus::WriteAs<::planus::Offset<str>>>
        ::planus::WriteAsOffset<InternalFieldAnnotation> for InternalFieldAnnotationBuilder<(T0,)>
    {
        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::planus::Offset<InternalFieldAnnotation> {
            let (v0,) = &self.0;
            InternalFieldAnnotation::create(builder, v0)
        }
    }

    /// Reference to a deserialized [InternalFieldAnnotation].
    #[derive(Copy, Clone)]
    pub struct InternalFieldAnnotationRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> InternalFieldAnnotationRef<'a> {
        /// Getter for the [`kind` field](InternalFieldAnnotation#structfield.kind).
        #[inline]
        pub fn kind(&self) -> ::planus::Result<&'a ::core::primitive::str> {
            self.0.access_required(0, "InternalFieldAnnotation", "kind")
        }
    }

    impl<'a> ::core::fmt::Debug for InternalFieldAnnotationRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("InternalFieldAnnotationRef");
            f.field("kind", &self.kind());
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<InternalFieldAnnotationRef<'a>> for InternalFieldAnnotation {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: InternalFieldAnnotationRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                kind: ::core::convert::Into::into(value.kind()?),
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for InternalFieldAnnotationRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for InternalFieldAnnotationRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location(
                    "[InternalFieldAnnotationRef]",
                    "get",
                    buffer.offset_from_start,
                )
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<InternalFieldAnnotation>>
        for InternalFieldAnnotation
    {
        type Value = ::planus::Offset<InternalFieldAnnotation>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<InternalFieldAnnotation>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for InternalFieldAnnotationRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[InternalFieldAnnotationRef]", "read_as_root", 0)
            })
        }
    }

    /// The table `HashLookup`
    ///
    /// Generated from these locations:
    /// * Table `HashLookup` in the file `/home/evgeneyr/amudai/proto_defs/shard_format/schema.fbs:107`
    #[derive(
        Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, ::serde::Serialize, ::serde::Deserialize,
    )]
    pub struct HashLookup {
        /// The field `buckets` in the table `HashLookup`
        pub buckets: ::planus::alloc::vec::Vec<u32>,
        /// The field `entries` in the table `HashLookup`
        pub entries: ::planus::alloc::vec::Vec<u32>,
    }

    #[allow(clippy::derivable_impls)]
    impl ::core::default::Default for HashLookup {
        fn default() -> Self {
            Self {
                buckets: ::core::default::Default::default(),
                entries: ::core::default::Default::default(),
            }
        }
    }

    impl HashLookup {
        /// Creates a [HashLookupBuilder] for serializing an instance of this table.
        #[inline]
        pub fn builder() -> HashLookupBuilder<()> {
            HashLookupBuilder(())
        }

        #[allow(clippy::too_many_arguments)]
        pub fn create(
            builder: &mut ::planus::Builder,
            field_buckets: impl ::planus::WriteAs<::planus::Offset<[u32]>>,
            field_entries: impl ::planus::WriteAs<::planus::Offset<[u32]>>,
        ) -> ::planus::Offset<Self> {
            let prepared_buckets = field_buckets.prepare(builder);
            let prepared_entries = field_entries.prepare(builder);

            let mut table_writer: ::planus::table_writer::TableWriter<8> =
                ::core::default::Default::default();
            table_writer.write_entry::<::planus::Offset<[u32]>>(0);
            table_writer.write_entry::<::planus::Offset<[u32]>>(1);

            unsafe {
                table_writer.finish(builder, |object_writer| {
                    object_writer.write::<_, _, 4>(&prepared_buckets);
                    object_writer.write::<_, _, 4>(&prepared_entries);
                });
            }
            builder.current_offset()
        }
    }

    impl ::planus::WriteAs<::planus::Offset<HashLookup>> for HashLookup {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<HashLookup> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl ::planus::WriteAsOptional<::planus::Offset<HashLookup>> for HashLookup {
        type Prepared = ::planus::Offset<Self>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<HashLookup>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl ::planus::WriteAsOffset<HashLookup> for HashLookup {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<HashLookup> {
            HashLookup::create(builder, &self.buckets, &self.entries)
        }
    }

    /// Builder for serializing an instance of the [HashLookup] type.
    ///
    /// Can be created using the [HashLookup::builder] method.
    #[derive(Debug)]
    #[must_use]
    pub struct HashLookupBuilder<State>(State);

    impl HashLookupBuilder<()> {
        /// Setter for the [`buckets` field](HashLookup#structfield.buckets).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn buckets<T0>(self, value: T0) -> HashLookupBuilder<(T0,)>
        where
            T0: ::planus::WriteAs<::planus::Offset<[u32]>>,
        {
            HashLookupBuilder((value,))
        }
    }

    impl<T0> HashLookupBuilder<(T0,)> {
        /// Setter for the [`entries` field](HashLookup#structfield.entries).
        #[inline]
        #[allow(clippy::type_complexity)]
        pub fn entries<T1>(self, value: T1) -> HashLookupBuilder<(T0, T1)>
        where
            T1: ::planus::WriteAs<::planus::Offset<[u32]>>,
        {
            let (v0,) = self.0;
            HashLookupBuilder((v0, value))
        }
    }

    impl<T0, T1> HashLookupBuilder<(T0, T1)> {
        /// Finish writing the builder to get an [Offset](::planus::Offset) to a serialized [HashLookup].
        #[inline]
        pub fn finish(self, builder: &mut ::planus::Builder) -> ::planus::Offset<HashLookup>
        where
            Self: ::planus::WriteAsOffset<HashLookup>,
        {
            ::planus::WriteAsOffset::prepare(&self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[u32]>>,
            T1: ::planus::WriteAs<::planus::Offset<[u32]>>,
        > ::planus::WriteAs<::planus::Offset<HashLookup>> for HashLookupBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<HashLookup>;

        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<HashLookup> {
            ::planus::WriteAsOffset::prepare(self, builder)
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[u32]>>,
            T1: ::planus::WriteAs<::planus::Offset<[u32]>>,
        > ::planus::WriteAsOptional<::planus::Offset<HashLookup>> for HashLookupBuilder<(T0, T1)>
    {
        type Prepared = ::planus::Offset<HashLookup>;

        #[inline]
        fn prepare(
            &self,
            builder: &mut ::planus::Builder,
        ) -> ::core::option::Option<::planus::Offset<HashLookup>> {
            ::core::option::Option::Some(::planus::WriteAsOffset::prepare(self, builder))
        }
    }

    impl<
            T0: ::planus::WriteAs<::planus::Offset<[u32]>>,
            T1: ::planus::WriteAs<::planus::Offset<[u32]>>,
        > ::planus::WriteAsOffset<HashLookup> for HashLookupBuilder<(T0, T1)>
    {
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> ::planus::Offset<HashLookup> {
            let (v0, v1) = &self.0;
            HashLookup::create(builder, v0, v1)
        }
    }

    /// Reference to a deserialized [HashLookup].
    #[derive(Copy, Clone)]
    pub struct HashLookupRef<'a>(::planus::table_reader::Table<'a>);

    impl<'a> HashLookupRef<'a> {
        /// Getter for the [`buckets` field](HashLookup#structfield.buckets).
        #[inline]
        pub fn buckets(&self) -> ::planus::Result<::planus::Vector<'a, u32>> {
            self.0.access_required(0, "HashLookup", "buckets")
        }

        /// Getter for the [`entries` field](HashLookup#structfield.entries).
        #[inline]
        pub fn entries(&self) -> ::planus::Result<::planus::Vector<'a, u32>> {
            self.0.access_required(1, "HashLookup", "entries")
        }
    }

    impl<'a> ::core::fmt::Debug for HashLookupRef<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            let mut f = f.debug_struct("HashLookupRef");
            f.field("buckets", &self.buckets());
            f.field("entries", &self.entries());
            f.finish()
        }
    }

    impl<'a> ::core::convert::TryFrom<HashLookupRef<'a>> for HashLookup {
        type Error = ::planus::Error;

        #[allow(unreachable_code)]
        fn try_from(value: HashLookupRef<'a>) -> ::planus::Result<Self> {
            ::core::result::Result::Ok(Self {
                buckets: value.buckets()?.to_vec()?,
                entries: value.entries()?.to_vec()?,
            })
        }
    }

    impl<'a> ::planus::TableRead<'a> for HashLookupRef<'a> {
        #[inline]
        fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::core::result::Result<Self, ::planus::errors::ErrorKind> {
            ::core::result::Result::Ok(Self(::planus::table_reader::Table::from_buffer(
                buffer, offset,
            )?))
        }
    }

    impl<'a> ::planus::VectorReadInner<'a> for HashLookupRef<'a> {
        type Error = ::planus::Error;
        const STRIDE: usize = 4;

        unsafe fn from_buffer(
            buffer: ::planus::SliceWithStartOffset<'a>,
            offset: usize,
        ) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(buffer, offset).map_err(|error_kind| {
                error_kind.with_error_location("[HashLookupRef]", "get", buffer.offset_from_start)
            })
        }
    }

    /// # Safety
    /// The planus compiler generates implementations that initialize
    /// the bytes in `write_values`.
    unsafe impl ::planus::VectorWrite<::planus::Offset<HashLookup>> for HashLookup {
        type Value = ::planus::Offset<HashLookup>;
        const STRIDE: usize = 4;
        #[inline]
        fn prepare(&self, builder: &mut ::planus::Builder) -> Self::Value {
            ::planus::WriteAs::prepare(self, builder)
        }

        #[inline]
        unsafe fn write_values(
            values: &[::planus::Offset<HashLookup>],
            bytes: *mut ::core::mem::MaybeUninit<u8>,
            buffer_position: u32,
        ) {
            let bytes = bytes as *mut [::core::mem::MaybeUninit<u8>; 4];
            for (i, v) in ::core::iter::Iterator::enumerate(values.iter()) {
                ::planus::WriteAsPrimitive::write(
                    v,
                    ::planus::Cursor::new(unsafe { &mut *bytes.add(i) }),
                    buffer_position - (Self::STRIDE * i) as u32,
                );
            }
        }
    }

    impl<'a> ::planus::ReadAsRoot<'a> for HashLookupRef<'a> {
        fn read_as_root(slice: &'a [u8]) -> ::planus::Result<Self> {
            ::planus::TableRead::from_buffer(
                ::planus::SliceWithStartOffset {
                    buffer: slice,
                    offset_from_start: 0,
                },
                0,
            )
            .map_err(|error_kind| {
                error_kind.with_error_location("[HashLookupRef]", "read_as_root", 0)
            })
        }
    }
}
