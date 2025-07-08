use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Field, Fields, GenericArgument, Path, Token, Type, parse_macro_input,
};

/// Custom input for the struct_builder macro that can handle an optional crate path
struct StructBuilderInput {
    struct_def: DeriveInput,
    crate_path: Option<Path>,
}

impl syn::parse::Parse for StructBuilderInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let struct_def = input.parse::<DeriveInput>()?;

        let crate_path = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            Some(input.parse::<Path>()?)
        } else {
            None
        };

        Ok(StructBuilderInput {
            struct_def,
            crate_path,
        })
    }
}

/// A procedural macro for generating struct builders with Arrow array fields.
///
/// # Usage
///
/// ```rust,ignore
/// use amudai_arrow_builders_macros::struct_builder;
///
/// struct_builder!(struct Foo {
///     name: String,
///     value: Int64,
///     blob: Binary,
///     measures: List<FixedSizeBinary<20>>,
/// });
///
/// // Or with a custom crate path:
/// struct_builder!(struct Bar {
///     name: String,
///     value: Int64,
/// }, crate);
/// ```
///
/// This generates a `FooFields` struct that implements `StructFieldsBuilder` and a type alias
/// `FooBuilder` for `StructBuilder<FooFields>`.
#[proc_macro]
pub fn struct_builder(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as StructBuilderInput);

    let struct_name = &input.struct_def.ident;
    let fields_name = syn::Ident::new(&format!("{struct_name}Fields"), struct_name.span());
    let builder_name = syn::Ident::new(&format!("{struct_name}Builder"), struct_name.span());

    // Determine the crate path to use
    let crate_path = if let Some(path) = &input.crate_path {
        quote! { #path }
    } else {
        quote! { amudai_arrow_builders }
    };

    let fields = match &input.struct_def.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let field_definitions = generate_field_definitions(fields, &crate_path);
    let field_accessors = generate_field_accessors(fields, &crate_path);
    let default_impl = generate_default_impl(fields, &fields_name);
    let struct_fields_builder_impl =
        generate_struct_fields_builder_impl(fields, &fields_name, &crate_path);

    let output = quote! {
        pub struct #fields_name {
            #(#field_definitions,)*
            next_pos: u64,
        }

        impl #fields_name {
            #(#field_accessors)*
        }

        #default_impl

        #struct_fields_builder_impl

        pub type #builder_name = #crate_path::StructBuilder<#fields_name>;
    };

    output.into()
}

fn generate_field_definitions(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    crate_path: &proc_macro2::TokenStream,
) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|field| {
            let field_name = &field.ident;
            let builder_type = map_type_to_builder(&field.ty, crate_path);

            quote! {
                #field_name: #builder_type
            }
        })
        .collect()
}

fn generate_field_accessors(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    crate_path: &proc_macro2::TokenStream,
) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|field| {
            let field_name = &field.ident;
            let field_name_ident = field_name.as_ref().unwrap();
            let accessor_name = syn::Ident::new(
                &format!("{field_name_ident}_field"),
                field_name_ident.span(),
            );
            let builder_type = map_type_to_builder(&field.ty, crate_path);

            quote! {
                #[inline]
                pub fn #accessor_name(&mut self) -> &mut #builder_type {
                    use #crate_path::ArrayBuilder;
                    self.#field_name.move_to_pos(self.next_pos);
                    &mut self.#field_name
                }
            }
        })
        .collect()
}

fn generate_default_impl(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    fields_name: &syn::Ident,
) -> proc_macro2::TokenStream {
    let field_names: Vec<_> = fields.iter().map(|field| &field.ident).collect();

    quote! {
        impl Default for #fields_name {
            fn default() -> Self {
                #fields_name {
                    #(#field_names: Default::default(),)*
                    next_pos: 0,
                }
            }
        }
    }
}

fn generate_struct_fields_builder_impl(
    fields: &syn::punctuated::Punctuated<Field, syn::Token![,]>,
    fields_name: &syn::Ident,
    crate_path: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let field_count = fields.len();
    let field_names: Vec<_> = fields
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().unwrap().to_string();
            quote! { #name }
        })
        .collect();

    let build_fields_moves: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = &field.ident;
            quote! {
                self.#field_name.move_to_pos(self.next_pos);
            }
        })
        .collect();

    let build_fields_calls: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = &field.ident;
            quote! {
                self.#field_name.build()
            }
        })
        .collect();
    let schema_fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = &field.ident;
            let field_name_str = field_name.as_ref().unwrap().to_string();
            quote! {
                arrow_schema::Field::new(#field_name_str, self.#field_name.data_type(), true)
            }
        })
        .collect();

    quote! {
        impl #crate_path::StructFieldsBuilder for #fields_name {
            fn schema(&self) -> arrow_schema::Fields {
                use #crate_path::ArrayBuilder;
                arrow_schema::Fields::from(vec![
                    #(#schema_fields,)*
                ])
            }

            #[inline]
            fn next_pos(&self) -> u64 {
                self.next_pos
            }

            #[inline]
            fn move_to_pos(&mut self, pos: u64) {
                self.next_pos = pos;
            }

            #[inline]
            fn field_count(&self) -> usize {
                #field_count
            }

            fn field_name(&self, index: usize) -> &str {
                const NAMES: &'static [&'static str] = &[#(#field_names),*];
                NAMES[index]
            }

            fn build_fields(&mut self) -> Vec<std::sync::Arc<dyn arrow_array::Array>> {
                use #crate_path::ArrayBuilder;
                #(#build_fields_moves)*
                self.next_pos = 0;
                vec![
                    #(#build_fields_calls,)*
                ]
            }
        }
    }
}

fn map_type_to_builder(
    ty: &Type,
    crate_path: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    match ty {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if let Some(segment) = path.segments.last() {
                match segment.ident.to_string().as_str() {
                    "string" | "String" => quote! { #crate_path::StringBuilder },
                    "binary" | "Binary" => quote! { #crate_path::BinaryBuilder },
                    "bool" | "Boolean" => quote! { #crate_path::BooleanBuilder },
                    "i8" | "Int8" => quote! { #crate_path::Int8Builder },
                    "i16" | "Int16" => quote! { #crate_path::Int16Builder },
                    "i32" | "Int32" => quote! { #crate_path::Int32Builder },
                    "i64" | "Int64" => quote! { #crate_path::Int64Builder },
                    "u8" | "UInt8" => quote! { #crate_path::UInt8Builder },
                    "u16" | "UInt16" => quote! { #crate_path::UInt16Builder },
                    "u32" | "UInt32" => quote! { #crate_path::UInt32Builder },
                    "u64" | "UInt64" => quote! { #crate_path::UInt64Builder },
                    "f32" | "Float32" => quote! { #crate_path::Float32Builder },
                    "f64" | "Float64" => quote! { #crate_path::Float64Builder },
                    "timestamp" | "Timestamp" => quote! { #crate_path::TimestampBuilder },
                    "List" => {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(GenericArgument::Type(inner_type)) = args.args.first() {
                                let inner_builder = map_type_to_builder(inner_type, crate_path);
                                return quote! { #crate_path::ListBuilder<#inner_builder> };
                            }
                        }
                        quote! { #crate_path::ListBuilder<_> }
                    }
                    "Map" => {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if args.args.len() == 2 {
                                if let (
                                    Some(GenericArgument::Type(key_type)),
                                    Some(GenericArgument::Type(value_type)),
                                ) = (args.args.first(), args.args.last())
                                {
                                    let key_builder = map_type_to_builder(key_type, crate_path);
                                    let value_builder = map_type_to_builder(value_type, crate_path);
                                    return quote! { #crate_path::MapBuilder<#key_builder, #value_builder> };
                                }
                            }
                        }
                        quote! { #crate_path::MapBuilder<_, _> }
                    }
                    "FixedSizeBinary" => {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(GenericArgument::Const(size_expr)) = args.args.first() {
                                return quote! { #crate_path::FixedSizeBinaryBuilder<#size_expr> };
                            }
                        }
                        quote! { #crate_path::FixedSizeBinaryBuilder<_> }
                    }
                    "FluidStruct" | "GenericStruct" => quote! { #crate_path::FluidStructBuilder },
                    _ => {
                        // For other types, try to append "Builder"
                        let builder_name = syn::Ident::new(
                            &format!("{}Builder", segment.ident),
                            segment.ident.span(),
                        );
                        quote! { #builder_name }
                    }
                }
            } else {
                quote! { _ }
            }
        }
        _ => quote! { _ },
    }
}
