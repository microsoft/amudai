Two major aspects:
 - Shard creation (building, writing)
 - Shard access (reading)

The low-level API is a "hierarchy" of building blocks that roughly correspond to the storage format components.
The major components are:
- Shard
    - Schema
    - ShardIndex (multiple)
    - ShardProperties
    - FieldDescriptor (multiple)
- Stripe (multiple)
    - StripeProperties
    - StripeFieldDescriptor (multiple)
        - FieldProperties
        - FieldEncoding (multiple)
            - EncodedBuffer (multiple)
    - StripeIndex (multiple)

For each conceptual component, there are typically a few API structures:
 - Element**Builder**: mutable, facilitates progressive creation. Concludes with `ElementBuilder::finish()`.
 - **Prepared**Element: result of `ElementBuilder::finish()`, a finished/prepared ephemeral `Element` that cannot be mutated anymore (but some properties may still be adjusted), located in the temp storage, awaiting to be written somewhere into the final blob. It is essentially an open `fd` of a temp file that stores the encoded data, plus all of the stats and properties.
 - **Sealed**Element: refers to an `Element` within an already existing shard, that cannot be mutated or moved anywhere. **Sealed**Element can be obtained in two ways: by sealing **Prepared**Element (and writing it to the final artifact as part of this sealing), or by locating and opening the storage component in the already sealed shard.
 - Bare `Element`: this is a slim descriptor of the storage element on the read/access side, usually a thin wrapper of the Protobuf message or FlatBuffers table.
 - Element**Reader** (or **Decoder**): low-level read access to the element.

Typical lifecycle:
- create *Parent*Builder
- create one or more *Element*Builder's (possibly by calling the *Parent*Builder)
- mutate and add stuff to *Element*Builder's
- call *Element*Builder::finish() for each one, when done.
- add Prepared*Element*'s to the parent builder
- *Parent*Builder::finish()

 E.g. for `EncodedBuffer`, we'll have:
- `EncodedBufferBuilder`: populated using `push_block()`, `push_values()`, etc.
- `PreparedEncodedBuffer`: can be added to the `FieldEncodingBuilder`.
- `SealedEncodedBuffer`: can be added to the `FieldEncodingBuilder`.
- `EncodedBuffer`: direct reflection of the relevant stripe/field metadata element.
- `EncodedBufferReader`: locating and reading the relevant data blocks, initiating optimized scans, etc.
