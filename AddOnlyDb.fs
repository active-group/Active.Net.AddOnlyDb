namespace Active.Net.Database

/// Our Key-Value database that only allows addition
/// see https://active-group.de/redmine/projects/tuev-sued-lokalisierung/wiki/Database_queries#Database-model
module AddOnlyDb =
    open DbCore
    open System.Data
    open System.Data.SQLite
    open System.Security.Cryptography // HashAlgorithm
    open Active.Net // Guid, Hash, Text
    open Active.Net.FuncUtils // swap, constantly
    open Active.Net.Log

    let private logger = getLogger "Active.Net.Database.AddOnlyDb"

    /// Meta information about a block record: who created it when and where
    module Meta =
        open FSharp.Data
        type T = { user:string
                   machine:string
                   datetime:DateTime.T}
        /// construct new Meta.T record
        let make user machine datetime =
            { user = user
              machine = machine
              datetime = datetime
            }

        /// json representation of Meta record
        let toJson (meta:T) =
            JsonValue.Record [|("user", JsonValue.String meta.user)
                               ("machine", JsonValue.String meta.machine)
                               ("datetime", JsonValue.String (DateTime.toIso8601String meta.datetime)) |]
        /// Meta record from its json representation
        let fromJson (j:JsonValue) =
            { user = j.GetProperty("user").AsString()
              machine = j.GetProperty("machine").AsString()
              datetime = j.GetProperty("datetime").AsString() |> DateTime.fromIso8601String}
        /// string representation of Meta record
        let toString = Json.toJsonString toJson
        /// Meta record from its string representation
        // note: only works via json because Meta's json is an object
        let fromString = Json.fromJsonString fromJson

        /// create new Meta record filled with current user, current machine, and current time (now)
        let create () =
            { user = AccountManagement.currentUser
              machine = System.Environment.MachineName
              datetime = DateTime.utcNow()}
        let user t = t.user
        let machine t = t.machine
        let datetime t = t.datetime

    type T = { db:DB;
               /// algorithm that calculates hashes - must support transforming multiple blocks
               newHashAlgorithm : Hash.NewAlgorithm
               kvTable:string     // facts table name
               hashesTable:string // obsoleted hashes table name
               kvCurrentTable:string // facts view w/o obsoleted entries
             }
    // data types
    type HashT = Hash.T
    type GuidT = Guid.T  /// test
    type PropertyT = Property of string
    let makeProperty = Property // for some reason, Property is not exported to Testing
    type ValueT = byte[]
    type MetaT = Meta.T
    type ObsoleteT = Hash.T
    type ObsoletesT = ObsoleteT []  (* lexicographically sorted *)
    type FactT = GuidT * PropertyT * ValueT * MetaT * ObsoletesT
    type BlockT = HashT * FactT

    // column types in database
    type DbHash = byte[]  // -> Hash.T, fixed size depending on hash algorithm (crypto.HashSize/8)
    type DbGuid = byte[]  // -> Guid.T, exactly 16 bytes
    type DbProperty = string  // utf8
    type DbValue = byte[] // anything
    type DbMeta = string  // -> Meta.T Json
    type DbObsolete = DbHash  // -> Hash.T
    type DbObsoletes = DbHash[] // lexicographically sorted
    type DbFact = DbGuid * DbProperty * DbValue * DbMeta * DbObsoletes
    type DbBlock = DbHash * DbFact

    // literals referring to column names in tables
    [<Literal>]
    let hashColumn = "hash"
    [<Literal>]
    let guidColumn = "guid"
    [<Literal>]
    let propertyColumn = "property"
    [<Literal>]
    let valueColumn = "value"
    [<Literal>]
    let metaColumn = "meta"
    [<Literal>]
    let keyHashColumn = "key_hash"
    [<Literal>]
    let obsoletedHashColumn = "obsoleted_hash"

    /// make AddOnlyDb.T
    let make db newHashAlgorithm kvTable hashesTable kvCurrentTable =
        assert (newHashAlgorithm() : Hash.Algorithm).CanTransformMultipleBlocks
        {   db = db
            newHashAlgorithm = newHashAlgorithm
            // facts table name
            kvTable = kvTable
            // obsoleted hashes table name
            hashesTable = hashesTable
            kvCurrentTable = kvCurrentTable
        }

    // module internal Construction =
    /// sql command that creates kv table of AddOnlyDb.T
    let createKvTableSql (aodb:T) =
        sprintf """
CREATE TABLE %s (
  %s  BLOB   PRIMARY KEY NOT NULL UNIQUE,  -- hash bytes
  %s  BLOB   NOT NULL,   -- guid bytes
  %s  STRING NOT NULL,   -- property name
  %s  BLOB   NOT NULL,   -- property value
  %s  STRING NOT NULL    -- meta JSON
);"""
               aodb.kvTable
               hashColumn
               guidColumn
               propertyColumn
               valueColumn
               metaColumn
        |> makeSimpleSql

    /// sql command that creates hashes table of AddOnlyDb.T
    let createHashesTableSql (aodb:T) =
        sprintf """
CREATE TABLE %s (
  %s BLOB NOT NULL,
  %s BLOB NOT NULL,
  FOREIGN KEY(%s) REFERENCES %s(%s)
);"""
               aodb.hashesTable
               keyHashColumn
               obsoletedHashColumn
               keyHashColumn
               aodb.kvTable
               hashColumn
        |> makeSimpleSql

    /// sql command that creates current kv table (kv table w/o obsoletes)
    // what is faster: NOT IN select-subquery -or- cross.product with kv.hash <> hashes.obsoleted_hash -or- doesn't matter?
    let createCurrentKvTableSql (aodb:T) =
        sprintf @"CREATE VIEW %s AS SELECT %s,%s,%s,%s,%s FROM %s WHERE %s.%s NOT IN (SELECT %s FROM %s)"
                aodb.kvCurrentTable  // view name
                hashColumn guidColumn propertyColumn valueColumn metaColumn // columns
                aodb.kvTable  // from table
                aodb.kvTable hashColumn  // condition
                obsoletedHashColumn aodb.hashesTable // sub-query
        |> makeSimpleSql

    /// sql command that creates index of kv table of AddOnlyDb.T
    let createKvTableIndexSql (aodb:T) (idxName:string) =
        sprintf """
CREATE INDEX %s ON %s(%s,%s); -- kv(guid, property)
"""
               idxName
               aodb.kvTable
               guidColumn
               propertyColumn
        |> makeSimpleSql

    /// sql command that creates index of hashes table of AddOnlyDb.T
    let createHashesIndexSql (aodb:T) (idxName:string) =
        sprintf """
CREATE INDEX %s ON %s(%s); -- hashes(obsoleted_hash)
"""
               idxName
               aodb.hashesTable
               obsoletedHashColumn
        |> makeSimpleSql

    /// sql commands that create tables and indices of AddOnlyDb.T
    let createAddOnlyDbSql (aodb:T) (kvIndexName:string) (hashesIdxName:string) : SqlCommand list =
        [ createKvTableSql aodb
          createHashesTableSql aodb
          createKvTableIndexSql aodb kvIndexName
          createHashesIndexSql aodb hashesIdxName
          createCurrentKvTableSql aodb
        ]

    /// create AddOnlyDb tables and indices, using the provided index names
    let createTablesAndIndicesHelper aodb (kvIndexName:string) (hashesIdxName:string) =
        let sqls = createAddOnlyDbSql aodb kvIndexName hashesIdxName
        for sql in sqls do
            DbCore.executeNonQuery aodb.db sql |> ignore

    /// query `sqlite_master` table to find unused name for an index
    // repeatedly come up with a new name <prefix><n:int> until it is not found in sqlite_master
    let private findNewIndexName (db:DB) (prefix:string) :string =
        let indicesNames =
            {qury = @"SELECT name FROM sqlite_master WHERE type = 'index'"; parameters=[]}
            |> DbCore.executeReader db
            |> Seq.map (fun u -> u.[0] :?> string)
            |> Set.ofSeq
        let rec search n =
            let idxName = prefix + n.ToString()
            if indicesNames.Contains idxName
            then search (n+1)
            else idxName
        search 1

    /// create AddOnlyDb tables, returning the generated index names
    let createTablesAndIndices (aodb:T) : (string * string) =
        let kvIndexPrefix = aodb.kvTable + "Idx" 
        let hashIndexPrefix = aodb.hashesTable + "Idx"
        let kvIndexName = kvIndexPrefix |> findNewIndexName aodb.db
        let hashIndexName = hashIndexPrefix |> findNewIndexName aodb.db
        createTablesAndIndicesHelper aodb kvIndexName hashIndexName
        kvIndexName, hashIndexName

    // module internal LowLevel =
    /// compute hash of (low-level) fact item
    /// uses UTF-8 to encode strings
    let computeHash_ (newHashAlgorithm:Hash.NewAlgorithm) (guid:DbGuid) (property:DbProperty) (value:DbValue) (meta:DbMeta) (obsoletes:DbObsoletes) =
        let propertyBytes = Text.utf8Bytes property
        let metaBytes = Text.utf8Bytes meta
        Seq.append (Seq.ofList [guid; propertyBytes; value; metaBytes])
                   (Seq.ofArray obsoletes)
        |> Hash.computeSeqHash newHashAlgorithm

    /// lowest-level: add kv
    let private addKv_ (aodb:T) (hash:DbHash) (guid:DbGuid) (property:DbProperty) (value:DbValue) (meta:DbMeta) =
        {qury = sprintf @"INSERT INTO %s(%s,%s,%s,%s,%s) VALUES(:hash,:guid,:property,:value,:meta)"
                                           aodb.kvTable hashColumn guidColumn propertyColumn valueColumn metaColumn
         parameters = [{name="hash"; value=hash}
                       {name="guid"; value=guid}
                       {name="property"; value=property}
                       {name="value"; value=value}
                       {name="meta"; value=meta}
                      ]
        }
        |> DbCore.executeNonQuery aodb.db
        |> ignore

    /// lowest-level: add obsolete
    let private addObsolete_ (aodb:T) (keyHash:DbHash) (obsoletedHash:DbObsolete) =
        {qury = sprintf @"INSERT INTO %s(%s,%s) VALUES(:key_hash,:obsoleted_hash)"
                                                  aodb.hashesTable keyHashColumn obsoletedHashColumn
         parameters = [{name="key_hash"; value=keyHash}
                       {name="obsoleted_hash"; value=obsoletedHash}]
        }
        |> DbCore.executeNonQuery aodb.db
        |> ignore

    /// lowest-level: add a new fact item
    /// needs to be run in a transaction, as we want the whole fact (including all obsoletes) to be in the db or nothing of it
    let private addFact_ (aodb:T) (hash:DbHash) (guid:DbGuid) (property:DbProperty) (value:DbValue) (meta:DbMeta) (obsoletes:DbHash[]) =
        addKv_ aodb hash guid property value meta
        for obsoleted_hash in obsoletes do
            addObsolete_ aodb hash obsoleted_hash

    /// simple where clause description, None means "don't care" / "any"
    type SearchKey = SimpleKey of DbGuid option * DbProperty option * DbValue option
    /// helper to create search key, operating on raw DB types
    let private makeSearchKey_ (guid:DbGuid option) (property: DbProperty option) (value: DbValue option) =
        SimpleKey (guid, property, value)
    
    // templates to construct where clause
    let private guidCondition = sprintf @"%s = :guid" guidColumn
    let private propCondition = sprintf @"%s = :property" propertyColumn
    let private valueCondition = sprintf @"%s = :value" valueColumn

    /// construct WHERE clause from search key, w/o 'WHERE',
    /// returns list of (where-clause-fragment, parameter-binding)
    let private createWhereClauses (searchKey:SearchKey) : (string * Parameter) list =
        let (SimpleKey (guid,property,value)) = searchKey
        [ guid     |> Option.map (fun guid  -> (guidCondition,  {name="guid"; value=guid}))
          property |> Option.map (fun prop  -> (propCondition,  {name="property"; value=prop}))
          value    |> Option.map (fun value -> (valueCondition, {name="value"; value=value}))
        ]
        |> List.choose id  // filter Option.isSome |> map Option.get in one step

    /// join where clauses with ' AND '.
    let private joinClauses (condClauses:string seq) =
        System.String.Join(" AND ", condClauses)

    /// low-level: retrieve raw columns of facts that satisfy given properties and have not been obsoleted yet
    let private search_ (aodb:T) (columns:string[]) (searchKey:SearchKey) : obj[][] =
        let columnsSelector = System.String.Join(", ", columns)
        let whereClauses = createWhereClauses searchKey
        let (condClauses, paramms) = List.unzip whereClauses
        let whereClause = joinClauses("1=1"::condClauses)
        {qury = (sprintf @"SELECT %s FROM %s WHERE %s"
                        columnsSelector aodb.kvCurrentTable whereClause)
                parameters = paramms}
        |> DbCore.executeReader aodb.db

    /// lowest-level: retrieve fact identified by hash key (primary key)
    /// fails if there is no such key; use idExists to check that first, if necessary
    let private getById_ (aodb:T) (hash:DbHash) : DbFact =
        let hashList = System.String.Join(",", hash|>Bytes.toHexString|>(fun s-> "X'" + s + "'"))
        let obsoletes =
            DbCore.makeSimpleSql (sprintf @"SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s ASC"
                                   obsoletedHashColumn aodb.hashesTable keyHashColumn hashList obsoletedHashColumn)
            |> DbCore.executeReader aodb.db
            |> Array.map (fun o -> o.[0] :?> DbHash)
        DbCore.makeSimpleSql (sprintf @"SELECT %s,%s,%s,%s FROM %s WHERE %s IN (%s)"
                                 guidColumn propertyColumn valueColumn metaColumn
                                 aodb.kvTable
                                 hashColumn
                                 hashList)
        |> DbCore.executeReader aodb.db
        |> (fun rows ->
            match rows.Length with
            | 0 -> failwith (sprintf "no such key %s" (Bytes.toHexString hash))
            | 1 ->
                let row = rows.[0]
                (row.[0]:?>DbGuid,
                 row.[1]:?>DbProperty,
                 row.[2]:?>DbValue,
                 row.[3]:?>DbMeta,
                 obsoletes)
            | _ -> failwith (sprintf " should not happen: duplicate hash key %s" (Bytes.toHexString hash))
            )

    let private getMultipleById_ (aodb:T) (hashes:DbHash seq) : DbBlock []=
        let hashList = System.String.Join(",", hashes|>Seq.map (Bytes.toHexString >> (fun s-> "X'" + s + "'")))
        let emptyObsoletesMap = hashes |> Seq.map (fun h -> (h,[])) |> Map.ofSeq
        let obsoletes =
            DbCore.makeSimpleSql (sprintf @"SELECT %s,%s FROM %s WHERE %s IN (%s) ORDER BY %s ASC, %s DESC"  // obsoletes DESC, as rev'ed in map accumulator
                                   keyHashColumn obsoletedHashColumn // SELECT
                                   aodb.hashesTable // FROM
                                   keyHashColumn hashList // WHERE
                                   keyHashColumn obsoletedHashColumn // ORDER BY
                                   )
            |> DbCore.executeReader aodb.db
            |> Array.fold (fun m o ->
                let key_hash = o.[0] :?> DbHash
                let obsoleted_hash = o.[1] :?> DbHash
                let oHashes = Map.find key_hash m
                Map.add key_hash (obsoleted_hash::oHashes) m // turns DESC -> ASC
                ) emptyObsoletesMap

        DbCore.makeSimpleSql (sprintf @"SELECT %s,%s,%s,%s,%s FROM %s WHERE %s IN (%s)"
                                 hashColumn guidColumn propertyColumn valueColumn metaColumn
                                 aodb.kvTable
                                 hashColumn
                                 hashList)
        |> DbCore.executeReader aodb.db
        |> Array.map (fun row ->
                let hash = row.[0]:?>DbHash
                hash,
                (row.[1]:?>DbGuid,
                 row.[2]:?>DbProperty,
                 row.[3]:?>DbValue,
                 row.[4]:?>DbMeta,
                 obsoletes.[hash] |> Array.ofList)
            )


    /// convert data type to database types, sorting obsoletes
    let toDbTypes (guid:GuidT) (property:PropertyT) (value:ValueT) (meta:MetaT) (obsoletes:ObsoletesT)
        : (DbGuid*DbProperty*DbValue*DbMeta*DbObsoletes) =
        let guidBytes = Guid.toByteArray guid
        let (Property propString) = property
        let metaString = Meta.toString meta
        let sortedObsoletes = obsoletes |> Array.sortWith Hash.compare
        let sortedObsoletesBytes = sortedObsoletes |> Array.map Hash.toByteArray
        (guidBytes, propString, value, metaString, sortedObsoletesBytes)
    
    /// convert data type from database types, assuming sorted obsoletes
    let fromDbTypes (dbGuid:DbGuid) (dbProperty:DbProperty) (value:DbValue) (dbMeta:DbMeta) (dbObsoletes:DbObsoletes) =
        let guid = Guid.fromByteArray dbGuid
        let property = Property dbProperty
        let meta = Meta.fromString dbMeta
        let obsoletes = dbObsoletes |> Array.map Hash.fromByteArray
        (guid,property,value,meta,obsoletes)

    // type-safe read/write operations
    // module internal HighLevel =
    let computeHash (aodb:T) (guid:GuidT) (property:PropertyT) (value:ValueT) (meta:MetaT) (obsoletes:ObsoletesT) =
        let (guid,property,value,meta,obsoletes) = toDbTypes guid property value meta obsoletes
        computeHash_ aodb.newHashAlgorithm guid property value meta obsoletes

    /// retrieve single fact identified by hash key (primary key)
    let private getById (aodb:T) (hash:HashT) : FactT =
        let (guid,prop,value,meta,obsoletes) = getMultipleById_ aodb [(Hash.toByteArray hash)] |> (fun bs -> snd bs.[0]) // = getById_ aodb (Hash.toByteArray hash)
        (guid |> Guid.fromByteArray,
         prop |> Property,
         value,
         meta |> Meta.fromString,
         obsoletes |> Array.map Hash.fromByteArray)

    /// true iff hash (primary key) already exists
    let idExists (aodb:T) (hash:HashT) : bool =
        {qury = sprintf @"SELECT %s FROM %s WHERE %s = :hash"
                        hashColumn aodb.kvTable hashColumn;
         parameters = [{name="hash"; value=Hash.toByteArray hash}]}
        |> DbCore.executeScalar aodb.db
        |> ((<>) null)

    /// add a new fact item, return its hash
    let private addFact (aodb:T) (guid:GuidT) (property:PropertyT) (value:ValueT) (meta:MetaT) (obsoletes:ObsoletesT) : Hash.T =
        let (guidBytes, prop, value, metaString, obsoletesBytes) = toDbTypes guid property value meta obsoletes
        let hash = computeHash_ aodb.newHashAlgorithm guidBytes prop value metaString obsoletesBytes
        if not <| idExists aodb hash // ignore duplicate facts
        then addFact_ aodb (Hash.toByteArray hash) guidBytes prop value metaString obsoletesBytes
        hash

    /// make search key type-safely
    let makeSearchKey (guid:GuidT option) (property: PropertyT option) (value: ValueT option) =
        makeSearchKey_ (guid |> Option.map Guid.toByteArray)
                      (property |> Option.map (fun (Property prop) -> prop))
                      (value)

    /// retrieve a fact item's value(s) that has not been obsoleted yet
    /// NB: there can be multiple values (eg. sync needs merging)
    let private retrieve (aodb:T) (guid:Guid.T) (property:PropertyT) : ValueT [] =
        search_ aodb [|valueColumn|] (makeSearchKey (Some guid) (Some property) (None))
        |> Array.map (fun o -> o.[0] :?> ValueT)

    /// retrieve a fact item's value(s) and hash key(s) that have not been obsoleted yet
    /// NB: there can be multiple values
    let private retrieveWithHashKey (aodb:T) (guid:GuidT) (property:PropertyT) : (HashT * ValueT) [] =
        search_ aodb [|hashColumn;valueColumn|] (makeSearchKey (Some guid) (Some property) (None))
        |> Array.map (fun o -> (o.[0] :?> DbHash |> Hash.fromByteArray, 
                                o.[1] :?> ValueT))

    /// retrieve hash key of facts that satisfy given properties and have not been obsoleted yet
    /// None means "don't care" / "any"
    // TODO more powerful search options, some places need "IS NOT"
    // TODO allow selection of columns to be retrieved instead of specialized retrieveXXX fns
    let private retrieveHashKey (aodb:T) (searchKey:SearchKey) : HashT [] =
        search_ aodb [|hashColumn|] searchKey
        |> Array.map (fun o -> o.[0] :?> DbHash |> Hash.fromByteArray)

    /// retrieve hash key, value, and meta, which have not been obsoleted yet
    let private retrieveRecord (aodb:T) guid prop : (HashT*ValueT*MetaT) [] =
        search_ aodb [|hashColumn;valueColumn;metaColumn|] (makeSearchKey (Some guid) (Some prop) None)
        |> Array.map (fun o -> o.[0] :?> DbHash |> Hash.fromByteArray,
                               o.[1] :?> ValueT,
                               o.[2] :?> DbMeta |> Meta.fromString)

    /// retrieve data block for given hash key
    let getBlock (aodb:T) (key: HashT) : BlockT =
        key, getById aodb key

    /// retrieve data blocks for given hash keys
    /// NB: blocks are not necessarily returned in the order in which their hashes were provided
    // this is supposed to be more performant than individual calls to getBlock
    let getBlocks (aodb:T) (keys:HashT seq) : BlockT [] =
        getMultipleById_ aodb (keys |> Seq.map Hash.toByteArray)
        |> Array.map (fun (h,(g,p,v,m,o)) ->
            h |> Hash.fromByteArray,
            (g |> Guid.fromByteArray,
             p |> Property,
             v,
             m |> Meta.fromString,
             o |> Array.map Hash.fromByteArray))

    /// retrieve hashes of all facts, no matter if obsoleted or not (for synchronization), in ascending order
    let getAllHashes (aodb:T) : HashT[] =
        // note: `retrieveHashKey aodb (makeSearchKey None None None)` only returns non-obsoleted facts
        DbCore.makeSimpleSql (sprintf @"SELECT %s FROM %s ORDER BY %s ASC" hashColumn aodb.kvTable hashColumn)
        |> DbCore.executeReader aodb.db
        |> Array.map (fun o -> o.[0] :?> DbHash |> Hash.fromByteArray)

    /// put given data block to database
    /// NOP for block whose hash is wrong
    let putBlock (aodb:T) (block:BlockT) : unit =
        let (hash,(guid,prop,value,meta,obsoletes)) = block
        let computedHash = computeHash aodb guid prop value meta obsoletes
        if computedHash = hash
        then
            let putHash = addFact aodb guid prop value meta obsoletes
            assert (putHash = hash) // hm, well...?
        else
            logWarn logger (sprintf "Discarding block as hash is wrong (expected hash: %s): (%s,%s,%s,%s,%s)"
                (Hash.toByteArray computedHash|> Bytes.toHexString)
                (Guid.toString guid)
                (prop.ToString()) // FIXME
                (value |> Bytes.toHexString)
                (meta |> Meta.toString)
                ("[" + (obsoletes |> Array.map (Hash.toByteArray >> Bytes.toHexString) |> (fun a -> System.String.Join(", ", a))) + "]"))
            ()

    /// close AddOnlyDB; you must not access the database after calling this fn
    let close (aodb:T) = DbCore.close aodb.db

    /// helper module to serialize/deserialize FactT in a binary stream
    module Data =
        open System.IO // MemoryStream, BinaryWriter
        open Active.Net.BinaryReaderMonad
        open Active.Net.BinaryWriterUtil

        type T = FactT

        /// write Guid.T to binary stream
        let writeGuid = writeBytes << Guid.toByteArray
        /// write Property (as string) to binary stream
        let writeProperty (Property prop) = writeString prop
        /// write Meta.T to binary stream
        let writeMeta = writeString << Meta.toString
        /// write ObsoletesT to binary stream (in given order)
        let writeObsoletes = writeBytesArrayOfFixedSize << (Array.map Hash.toByteArray)
        /// read Guid.T from binary stream
        let readGuid = readBytes 16 >>= (fun bs -> result <| Guid.fromByteArray bs)
        /// read (string as) Property from binary stream
        let readProperty = readString >>= (fun prop -> result <| Property prop)
        /// read Meta.T from binary stream
        let readMeta = readString >>= (fun s -> result <| Meta.fromString s)
        /// read ObsoletesT from binary stream (ignoring any ordering)
        let readObsoletes = readBytesArrayOfFixedSize >>= (fun bs -> result <| (Array.map Hash.fromByteArray bs))
        /// write data into BinaryWriter, version=1
        //  byte array layout:
        //                      0 version
        //                      4 guid
        //                     20 property (length + utf8 bytes)
        //       + propertyLength value.Length
        //       +              4 value
        //       +   value.Length meta (length + utf8 bytes)
        //       +     metaLength obsoletes.Length
        //       +              4 obsoletes.[0].Length, if any
        //       +              8 obsoletes, with no length prefix
        let dataWriter ((guid,property,value,meta,obsoletes):T) =
            let version = 1
            writeInt32 version
            >> writeGuid guid
            >> writeProperty property
            >> writeBytesArray value
            >> writeMeta meta
            >> writeObsoletes obsoletes
            >> ignore
        /// turn T to byte array; obsoletes should already be in lexicographical order
        let toBytesArray (data:T) : byte[]=
            // guids are exactly 16 bytes long
            // obsoletes elements are all of the same size, although unknown
//            let size = 16 (*guid.Length*) + propertyBytes.Length + value.Length + metaBytes.Length + obsoletesLength * obsoletes.Length
//                       + 4 * sizeof<int32>
            let mem = new MemoryStream()
            use result = new BinaryWriter(mem, Text.utf8Encoding)
            dataWriter data result |> ignore
            mem.ToArray()
        /// read T from binary stream, failing if version<>1
        let dataReader =
            readInt32      >>= (fun version ->
            assert (version = 1)
            readGuid       >>= (fun guid ->
            readProperty   >>= (fun property ->
            readBytesArray >>= (fun value ->
            readMeta       >>= (fun meta ->
            readObsoletes  >>= (fun obsoletes ->
            result (guid,property,value,meta,obsoletes)))))))

        /// extract T from bytes array; see comment at toBytesArray for byte layout
        let fromBytesArray (bytes:byte[]) : T =
            let mem = new System.IO.MemoryStream(bytes)
            use reader = new BinaryReader(mem, Text.utf8Encoding)
            dataReader reader
    
    /// re-export private functions for testing
    module Testing =
        open FsCheck
        open Active.Net.TestUtil
        let addFact = addFact
        let retrieve = retrieve
        let retrieveWithHashKey = retrieveWithHashKey
        let retrieveHashKey = retrieveHashKey

        /// generator of Key
        let keyGenerator = Hash.Testing.generator
        /// generator of Property with non-null name
        let propertyGenerator = nonNullStringGenerator |> Gen.map makeProperty
        /// generator of Meta.T with non-null user,machine entries and random time
        let metaGenerator =
            let dateTimeGen = Arb.generate<DateTime.T>
            Gen.map3 Meta.make (*user machine datetime*) nonNullStringGenerator nonNullStringGenerator dateTimeGen
        /// generator of obsolete hash (16 bytes) // TODO variable array length
        let obsoleteGenerator =
            Arb.generate<byte>
            |> Gen.arrayOfLength 16
            |> Gen.map Hash.make
        
        /// generator of unsorted obsoletes array
        let unsortedObsoletesGenerator = Gen.arrayOf obsoleteGenerator

        /// generator of obsoletes array, sorted
        let obsoletesGenerator =
            gen {
                let! unsorted = unsortedObsoletesGenerator
                return Array.sort unsorted
            }

        /// shrinker of obsoletes array (halfed at each step)
        let obsoletesShrinker (t:ObsoletesT) =
            let newLength = (t.Length/2)
            seq [Array.sub t 0 newLength]

        /// helper type to register PropertyT, MetaT, ObsoleteT, ObsoletesT generators
        type DataGenerators() =
            static member PropertyT() =
                {new Arbitrary<PropertyT>() with
                    override x.Generator = propertyGenerator
                    override x.Shrinker t = Seq.empty }
            static member MetaT() =
                {new Arbitrary<MetaT>() with
                    override x.Generator = metaGenerator
                    override x.Shrinker t = Seq.empty}
            static member ObsoleteT() =
                {new Arbitrary<ObsoleteT>() with
                    override x.Generator = obsoleteGenerator
                    override x.Shrinker t = Seq.empty }
            static member ObsoletesT() =
                {new Arbitrary<ObsoletesT>() with
                    override x.Generator = obsoletesGenerator
                    override x.Shrinker t = obsoletesShrinker t
                }

    /// monadic operators to access AddOnlyDb
    module API =
        /// operation on AddOnlyDb
        type 'a Op =
            /// just provide/return this value
            | Return of 'a
            /// retrieve values from database filed under key (guid,property)
            | Get of GuidT * PropertyT * (ValueT [] -> 'a Op) // retrieve
            /// store data into database
            | Put of FactT * (HashT -> 'a Op) // addFact
            /// retrieve hash key of database entries that match search key
            | GetHashKey of SearchKey * (HashT [] -> 'a Op) // retrieveHashKey
            /// retrieve hash key and value from database filed under key (guid, property)
            | GetWithHashKey of GuidT * PropertyT * ((HashT * ValueT) [] -> 'a Op) // retrieveWithHashKey
            /// retrieve hash key, value, and meta from database filed under key (guid, property)
            // TODO consider interface with fewer GetXXX ops
            | GetRecord of GuidT * PropertyT * ((HashT * ValueT * MetaT) [] -> 'a Op)
            /// retrieve block
            | GetFact of HashT * (FactT -> 'a Op) // getBlock
            /// execute the given operation en-bloc or not at all
            | Atomically of unit Op * (unit -> 'a Op)

        // convenience fns
        /// just provide/return this value
        let ret a = Return a
        let ignoreAndReturn a = constantly (Return a)
        /// retrieve values from database filed under key (guid,property)
        let get guid prop = Get (guid,prop,ret)
        /// store data into database
        let put guid prop value meta obsoletes = Put((guid,prop,value,meta,obsoletes),ignoreAndReturn ())
        /// store data into database and return calculated hash
        let putAndReturnHash guid prop value meta obsoletes = Put((guid,prop,value,meta,obsoletes), ret)
        /// store value under (guid,prop) into database, creating new meta entry, w/o obsoletes
        let putValue guid prop value = Put((guid,prop,value,Meta.create(),[||]),ignoreAndReturn ())
        /// store value under (guid,prop) into database, creating new meta entry, w/o obsoletes
        /// return generated hash
        let putValueAndReturnHash guid prop value = Put((guid,prop,value,Meta.create(),[||]), ret)
        /// store value under (guid,prop) into database with given obsoletes, creating new meta entry
        let putWithObsoletes guid prop value obsoletes = Put((guid,prop,value,Meta.create(),obsoletes),ignoreAndReturn ())
        /// store value under (guid,prop) into database with given obsoletes, creating new meta entry
        /// return generated hash
        let putWithObsoletesAndReturnHash guid prop value obsoletes = Put((guid,prop,value,Meta.create(),obsoletes), ret)
        /// retrieve hash key of database entries that match search key
        let getHashKey searchKey = GetHashKey(searchKey, ret)
        /// retrieve hash key and value from database filed under key (guid, property)
        let getWithHashKey guid prop = GetWithHashKey(guid,prop,ret)
        /// retrieve hash key, value, and meta record from database filed under key (guid, property)
        let getRecord guid prop = GetRecord(guid, prop, ret)
        /// retrieve block
        let getFact hash = GetFact(hash, ret)
        /// execute the given operation en-bloc or not at all
        let atomically op = Atomically (op, Return)
        /// bind one operation to another: execute opB and provide its result to f
        let rec bind (opB: 'b Op) (f: 'b -> 'a Op) : ('a Op) =
            match opB with
            | Return b -> f b
            | Get (g, p, cont) -> Get (g, p, (fun v -> bind (cont v) f))
            | Put (d, cont) -> Put(d, (fun v -> bind (cont v) f))
            | GetHashKey (s, cont) -> GetHashKey(s, (fun v -> bind (cont v) f))
            | GetWithHashKey (g, p, cont) -> GetWithHashKey(g, p, (fun v -> bind (cont v) f))
            | GetRecord(g,p,cont) -> GetRecord(g,p,(fun v -> bind (cont v) f))
            | GetFact(h,cont) -> GetFact (h,(fun v -> bind (cont v) f))
            | Atomically (op, cont) -> Atomically (op, (fun () -> bind (cont ()) f))

        (*
        let rec bind0 (opB: 'b Op) (f: unit -> 'a Op) : 'a Op =
            match opB with
            | Return b -> f()
            | Get (k, cont) -> Get(k, (fun v -> bind0 (cont v) f))
            | Put (k, v, m, o, cont) -> Put(k, v, m, o, (fun v -> bind0 (cont v) f))
            | GetHashKey (s, cont) -> GetHashKey(s, (fun v -> bind0 (cont v) f))
            | GetWithHashKey (k, cont) -> GetWithHashKey(k, (fun v -> bind0 (cont v) f))
        *)

        /// helper fn for run -- type annotations only on module-level bindings
        let rec private runLoop<'a> (db:T) (inTransaction:bool) (query:'a Op) : 'a =
            match query with
            | Return v -> v
            | Get (guid, prop, cont) ->
                retrieve db guid prop |> (cont >> (runLoop db inTransaction))
            | Put ((guid, prop, value, meta, obsoletes), cont) ->
                let a = runInTransaction db inTransaction (Atomically (Return (), (fun _ -> Return (addFact db guid prop value meta obsoletes))))
                (cont a |> (runLoop db inTransaction))
            | GetHashKey (searchKey, cont) ->
                retrieveHashKey db searchKey |> (cont >> (runLoop db inTransaction))
            | GetWithHashKey (guid, prop, cont) ->
                retrieveWithHashKey db guid prop |> (cont >> (runLoop db inTransaction))
            | GetRecord (guid, prop, cont) ->
                retrieveRecord db guid prop |> (cont >> (runLoop db inTransaction))
            | GetFact (hash, cont) ->
                getBlock db hash |> (snd >> cont >> (runLoop db inTransaction))
            | Atomically (op, cont) ->
                let a = runInTransaction db inTransaction op
                (cont a |> (runLoop db inTransaction))

        and runInTransaction<'b> (db:T) (inTransaction:bool) (opA: 'b Op) : 'b =
            if inTransaction
            then runLoop db true opA
            else
                try
                    makeSimpleSql "BEGIN TRANSACTION" |> DbCore.executeNonQuery db.db |> ignore
                    let res = runLoop db true opA
                    makeSimpleSql "COMMIT" |> DbCore.executeNonQuery db.db |> ignore
                    res
                with
                | e ->
                    makeSimpleSql "ROLLBACK" |> DbCore.executeNonQuery db.db |> ignore
                    raise e

        /// run query operation on database
        let run (db:T) (query0:'a Op) : 'a =
            runLoop db false query0

        // TODO bind that discards bind: 'b Op -> ( unit -> 'a Op) : 'a Op

        /// monadic fold
        let foldM (f: ('s -> 'a -> 's Op)) (s:'s) (seqA: 'a seq) : 's Op =
            let aEnum = seqA.GetEnumerator()
            let rec loop s =
                if aEnum.MoveNext()
                then let a = aEnum.Current
                     bind (f s a) loop
                else Return s
            loop s

        
        let sequM (mseq: 'a Op seq) : 'a list Op =
            bind (foldM (fun res a -> bind a (fun v -> Return(v :: res))) [] mseq)
              (fun r -> Return (List.rev r))

        /// monadic map: apply f to each element of seqA, returning the results as a list
        let mapM (f: 'a -> 'b Op) (seqA: 'a seq) : ('b list) Op =
            sequM (seqA |> Seq.map f)

        let sequM_ (mseq : unit Op seq) : unit Op =
           bind (sequM mseq) (fun _ -> Return ())

        /// monadic iteration: like mapM, but discard results
        let iterM (f: 'a -> unit Op) (seqA: 'a seq) : unit Op =
            sequM_ (seqA |> Seq.map f)

        /// monadic filter
        let filterM (f: ('a -> bool Op)) (seqA: 'a seq) : 'a list Op =
            let aEnum = seqA.GetEnumerator()
            let rec loop res =
                if aEnum.MoveNext()
                then let a = aEnum.Current
                     bind (f a) 
                          (function
                          | true  -> loop (a::res)
                          | false -> loop res)
                else Return (List.rev res)
            loop []


        type DbBuilder() =
            member this.Return(v) = Return v
            member this.ReturnFrom(m) = m
            member this.Bind(m,f) = bind m f
            (*
            member this.Zero() = Return ()
            member this.Combine(ma,mb) = bind ma (fun _ -> mb())
            member this.Delay(f) = (fun () -> f())
            member this.Run(f) = f()
            *)
        /// operation builder for AddOnlyDb
        // TODO (better name?)
        let db = DbBuilder()


