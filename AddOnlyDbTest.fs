namespace Active.Net.Test

module AddOnlyDbTest =
    open Active.Net // Guid, Hash, Bytes, Text
    open NUnit.Framework
    open Active.Net.TestUtil
    open FsUnit
    open FsCheck
    open FSharpx.Functional.Prelude // curry
    open Active.Net.Database
    open Active.Net.Database.AddOnlyDb
    open Active.Net.Database.AddOnlyDb.API


    [<TestFixture>]
    type AddOnlyDbTest () =
        let openMemDb () = DbCore.access DbCore.InMemory
        let makeAoDb db = make db Hash.sha256 "kv" "hashes" "kvCurrent"
        let makeAoDbInMemory () =
            let db = openMemDb()
            makeAoDb db
        let makeAoDbInMemoryAndCreateTables() =
            let aodb = makeAoDbInMemory()
            createTablesAndIndices aodb |> ignore
            aodb

        // sample data for 3 rows 
        // (hash1, guid1, prop1, value1, meta1, [])
        // (hash2, guid2, prop2, value2, meta2, [hash1])
        // (hash3, guid3, prop3, value3, meta3, [hash2, hash1])   // (hash2 < hash1)
        let guid1 = [| 0uy..15uy|] |> Guid.fromByteArray  // {03020100-0504-0706-0809-0a0b0c0d0e0f}
        let guid2 = [|16uy..31uy|] |> Guid.fromByteArray // {13121110-1514-1716-1819-1a1b1c1d1e1f}
        let guid3 = [|32uy..47uy|] |> Guid.fromByteArray // {23222120-2524-2726-2829-2a2b2c2d2e2f}

        let prop1 = Property "test1"
        let prop2 = Property "test2"
        let prop3 = Property "test3"

        let value1 = [|64uy;32uy;65uy|]
        let value2 = [|66uy;32uy;67uy|]
        let value3 = [|68uy;32uy;69uy|]

        let meta1 = Meta.make "heli" "wien" (DateTime.fromIso8601String "2015-08-11T06:00:15.0000000Z") // "{"user":"heli","machine":"wien","datetime":"2015-08-11T06:00:15.0000000+00:00"}"
        let meta2 = Meta.make "andi" "playmobil" (DateTime.fromIso8601String "2015-08-11T06:00:16.0000000Z") // "{"user":"andi","machine":"playmobil","datetime":"2015-08-11T06:00:16.0000000+00:00"}"
        let meta3 = Meta.make "mike" "tübingen" (DateTime.fromIso8601String "2015-08-11T06:00:17.0000000Z") // "{"user":"mike","machine":"tübingen","datetime":"2015-08-11T06:00:17.0000000+00:00"}"

        let hash1 = Bytes.fromHexString "e3bb59b251e0396b144459d726f39f5ad3c3d72c90830139ecc8343f81f93033"
                    |> Hash.fromByteArray
        let hash2 = Bytes.fromHexString "e0780ce2f965bbc11dd62ad359ad903acbc3448495a82b2fd68a16fc3ea01093"  // gsha256sum file2
                    |> Hash.fromByteArray
        let hash3 = Bytes.fromHexString "06a56aad5438ce6e5f79ed55fd7c2df0908fb4063b7f12d831298165191d2fec" // gsha256sum file3
                    |> Hash.fromByteArray

        let row1 = (hash1, guid1, prop1, value1, meta1, Array.empty<Hash.T>)
        let row2 = (hash2, guid2, prop2, value2, meta2, [|hash1|])
        let row3 = (hash3, guid3, prop3, value3, meta3, [|hash2; hash1|])
        let rows = [row1;row2;row3]

        /// hash algorithm used in tests
        let sha256 = Hash.sha256
        /// test that Testing.computeHash calculates correct hash
        let computeHashTest (hash, guid, (Property property), value, meta, obsoletes) =
            let guidBytes = Guid.toByteArray guid
            let metaString = Meta.toString meta
            let obsoletesBytes = obsoletes |> Array.map Hash.toByteArray
            computeHash_ sha256 guidBytes property value metaString obsoletesBytes
            |> should equal hash

        /// test that adding the fact returns the expected hash
        // args as tuple because rows definition above
        let addTest aodb (hash, guid, property, value, meta, obsoletes) =
            Testing.addFact aodb guid property value meta obsoletes
            |> should equal hash

        /// return n distinct values as byte[]
        let distinctValues seed n : byte[] list =
            let rnd = new System.Random(seed)
            // assume init walks sequentially to have repeatable results
            List.init n (fun _ -> let bs : byte[] = Array.zeroCreate 10 // assume 256^10 is large enough to avoid duplicates
                                  rnd.NextBytes(bs)
                                  bs)

        [<TestFixtureSetUp>]
        member x.``open in-memory db`` () =
            Arb.register<Testing.DataGenerators>() |> ignore


        [<Test>]
        member x.``syntax check create commands`` () =
            let aodb = makeAoDbInMemory ()
            createTablesAndIndices aodb |> ignore

        [<Test>]
        member x.``computeHash calculates correct sha256 hash w/o obsoletes`` () =
            computeHashTest row1

        [<Test>]
        member x.``computeHash calculates correct sha256 hash with 1 obsoletes`` () =
            computeHashTest row2

        [<Test>]
        member x.``computeHash calculates correct sha256 hash with 2 obsoletes`` () =
            computeHashTest row3

        [<Test>]
        member x.``can insert values`` () =
            let aodb = makeAoDbInMemoryAndCreateTables ()
            rows
            |> List.iter (addTest aodb)

        [<Test>]
        member x.``can retrieve non-existing value`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [])

        [<Test>]
        member x.``can retrieve non-obsoleted value`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.addFact aodb guid1 prop1 value1 meta1 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value1])

        [<Test>]
        member x.``can retrieve non-obsoleted values of different keys`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.addFact aodb guid1 prop1 value1 meta1 [||] |> ignore
            Testing.addFact aodb guid2 prop2 value2 meta2 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value1])
            Testing.retrieve aodb guid2 prop2
            |> should equal (seq [value2])

        [<Test>]
        member x.``can retrieve non-obsoleted values of different properties`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.addFact aodb guid1 prop1 value1 meta1 [||] |> ignore
            Testing.addFact aodb guid1 prop2 value2 meta2 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value1])
            Testing.retrieve aodb guid1 prop2
            |> should equal (seq [value2])

        [<Test>]
        member x.``can retrieve 2 non-obsoleted values of same key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.addFact aodb guid1 prop1 value1 meta1 [||] |> ignore
            Testing.addFact aodb guid1 prop1 value2 meta1 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> Set.ofSeq
            |> should equal (Set.ofList [value1; value2])

        [<Test>]
        member x.``can retrieve 3 non-obsoleted values of same key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            Testing.addFact aodb guid1 prop1 value1 meta1 [||] |> ignore
            Testing.addFact aodb guid1 prop1 value2 meta1 [||] |> ignore
            Testing.addFact aodb guid1 prop1 value3 meta1 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> Set.ofSeq
            |> should equal (Set.ofList [value1; value2; value3])

        [<Test>]
        member x.``can obsolete 1 previous value`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            Testing.addFact aodb guid1 prop1 value2 meta1 [|hash1|] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value2])

        [<Test>]
        member x.``can obsolete 2 previous values at once`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid1 prop1 value2 meta1 [||]
            Testing.addFact aodb guid1 prop1 value3 meta1 [|hash1;hash2|] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value3])

        [<Test>]
        member x.``can obsolete 2 previous values in chain`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid1 prop1 value2 meta1 [|hash1|]
            Testing.addFact aodb guid1 prop1 value3 meta1 [|hash2|] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> should equal (seq [value3])

        [<Test>]
        member x.``can obsolete 10000 previous values in chain`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let values = distinctValues 0x45 10000
            let hash1 = Testing.addFact aodb guid1 prop1 values.[0] meta1 [||]
            values.Tail
            |> List.fold (fun lastHash value ->
                              Testing.addFact aodb guid1 prop1 value meta1 [|lastHash|])
                         hash1
            |> ignore
            let sw = System.Diagnostics.Stopwatch()
            sw.Start()
            let v = Testing.retrieve aodb guid1 prop1
            sw.Stop ()
            System.Diagnostics.Debug.Print("retrieving with 10000 obsoletes took {0} ms", sw.ElapsedMilliseconds)
            v
            |> should equal (seq [values |> List.rev |> List.head])

        [<Test>]
        member x.``can obsolete 2 previous values in chain of different keys`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid2 prop1 value2 meta1 [|hash1|]
            Testing.addFact aodb guid3 prop1 value3 meta1 [|hash2|] |> ignore
            Testing.retrieve aodb guid3 prop1
            |> should equal (seq [value3])

        [<Test>]
        member x.``can store 1000 different values under same key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let values = distinctValues 0x12 1000
            for value in values do
                Testing.addFact aodb guid1 prop1 value meta1 [||] |> ignore

        [<Test>]
        member x.``can retrieve 1000 different values from same key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let values = distinctValues 0x12 1000
            for value in values do
                Testing.addFact aodb guid1 prop1 value meta1 [||] |> ignore
            Testing.retrieve aodb guid1 prop1
            |> Set.ofSeq
            |> should equal (Set.ofList values)

        [<Test>]
        member x.``can store 1000 values under different keys`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let values = distinctValues 0x12 1000
            let props = values |> List.map (Bytes.toHexString >> Property)
            for prop,value in (List.zip props values) do
                Testing.addFact aodb guid1 prop value meta1 [||] |> ignore

        [<Test>]
        member x.``can retrieve 1000 values from different keys`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let values = distinctValues 0x12 1000
            let props = values |> List.map (Bytes.toHexString >> Property)
            for prop,value in (List.zip props values) do
                Testing.addFact aodb guid1 prop value meta1 [||] |> ignore
            for prop,value in (List.zip props values) do
                Testing.retrieve aodb guid1 prop
                |> should equal (seq [value])

        [<Test>]
        member x.``can retrieve hash key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            Testing.retrieveHashKey aodb (makeSearchKey (Some guid1) (Some prop1) (Some value1))
            |> should equal (seq [hash1])

        [<Test>]
        member x.``does not retrieve obsoleted hash key`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid1 prop1 value2 meta1 [|hash1|]
            Testing.retrieveHashKey aodb (makeSearchKey (Some guid1) (Some prop1) (Some value1))
            |> should equal (seq [])
            Testing.retrieveHashKey aodb (makeSearchKey (Some guid1) (Some prop1) (Some value2))
            |> should equal (seq [hash2])

        [<Test>]
        member x.``can retrieve hash keys for any value`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid1 prop1 value2 meta1 [||]
            Testing.retrieveHashKey aodb (makeSearchKey (Some guid1) (Some prop1) None)
            |> Set.ofSeq
            |> should equal (Set.ofList [hash1;hash2])

        [<Test>]
        member x.``can retrieve hash keys for any property`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid1 prop2 value1 meta1 [||]
            Testing.retrieveHashKey aodb (makeSearchKey (Some guid1) None (Some value1))
            |> Set.ofSeq
            |> should equal (Set.ofList [hash1;hash2])

        [<Test>]
        member x.``can retrieve hash keys for any guid`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid2 prop1 value1 meta1 [||]
            Testing.retrieveHashKey aodb (makeSearchKey None (Some prop1) (Some value1))
            |> Set.ofSeq
            |> should equal (Set.ofList [hash1;hash2])

        [<Test>]
        member x.``can retrieve all hash keys`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid2 prop1 value1 meta1 [||]
            Testing.retrieveHashKey aodb (makeSearchKey None None None)
            |> Set.ofSeq
            |> should equal (Set.ofList [hash1;hash2])

        [<Test>]
        member x.``can retrieve all hash keys but no obsoleted ones`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let hash1 = Testing.addFact aodb guid1 prop1 value1 meta1 [||]
            let hash2 = Testing.addFact aodb guid2 prop1 value1 meta1 [||]
            let hash3 = Testing.addFact aodb guid3 prop1 value1 meta1 [|hash2|]
            Testing.retrieveHashKey aodb (makeSearchKey None None None)
            |> Set.ofSeq
            |> should equal (Set.ofList [hash1;hash3])

        [<QuietProperty>]
        member x.``Data binary serialization roundtrip`` (d:Data.T) =
            Data.toBytesArray d
            |> Data.fromBytesArray
            |> (curry Assert.AreEqual) d

        [<Test>]
        member x.``API put row1`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            putAndReturnHash guid1 prop1 value1 meta1 obsoletes1
            |> run aodb
            |> (curry Assert.AreEqual hash1)

        [<Test>]
        member x.``API put row1-3`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            let (hash2,guid2,prop2,value2,meta2,obsoletes2) = row2
            let (hash3,guid3,prop3,value3,meta3,obsoletes3) = row3
            db {
                let! hash1 = putAndReturnHash guid1 prop1 value1 meta1 obsoletes1
                let! hash2 = putAndReturnHash guid2 prop2 value2 meta2 obsoletes2
                let! hash3 = putAndReturnHash guid3 prop3 value3 meta3 obsoletes3
                return (hash1,hash2,hash3)
            }
            |> run aodb
            |> (curry Assert.AreEqual (hash1,hash2,hash3))

        [<Test>]
        member x.``API get row1`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            db {
                do! put guid1 prop1 value1 meta1 obsoletes1
                let! v1 = get guid1 prop1
                return v1
            }
            |> run aodb
            |> (curry Assert.AreEqual ([|value1|]))

        [<Test>]
        member x.``API get row2 but not row1`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            let (hash2,guid2,prop2,value2,meta2,obsoletes2) = row2
            db {
                do! put guid1 prop1 value1 meta1 obsoletes1
                do! put guid2 prop2 value2 meta2 obsoletes2
                let! v1 = get guid1 prop1
                let! v2 = get guid2 prop2
                return (v1, v2)
            }
            |> run aodb
            |> (curry Assert.AreEqual ([||], [|value2|]))

        [<Test>]
        member x.``API get row3 but not row1 or row2`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            let (hash2,guid2,prop2,value2,meta2,obsoletes2) = row2
            let (hash3,guid3,prop3,value3,meta3,obsoletes3) = row3
            db {
                do! put guid1 prop1 value1 meta1 obsoletes1
                do! put guid2 prop2 value2 meta2 obsoletes2
                do! put guid3 prop3 value3 meta3 obsoletes3
                let! v1 = get guid1 prop1
                let! v2 = get guid2 prop2
                let! v3 = get guid3 prop3
                return (v1, v2, v3)
            }
            |> run aodb
            |> (curry Assert.AreEqual ([||], [||], [|value3|]))

        [<Test>]
        member x.``API getHashKey`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            db {
                do! put guid1 prop1 value1 meta1 obsoletes1
                let! v1 = getHashKey (makeSearchKey (Some guid1) (Some prop1) (None))
                return v1
            }
            |> run aodb
            |> (curry Assert.AreEqual ([|hash1|]))

        [<Test>]
        member x.``API getWithHashKey`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            db {
                do! put guid1 prop1 value1 meta1 obsoletes1
                let! v1 = getWithHashKey guid1 prop1
                return v1
            }
            |> run aodb
            |> (curry Assert.AreEqual ([|hash1,value1|]))

        [<Test>]
        member x.``API putValue`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            db {
                let! hash = putValueAndReturnHash guid1 prop1 value1
                let! gotHash = getHashKey (makeSearchKey (Some guid1) (Some prop1) (None))
                return (hash,gotHash)
            }
            |> run aodb
            |> (fun (hash,gotHashList) ->
                Assert.AreEqual([|hash|], gotHashList))

        [<Test>]
        member x.``API putWithObsoletes`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let (hash1,guid1,prop1,value1,meta1,obsoletes1) = row1
            let (hash2,guid2,prop2,value2,meta2,obsoletes2) = row2
            db {
                let! hash1 = putValueAndReturnHash guid1 prop1 value1
                let! hash2 = putWithObsoletesAndReturnHash guid2 prop2 value2 [|hash1|]
                let! gotHash1 = getHashKey (makeSearchKey (Some guid1) (Some prop1) (None))
                let! gotHash2 = getHashKey (makeSearchKey (Some guid2) (Some prop2) (None))
                return (hash2,gotHash1,gotHash2)
            }
            |> run aodb
            |> (fun (hash2, gotHashList1, gotHashList2) ->
                Assert.AreEqual([||], gotHashList1)
                Assert.AreEqual([|hash2|], gotHashList2))

        [<Test>]
        member x.``hashExists`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                return! putAndReturnHash guid1 prop1 value1 meta1 Array.empty
            }
            |> run aodb
            |> (fun hash1 ->
                idExists aodb hash1 |> Assert.IsTrue
                idExists aodb hash2 |> Assert.IsFalse
                )

        [<Test>]
        member x.``duplicate insert is silently dropped`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                let! hash1 = putAndReturnHash guid1 prop1 value1 meta1 Array.empty
                // same fact as previously
                let! hash2 = putAndReturnHash guid1 prop1 value1 meta1 Array.empty
                return (hash1, hash2)
            }
            |> run aodb
            |> (Assert.AreEqual)

        [<Test>]
        member x.``atomically executes 1 op sucessfully and returns value`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                do! atomically (put guid1 prop1 value1 meta1 Array.empty)
                let! value1Get = get guid1 prop1
                return value1Get
            }
            |> run aodb
            |> (fun (value1Get) ->
                Assert.AreEqual([|value1|], value1Get)
                )

        [<Test>]
        member x.``atomically executes 2 ops sucessfully`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                do! atomically (db {
                    do! put guid1 prop1 value1 meta1 Array.empty
                    do! put guid2 prop2 value2 meta2 Array.empty
                    return ()
                    })
                let! value1Get = get guid1 prop1
                let! value2Get = get guid2 prop2
                return (value1Get, value2Get)
            }
            |> run aodb
            |> (fun (value1Get, value2Get) ->
                Assert.AreEqual([|value1|], value1Get)
                Assert.AreEqual([|value2|], value2Get)
                )

        [<Test>]
        member x.``atomically can be nested`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                do! atomically (db {
                    do! put guid1 prop1 value1 meta1 Array.empty
                    do! atomically (db {
                        do! put guid2 prop2 value2 meta2 Array.empty
                    })
                    return ()
                    })
                let! value1Get = get guid1 prop1
                let! value2Get = get guid2 prop2
                return (value1Get, value2Get)
            }
            |> run aodb
            |> (fun (value1Get, value2Get) ->
                Assert.AreEqual([|value1|], value1Get)
                Assert.AreEqual([|value2|], value2Get)
                )

        [<Test>]
        member x.``atomically can be nested 3 times`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                do! atomically (db {
                    do! put guid1 prop1 value1 meta1 Array.empty
                    do! atomically (db {
                        do! put guid2 prop2 value2 meta2 Array.empty
                        return! atomically (put guid3 prop3 value3 meta3 Array.empty)
                    })
                    return ()
                    })
                let! value1Get = get guid1 prop1
                let! value2Get = get guid2 prop2
                let! value3Get = get guid3 prop3
                return (value1Get, value2Get, value3Get)
            }
            |> run aodb
            |> (fun (value1Get, value2Get, value3Get) ->
                Assert.AreEqual([|value1|], value1Get)
                Assert.AreEqual([|value2|], value2Get)
                Assert.AreEqual([|value3|], value3Get)
                )

        [<Test>]
        member x.``atomically aborts 1 op sucessfully`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            try
                db {
                    do! atomically (db {
                        do! put guid1 prop1 value1 meta1 Array.empty
                        let a = raise (System.Exception("aborted"))
                        return ()
                        })
                    let! value1Get = get guid1 prop1
                    return value1Get
                }
                |> run aodb
                |> (fun _ -> Assert.Fail("exception did not propagate"))
            with
            | _ ->
                get guid1 prop1
                |> run aodb
                |> (fun v1 -> Assert.AreEqual(0, v1.Length, "there should be no value stored"))

        [<Test>]
        member x.``atomically aborts 1 op sucessfully, but not previous ops`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            try
                db {
                    do! put guid2 prop2 value2 meta2 Array.empty
                    do! atomically (db {
                        do! put guid1 prop1 value1 meta1 Array.empty
                        let a = raise (System.Exception("aborted"))
                        return ()
                        })
                    let! value1Get = get guid1 prop1
                    return value1Get
                }
                |> run aodb
                |> (fun _ -> Assert.Fail("exception did not propagate"))
            with
            | _ ->
                db {
                    let! v2 = get guid2 prop2
                    let! v1 = get guid1 prop1
                    return (v2, v1)
                }
                |> run aodb
                |> (fun (v2,v1) ->
                    Assert.AreEqual([|value2|], v2)
                    Assert.AreEqual([||], v1, "there should be no value stored"))

        [<Test>]
        member x.``atomically aborts 2 ops sucessfully`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            try
                db {
                    do! atomically (db {
                        do! put guid1 prop1 value1 meta1 Array.empty
                        let a = raise (System.Exception("aborted"))
                        do! put guid2 prop2 value2 meta2 [|hash1|]
                        return ()
                        })
                    let! value1Get = get guid1 prop1
                    let! value2Get = get guid2 prop2
                    return (value1Get,value2Get)
                }
                |> run aodb
                |> (fun _ -> Assert.Fail("exception did not propagate"))
            with
            | _ ->
                db {
                    let! v1 = get guid1 prop1
                    let! v2 = get guid2 prop2
                    do! put guid3 prop3 value3 meta3 Array.empty
                    let! v3 = get guid3 prop3
                    return (v1, v2,v3)
                }
                |> run aodb
                |> (fun (v1,v2,v3) ->
                    Assert.AreEqual(0, v1.Length, "there should be no value stored for guid1.prop1")
                    Assert.AreEqual(0, v2.Length, "there should be no value stored for guid2.prop2")
                    Assert.AreEqual([|value3|], v3)
                    )

        [<Test>]
        member x.``atomically does not extend beyond its op`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            try
                db {
                    do! atomically (put guid1 prop1 value1 meta1 Array.empty)
                    do! put guid2 prop2 value2 meta2 Array.empty
                    raise (System.Exception("aborted"))
                    return ()
                }
                |> run aodb
                |> (fun _ -> Assert.Fail("exception did not propagate"))
            with
            | e ->
                db {
                    let! v1 = get guid1 prop1
                    let! v2 = get guid2 prop2
                    return (v1, v2)
                }
                |> run aodb
                |> (fun (v1,v2) ->
                    Assert.AreEqual([|value1|], v1)
                    Assert.AreEqual([|value2|], v2))

        [<QuietProperty>]
        member x.``monadic filter works like List.filter`` (ints : int list) =
             let f = (fun i -> i % 2 = 0)
             let expect = ints |> List.filter f 
             let actual_ = filterM (fun i -> Return(f i)) ints
             let actual =
               let aodb = makeAoDbInMemoryAndCreateTables()
               actual_ |> run aodb
             Assert.AreEqual(expect, actual)

        [<QuietProperty>]
        member x.``monadic map works like List.map`` (ints: int list, f: int -> int) =
            let expect = ints |> List.map f
            let actual_ = mapM (fun i -> Return(f i)) ints
            let actual = 
                let aodb = makeAoDbInMemoryAndCreateTables()
                actual_ |> run aodb
            Assert.AreEqual(expect, actual)

        [<QuietProperty>]
        member x.``monadic fold works like List.fold`` (strings: string list) =
            let accumulate acc s = acc + s
            let expect = strings |> List.fold accumulate "" 
            let actual_ = foldM (fun acc i -> Return(accumulate acc i)) "" strings
            let actual =
                let aodb = makeAoDbInMemoryAndCreateTables()
                actual_ |> run aodb
            Assert.AreEqual(expect, actual)


        [<QuietProperty>]
        member x.``monadic iter works like List.Iter`` (strings: string list) =
            let acc1: string ref = ref ""
            let acc2: string ref = ref ""
            let conc1 s = acc1.Value <- acc1.Value + s
            let conc2 s = acc2.Value <- acc2.Value + s
            let expect = strings |> List.iter conc1
            let actual_ = iterM (fun i -> Return(conc2 i)) strings
            let actual =
                let aodb = makeAoDbInMemoryAndCreateTables()
                actual_ |> run aodb
            Assert.AreEqual(acc1, acc2)

        [<QuietProperty>]
        member x.``idExists is true iff hash is already in DB`` (guid, prop, value, meta, obsoletes) =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {    
                let! hash = putAndReturnHash guid prop value meta obsoletes    
                let shouldExist = idExists aodb hash    
                let shouldNotExist = idExists aodb hash1 |> not// example hash    
                return (shouldExist && shouldNotExist)    
                }|> run aodb    
            
       
        [<QuietProperty>]
        member x.``putBlock writes block in DB that can effectively be recovered`` (guid, prop, value, meta, obsoletes) =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                let hash = computeHash aodb guid prop value meta obsoletes    
                let validBlock = (hash, (guid , prop, value, meta, obsoletes)) 
                let invalidBlock = (hash1, (guid , prop, value, meta, obsoletes))
                do putBlock aodb validBlock
                do putBlock aodb invalidBlock // blocks with wrong hash should not be written to DB   
                let actualBlock = getBlock aodb hash    
                let shouldNotExist = idExists aodb hash1 |> not // example hash 
                return ((actualBlock = validBlock) && shouldNotExist)   
                } |> run aodb    
        
        [<QuietProperty>]
        member x.``getBlock recovers block written before`` (guid, prop, value, meta, obsoletes) =
            let aodb = makeAoDbInMemoryAndCreateTables()
            db {
                let! hash = putAndReturnHash guid prop value meta obsoletes
                let expectedBlock = (hash, (guid, prop, value, meta, obsoletes))
                let actualBlock = getBlock aodb hash    
                return (actualBlock = expectedBlock)  
                } |> run aodb    

        [<QuietProperty>]
        member x.``getBlocks recovers 3 blocks written before`` () = 
            let aodb = makeAoDbInMemoryAndCreateTables()         
            db {                                                 
                let! h1 = putAndReturnHash guid1 prop1 value1 meta1 Array.empty
                let! h2 = putAndReturnHash guid2 prop2 value2 meta2 Array.empty
                let! h3 = putAndReturnHash guid3 prop3 value3 meta3 Array.empty
                let b1: BlockT = (h1, (guid1, prop1, value1, meta1, Array.empty))
                let b2: BlockT = (h2, (guid2, prop2, value2, meta2, Array.empty))
                let b3: BlockT = (h3, (guid3, prop3, value3, meta3, Array.empty))
                let actualBlocks = getBlocks aodb [|h1; h2; h3|] |> Array.sort   // sort arrays before checking for equality
                return (([|b1; b2; b3|] |> Array.sort) = actualBlocks)
                } |>run aodb 

        
            
        [<QuietProperty>]
        member x.``getBlocks delivers sorted obsoletes`` () =
            let aodb = makeAoDbInMemoryAndCreateTables()
            let unsorted: ObsoletesT  = Testing.obsoleteGenerator |> Gen.sample 10 20 |> List.toArray 
            let sorted: ObsoletesT = unsorted |> Array.sort
            let actual =
                db {
                    let! h = putAndReturnHash guid1 prop1 value1 meta1 unsorted
                    let  blocks = getBlocks aodb [|h|]
                    let  (_, (_ , _, _, _, obsoletes))  = blocks.[0]
                    return obsoletes
                    } |> run aodb
            Assert.AreEqual (sorted, actual) 

        [<QuietProperty>]
        member x.``fromDbTypes and toDbBytes are almost inverses (module sorting obsoletes)`` ((g,p,v,m,o):FactT) =
            let expectedFact = g,p,v,m,(o|> Array.sortWith Hash.compare)
            // where is quadCurry when you need it...
            toDbTypes g p v m o
            |> (fun (g,p,v,m,o) -> fromDbTypes g p v m o)
            |> ((=) expectedFact)

        [<TestFixtureTearDown>]
        member x.``close in-memory db`` () =
            ()
