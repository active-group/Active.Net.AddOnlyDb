namespace Active.Net.Test.Database

/// fns to test module KeyValueDb
module KeyValueDbTest =

    open NUnit.Framework
    open FsUnit
    open System.Configuration
    open Active.Net.Database

    [<TestFixture>]
    type ``KeyValueDBTest `` () =
        let mutable db: Option<KeyValueDB.T> = None
        let value ="value"
        let value2 = "value2"

        [<TestFixtureSetUp>]
        member x.``set up KV database`` () =
            let inMemoryDb = new DbCore.InMemoryDB() :> DbCore.DB
            let kvdb = KeyValueDB.ofDb inMemoryDb "KV" "K" "V"
            DbCore.executeNonQuery inMemoryDb (KeyValueDB.createKVTableSql kvdb) |> ignore
            db <- Some(kvdb)

        [<Test>]
        member x.``insert key,value`` () =
            KeyValueDB.assoc db.Value "test" value |> should equal ()

        [<Test>]
        member x.``inserted key,value is there`` () =
            x.``insert key,value``()
            KeyValueDB.getValue db.Value "test" |> should equal (Some value)

        [<Test>]
        member x.``overwrite key``() =
            KeyValueDB.assoc db.Value "key" value
            KeyValueDB.assoc db.Value "key" value2
            KeyValueDB.getValue db.Value "key" |> should equal (Some value2)

        [<TestFixtureTearDown>]
        member x.``close KV database`` () =
            db <- None


