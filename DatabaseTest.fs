namespace Active.Net.Test.Database

/// fns to test module Active.Net.Database.DbCore
module DbCoreTest =

    open NUnit.Framework
    open FsUnit
    open System.Configuration
    open Active.Net.Database

    [<TestFixture>]
    type ``DatabaseTest `` () =

        [<TestFixtureSetUp>]
        member x.``set up`` () = ()

        [<Test>]
        member x.``test`` () = ()

        [<TestFixtureTearDown>]
        member x.``tear down`` () = ()
