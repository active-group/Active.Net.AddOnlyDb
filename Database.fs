namespace Active.Net.Database

module DbCore =
    open System.Data
    open System.Data.SQLite

    /// database is closed while executing query
    exception DatabaseClosedException of System.InvalidOperationException

    /// sql query parameter
    // simple tuple list ("name", value) does not work, as value have differing types
    type Parameter = {name:string; value:obj}
    /// sql command is a query with parameters
    type SqlCommand =
        {qury: string;             // `qury` to avoid name clash with some LINQ structure I cannot get rid of
         parameters: Parameter list}
    let makeSql query parameters = {qury = query; parameters=parameters}
    let makeSimpleSql query = {qury=query; parameters=[]}

    /// location of a database, either file-based or in-memory
    type DBLocation =
        | File of string
        | InMemory

    /// add SQLite command parameters
    let applyParameters (cmd:SQLiteCommand) (parameters:Parameter list) =
        for parameter in parameters do
            cmd.Parameters.AddWithValue(parameter.name, parameter.value) |> ignore

    /// create new SQLite command
    let newCommand (conn:SQLiteConnection) (sql:SqlCommand) =
        let cmd = new SQLiteCommand(sql.qury, conn)
        cmd.CommandType <- CommandType.Text  // sqlite supports only CommandType.Text
        applyParameters cmd sql.parameters
        cmd

    /// re-raise exception `e`, but transform to DatabaseClosedException if it was due to database being closed
    let reRaise (connState:ConnectionState) (e:System.InvalidOperationException) =
        if connState = ConnectionState.Closed
        then raise <| DatabaseClosedException(e)
        else raise e

    /// execute query that returns scalar value (first column of first row, if any)
    let _executeScalar (conn:SQLiteConnection) (sql:SqlCommand) : obj =
        let cmd = newCommand conn sql
        try
            cmd.ExecuteScalar()
        with :? System.InvalidOperationException as e ->
            reRaise conn.State e
    /// execute query that manipulates the database, returning #rows affected
    let _executeNonQuery (conn:SQLiteConnection) (sql:SqlCommand) : int =
        let cmd = newCommand conn sql
        try
            cmd.ExecuteNonQuery()
        with :? System.InvalidOperationException as e ->
            reRaise conn.State e
    /// execute query that returns list of values
    /// returns rows of columns as seq of obj[]
    /// NB: use only if performance is of value or you expect large result sets,
    /// otherwise, prefer _executeReader. This avoids subtle bugs related to
    /// this sequence being traversable only once. (I might have misunderstood something here...)
    let _executeReaderSeq (conn:SQLiteConnection) (sql:SqlCommand) : obj [] seq =
        let cmd = newCommand conn sql
        let reader =
            try
                cmd.ExecuteReader()
            with :? System.InvalidOperationException as e ->
                reRaise conn.State e
        let n = reader.FieldCount
        seq { while reader.Read() do
                let res : obj[] = Array.zeroCreate n
                reader.GetValues(res) |> ignore 
                yield res}
    let _executeReader conn sql : obj[][] =
        _executeReaderSeq conn sql |> Array.ofSeq

    /// A database provides an (open) connection to execute commands
    type DB =
        abstract member Execute<'T> : (SQLiteConnection -> 'T) -> 'T
        abstract member Close : unit -> unit

    /// run scalar sql on db
    let executeScalar (db:DB) (sql:SqlCommand) : obj =
        db.Execute (fun conn -> _executeScalar conn sql)
    /// run non-query sql on db
    let executeNonQuery (db:DB) (sql:SqlCommand) : int =
        db.Execute (fun conn -> _executeNonQuery conn sql)
    /// run reader sql on db
    let executeReader (db:DB) (sql:SqlCommand) : obj[][] =
        db.Execute (fun conn -> _executeReader conn sql)
    /// execute thunk in db transaction
    let inTransaction (db:DB) (thunk: SQLiteConnection -> 'T) : 'T =
        db.Execute(fun conn ->
            use tra = conn.BeginTransaction()
            let res = thunk(conn)
            tra.Commit()
            res)
    /// close database, ending all queries and freeing all ressources
    /// must reopen for new statements
    let close (db:DB) = db.Close()

    // Some Notes:
    // Connections should be opened immediate before the query, and closed immediate afterwards
    // (see https://msdn.microsoft.com/en-us/library/8xx3tyca.aspx?f=255&MSPPError=-2147217396)
    // So that should be part of the querys, not of the database.
    // However, for in-memory databases, we loose all changes to the database, when we
    // close the connection (see https://www.sqlite.org/c3ref/open.html)
    // 2015-08-14 AB: closing connection fails executeReader (connection closed when seq is accessed)
    //   -- Further, connection pooling must be turned on
    //   (https://www.connectionstrings.com/sqlite-net-provider/with-connection-pooling/),
    //   which we haven't. Last, the referenced article seems to refer to Microsoft's "SQL Server"
    //   database, which is commonly accessed via network and where pooling makes sense (it's "costly"
    //   to connect to the database via network). Thus, we keep a single connection open.

    /// SQLite DB located in memory
    type InMemoryDB() =
        let conn = new SQLiteConnection("Data Source=:memory:")
        do conn.Open()
        interface DB with
            member db.Execute f = f conn
            member db.Close () = conn.Close()

    /// SQLite DB located on disk (in file)
    type InFileDB(path:string, createIfMissing:bool) =
        let connString = sprintf "Data Source=%s; FailIfMissing=%s" path (if createIfMissing then "false" else "true")
        let conn =
            if path.Contains(";") || path.Contains("=")
            then failwith "Invalid path"
            else
                if createIfMissing 
                then System.IO.Path.GetDirectoryName path
                     |> System.IO.Directory.CreateDirectory
                     |> ignore
                (new SQLiteConnection(connString)).OpenAndReturn()
        interface DB with
            member db.Execute f = f conn
            member db.Close () = conn.Close()

    /// open database ('open' is a keyword), maybe creating it
    // helper fn, but possible useful for ohers, too, thus not private but clumsy name
    let maybeCreateAndAccess (dbLocation:DBLocation) (createIfMissing:bool) =
        match dbLocation with
        | InMemory -> new InMemoryDB() :> DB
        | File path -> new InFileDB(path, createIfMissing) :> DB

    /// open database at `dbLocation` (either in memory or file-based), fail if missing
    let access (dbLocation:DBLocation) = maybeCreateAndAccess dbLocation false

    /// create db at `dbLocation` -- note that you have to open it separately
    let create(dbLocation:DBLocation) =
        match dbLocation with
        | InMemory -> ()
        | File path ->
            (maybeCreateAndAccess dbLocation true).Close()

    /// true iff db exists at dbLocation
    let exists(dbLocation:DBLocation) =
        match dbLocation with
        | InMemory -> true
        | File path -> System.IO.File.Exists(path)

