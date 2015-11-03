namespace Active.Net.Database

/// Key-Value Database, storing data in `table`, using column `keyColumn
/// for keys and `valueColumn` for values (blob/byte[])
module KeyValueDB =
    open DbCore
    open System.Data
    open System.Data.SQLite

    open FSharpx.Option // maybe monad

    /// A KeyValueDB has an underlying (sqlite3) database
    /// and a key-value table named `table`,
    /// whose key column is named `keyColumn`
    /// and whose value column is named `valueColumn`
    type T = { db:DB; table:string; keyColumn:string; valueColumn:string}
    /// type of the key column
    type Key = string
    /// type of the value column
    type Value = string

    /// create KeyValueDB.T from db, table name, column names
    let ofDb db table keyColumn valueColumn =
        {db=db; table=table; keyColumn=keyColumn; valueColumn=valueColumn}

    /// Insert key,value; return #rows affected
    /// key should not exist (but that is not checked)
    /// conn is already opened
    let private insert (kvdb:T) (conn:SQLiteConnection) (key:Key) (value:Value) =
        let sql = {qury= sprintf @"INSERT INTO %s VALUES(:key,:value)" kvdb.table;
            parameters = [{name=":key"; value=key};
                            {name=":value"; value=value}]}
        _executeNonQuery conn sql

    /// Set key,value (sql "UPDATE"); return #rows affected
    /// key should exist (but that is not checked)
    /// conn is already opened
    let private set (kvdb:T) (conn:SQLiteConnection) (key:Key) (value:Value) =
        let sql = {qury= sprintf @"UPDATE %s SET %s = :value WHERE %s = :key" kvdb.table kvdb.valueColumn kvdb.keyColumn;
                    parameters = [{name="key"; value=key}
                                  {name="value"; value=value}]}
        _executeNonQuery conn sql

    /// sql command that creates key value table of KeyValueDB.T
    let createKVTableSql (kvdb:T) =
        { qury =
            sprintf """CREATE TABLE "%s" ("%s" VARCHAR(255) PRIMARY KEY NOT NULL,"%s" TEXT DEFAULT (null) )"""
                kvdb.table kvdb.keyColumn kvdb.valueColumn;
          parameters = [] }

    /// Retrieve value for given key from key-value database, if any
    let getValue (kvdb:T) (key:Key) =
        let sql = {qury = sprintf @"SELECT %s FROM %s WHERE %s = :key" kvdb.valueColumn kvdb.table kvdb.keyColumn;
                    parameters = [{name = ":key"; value=key}]}
        let v = executeScalar kvdb.db sql
        match v with
        | null -> None
        | x    -> Some (v :?> Value)

    /// Associate key with value; overwrites entry, if exists
    let assoc (kvdb:T) (key:Key) (value:Value) =
        inTransaction kvdb.db (fun(conn) ->
            let sql = {qury = sprintf @"SELECT COUNT(*) FROM %s WHERE %s = :key" kvdb.table kvdb.keyColumn;
                        parameters = [{name=":key"; value=key}]}
            match _executeScalar conn sql with
            | :? System.Int64 as N ->
                match N with
                | 0L -> insert kvdb conn key value
                | 1L -> set kvdb conn key value
                | _ -> failwithf "too many entries in db for key '%s'" key
            | e -> failwithf "did not return int64, but %s" (e.GetType().ToString())
            ) |> ignore

    /// Dissociate key, ie. remove key from database; return number of deleted entries
    let dissoc (kvdb:T) (key:Key) : int (*Value option*) =
        let sql = {qury = sprintf @"DELETE FROM %s WHERE %s = :key" kvdb.table kvdb.keyColumn;
                   parameters = [{name=":key"; value=key}]}
        executeNonQuery kvdb.db sql

    /// delete all entries
    let deleteAll (kvdb:T) =
        let sql = {qury = sprintf @"DELETE FROM %s" kvdb.table; parameters = []}
        executeNonQuery kvdb.db sql

    /// open Key-Value database
    let access (dbLocation:DBLocation) =
        let db = access dbLocation
        {db=db; table="KV"; keyColumn="K"; valueColumn="V"}
