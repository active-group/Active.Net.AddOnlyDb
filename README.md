# Active.Net.AddOnlyDb

F# package for lightweight, easily synchronizable data store based
on SQLite.

# Usage

Quick and detailed usage examples.

[Active.Net.Net](https://github.com/active-group/Active.Net.Net) has helper functions for common stuff (Guid, Hash, Text
to/from bytes, etc.)

## Quick

    open Active.Net.Database
	// hash algorithm to calculate fact ids; use Active.Net.Hash for functional interface
	let sha256() = new System.Security.Cryptography.SHA256Managed() :> HashAlgorithm

	// create
	let db = DbCore.access (DbCore.File "addonly.sqlite3") // or DbCore.InMemory
	let aodb = AddOnlyDb.make db sha256 "kv" "hashes" "kvCurrent"
	AddOnlyDb.createTablesAndIndices aodb |> ignore

	// read
	let printers =
		AddOnlyDb.API.db {
			let! values = AddOnlyDb.API.get guid (Property "printer")
			return values
		}

	// write
	let setPrinter printer =
		AddOnlyDb.API.db {
			return! AddOnlyDb.API.putWithObsoletes guid (Property "printer") printer printers
		}

	// close
	AddOnlyDb.close aodb


## Detailed

    open Active.Net.Database

	/// open existing (but 'open' is a keyword); fails if missing
	// DBLocation allows File or in-memory DB
    let access (dbLocation:DbCore.DBLocation) =
        let db = DbCore.access dbLocation
        // turn on foreign-key check (optional)
        DbCore.executeNonQuery db {qury="PRAGMA foreign_keys=on";parameters=[]} |> ignore
		// hash algorithm to calculate fact ids; use Active.Net.Hash for functional interface
		let sha256() = new System.Security.Cryptography.SHA256Managed() :> HashAlgorithm
		// kv, hashes, kvCurrent: names of AddOnlyDb tables
        AddOnlyDb.make db Hash.sha256 "kv" "hashes" "kvCurrent"

	/// create new
	let create (dbLocation:DbCore.DBLocation) =
		DbCore.create dbLocation
		let aodb = access dbLocation
		AddOnlyDb.createTablesAndIndices aodb |> ignore
		aodb

	/// close
    let close aodb =
        AddOnlyDb.close aodb

	// some GUID; use Active.Net.Guid for functional interface
	let guid = System.Guid.ParseExact("66344F65-B009-4617-ADC7-93404DBABFC3", "D")

	/// overwrite value, obsoleting existing
	let private overwrite guid prop value =
        API.db {
            let! obsoletes = API.getHashKey (makeSearchKey (Some guid) (Some prop) None)
            do! API.putWithObsoletes guid prop value obsoletes
            return ()
        }

    /// get single-valued property;
    /// if there was a conflicting sync, this will return several values;
    /// if there is no value, will return empty array
    let getProp guid prop (unmarshal:byte[] -> 'a) : 'a[] Op= db {
        let! values = API.get guid prop
        return Array.map unmarshal values
    }

	// from Active.Net.Net
    let private utf8Encoding = new System.Text.UTF8Encoding(false, true) // no BOM, throw on invalid bytes

    let private utf8Bytes (s:string) = utf8Encoding.GetBytes(s)
    let private utf8String (utf8bytes:byte[]) = utf8Encoding.GetString utf8bytes

    let private displayNameToBytes = utf8Bytes
    let private displayNameFromBytes = utf8String

    let private stationDisplayNameProp = AddOnlyDb.Property "displayname"

    let stationDisplayName stationId : string[] Op =
        getProp stationId stationDisplayNameProp displayNameFromBytes

    let stationDisplayNameSet displayName stationId : unit Op =
        updateProp stationId stationDisplayNameProp (displayNameToBytes displayName)

