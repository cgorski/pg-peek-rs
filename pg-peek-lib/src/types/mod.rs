use bitflags::bitflags;
use lazy_static::lazy_static;

use serde::{Deserialize, Serialize};
use serde_json;

use std::io::{BufRead};
use std::ops::Deref;
use std::str::FromStr;
use strum_macros::{Display, EnumString};
#[derive(Debug, Serialize, Deserialize)]
pub struct Oid(u32);

#[derive(Debug, Serialize, Deserialize)]
pub struct Regproc(u32);

impl Deref for Oid {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for Regproc {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum TypeType {
    #[strum(serialize = "b")]
    Base,
    #[strum(serialize = "c")]
    Composite,
    #[strum(serialize = "d")]
    Domain,
    #[strum(serialize = "e")]
    Enum,
    #[strum(serialize = "m")]
    Multirange,
    #[strum(serialize = "p")]
    Pseudo,
    #[strum(serialize = "r")]
    Range,
}

#[derive(Debug, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum TypeCategory {
    #[strum(serialize = "\0")]
    Invalid,
    #[strum(serialize = "A")]
    Array,
    #[strum(serialize = "B")]
    Boolean,
    #[strum(serialize = "C")]
    Composite,
    #[strum(serialize = "D")]
    DateTime,
    #[strum(serialize = "E")]
    Enum,
    #[strum(serialize = "G")]
    Geometric,
    #[strum(serialize = "I")]
    Network,
    #[strum(serialize = "N")]
    Numeric,
    #[strum(serialize = "P")]
    PseudoType,
    #[strum(serialize = "R")]
    Range,
    #[strum(serialize = "S")]
    String,
    #[strum(serialize = "T")]
    TimeSpan,
    #[strum(serialize = "U")]
    User,
    #[strum(serialize = "V")]
    BitString,
    #[strum(serialize = "X")]
    Unknown,
    #[strum(serialize = "Z")]
    Internal,
}

#[derive(Debug, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum TypeAlign {
    #[strum(serialize = "c")]
    Char,
    #[strum(serialize = "s")]
    Short,
    #[strum(serialize = "i")]
    Int,
    #[strum(serialize = "d")]
    Double,
}

#[derive(Debug, PartialEq, EnumString, Display, Serialize, Deserialize)]
pub enum TypeStorage {
    #[strum(serialize = "p")]
    Plain,
    #[strum(serialize = "e")]
    External,
    #[strum(serialize = "x")]
    Extended,
    #[strum(serialize = "m")]
    Main,
}

bitflags! {
    #[derive(Debug, Serialize, Deserialize)]
    pub struct AclMode: u64 {
        const READ = 0b0001;
        const WRITE = 0b0010;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AclItem {
    grantee: Oid,
    grantor: Oid,
    privileges: AclMode,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PgType {
    oid: Oid,                   // Object ID of this type
    name: String,               // Name of the type
    namespace: Oid,             // OID of namespace containing this type
    owner: Oid,                 // Owner of the type
    length: i16,                // Fixed size of the type; negative for variable-length types
    by_value: bool, // Whether internal Postgres routines pass a value of this type by value or by reference
    type_type: TypeType, // Classification of the type (base, composite, domain, etc.)
    category: TypeCategory, // Arbitrary type classification, helps parser with coercions
    is_preferred: bool, // Indicates if the type is "preferred" within its category
    is_defined: bool, // If false, only a placeholder and not fully defined
    delimiter: char, // Delimiter for arrays of this type
    relation_id: Option<Oid>, // Associated pg_class OID if a composite type, else 0
    subscript: Option<Regproc>, // Type-specific subscripting handler
    element: Option<Oid>, // OID of type yielded by subscripting, if applicable
    array: Option<Oid>, // If "true" array type exists with this type as element type, links to it
    input: Regproc, // I/O conversion procedures (text format) for the datatype
    output: Regproc,
    receive: Regproc, // I/O conversion procedures (binary format) for the datatype
    send: Regproc,
    mod_in: Regproc, // I/O functions for optional type modifiers
    mod_out: Regproc,
    analyze: Regproc,               // Custom ANALYZE procedure for the datatype
    align: TypeAlign,               // Alignment required when storing a value of this type
    storage: TypeStorage, // Specifies if the type is prepared for toasting and the default strategy
    not_null: bool,       // Represents a "NOT NULL" constraint against this datatype
    base_type: Option<Oid>, // If a domain, shows the base (or domain) type it is based on
    type_mod: Option<i32>, // Used by domains to record the typmod to be applied to their base type
    dimensions: i32,      // Declared number of dimensions for an array domain type
    collation: Option<Oid>, // Collation for collatable types
    default_binary: Option<String>, // Binary representation of default expression for the type (mostly for domains)
    default: Option<String>,        // Human-readable version of the default expression
    acl: Vec<AclItem>,              // Access permissions
}

fn json_to_pg_type(json_string: &str) -> Result<PgType, serde_json::Error> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Intermediate {
        oid: u32,
        typname: String,
        typnamespace: u32,
        typowner: u32,
        typlen: i16,
        typbyval: bool,
        typtype: String,
        typcategory: String,
        typispreferred: bool,
        typisdefined: bool,
        typdelim: char,
        typrelid: u32,
        typarray: u32,
        typinput: u32,
        typoutput: u32,
        typreceive: u32,
        typsend: u32,
        typmodin: u32,
        typmodout: u32,
        typanalyze: u32,
        typalign: String,
        typstorage: String,
        typnotnull: bool,
        typbasetype: u32,
        typtypmod: i32,
        typndims: i32,
        typcollation: u32,
        // assuming typdefaultbin, typdefault, typacl are optional since they're null in the sample
        typdefaultbin: Option<String>,
        typdefault: Option<String>,
        typacl: Option<Vec<AclItem>>,
    }

    let intermediate: Intermediate = serde_json::from_str(json_string)?;

    let pg_type = PgType {
        oid: Oid(intermediate.oid),
        name: intermediate.typname,
        namespace: Oid(intermediate.typnamespace),
        owner: Oid(intermediate.typowner),
        length: intermediate.typlen,
        by_value: intermediate.typbyval,
        type_type: TypeType::from_str(&intermediate.typtype).unwrap(),
        category: TypeCategory::from_str(&intermediate.typcategory).unwrap(),
        is_preferred: intermediate.typispreferred,
        is_defined: intermediate.typisdefined,
        delimiter: intermediate.typdelim,
        relation_id: Some(Oid(intermediate.typrelid)),
        subscript: None, // There is no mapping field from the JSON
        element: None,   // Assuming this since there's no "typelem" in the sample
        array: Some(Oid(intermediate.typarray)),
        input: Regproc(intermediate.typinput),
        output: Regproc(intermediate.typoutput),
        receive: Regproc(intermediate.typreceive),
        send: Regproc(intermediate.typsend),
        mod_in: Regproc(intermediate.typmodin),
        mod_out: Regproc(intermediate.typmodout),
        analyze: Regproc(intermediate.typanalyze),
        align: TypeAlign::from_str(&intermediate.typalign).unwrap(),
        storage: TypeStorage::from_str(&intermediate.typstorage).unwrap(),
        not_null: intermediate.typnotnull,
        base_type: Some(Oid(intermediate.typbasetype)),
        type_mod: Some(intermediate.typtypmod),
        dimensions: intermediate.typndims,
        collation: Some(Oid(intermediate.typcollation)),
        default_binary: intermediate.typdefaultbin,
        default: intermediate.typdefault,
        acl: intermediate.typacl.unwrap_or_default(),
    };

    Ok(pg_type)
}

// lazy load the pg_type data from src/data/pg_type.json with include_str!
lazy_static! {
    static ref BOOTSTRAPED_PG_TYPE: Vec<PgType> = {
        let mut pg_types: Vec<PgType> = Vec::new();
        let json = include_str!("../data/pg_type.json");
        let json_lines = json.lines();
        for line in json_lines {
            let pg_type = json_to_pg_type(line).unwrap();
            pg_types.push(pg_type);
        }
        pg_types
    };
}
