use bitflags::{bitflags, Flags};
use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read};

#[derive(Clone, Copy)]
pub enum Endianness {
    LittleEndian,
    BigEndian,
}

pub fn get_system_endianness() -> Endianness {
    if cfg!(target_endian = "little") {
        Endianness::LittleEndian
    } else if cfg!(target_endian = "big") {
        Endianness::BigEndian
    } else {
        panic!("Unknown system endianness");
    }
}

fn read_u16<R: Read>(reader: &mut R, endianness: Endianness) -> io::Result<u16> {
    match endianness {
        Endianness::LittleEndian => reader.read_u16::<byteorder::LittleEndian>(),
        Endianness::BigEndian => reader.read_u16::<byteorder::BigEndian>(),
    }
}

fn read_u32<R: Read>(reader: &mut R, endianness: Endianness) -> io::Result<u32> {
    match endianness {
        Endianness::LittleEndian => reader.read_u32::<byteorder::LittleEndian>(),
        Endianness::BigEndian => reader.read_u32::<byteorder::BigEndian>(),
    }
}

fn read_u64<R: Read>(reader: &mut R, endianness: Endianness) -> io::Result<u64> {
    match endianness {
        Endianness::LittleEndian => reader.read_u64::<byteorder::LittleEndian>(),
        Endianness::BigEndian => reader.read_u64::<byteorder::BigEndian>(),
    }
}

// Basic types
#[derive(Debug)]
pub struct PageXLogRecPtr(u64);
#[derive(Debug)]
pub struct TransactionId(u32);
#[derive(Debug)]
pub struct CommandId(u32);
#[derive(Debug)]
pub struct LocationIndex(u16);
#[derive(Debug)]
pub struct ItemPointerData([u8; 6]);

bitflags! {
    #[derive(Debug)]
    pub struct PageFlags: u16 {
        const PD_HAS_FREE_LINES  = 0x0001;
        const PD_PAGE_FULL       = 0x0002;
        const PD_ALL_VISIBLE     = 0x0004;
    }
}

#[derive(Debug)]
pub struct PageHeaderData {
    pd_lsn: PageXLogRecPtr,
    pd_checksum: u16,
    pd_flags: PageFlags,
    pd_lower: LocationIndex,
    pd_upper: LocationIndex,
    pd_special: LocationIndex,
    pd_pagesize_version: u16,
    pd_prune_xid: TransactionId,
}

// ItemIdData structure

bitflags! {
    #[derive(Debug)]
    pub struct LPFlags: u8 {
        const LP_UNUSED   = 0x00;
        const LP_NORMAL   = 0x01;
        const LP_REDIRECT = 0x02;
        const LP_DEAD     = 0x03;
    }
}

#[derive(Debug)]
pub struct ItemIdData {
    lp_off: u16,
    lp_flags: LPFlags,
    lp_len: u16,
}

impl ItemIdData {
    pub fn from_reader<R: Read>(reader: &mut R, endianness: Endianness) -> io::Result<Self> {
        let first_part = read_u16(reader, endianness)?;
        let second_part = read_u16(reader, endianness)?;

        let lp_off = first_part & 0x7FFF; // Get first 15 bits
        let raw_flags = ((first_part >> 15) & 0x03) as u8; // Get next 2 bits
        let lp_flags = LPFlags::from_bits_truncate(raw_flags);
        let lp_len = second_part & 0x7FFF; // Get 15 bits

        Ok(ItemIdData {
            lp_off,
            lp_flags,
            lp_len,
        })
    }
}

// HeapTupleHeaderData structure
#[derive(Debug)]
pub struct HeapTupleHeaderData {
    t_xmin: TransactionId,
    t_xmax: TransactionId,
    t_cid: CommandId,
    t_xvac: TransactionId,
    t_ctid: ItemPointerData,
    t_infomask2: u16,
    t_infomask: u16,
    t_hoff: u8,
}

// Varlena structure
#[derive(Debug)]
pub struct Varlena {
    length: u32,
    data: Vec<u8>,
}

// BTreeIndex structure
#[derive(Debug)]
pub struct BTreeIndex {
    left_sibling: Option<u32>,
    right_sibling: Option<u32>,
    other_data: Vec<u8>,
}

// SpecialSection structure
#[derive(Debug)]
pub struct SpecialSection {
    data: Vec<u8>,
}

// TableRow structure
#[derive(Debug)]
pub struct TableRow {
    header: HeapTupleHeaderData,
    null_bitmap: Option<Vec<u8>>,
    oid: Option<u32>,
    user_data: Vec<u8>,
}

// PageLayout structure
#[derive(Debug)]
pub struct PageLayout {
    header: PageHeaderData,
    item_identifiers: Vec<ItemIdData>,
    free_space: Vec<u8>,
    items: Vec<Varlena>,
    special_space: Option<SpecialSection>,
}

// Table structure
#[derive(Debug)]
struct Table {
    rows: Vec<TableRow>,
}

// Index structure
#[derive(Debug)]
struct Index {
    btree: BTreeIndex,
}

pub fn read_page_header<R: Read>(
    reader: &mut R,
    endianness: Endianness,
) -> io::Result<PageHeaderData> {
    let pd_lsn = PageXLogRecPtr(read_u64(reader, endianness)?);
    let pd_checksum = read_u16(reader, endianness)?;
    let flags = read_u16(reader, endianness)?;
    let pd_flags = PageFlags::from_bits_truncate(flags);
    let pd_lower = LocationIndex(read_u16(reader, endianness)?);
    let pd_upper = LocationIndex(read_u16(reader, endianness)?);
    let pd_special = LocationIndex(read_u16(reader, endianness)?);
    let pd_pagesize_version = read_u16(reader, endianness)?;
    let pd_prune_xid = TransactionId(read_u32(reader, endianness)?);

    Ok(PageHeaderData {
        pd_lsn,
        pd_checksum,
        pd_flags,
        pd_lower,
        pd_upper,
        pd_special,
        pd_pagesize_version,
        pd_prune_xid,
    })
}

pub fn read_item_identifiers<R: Read>(
    reader: &mut R,
    header: &PageHeaderData,
    endianness: Endianness,
) -> io::Result<Vec<ItemIdData>> {
    let num_identifiers = (header.pd_lower.0 as usize - std::mem::size_of::<PageHeaderData>()) / 4; // assuming 4 bytes per ItemIdData

    let mut item_identifiers = Vec::with_capacity(num_identifiers);

    for _ in 0..num_identifiers {
        let item_id = ItemIdData::from_reader(reader, endianness)?;
        item_identifiers.push(item_id);
    }

    Ok(item_identifiers)
}

pub const DEFAULT_POSTGRES_PAGE_SIZE: usize = 8192; // Default Postgres page size in bytes

pub fn read_all_pages<R: Read>(
    reader: &mut R,
    endianness: Endianness,
) -> io::Result<Vec<PageLayout>> {
    let mut pages = Vec::new();
    let mut buffer = [0u8; DEFAULT_POSTGRES_PAGE_SIZE];

    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }
        if bytes_read != DEFAULT_POSTGRES_PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Incomplete page data",
            ));
        }

        let mut cursor = io::Cursor::new(buffer);
        let header = read_page_header(&mut cursor, endianness)?;
        let item_identifiers = read_item_identifiers(&mut cursor, &header, endianness)?;

        let page_layout = PageLayout {
            header,
            item_identifiers,
            free_space: Vec::new(),
            items: Vec::new(),
            special_space: None,
        };

        pages.push(page_layout);
    }

    Ok(pages)
}
