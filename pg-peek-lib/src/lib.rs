pub mod types;

use bitflags::Flags;
use byteorder::ReadBytesExt;
use std::io::{self, Read};
use std::ops::Deref;

/// Enum representing the possible byte order (endianness) of a system.
#[derive(Clone, Copy)]
pub enum Endianness {
    LittleEndian,
    BigEndian,
}

/// Returns the system's endianness.
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

/// Represents a pointer to a location in the PostgreSQL write-ahead log.
#[derive(Debug)]
pub struct PageXLogRecPtr(u64);

/// Represents a unique identifier for a PostgreSQL transaction.
#[derive(Debug)]
pub struct TransactionId(u32);

/// Represents a command identifier within a PostgreSQL transaction.
#[derive(Debug)]
pub struct CommandId(u32);

/// Represents an index into a specific location on a PostgreSQL page.
#[derive(Debug)]
pub struct LocationIndex(u16);

/// Represents a pointer to a specific item within a PostgreSQL page.
#[derive(Debug)]
pub struct ItemPointerData([u8; 6]);

impl Deref for PageXLogRecPtr {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for TransactionId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for CommandId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for LocationIndex {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for ItemPointerData {
    type Target = [u8; 6];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

bitflags! {
    /// Flags representing the status and properties of a PostgreSQL page.
    #[derive(Debug)]
    pub struct PageFlags: u16 {
        const PD_HAS_FREE_LINES  = 0x0001;
        const PD_PAGE_FULL       = 0x0002;
        const PD_ALL_VISIBLE     = 0x0004;
    }
}

/// Represents the header data for a PostgreSQL page.
#[derive(Debug)]
pub struct PageHeaderData {
    /// Log sequence number: identifies a position in the PostgreSQL write-ahead log.
    pd_lsn: PageXLogRecPtr,
    /// Optional checksum for the page.
    pd_checksum: u16,
    /// Flags providing info about the status and properties of the page.
    pd_flags: PageFlags,
    /// Offset to the start of the free space in the page.
    pd_lower: LocationIndex,
    /// Offset to the end of the free space in the page.
    pd_upper: LocationIndex,
    /// Offset to the beginning of the special space at the end of the page.
    pd_special: LocationIndex,
    /// Encodes the page version and the size of the page.
    pd_pagesize_version: u16,
    /// The oldest unpruned transaction ID on the page.
    pd_prune_xid: TransactionId,
}

bitflags! {
    /// Flags representing the status and type of a PostgreSQL item.
    #[derive(Debug, PartialEq)]
    pub struct LPFlags: u8 {
        const LP_UNUSED   = 0x00;
        const LP_NORMAL   = 0x01;
        const LP_REDIRECT = 0x02;
        const LP_DEAD     = 0x03;
    }
}

/// Represents the data for an item on a PostgreSQL page.
#[derive(Debug)]
pub struct ItemIdData {
    /// Offset to the item's data.
    lp_off: u16,
    /// Flags providing info about the status and type of the item.
    lp_flags: LPFlags,
    /// Length of the item.
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
    items: Vec<HeapTuple>,
    special_space: Option<SpecialSection>,
}

impl PageLayout {
    pub fn from_reader<R: Read>(reader: &mut R, endianness: Endianness) -> io::Result<PageLayout> {
        let header = read_page_header(reader, endianness)?;
        let item_identifiers = read_item_identifiers(reader, &header, endianness)?;

        // Read HeapTuples for each item identifier
        let mut items = Vec::new();
        for item_id in &item_identifiers {
            if item_id.lp_flags == LPFlags::LP_NORMAL {
                let tuple_length = item_id.lp_len as u32; // Assuming lp_len is the length including the header
                let tuple = HeapTuple::from_reader(reader, tuple_length, endianness)?;
                items.push(tuple);
            } else {
                // Skip non-NORMAL items based on their length
                let mut buffer = vec![0u8; item_id.lp_len as usize];
                reader.read_exact(&mut buffer)?;
            }
        }

        // Handle special space (assuming you have the logic for this)
        let special_space = None; // Placeholder, adjust as needed

        Ok(PageLayout {
            header,
            item_identifiers,
            items,
            special_space,
        })
    }
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
    let num_identifiers = (*header.pd_lower as usize - std::mem::size_of::<PageHeaderData>()) / 4; // assuming 4 bytes per ItemIdData

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

        let mut cursor = io::Cursor::new(&buffer);
        let page_layout = PageLayout::from_reader(&mut cursor, endianness)?;
        pages.push(page_layout);
    }

    Ok(pages)
}

use bitflags::bitflags;

bitflags! {
    #[derive(Debug)]
    struct Infomask2: u16 {
        const HEAP_NATTS_MASK = 0x07FF;
        const HEAP_KEYS_UPDATED = 0x2000;
        const HEAP_HOT_UPDATED = 0x4000;
        const HEAP_ONLY_TUPLE = 0x8000;
        const HEAP2_XACT_MASK = 0xE000;
    }
}

bitflags! {
    #[derive(Debug)]
    struct Infomask: u16 {
        const HEAP_HASNULL = 0x0001;
        const HEAP_HASVARWIDTH = 0x0002;
        const HEAP_HASEXTERNAL = 0x0004;
        const HEAP_HASOID_OLD = 0x0008;
        const HEAP_XMAX_KEYSHR_LOCK = 0x0010;
        const HEAP_COMBOCID = 0x0020;
        const HEAP_XMAX_EXCL_LOCK = 0x0040;
        const HEAP_XMAX_LOCK_ONLY = 0x0080;
        const HEAP_XMAX_SHR_LOCK = Self::HEAP_XMAX_EXCL_LOCK.bits() | Self::HEAP_XMAX_KEYSHR_LOCK.bits();
        const HEAP_LOCK_MASK = Self::HEAP_XMAX_SHR_LOCK.bits() | Self::HEAP_XMAX_EXCL_LOCK.bits() | Self::HEAP_XMAX_KEYSHR_LOCK.bits();
        const HEAP_XMIN_COMMITTED = 0x0100;
        const HEAP_XMIN_INVALID = 0x0200;
        const HEAP_XMIN_FROZEN = Self::HEAP_XMIN_COMMITTED.bits() | Self::HEAP_XMIN_INVALID.bits();
        const HEAP_XMAX_COMMITTED = 0x0400;
        const HEAP_XMAX_INVALID = 0x0800;
        const HEAP_XMAX_IS_MULTI = 0x1000;
        const HEAP_UPDATED = 0x2000;
        const HEAP_MOVED_OFF = 0x4000;
        const HEAP_MOVED_IN = 0x8000;
        const HEAP_MOVED = Self::HEAP_MOVED_OFF.bits() | Self::HEAP_MOVED_IN.bits();
        const HEAP_XACT_MASK = 0xFFF0;
    }
}

#[derive(Debug)]
pub struct HeapTuple {
    header: HeapTupleHeaderData,
    data: Vec<u8>,
}

impl HeapTuple {
    pub fn from_reader<R: Read>(
        reader: &mut R,
        total_length: u32,
        endianness: Endianness,
    ) -> io::Result<HeapTuple> {
        let header = HeapTupleHeaderData::read_from(reader, endianness)?;

        // Calculate the size of data by subtracting the size of the header from the total length.
        let data_length = total_length as usize - std::mem::size_of::<HeapTupleHeaderData>();
        let mut data = vec![0u8; data_length];
        reader.read_exact(&mut data)?;

        Ok(HeapTuple { header, data })
    }
}

#[derive(Debug)]
pub struct HeapTupleHeaderData {
    t_xmin: TransactionId,
    t_xmax: TransactionId,
    t_cid: CommandId,
    t_ctid: ItemPointerData,
    t_infomask2: Infomask2,
    t_infomask: Infomask,
    t_hoff: u8,
}

impl HeapTupleHeaderData {
    pub fn read_from<R: Read>(
        reader: &mut R,
        endianness: Endianness,
    ) -> io::Result<HeapTupleHeaderData> {
        let t_xmin = TransactionId(read_u32(reader, endianness)?);
        let t_xmax = TransactionId(read_u32(reader, endianness)?);
        let t_cid = CommandId(read_u32(reader, endianness)?); // same as t_xvac
        let t_ctid = {
            let mut buffer = [0u8; 6];
            reader.read_exact(&mut buffer)?;
            ItemPointerData(buffer)
        };
        let t_infomask2 = Infomask2::from_bits_truncate(read_u16(reader, endianness)?);
        let t_infomask = Infomask::from_bits_truncate(read_u16(reader, endianness)?);
        let t_hoff = reader.read_u8()?;

        Ok(HeapTupleHeaderData {
            t_xmin,
            t_xmax,
            t_cid,
            t_ctid,
            t_infomask2,
            t_infomask,
            t_hoff,
        })
    }
}
