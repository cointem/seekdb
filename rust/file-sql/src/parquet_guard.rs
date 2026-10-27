//! Validate allocation sizes and compact-protocol framing before handing local
//! file bytes to the Parquet decoder. Its slice protocol can panic on truncated
//! unknown DOUBLE fields, and container/page sizes otherwise control allocation.
use super::{err, Result, MAX_COLUMNS};
use bytes::Bytes;
use parquet::file::reader::{ChunkReader, FileReader, Length};
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::format::PageHeader;
use parquet::thrift::TSerializable;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, RwLock};
use thrift::protocol::TCompactInputProtocol;
use thrift::protocol::{
    TFieldIdentifier, TInputProtocol, TListIdentifier, TMapIdentifier, TMessageIdentifier,
    TSetIdentifier, TStructIdentifier,
};

const MAX_FOOTER: usize = 16 * 1024 * 1024;
const MAX_HEADER: usize = 64 * 1024;
const MAX_PAGE: u64 = 64 * 1024 * 1024;
const MAX_WORKING_PAGES: u64 = 128 * 1024 * 1024;
const MAX_VALUES: u64 = 1_000_000;

// Freeze the validated Footer so a concurrent replacement cannot make the
// dependency parse different, unvalidated bytes after our framing check.
pub(super) struct GuardedFile {
    file: File,
    length: u64,
    footer_start: u64,
    footer: Bytes,
    headers: Arc<RwLock<HashMap<u64, Bytes>>>,
}
impl Length for GuardedFile {
    fn len(&self) -> u64 {
        self.length
    }
}
impl ChunkReader for GuardedFile {
    type T = Box<dyn Read + Send>;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start >= self.footer_start && start <= self.length {
            return Ok(Box::new(Cursor::new(
                self.footer.slice((start - self.footer_start) as usize..),
            )));
        }
        if let Some(header) = self
            .headers
            .read()
            .map_err(|_| {
                parquet::errors::ParquetError::General("Parquet header lock failed".into())
            })?
            .get(&start)
            .cloned()
        {
            let body = self
                .file
                .get_read(start + header.len() as u64)?
                .take(MAX_PAGE);
            return Ok(Box::new(Cursor::new(header).chain(body)));
        }
        Ok(Box::new(
            self.file
                .get_read(start)?
                .take(MAX_PAGE + MAX_HEADER as u64),
        ))
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if length as u64 > MAX_PAGE {
            return Err(parquet::errors::ParquetError::General(
                "Parquet read exceeds resource limit".into(),
            ));
        }
        let end = start.checked_add(length as u64).ok_or_else(|| {
            parquet::errors::ParquetError::General("Parquet range overflow".into())
        })?;
        if start >= self.footer_start && end <= self.length {
            return Ok(self
                .footer
                .slice((start - self.footer_start) as usize..(end - self.footer_start) as usize));
        }
        self.file.get_bytes(start, length)
    }
}

struct Compact<'a> {
    bytes: &'a [u8],
    position: usize,
    budget: u64,
    trace: Vec<u8>,
}
impl<'a> Compact<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            budget: MAX_VALUES,
            trace: Vec::new(),
        }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or("Parquet metadata size overflow")?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or("truncated Parquet compact metadata")?;
        self.position = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err("Parquet metadata integer overflow".into());
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err("Parquet metadata integer overflow".into())
    }
    fn charge(&mut self, count: u64) -> Result<()> {
        self.budget = self
            .budget
            .checked_sub(count)
            .ok_or("Parquet metadata exceeds value limit")?;
        Ok(())
    }
    fn structure(&mut self, depth: usize) -> Result<()> {
        if depth > 64 {
            return Err("Parquet metadata nesting exceeds limit".into());
        }
        self.trace.push(12);
        loop {
            let header = self.byte()?;
            let kind = header & 15;
            self.trace.push(64);
            if kind == 0 {
                self.trace.push(44);
                return Ok(());
            }
            self.charge(1)?;
            if header >> 4 == 0 {
                self.varint()?;
            }
            self.value(kind, depth + 1, true)?;
            self.trace.push(65);
        }
    }
    fn value(&mut self, kind: u8, depth: usize, inline_bool: bool) -> Result<()> {
        if depth > 64 {
            return Err("Parquet metadata nesting exceeds limit".into());
        }
        if kind != 12 {
            self.trace.push(if kind == 2 { 1 } else { kind });
        }
        match kind {
            1 | 2 => {
                if !inline_bool && self.byte()? > 2 {
                    return Err("invalid compact boolean".into());
                }
            }
            3 => {
                self.take(1)?;
            }
            4..=6 => {
                self.varint()?;
            }
            7 => {
                self.take(8)?;
            }
            8 => {
                let count = usize::try_from(self.varint()?).map_err(err)?;
                self.take(count)?;
            }
            9 | 10 => {
                let header = self.byte()?;
                let mut count = u64::from(header >> 4);
                if count == 15 {
                    count = self.varint()?;
                }
                self.charge(count)?;
                for _ in 0..count {
                    self.value(header & 15, depth + 1, false)?;
                }
                self.trace.push(32 + kind);
            }
            11 => {
                let count = self.varint()?;
                self.charge(
                    count
                        .checked_mul(2)
                        .ok_or("Parquet metadata size overflow")?,
                )?;
                if count != 0 {
                    let kinds = self.byte()?;
                    for _ in 0..count {
                        self.value(kinds >> 4, depth + 1, false)?;
                        self.value(kinds & 15, depth + 1, false)?;
                    }
                }
                self.trace.push(43);
            }
            12 => self.structure(depth + 1)?,
            _ => return Err("invalid Parquet compact type".into()),
        }
        Ok(())
    }
}

// The generated decoder trusts field IDs rather than checking wire types.
// Match every requested read against the bounded wire trace before allocation.
struct Checked<'a> {
    protocol: TCompactInputProtocol<Cursor<&'a [u8]>>,
    trace: &'a [u8],
}
impl Checked<'_> {
    fn expect(&mut self, token: u8) -> thrift::Result<()> {
        if self.trace.first() != Some(&token) {
            return Err(thrift::Error::Protocol(thrift::ProtocolError::new(
                thrift::ProtocolErrorKind::InvalidData,
                "Parquet field wire type mismatch",
            )));
        }
        self.trace = &self.trace[1..];
        Ok(())
    }
}
macro_rules! checked_read {
    ($name:ident, $result:ty, $token:expr) => {
        fn $name(&mut self) -> thrift::Result<$result> {
            self.expect($token)?;
            self.protocol.$name()
        }
    };
}
impl TInputProtocol for Checked<'_> {
    checked_read!(read_message_begin, TMessageIdentifier, 255);
    checked_read!(read_message_end, (), 255);
    checked_read!(read_struct_begin, Option<TStructIdentifier>, 12);
    checked_read!(read_struct_end, (), 44);
    checked_read!(read_field_begin, TFieldIdentifier, 64);
    checked_read!(read_field_end, (), 65);
    checked_read!(read_bool, bool, 1);
    checked_read!(read_bytes, Vec<u8>, 8);
    checked_read!(read_i8, i8, 3);
    checked_read!(read_byte, u8, 3);
    checked_read!(read_i16, i16, 4);
    checked_read!(read_i32, i32, 5);
    checked_read!(read_i64, i64, 6);
    checked_read!(read_double, f64, 7);
    checked_read!(read_string, String, 8);
    checked_read!(read_list_begin, TListIdentifier, 9);
    checked_read!(read_list_end, (), 41);
    checked_read!(read_set_begin, TSetIdentifier, 10);
    checked_read!(read_set_end, (), 42);
    checked_read!(read_map_begin, TMapIdentifier, 11);
    checked_read!(read_map_end, (), 43);
}
fn decode_checked<T: TSerializable>(bytes: &[u8], trace: &[u8]) -> Result<T> {
    let mut protocol = Checked {
        protocol: TCompactInputProtocol::new(Cursor::new(bytes)),
        trace,
    };
    let value = T::read_from_in_protocol(&mut protocol).map_err(err)?;
    if !protocol.trace.is_empty() {
        return Err("incomplete Parquet metadata decoding".into());
    }
    Ok(value)
}

pub(super) fn reader(
    mut file: File,
    projection: Option<&[usize]>,
    check_pages: bool,
    check: &dyn Fn() -> Result<()>,
) -> Result<super::parquet_pages::Reader<SerializedFileReader<GuardedFile>>> {
    check()?;
    let length = file.metadata().map_err(err)?.len();
    if length < 12 {
        return Err("truncated Parquet file".into());
    }
    let mut magic = [0; 4];
    file.seek(SeekFrom::Start(0)).map_err(err)?;
    file.read_exact(&mut magic).map_err(err)?;
    if &magic != b"PAR1" {
        return Err("invalid Parquet file header".into());
    }
    file.seek(SeekFrom::End(-8)).map_err(err)?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(err)?;
    let size = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
    if &trailer[4..] != b"PAR1" || size > MAX_FOOTER || size as u64 > length - 12 {
        return Err("invalid or oversized Parquet Footer".into());
    }
    let footer_start = length - 8 - size as u64;
    file.seek(SeekFrom::Start(footer_start)).map_err(err)?;
    let mut footer = vec![0; size];
    file.read_exact(&mut footer).map_err(err)?;
    let mut compact = Compact::new(&footer);
    compact.structure(0)?;
    if compact.position != footer.len() {
        return Err("trailing Parquet Footer bytes".into());
    }
    let metadata = decode_checked::<parquet::format::FileMetaData>(&footer, &compact.trace)?;
    if metadata.schema.is_empty()
        || metadata.schema.len() > MAX_COLUMNS + 1
        || metadata.schema[0].type_.is_some()
        || metadata.schema[0].num_children != Some((metadata.schema.len() - 1) as i32)
        || metadata
            .schema
            .iter()
            .skip(1)
            .any(|field| field.type_.is_none() || field.num_children.unwrap_or(0) != 0)
    {
        return Err("unsupported or inconsistent Parquet flat schema".into());
    }
    for field in metadata.schema.iter().skip(1) {
        use parquet::basic::Type;
        let physical =
            Type::try_from(field.type_.ok_or("missing Parquet leaf type")?).map_err(err)?;
        if !matches!(
            physical,
            Type::BOOLEAN
                | Type::INT32
                | Type::INT64
                | Type::FLOAT
                | Type::DOUBLE
                | Type::BYTE_ARRAY
        ) {
            return Err("unsupported Parquet physical type".into());
        }
    }
    let rows = u64::try_from(metadata.num_rows).map_err(err)?;
    for group in &metadata.row_groups {
        if group.columns.len() + 1 != metadata.schema.len()
            || group.columns.iter().any(|column| {
                column.meta_data.as_ref().is_none_or(|data| {
                    data.num_values != group.num_rows
                        || data.total_compressed_size < 0
                        || data.total_uncompressed_size < 0
                })
            })
        {
            return Err("inconsistent Parquet column cardinality".into());
        }
        for (index, column) in group.columns.iter().enumerate() {
            let data = column
                .meta_data
                .as_ref()
                .ok_or("missing Parquet column metadata")?;
            if metadata.schema[index + 1].type_ != Some(data.type_) {
                return Err("Parquet column type disagrees with schema".into());
            }
            super::parquet_pages::statistics(
                data.statistics.as_ref(),
                parquet::basic::Type::try_from(data.type_).map_err(err)?,
                group.num_rows,
            )?;
        }
    }
    let group_rows = metadata.row_groups.iter().try_fold(0u64, |sum, group| {
        sum.checked_add(u64::try_from(group.num_rows).map_err(err)?)
            .ok_or_else(|| "Parquet row count overflow".to_owned())
    })?;
    if rows != group_rows {
        return Err("inconsistent Parquet row counts".into());
    }
    drop(metadata);
    let handle = file.try_clone().map_err(err)?;
    footer.extend_from_slice(&trailer);
    let file = GuardedFile {
        file,
        length,
        footer_start,
        footer: footer.into(),
        headers: Arc::new(RwLock::new(HashMap::new())),
    };
    let headers = Arc::clone(&file.headers);
    let reader = SerializedFileReader::new(file).map_err(err)?;
    if check_pages {
        let pages = validate_pages(&reader, &handle, projection, footer_start, check)?;
        *headers.write().map_err(err)? = pages;
    }
    Ok(super::parquet_pages::Reader(reader))
}

fn validate_pages(
    reader: &SerializedFileReader<GuardedFile>,
    file: &File,
    projection: Option<&[usize]>,
    footer_start: u64,
    check: &dyn Fn() -> Result<()>,
) -> Result<HashMap<u64, Bytes>> {
    use parquet::file::reader::ChunkReader;
    let mut headers = HashMap::new();
    let mut header_bytes = 0usize;
    for group in reader.metadata().row_groups() {
        let mut working_pages = 0u64;
        for (index, column) in group.columns().iter().enumerate() {
            check()?;
            if projection.is_some_and(|ids| !ids.contains(&index)) {
                continue;
            }
            if column.file_path().is_some_and(|path| !path.is_empty()) {
                return Err("external Parquet column chunks are unsupported".into());
            }
            let (mut offset, length) = column.byte_range();
            let end = offset
                .checked_add(length)
                .ok_or("Parquet column size overflow")?;
            if end > footer_start || offset < 4 {
                return Err("invalid Parquet column byte range".into());
            }
            let mut largest = 0;
            let mut values = 0u64;
            while offset < end {
                check()?;
                let amount = usize::try_from((end - offset).min(MAX_HEADER as u64)).map_err(err)?;
                let bytes = file.get_bytes(offset, amount).map_err(err)?;
                let mut compact = Compact::new(&bytes);
                compact.structure(0)?;
                let header: PageHeader =
                    decode_checked(&bytes[..compact.position], &compact.trace)?;
                match header.type_ {
                    parquet::format::PageType::DATA_PAGE if header.data_page_header.is_some() => {}
                    parquet::format::PageType::DICTIONARY_PAGE
                        if header.dictionary_page_header.is_some() => {}
                    parquet::format::PageType::DATA_PAGE_V2
                        if header.data_page_header_v2.is_some() => {}
                    parquet::format::PageType::INDEX_PAGE => {}
                    _ => return Err("unknown Parquet page type or missing page header".into()),
                }
                if let Some(page) = &header.data_page_header {
                    super::parquet_pages::statistics(
                        page.statistics.as_ref(),
                        column.column_type(),
                        page.num_values as i64,
                    )?;
                }
                if let Some(page) = &header.data_page_header_v2 {
                    if page.num_rows != page.num_values
                        || page.num_nulls < 0
                        || page.num_nulls > page.num_values
                        || page.repetition_levels_byte_length != 0
                        || page.definition_levels_byte_length < 0
                        || page.definition_levels_byte_length > header.uncompressed_page_size
                    {
                        return Err("invalid flat Parquet V2 page header".into());
                    }
                    super::parquet_pages::statistics(
                        page.statistics.as_ref(),
                        column.column_type(),
                        page.num_values as i64,
                    )?;
                }
                header_bytes = header_bytes
                    .checked_add(compact.position)
                    .ok_or("Parquet header size overflow")?;
                if header_bytes > MAX_FOOTER || headers.len() >= 100_000 {
                    return Err("Parquet page headers exceed resource limit".into());
                }
                headers.insert(offset, Bytes::copy_from_slice(&bytes[..compact.position]));
                let data_values = header
                    .data_page_header
                    .as_ref()
                    .map(|h| h.num_values)
                    .or_else(|| header.data_page_header_v2.as_ref().map(|h| h.num_values))
                    .unwrap_or(0);
                values = values
                    .checked_add(u64::try_from(data_values).map_err(err)?)
                    .ok_or("Parquet page value overflow")?;
                let compressed = u64::try_from(header.compressed_page_size).map_err(err)?;
                let uncompressed = u64::try_from(header.uncompressed_page_size).map_err(err)?;
                if compressed > MAX_PAGE || uncompressed > MAX_PAGE {
                    return Err("Parquet page exceeds resource limit".into());
                }
                for count in [
                    header.data_page_header.as_ref().map(|h| h.num_values),
                    header.data_page_header_v2.as_ref().map(|h| h.num_values),
                    header.dictionary_page_header.as_ref().map(|h| h.num_values),
                ]
                .into_iter()
                .flatten()
                {
                    if !(0..=MAX_VALUES as i32).contains(&count) {
                        return Err("Parquet page exceeds value limit".into());
                    }
                }
                largest = largest.max(uncompressed);
                offset = offset
                    .checked_add(compact.position as u64)
                    .and_then(|n| n.checked_add(compressed))
                    .ok_or("Parquet page size overflow")?;
                if offset > end {
                    return Err("Parquet page exceeds column byte range".into());
                }
            }
            if values != u64::try_from(column.num_values()).map_err(err)? {
                return Err("Parquet page cardinality does not match Footer".into());
            }
            working_pages = working_pages
                .checked_add(largest)
                .ok_or("Parquet page budget overflow")?;
            if working_pages > MAX_WORKING_PAGES {
                return Err("Parquet projected pages exceed memory budget".into());
            }
        }
    }
    Ok(headers)
}
