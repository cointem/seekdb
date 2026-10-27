//! Validate the actual decompressed page before the record reader uses unchecked
//! slices, RLE levels or dictionary indices. Validation remains page-at-a-time.
use super::{err, Result, MAX_ROW};
use parquet::basic::{Encoding, Type};
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{FileReader, RowGroupReader};
use parquet::record::reader::RowIter;
use parquet::schema::types::Type as Schema;

pub(super) struct Reader<R: FileReader>(pub R);
impl<R: FileReader> FileReader for Reader<R> {
    fn metadata(&self) -> &ParquetMetaData {
        self.0.metadata()
    }
    fn num_row_groups(&self) -> usize {
        self.0.num_row_groups()
    }
    fn get_row_group(&self, index: usize) -> parquet::errors::Result<Box<dyn RowGroupReader + '_>> {
        Ok(Box::new(Group(self.0.get_row_group(index)?)))
    }
    fn get_row_iter(&self, projection: Option<Schema>) -> parquet::errors::Result<RowIter<'_>> {
        RowIter::from_file(projection, self)
    }
}
struct Group<'a>(Box<dyn RowGroupReader + 'a>);
impl RowGroupReader for Group<'_> {
    fn metadata(&self) -> &RowGroupMetaData {
        self.0.metadata()
    }
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
    fn get_column_page_reader(&self, index: usize) -> parquet::errors::Result<Box<dyn PageReader>> {
        let descriptor = self.metadata().schema_descr().column(index);
        Ok(Box::new(Pages {
            inner: self.0.get_column_page_reader(index)?,
            physical: descriptor.physical_type(),
            optional: descriptor.max_def_level() != 0,
            dictionary: None,
        }))
    }
    fn get_column_bloom_filter(&self, index: usize) -> Option<&parquet::bloom_filter::Sbbf> {
        self.0.get_column_bloom_filter(index)
    }
    fn get_row_iter(&self, projection: Option<Schema>) -> parquet::errors::Result<RowIter<'_>> {
        RowIter::from_row_group(projection, self)
    }
}
struct Pages {
    inner: Box<dyn PageReader>,
    physical: Type,
    optional: bool,
    dictionary: Option<usize>,
}
impl Iterator for Pages {
    type Item = parquet::errors::Result<Page>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}
impl PageReader for Pages {
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<Page>> {
        let page = self.inner.get_next_page()?;
        if let Some(page) = &page {
            self.validate(page)
                .map_err(parquet::errors::ParquetError::General)?;
        }
        Ok(page)
    }
    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        self.inner.peek_next_page()
    }
    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        self.inner.skip_next_page()
    }
    fn at_record_boundary(&mut self) -> parquet::errors::Result<bool> {
        self.inner.at_record_boundary()
    }
}
impl Pages {
    fn validate(&mut self, page: &Page) -> Result<()> {
        match page {
            Page::DictionaryPage {
                buf,
                num_values,
                encoding,
                ..
            } => {
                #[allow(deprecated)]
                if !matches!(encoding, Encoding::PLAIN | Encoding::PLAIN_DICTIONARY) {
                    return Err("unsupported Parquet dictionary encoding".into());
                }
                plain(buf, *num_values as usize, self.physical)?;
                self.dictionary = Some(*num_values as usize);
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                def_level_encoding,
                ..
            } => {
                let mut data = buf.as_ref();
                let mut count = *num_values as usize;
                if self.optional {
                    if *def_level_encoding != Encoding::RLE {
                        return Err("unsupported Parquet level encoding".into());
                    }
                    let length =
                        u32::from_le_bytes(take(&mut data, 4)?.try_into().unwrap()) as usize;
                    let levels = take(&mut data, length)?;
                    count = 0;
                    hybrid(levels, 1, *num_values as usize, |value| {
                        if value > 1 {
                            return Err("invalid flat Parquet definition level".into());
                        }
                        count += value as usize;
                        Ok(())
                    })?;
                }
                self.values(data, count, *encoding)?;
            }
            Page::DataPageV2 {
                buf,
                num_values,
                num_nulls,
                num_rows,
                encoding,
                def_levels_byte_len,
                rep_levels_byte_len,
                ..
            } => {
                if num_rows != num_values || num_nulls > num_values || *rep_levels_byte_len != 0 {
                    return Err("invalid flat Parquet V2 levels".into());
                }
                let mut data = buf.as_ref();
                let levels = take(&mut data, *def_levels_byte_len as usize)?;
                let mut count = *num_values as usize;
                if self.optional {
                    count = 0;
                    hybrid(levels, 1, *num_values as usize, |value| {
                        if value > 1 {
                            return Err("invalid V2 definition level".into());
                        }
                        count += value as usize;
                        Ok(())
                    })?;
                } else if !levels.is_empty() {
                    return Err("unexpected required-column definition levels".into());
                }
                if count != (*num_values - *num_nulls) as usize {
                    return Err("inconsistent Parquet V2 null count".into());
                }
                self.values(data, count, *encoding)?;
            }
        }
        Ok(())
    }
    fn values(&self, data: &[u8], count: usize, encoding: Encoding) -> Result<()> {
        #[allow(deprecated)]
        match encoding {
            Encoding::PLAIN => plain(data, count, self.physical),
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                let dictionary = self
                    .dictionary
                    .ok_or("Parquet dictionary page is missing")?;
                if count > 0 {
                    let width = *data.first().ok_or("missing dictionary bit width")?;
                    hybrid(&data[1..], width, count, |value| {
                        if value as usize >= dictionary {
                            Err("Parquet dictionary index is out of bounds".into())
                        } else {
                            Ok(())
                        }
                    })?;
                }
                Ok(())
            }
            _ => Err("unsupported Parquet value encoding".into()),
        }
    }
}
fn take<'a>(data: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    let value = data.get(..length).ok_or("truncated Parquet page value")?;
    *data = &data[length..];
    Ok(value)
}
fn plain(mut data: &[u8], count: usize, physical: Type) -> Result<()> {
    let width = match physical {
        Type::BOOLEAN => {
            take(&mut data, count.div_ceil(8))?;
            return Ok(());
        }
        Type::INT32 | Type::FLOAT => 4,
        Type::INT64 | Type::DOUBLE => 8,
        Type::BYTE_ARRAY => {
            for _ in 0..count {
                let length = u32::from_le_bytes(take(&mut data, 4)?.try_into().unwrap()) as usize;
                if length > MAX_ROW {
                    return Err("Parquet string exceeds resource limit".into());
                }
                take(&mut data, length)?;
            }
            return Ok(());
        }
        _ => return Err("unsupported Parquet physical type".into()),
    };
    take(
        &mut data,
        count
            .checked_mul(width)
            .ok_or("Parquet value size overflow")?,
    )?;
    Ok(())
}
fn varint(data: &mut &[u8]) -> Result<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = take(data, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err("Parquet RLE integer overflow".into());
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err("Parquet RLE integer overflow".into())
}
fn hybrid(
    mut data: &[u8],
    width: u8,
    mut count: usize,
    mut visit: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    if width > 32 {
        return Err("Parquet bit width exceeds limit".into());
    }
    while count > 0 {
        let header = varint(&mut data)?;
        let run = usize::try_from(header >> 1).map_err(err)?;
        if run == 0 {
            return Err("empty Parquet RLE run".into());
        }
        if header & 1 == 0 {
            let mut value = [0; 4];
            let length = usize::from(width).div_ceil(8);
            value[..length].copy_from_slice(take(&mut data, length)?);
            let value = u32::from_le_bytes(value);
            if width < 32 && u64::from(value) >= 1u64 << width {
                return Err("invalid RLE value".into());
            }
            let amount = run.min(count);
            for _ in 0..amount {
                visit(value)?;
            }
            count -= amount;
        } else {
            let bytes = take(
                &mut data,
                run.checked_mul(width as usize)
                    .ok_or("Parquet bitpack size overflow")?,
            )?;
            let amount = run
                .checked_mul(8)
                .ok_or("Parquet bitpack count overflow")?
                .min(count);
            for index in 0..amount {
                let bit = index * width as usize;
                let length = ((bit % 8) + width as usize).div_ceil(8);
                let mut value = [0; 8];
                value[..length].copy_from_slice(
                    bytes
                        .get(bit / 8..bit / 8 + length)
                        .ok_or("truncated Parquet bitpack")?,
                );
                let value = (u64::from_le_bytes(value) >> (bit % 8)) & ((1u64 << width) - 1);
                visit(value as u32)?;
            }
            count -= amount;
        }
    }
    Ok(())
}

pub(super) fn statistics(
    statistics: Option<&parquet::format::Statistics>,
    physical: Type,
    count: i64,
) -> Result<()> {
    if let Some(statistics) = statistics {
        if statistics.null_count.is_some_and(|n| n < 0 || n > count)
            || statistics
                .distinct_count
                .is_some_and(|n| n < 0 || n > count)
        {
            return Err("invalid Parquet statistics cardinality".into());
        }
        let width = match physical {
            Type::BOOLEAN => Some(1),
            Type::INT32 | Type::FLOAT => Some(4),
            Type::INT64 | Type::DOUBLE => Some(8),
            Type::BYTE_ARRAY => None,
            _ => return Err("unsupported Parquet statistics type".into()),
        };
        for value in [
            &statistics.min,
            &statistics.max,
            &statistics.min_value,
            &statistics.max_value,
        ]
        .into_iter()
        .flatten()
        {
            if width.is_some_and(|width| value.len() != width) {
                return Err("invalid Parquet statistics width".into());
            }
        }
    }
    Ok(())
}
