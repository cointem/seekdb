//! Streaming file readers used by the native SQL table operator.
use chrono::{NaiveDate, NaiveDateTime};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::record::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{File, Metadata};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const MAX_ROW: usize = 16 * 1024 * 1024;
const MAX_COLUMNS: usize = 1024;
type Result<T> = std::result::Result<T, String>;
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

#[cxx::bridge(namespace = "oceanbase::file_sql")]
mod ffi {
    extern "Rust" {
        type FileScan;
        fn bind_file(options: &str, secure_root: &str) -> Result<String>;
        fn open_file(binding: &str, secure_root: &str) -> Result<Box<FileScan>>;
        fn open_projected_file(
            binding: &str,
            secure_root: &str,
            projection: &str,
        ) -> Result<Box<FileScan>>;
        fn next_row(self: &mut FileScan) -> Result<String>;
        fn rescan(self: &mut FileScan) -> Result<()>;
        type FileWriter;
        fn create_writer(options: &str, secure_root: &str) -> Result<Box<FileWriter>>;
        fn write_row(self: &mut FileWriter, row: &str) -> Result<()>;
        fn finish(self: &mut FileWriter) -> Result<()>;
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    format: String,
    path: String,
    #[serde(default = "yes")]
    header: bool,
    #[serde(default = "comma")]
    delimiter: String,
    #[serde(default = "nullstr")]
    nullstr: String,
    #[serde(default = "sample")]
    sample_size: i64,
}
fn yes() -> bool {
    true
}
fn comma() -> String {
    ",".into()
}
fn nullstr() -> String {
    "NULL".into()
}
fn sample() -> i64 {
    2048
}
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
enum Kind {
    Null,
    Boolean,
    Bigint,
    Double,
    Varchar,
    Date,
    Datetime,
}
#[derive(Clone, Serialize, Deserialize)]
struct Column {
    name: String,
    kind: Kind,
    nullable: bool,
    #[serde(default)]
    precision: u8,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Identity {
    size: u64,
    modified: u128,
    device: u64,
    inode: u64,
}
impl Identity {
    fn of(meta: &Metadata) -> Result<Self> {
        #[cfg(unix)]
        let (device, inode) = {
            use std::os::unix::fs::MetadataExt;
            (meta.dev(), meta.ino())
        };
        #[cfg(not(unix))]
        let (device, inode) = (0, 0);
        Ok(Self {
            size: meta.len(),
            modified: meta
                .modified()
                .map_err(err)?
                .duration_since(UNIX_EPOCH)
                .map_err(err)?
                .as_nanos(),
            device,
            inode,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Binding {
    options: Options,
    columns: Vec<Column>,
    identity: Option<Identity>,
    files: Vec<String>,
}

struct LimitedFile {
    file: File,
    remaining: usize,
    quote_state: u8,
    delimiter: u8,
}
impl Read for LimitedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CSV row exceeds resource limit",
            ));
        }
        let len = buf.len().min(self.remaining);
        let n = self.file.read(&mut buf[..len])?;
        if n == 0 && self.quote_state == 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unterminated quoted CSV field",
            ));
        }
        for &byte in &buf[..n] {
            self.quote_state = match (self.quote_state, byte) {
                (0, b'"') => 2,
                (2, b'"') => 3,
                (2, _) => 2,
                (3, b'"') => 2,
                (_, b'\r' | b'\n') => 0,
                (_, b) if b == self.delimiter => 0,
                (0 | 1, b) if b != b'"' => 1,
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "malformed CSV quoting",
                    ))
                }
            };
        }
        self.remaining -= n;
        Ok(n)
    }
}
enum Source {
    Csv(csv::Reader<LimitedFile>, Vec<String>),
    Json(BufReader<File>),
    Parquet(RowIter<'static>),
    Glob(std::vec::IntoIter<String>),
}
pub struct FileScan {
    binding: Binding,
    root: String,
    source: Source,
    handle: Option<File>,
    projection: Option<Vec<usize>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteOptions {
    path: String,
    #[serde(default)]
    header: bool,
    #[serde(default = "comma")]
    delimiter: String,
    #[serde(default = "nullstr")]
    nullstr: String,
    columns: Vec<String>,
}
pub struct FileWriter {
    temporary: Option<tempfile::NamedTempFile>,
    writer: csv::Writer<File>,
    target: PathBuf,
    parent: PathBuf,
    parent_identity: (u64, u64),
    root: String,
    options: WriteOptions,
}
pub fn create_writer(options: &str, root: &str) -> Result<Box<FileWriter>> {
    let options: WriteOptions = serde_json::from_str(options).map_err(err)?;
    validate_delimiter(&options.delimiter)?;
    let requested = Path::new(&options.path);
    let name = requested
        .file_name()
        .ok_or("output file name is required")?;
    let requested_parent = requested.parent().ok_or("output directory is required")?;
    let parent = secure_path(
        if requested_parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            requested_parent
        },
        root,
    )?;
    let target = parent.join(name);
    if target.symlink_metadata().is_ok() {
        return Err("output file already exists".into());
    }
    let identity = Identity::of(&parent.metadata().map_err(err)?)?;
    let temporary = tempfile::NamedTempFile::new_in(&parent).map_err(err)?;
    let mut writer = csv::WriterBuilder::new()
        .delimiter(options.delimiter.as_bytes()[0])
        .from_writer(temporary.as_file().try_clone().map_err(err)?);
    if options.header {
        writer.write_record(&options.columns).map_err(err)?;
    }
    Ok(Box::new(FileWriter {
        temporary: Some(temporary),
        writer,
        target,
        parent,
        parent_identity: (identity.device, identity.inode),
        root: root.into(),
        options,
    }))
}
impl FileWriter {
    pub fn write_row(&mut self, row: &str) -> Result<()> {
        if self.temporary.is_none() {
            return Err("writer is already finished".into());
        }
        let values: Vec<Option<String>> = serde_json::from_str(row).map_err(err)?;
        if values.len() != self.options.columns.len() {
            return Err("output column count changed".into());
        }
        self.writer
            .write_record(
                values
                    .iter()
                    .map(|v| v.as_deref().unwrap_or(&self.options.nullstr)),
            )
            .map_err(err)
    }
    pub fn finish(&mut self) -> Result<()> {
        self.writer.flush().map_err(err)?;
        let parent = secure_path(&self.parent, &self.root)?;
        let identity = Identity::of(&parent.metadata().map_err(err)?)?;
        if parent != self.parent || (identity.device, identity.inode) != self.parent_identity {
            return Err("output directory changed during export".into());
        }
        let temporary = self.temporary.take().ok_or("writer is already finished")?;
        temporary.as_file().sync_all().map_err(err)?;
        temporary.persist_noclobber(&self.target).map_err(err)?;
        Ok(())
    }
}

fn secure_path(path: &Path, root: &str) -> Result<PathBuf> {
    if root.is_empty() || root.eq_ignore_ascii_case("null") {
        return Err("secure_file_priv does not permit file access".into());
    }
    let root = Path::new(root).canonicalize().map_err(err)?;
    let path = path.canonicalize().map_err(err)?;
    if !path.starts_with(root) {
        return Err("file is outside secure_file_priv".into());
    }
    Ok(path)
}
fn validate_delimiter(delimiter: &str) -> Result<()> {
    if delimiter.len() != 1
        || !delimiter.is_ascii()
        || matches!(delimiter.as_bytes()[0], 0 | b'"' | b'\r' | b'\n')
    {
        return Err("CSV delimiter must be one non-quote, non-newline ASCII byte".into());
    }
    Ok(())
}
fn unique_names(names: impl Iterator<Item = String>) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (i, name) in names.enumerate() {
        if i >= MAX_COLUMNS {
            return Err("too many file columns".into());
        }
        let base = if name.is_empty() {
            format!("column{}", i + 1)
        } else {
            name
        };
        let mut name = base.clone();
        let mut suffix = 2;
        while !seen.insert(name.clone()) {
            name = format!("{base}__{suffix}");
            suffix += 1;
        }
        out.push(name);
    }
    Ok(out)
}
fn source(
    options: &Options,
    root: &str,
    files: &[String],
    projection: Option<&[usize]>,
) -> Result<(Source, Option<File>, Vec<String>)> {
    if options.format == "glob" {
        return Ok((
            Source::Glob(files.to_vec().into_iter()),
            None,
            vec!["file".into()],
        ));
    }
    let path = secure_path(Path::new(&options.path), root)?;
    let file = File::open(&path).map_err(err)?;
    if !file.metadata().map_err(err)?.is_file() {
        return Err("source must be a regular file".into());
    }
    let handle = file.try_clone().map_err(err)?;
    if Identity::of(&file.metadata().map_err(err)?)?
        != Identity::of(&std::fs::metadata(path).map_err(err)?)?
    {
        return Err("file changed while opening".into());
    }
    let mut names = Vec::new();
    let source = match options.format.as_str() {
        "csv" => {
            validate_delimiter(&options.delimiter)?;
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(options.header)
                .delimiter(options.delimiter.as_bytes()[0])
                .from_reader(LimitedFile {
                    file,
                    remaining: MAX_ROW,
                    quote_state: 0,
                    delimiter: options.delimiter.as_bytes()[0],
                });
            names = if options.header {
                unique_names(reader.headers().map_err(err)?.iter().map(str::to_owned))?
            } else {
                let count = reader.headers().map_err(err)?.len();
                unique_names((1..=count).map(|i| format!("column{i}")))?
            };
            Source::Csv(reader, names.clone())
        }
        "json" => Source::Json(BufReader::new(file)),
        "parquet" => {
            let reader = SerializedFileReader::new(file).map_err(err)?;
            let schema = reader.metadata().file_metadata().schema_descr();
            for column in schema.columns() {
                if column.path().parts().len() != 1 || column.max_rep_level() != 0 {
                    return Err("nested Parquet fields are unsupported".into());
                }
                names.push(column.name().to_owned());
            }
            if names.len() > MAX_COLUMNS {
                return Err("too many Parquet columns".into());
            }
            let projected = projection
                .map(|ids| {
                    let root = schema.root_schema();
                    let fields = ids
                        .iter()
                        .map(|&i| {
                            root.get_fields()
                                .get(i)
                                .cloned()
                                .ok_or("invalid projection")
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    parquet::schema::types::Type::group_type_builder(root.name())
                        .with_fields(fields)
                        .build()
                        .map_err(err)
                })
                .transpose()?;
            Source::Parquet(
                RowIter::from_file_into(Box::new(reader))
                    .project(projected)
                    .map_err(err)?,
            )
        }
        _ => return Err("unsupported file format".into()),
    };
    Ok((source, Some(handle), names))
}
fn parquet_value(field: &Field) -> Result<Value> {
    Ok(match field {
        Field::Null => Value::Null,
        Field::Bool(v) => (*v).into(),
        Field::Byte(v) => (*v as i64).into(),
        Field::Short(v) => (*v as i64).into(),
        Field::Int(v) => (*v as i64).into(),
        Field::Long(v) => (*v).into(),
        Field::UByte(v) => (*v as i64).into(),
        Field::UShort(v) => (*v as i64).into(),
        Field::UInt(v) => (*v as i64).into(),
        Field::ULong(v) => i64::try_from(*v).map_err(err)?.into(),
        Field::Float(v) => serde_json::Number::from_f64(*v as f64)
            .ok_or("nonfinite Parquet number")?
            .into(),
        Field::Double(v) => serde_json::Number::from_f64(*v)
            .ok_or("nonfinite Parquet number")?
            .into(),
        Field::Str(v) => v.clone().into(),
        Field::Bytes(v) => std::str::from_utf8(v.data())
            .map_err(err)?
            .to_owned()
            .into(),
        Field::Date(v) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .checked_add_signed(chrono::Duration::days(*v as i64))
            .ok_or("date overflow")?
            .to_string()
            .into(),
        Field::TimestampMillis(v) => chrono::DateTime::from_timestamp_millis(*v)
            .ok_or("timestamp overflow")?
            .naive_utc()
            .to_string()
            .into(),
        Field::TimestampMicros(v) => chrono::DateTime::from_timestamp_micros(*v)
            .ok_or("timestamp overflow")?
            .naive_utc()
            .to_string()
            .into(),
        _ => return Err("unsupported Parquet field type".into()),
    })
}
impl Source {
    fn row(&mut self, options: &Options) -> Result<Option<Vec<(String, Value)>>> {
        let row: Vec<(String, Value)> = match self {
            Self::Csv(reader, names) => {
                reader.get_mut().remaining = MAX_ROW;
                let mut record = csv::StringRecord::new();
                if !reader.read_record(&mut record).map_err(err)? {
                    return Ok(None);
                }
                if record.len() != names.len() {
                    return Err("CSV row has an inconsistent column count".into());
                }
                names
                    .iter()
                    .cloned()
                    .zip(record.iter().map(|s| {
                        if s.is_empty() || s == options.nullstr {
                            Value::Null
                        } else {
                            Value::String(s.to_owned())
                        }
                    }))
                    .collect()
            }
            Self::Json(reader) => {
                let mut line = Vec::new();
                loop {
                    line.clear();
                    if (&mut *reader)
                        .take(MAX_ROW as u64 + 1)
                        .read_until(b'\n', &mut line)
                        .map_err(err)?
                        == 0
                    {
                        return Ok(None);
                    }
                    if line.len() > MAX_ROW {
                        return Err("JSON row exceeds resource limit".into());
                    }
                    if !line.iter().all(u8::is_ascii_whitespace) {
                        break;
                    }
                }
                let value: Value = serde_json::from_slice(&line).map_err(err)?;
                let object = value.as_object().ok_or("JSONL row must be an object")?;
                if object.values().any(|v| v.is_object() || v.is_array()) {
                    return Err("nested JSON fields are unsupported".into());
                }
                object.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            Self::Parquet(reader) => match reader.next() {
                None => return Ok(None),
                Some(row) => row
                    .map_err(err)?
                    .get_column_iter()
                    .map(|(k, v)| parquet_value(v).map(|v| (k.clone(), v)))
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::Glob(files) => match files.next() {
                None => return Ok(None),
                Some(path) => vec![("file".into(), path.into())],
            },
        };
        if row.len() > MAX_COLUMNS {
            return Err("too many file columns".into());
        }
        Ok(Some(row))
    }
}
fn datetime(text: &str) -> Result<NaiveDateTime> {
    if text.len() == 10 {
        return NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(err)?
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid date".into());
    }
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .map_err(err)
}
fn kind(value: &Value, csv: bool) -> Kind {
    match value {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Boolean,
        Value::Number(v) if v.is_i64() => Kind::Bigint,
        Value::Number(_) => Kind::Double,
        Value::String(s) => {
            if csv && (s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false")) {
                Kind::Boolean
            } else if csv && s.parse::<i64>().is_ok() {
                Kind::Bigint
            } else if csv && s.parse::<f64>().map(f64::is_finite).unwrap_or(false) {
                Kind::Double
            } else if s.len() == 10 && NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
                Kind::Date
            } else if datetime(s).is_ok() {
                Kind::Datetime
            } else {
                Kind::Varchar
            }
        }
        _ => Kind::Varchar,
    }
}
fn widen(a: Kind, b: Kind) -> Kind {
    if a == b || b == Kind::Null {
        a
    } else if a == Kind::Null {
        b
    } else if matches!(
        (a, b),
        (Kind::Bigint, Kind::Double) | (Kind::Double, Kind::Bigint)
    ) {
        Kind::Double
    } else if matches!(
        (a, b),
        (Kind::Date, Kind::Datetime) | (Kind::Datetime, Kind::Date)
    ) {
        Kind::Datetime
    } else {
        Kind::Varchar
    }
}
fn bind_file(options: &str, secure_root: &str) -> Result<String> {
    let mut options: Options = serde_json::from_str(options).map_err(err)?;
    secure_path(Path::new(secure_root), secure_root)?;
    if options.sample_size != -1 && options.sample_size != 2048 {
        return Err("only default sampling or sample_size=-1 is supported".into());
    }
    let mut files = Vec::new();
    if options.format == "glob" {
        let prefix: PathBuf = Path::new(&options.path)
            .components()
            .take_while(|part| !part.as_os_str().to_string_lossy().contains(['*', '?', '[']))
            .collect();
        secure_path(
            if prefix.is_file() {
                prefix.parent().unwrap()
            } else {
                &prefix
            },
            secure_root,
        )?;
        for entry in glob::glob(&options.path).map_err(err)? {
            let path = secure_path(&entry.map_err(err)?, secure_root)?;
            if path.is_file() {
                files.push(path.to_str().ok_or("path is not UTF-8")?.to_owned());
            }
            if files.len() > 10000 {
                return Err("glob exceeds file limit".into());
            }
        }
        files.sort();
        files.dedup();
    } else {
        options.path = secure_path(Path::new(&options.path), secure_root)?
            .to_str()
            .ok_or("path is not UTF-8")?
            .to_owned();
    }
    let (mut source, handle, names) = source(&options, secure_root, &files, None)?;
    let identity = handle
        .as_ref()
        .map(|f| Identity::of(&f.metadata().map_err(err)?))
        .transpose()?;
    let mut columns: Vec<_> = names
        .into_iter()
        .map(|name| Column {
            name,
            kind: Kind::Null,
            nullable: false,
            precision: 0,
        })
        .collect();
    // Parquet's declared schema is authoritative, including files with no rows.
    // Inferring types from values would incorrectly turn date-looking strings into DATE.
    if options.format == "parquet" {
        use parquet::basic::{ConvertedType as C, LogicalType as L, Type as P};
        let reader = SerializedFileReader::new(handle.as_ref().unwrap().try_clone().map_err(err)?)
            .map_err(err)?;
        for (column, descriptor) in columns
            .iter_mut()
            .zip(reader.metadata().file_metadata().schema_descr().columns())
        {
            column.nullable = descriptor.max_def_level() > 0;
            column.kind = match descriptor.logical_type() {
                Some(L::Date) => Kind::Date,
                Some(L::Timestamp { unit, .. }) => {
                    column.precision = match unit {
                        parquet::basic::TimeUnit::MILLIS(_) => 3,
                        parquet::basic::TimeUnit::MICROS(_) => 6,
                        _ => return Err("nanosecond Parquet timestamps are unsupported".into()),
                    };
                    Kind::Datetime
                }
                Some(L::String) | Some(L::Json) => Kind::Varchar,
                Some(L::Integer { .. }) => Kind::Bigint,
                Some(L::Unknown) => Kind::Null,
                Some(_) => return Err("unsupported Parquet logical type".into()),
                None => match descriptor.converted_type() {
                    C::DATE => Kind::Date,
                    C::TIMESTAMP_MILLIS | C::TIMESTAMP_MICROS => {
                        column.precision = if descriptor.converted_type() == C::TIMESTAMP_MILLIS {
                            3
                        } else {
                            6
                        };
                        Kind::Datetime
                    }
                    C::UTF8 | C::JSON => Kind::Varchar,
                    C::NONE
                    | C::INT_8
                    | C::INT_16
                    | C::INT_32
                    | C::INT_64
                    | C::UINT_8
                    | C::UINT_16
                    | C::UINT_32
                    | C::UINT_64 => match descriptor.physical_type() {
                        P::BOOLEAN => Kind::Boolean,
                        P::INT32 | P::INT64 => Kind::Bigint,
                        P::FLOAT | P::DOUBLE => Kind::Double,
                        P::BYTE_ARRAY => Kind::Varchar,
                        _ => return Err("unsupported Parquet physical type".into()),
                    },
                    _ => return Err("unsupported Parquet converted type".into()),
                },
            };
        }
    }
    let mut rows = 0;
    while options.format != "parquet" && (options.sample_size == -1 || rows < options.sample_size) {
        let Some(row) = source.row(&options)? else {
            break;
        };
        for column in &mut columns {
            if !row
                .iter()
                .any(|(name, value)| name == &column.name && !value.is_null())
            {
                column.nullable = true;
            }
        }
        for (name, value) in row {
            let precision = value
                .as_str()
                .and_then(|s| s.split_once('.'))
                .map(|(_, fraction)| fraction.trim_end_matches('0').len().min(6) as u8)
                .unwrap_or(0);
            if let Some(column) = columns.iter_mut().find(|c| c.name == name) {
                column.kind = widen(column.kind, kind(&value, options.format == "csv"));
                column.precision = column.precision.max(precision);
            } else {
                if columns.len() >= MAX_COLUMNS {
                    return Err("too many inferred columns".into());
                }
                columns.push(Column {
                    name,
                    kind: kind(&value, false),
                    nullable: rows > 0 || value.is_null(),
                    precision,
                });
            }
        }
        rows += 1;
    }
    if let Some(file) = handle {
        if Some(Identity::of(&file.metadata().map_err(err)?)?) != identity {
            return Err("file changed during schema inference".into());
        }
    }
    if columns.is_empty() {
        return Err("cannot infer an empty file schema".into());
    }
    serde_json::to_string(&Binding {
        options,
        columns,
        identity,
        files,
    })
    .map_err(err)
}
fn open_file(binding: &str, secure_root: &str) -> Result<Box<FileScan>> {
    open_projected_file(binding, secure_root, "null")
}
fn open_projected_file(
    binding: &str,
    secure_root: &str,
    projection: &str,
) -> Result<Box<FileScan>> {
    let binding: Binding = serde_json::from_str(binding).map_err(err)?;
    let mut projection: Option<Vec<usize>> = serde_json::from_str(projection).map_err(err)?;
    if let Some(ids) = &mut projection {
        if ids.iter().any(|&i| i >= binding.columns.len()) {
            return Err("invalid file column projection".into());
        }
        ids.sort_unstable();
        ids.dedup();
        // Keep a single physical column for COUNT(*) so the row iterator retains cardinality.
        if ids.is_empty() && !binding.columns.is_empty() {
            ids.push(0);
        }
    }
    let (source, handle, _) = source(
        &binding.options,
        secure_root,
        &binding.files,
        projection.as_deref(),
    )?;
    let scan = Box::new(FileScan {
        binding,
        root: secure_root.into(),
        source,
        handle,
        projection,
    });
    scan.check_identity()?;
    Ok(scan)
}
impl FileScan {
    fn check_identity(&self) -> Result<()> {
        if let Some(file) = &self.handle {
            let fd = Identity::of(&file.metadata().map_err(err)?)?;
            let path = Identity::of(&std::fs::metadata(&self.binding.options.path).map_err(err)?)?;
            if self.binding.identity.as_ref() != Some(&fd) || fd != path {
                return Err("source file changed since binding".into());
            }
        }
        Ok(())
    }
    fn next_row(&mut self) -> Result<String> {
        self.check_identity()?;
        let Some(row) = self.source.row(&self.binding.options)? else {
            return Ok(String::new());
        };
        self.check_identity()?;
        if row
            .iter()
            .any(|(name, _)| !self.binding.columns.iter().any(|c| &c.name == name))
        {
            return Err("file schema changed beyond sample".into());
        }
        let mut values = Vec::with_capacity(self.binding.columns.len());
        for column in &self.binding.columns {
            let value = row
                .iter()
                .find(|(name, _)| name == &column.name)
                .map(|(_, v)| v)
                .unwrap_or(&Value::Null);
            let value = if value.is_null() {
                Value::Null
            } else {
                let text = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                match column.kind {
                    Kind::Null => return Err("non-null value outside schema sample".into()),
                    Kind::Boolean => match text.to_ascii_lowercase().as_str() {
                        "true" => true.into(),
                        "false" => false.into(),
                        _ => return Err("invalid boolean value".into()),
                    },
                    Kind::Bigint => text.parse::<i64>().map_err(err)?.into(),
                    Kind::Double => serde_json::Number::from_f64(text.parse::<f64>().map_err(err)?)
                        .ok_or("nonfinite number")?
                        .into(),
                    Kind::Date => {
                        NaiveDate::parse_from_str(&text, "%Y-%m-%d").map_err(err)?;
                        text.into()
                    }
                    Kind::Datetime => {
                        if text.len() == 10 {
                            NaiveDate::parse_from_str(&text, "%Y-%m-%d").map_err(err)?;
                            format!("{text} 00:00:00").into()
                        } else {
                            datetime(&text)?
                                .format("%Y-%m-%d %H:%M:%S%.f")
                                .to_string()
                                .into()
                        }
                    }
                    Kind::Varchar => text.into(),
                }
            };
            values.push(value);
        }
        serde_json::to_string(&values).map_err(err)
    }
    fn rescan(&mut self) -> Result<()> {
        self.check_identity()?;
        let (source, handle, _) = source(
            &self.binding.options,
            &self.root,
            &self.binding.files,
            self.projection.as_deref(),
        )?;
        self.source = source;
        self.handle = handle;
        self.check_identity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture(format: &str, name: &str) -> (String, String) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tools/deploy/mysql_test/test_suite/file_sql/data")
            .canonicalize()
            .unwrap();
        let options = json!({"format":format,"path":root.join(name),"sample_size":-1}).to_string();
        (options, root.to_str().unwrap().to_owned())
    }
    fn all(scan: &mut FileScan) -> Vec<Value> {
        let mut rows = Vec::new();
        loop {
            let row = scan.next_row().unwrap();
            if row.is_empty() {
                break;
            }
            rows.push(serde_json::from_str(&row).unwrap());
        }
        rows
    }
    #[test]
    fn csv_types_quotes_and_rescan() {
        let (options, root) = fixture("csv", "basic.csv");
        let binding = bind_file(&options, &root).unwrap();
        let schema: Binding = serde_json::from_str(&binding).unwrap();
        assert_eq!(
            schema.columns.iter().map(|c| c.kind).collect::<Vec<_>>(),
            [
                Kind::Bigint,
                Kind::Varchar,
                Kind::Double,
                Kind::Boolean,
                Kind::Date,
                Kind::Datetime,
                Kind::Varchar,
                Kind::Bigint
            ]
        );
        assert_eq!(schema.columns[7].name, "id__2");
        let mut scan = open_file(&binding, &root).unwrap();
        let rows = all(&mut scan);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][6], "hello,\nworld");
        assert_eq!(rows[1][4], Value::Null);
        assert_eq!(rows[2][5], "2026-08-03 12:13:14");
        scan.rescan().unwrap();
        assert_eq!(all(&mut scan), rows);
    }
    #[test]
    fn json_union_and_numeric_widening() {
        let (options, root) = fixture("json", "basic.jsonl");
        let binding = bind_file(&options, &root).unwrap();
        let schema: Binding = serde_json::from_str(&binding).unwrap();
        assert_eq!(
            schema
                .columns
                .iter()
                .find(|c| c.name == "score")
                .unwrap()
                .kind,
            Kind::Double
        );
        let mut scan = open_file(&binding, &root).unwrap();
        let rows = all(&mut scan);
        assert_eq!(rows.len(), 3);
        let note = schema
            .columns
            .iter()
            .position(|c| c.name == "note")
            .unwrap();
        assert_eq!(rows[0][note], Value::Null);
        assert_eq!(rows[2][note], "ok");
    }
    #[test]
    fn parquet_fixture_and_path_restriction() {
        use base64::Engine;
        let (_, fixtures) = fixture("parquet", "simple.parquet");
        let directory = std::env::temp_dir().join(format!("seekdb-parquet-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let encoded =
            std::fs::read_to_string(Path::new(&fixtures).join("simple.parquet.b64")).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .unwrap();
        let path = directory.join("simple.parquet");
        std::fs::write(&path, decoded).unwrap();
        let options = json!({"format":"parquet","path":path}).to_string();
        let root = directory.to_str().unwrap().to_owned();
        let binding = bind_file(&options, &root).unwrap();
        let mut scan = open_file(&binding, &root).unwrap();
        assert_eq!(all(&mut scan).len(), 7);
        let mut projected = open_projected_file(&binding, &root, "[0]").unwrap();
        assert_eq!(all(&mut projected).len(), 7);
        projected.rescan().unwrap();
        assert_eq!(all(&mut projected).len(), 7);
        assert!(open_file(&binding, &fixtures).is_err());
        assert!(bind_file(&options, "").is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn parquet_empty_file_uses_declared_types() {
        use std::sync::Arc;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.parquet");
        let schema = parquet::schema::parser::parse_message_type(
            "message root { required int64 id; optional binary text (UTF8); }",
        )
        .unwrap();
        let writer = parquet::file::writer::SerializedFileWriter::new(
            File::create(&path).unwrap(),
            Arc::new(schema),
            Default::default(),
        )
        .unwrap();
        writer.close().unwrap();
        let root = directory.path().to_str().unwrap();
        let binding =
            bind_file(&json!({"format":"parquet","path":path}).to_string(), root).unwrap();
        let schema: Binding = serde_json::from_str(&binding).unwrap();
        assert_eq!(
            schema.columns.iter().map(|c| c.kind).collect::<Vec<_>>(),
            [Kind::Bigint, Kind::Varchar]
        );
        assert!(!schema.columns[0].nullable);
        assert!(schema.columns[1].nullable);
        assert!(all(&mut open_file(&binding, root).unwrap()).is_empty());
    }
    #[test]
    fn malformed_csv_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bad.csv");
        let root = directory.path().to_str().unwrap();
        for text in [
            "a,b\n1,\"unfinished\n",
            "a,b\n1,\"closed\"junk\n",
            "a,b\n1,un\"quoted\n",
            "a,b\n1,2,3\n",
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(
                bind_file(&json!({"format":"csv","path":path}).to_string(), root).is_err(),
                "{text}"
            );
        }
    }
    #[test]
    fn copy_is_atomic_and_never_overwrites() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let path = directory.path().join("export.csv");
        let options = json!({"path":path,"header":true,"columns":["id","note"]}).to_string();
        {
            let mut writer = create_writer(&options, root).unwrap();
            writer.write_row(r#"["1","line\nquote\"comma,"]"#).unwrap();
            assert!(!path.exists());
        }
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let mut writer = create_writer(&options, root).unwrap();
        writer.write_row(r#"["1","line\nquote\"comma,"]"#).unwrap();
        writer.write_row(r#"["2",null]"#).unwrap();
        writer.finish().unwrap();
        assert!(create_writer(&options, root).is_err());
        let binding = bind_file(&json!({"format":"csv","path":path}).to_string(), root).unwrap();
        assert_eq!(
            all(&mut open_file(&binding, root).unwrap()),
            vec![json!([1, "line\nquote\"comma,"]), json!([2, null])]
        );
    }
    #[test]
    fn detects_changed_file_and_rejects_nested_json() {
        let root = std::env::temp_dir().join(format!("seekdb-file-sql-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("rows.jsonl");
        std::fs::write(&path, "{\"a\":1}\n").unwrap();
        let options = json!({"format":"json","path":path}).to_string();
        let binding = bind_file(&options, root.to_str().unwrap()).unwrap();
        let mut scan = open_file(&binding, root.to_str().unwrap()).unwrap();
        std::fs::write(&path, "{\"a\":123}\n").unwrap();
        assert!(scan.next_row().is_err());
        assert!(scan.rescan().is_err());
        std::fs::write(&path, "{\"a\":{\"nested\":1}}\n").unwrap();
        assert!(bind_file(&options, root.to_str().unwrap()).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
