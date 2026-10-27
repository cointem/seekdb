//! Streaming file readers used by the native SQL table operator.
use chrono::{NaiveDate, NaiveDateTime};
use parquet::file::reader::FileReader;
mod file_discovery;
mod parquet_guard;
mod parquet_pages;
use parquet::record::reader::RowIter;
use parquet::record::Field;
use parquet_guard::reader as parquet_reader;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
    unsafe extern "C++" {
        include!("file_sql_status.h");
        type FileSqlStatus;
        fn check(self: &FileSqlStatus) -> i32;
    }
    extern "Rust" {
        type FileScan;
        fn bind_file(options: &str, secure_root: &str) -> Result<String>;
        fn bind_file_with_status(
            options: &str,
            secure_root: &str,
            status: &FileSqlStatus,
        ) -> Result<String>;
        fn open_file(binding: &str, secure_root: &str) -> Result<Box<FileScan>>;
        fn open_projected_file(
            binding: &str,
            secure_root: &str,
            projection: &str,
        ) -> Result<Box<FileScan>>;
        fn open_projected_file_with_status(
            binding: &str,
            secure_root: &str,
            projection: &str,
            status: &FileSqlStatus,
        ) -> Result<Box<FileScan>>;
        fn next_row(self: &mut FileScan) -> Result<String>;
        fn rescan(self: &mut FileScan) -> Result<()>;
        fn rescan_with_status(self: &mut FileScan, status: &FileSqlStatus) -> Result<()>;
        type FileWriter;
        fn create_writer(options: &str, secure_root: &str) -> Result<Box<FileWriter>>;
        fn write_row(self: &mut FileWriter, row: &str) -> Result<()>;
        fn finish(self: &mut FileWriter) -> Result<()>;
    }
}

fn check_status(status: &ffi::FileSqlStatus) -> Result<()> {
    if status.check() == 0 {
        Ok(())
    } else {
        Err("file SQL was cancelled or timed out".into())
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
    -1
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
    record_bytes: usize,
    record: usize,
    quote_state: u8,
    delimiter: u8,
}
impl Read for LimitedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?;
        if n == 0 && self.quote_state == 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CSV record {}: unterminated quoted field", self.record + 1),
            ));
        }
        for &byte in &buf[..n] {
            // Account for raw logical records here, before the CSV parser's
            // read-ahead buffer. Resetting in read_record lets prefetched bytes
            // escape the limit. Record terminators do not count toward the cap.
            if self.quote_state != 2 && matches!(byte, b'\r' | b'\n') {
                if self.record_bytes > 0 {
                    self.record += 1;
                }
                self.record_bytes = 0;
            } else {
                self.record_bytes += 1;
                if self.record_bytes > MAX_ROW {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("CSV record {} exceeds resource limit", self.record + 1),
                    ));
                }
            }
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
                        format!("CSV record {}: malformed quoting", self.record + 1),
                    ))
                }
            };
        }
        Ok(n)
    }
}
enum Source {
    Csv(csv::Reader<LimitedFile>, Vec<String>),
    Json(BufReader<File>, usize),
    Parquet(RowIter<'static>),
    ParquetCount(u64),
    Glob(std::vec::IntoIter<String>),
}
struct OpenedSource {
    source: Source,
    handle: Option<File>,
    names: Vec<String>,
    identity: Option<Identity>,
}
pub struct FileScan {
    binding: Binding,
    root: String,
    source: Source,
    handle: Option<File>,
    projection: Option<Vec<usize>>,
    column_indices: HashMap<String, usize>,
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
// Keep creation, publication and cleanup anchored to the same directory even
// if its pathname is renamed or replaced while a COPY is running.
#[cfg(unix)]
struct TemporaryOutput {
    directory: File,
    name: String,
    file: File,
}
#[cfg(unix)]
impl TemporaryOutput {
    fn new_in(parent: &Path) -> Result<Self> {
        use rustix::fs::{openat, Mode, OFlags};
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let directory = open_canonical_path(parent, true)?;
        for _ in 0..128 {
            let name = format!(
                ".seekdb-copy-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            );
            let flags =
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            match openat(&directory, &name, flags, Mode::RUSR | Mode::WUSR) {
                Ok(file) => {
                    return Ok(Self {
                        directory,
                        name,
                        file: File::from(file),
                    })
                }
                Err(rustix::io::Errno::EXIST) => continue,
                Err(error) => return Err(err(error)),
            }
        }
        Err("cannot allocate an exclusive COPY temporary file".into())
    }
    fn as_file(&self) -> &File {
        &self.file
    }
    fn owns_name(&self, name: &std::ffi::OsStr) -> bool {
        match (
            rustix::fs::fstat(&self.file),
            rustix::fs::statat(&self.directory, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW),
        ) {
            (Ok(file), Ok(path)) => file.st_dev == path.st_dev && file.st_ino == path.st_ino,
            _ => false,
        }
    }
    fn persist_noclobber(self, target: &Path) -> Result<()> {
        let name = target.file_name().ok_or("output file name is required")?;
        if !self.owns_name(self.name.as_ref()) {
            return Err("COPY temporary file was replaced".into());
        }
        rustix::fs::linkat(
            &self.directory,
            &self.name,
            &self.directory,
            name,
            rustix::fs::AtFlags::empty(),
        )
        .map_err(err)?;
        if !self.owns_name(name) {
            let _ = rustix::fs::unlinkat(&self.directory, name, rustix::fs::AtFlags::empty());
            return Err("COPY temporary file changed during publication".into());
        }
        Ok(())
    }
}
#[cfg(unix)]
impl Drop for TemporaryOutput {
    fn drop(&mut self) {
        if self.owns_name(self.name.as_ref()) {
            let _ = rustix::fs::unlinkat(&self.directory, &self.name, rustix::fs::AtFlags::empty());
        }
    }
}
#[cfg(not(unix))]
type TemporaryOutput = tempfile::NamedTempFile;

pub struct FileWriter {
    temporary: Option<TemporaryOutput>,
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
    let temporary = TemporaryOutput::new_in(&parent).map_err(err)?;
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
    if !cfg!(unix) {
        return Err(
            "file SQL is unsupported on platforms without secure directory-handle operations"
                .into(),
        );
    }
    if root.is_empty() || root.eq_ignore_ascii_case("null") {
        return Err("secure_file_priv does not permit file access".into());
    }
    let root = Path::new(root).canonicalize().map_err(err)?;
    if !root.is_dir() {
        return Err("secure_file_priv must be a directory".into());
    }
    let path = path.canonicalize().map_err(err)?;
    if !path.starts_with(root) {
        return Err("file is outside secure_file_priv".into());
    }
    Ok(path)
}

// Walk the already canonicalized absolute path through directory descriptors.
// A symlink introduced after canonicalize must not redirect open outside the
// permitted root. NONBLOCK also prevents a substituted FIFO from hanging bind.
#[cfg(unix)]
fn open_regular_file(path: &Path) -> Result<File> {
    open_canonical_path(path, false)
}
#[cfg(unix)]
fn open_canonical_path(path: &Path, is_directory: bool) -> Result<File> {
    use rustix::fs::{open, openat, Mode, OFlags};
    let mut directory = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(err)?;
    let mut components = path.components().peekable();
    if components.next() != Some(std::path::Component::RootDir) {
        return Err("file path must be absolute after canonicalization".into());
    }
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err("file path is not canonical".into());
        };
        let mut flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
        if components.peek().is_some() || is_directory {
            flags |= OFlags::DIRECTORY;
        }
        directory = openat(&directory, name, flags, Mode::empty()).map_err(err)?;
    }
    let file = File::from(directory);
    let metadata = file.metadata().map_err(err)?;
    if !(if is_directory {
        metadata.is_dir()
    } else {
        metadata.is_file()
    }) {
        return Err("unexpected file type while opening canonical path".into());
    }
    Ok(file)
}
#[cfg(not(unix))]
fn open_regular_file(path: &Path) -> Result<File> {
    let file = File::open(path).map_err(err)?;
    if !file.metadata().map_err(err)?.is_file() {
        return Err("source must be a regular file".into());
    }
    Ok(file)
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
        while !seen.insert(name.to_ascii_lowercase()) {
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
    check_pages: bool,
    check: &dyn Fn() -> Result<()>,
) -> Result<OpenedSource> {
    check()?;
    if options.format == "glob" {
        return Ok(OpenedSource {
            source: Source::Glob(Vec::from(files).into_iter()),
            handle: None,
            names: vec!["file".into()],
            identity: None,
        });
    }
    let path = secure_path(Path::new(&options.path), root)?;
    let file = open_regular_file(&path).map_err(|e| format!("{}: {e}", options.path))?;
    let handle = file.try_clone().map_err(err)?;
    let identity = Identity::of(&file.metadata().map_err(err)?)?;
    if identity != Identity::of(&std::fs::metadata(path).map_err(err)?)? {
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
                    record_bytes: 0,
                    record: 0,
                    quote_state: 0,
                    delimiter: options.delimiter.as_bytes()[0],
                });
            names = if options.header {
                unique_names(
                    reader
                        .headers()
                        .map_err(|e| format!("{}: CSV header: {e}", options.path))?
                        .iter()
                        .map(str::to_owned),
                )?
            } else {
                let count = reader
                    .headers()
                    .map_err(|e| format!("{}: CSV record 1: {e}", options.path))?
                    .len();
                unique_names((1..=count).map(|i| format!("column{i}")))?
            };
            Source::Csv(reader, names.clone())
        }
        "json" => Source::Json(BufReader::new(file), 0),
        "parquet" => {
            let reader = parquet_reader(file, projection, check_pages, check)?;
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
            let mut seen = HashSet::new();
            if names
                .iter()
                .any(|name| name.is_empty() || !seen.insert(name.to_ascii_lowercase()))
            {
                return Err("empty or ambiguous Parquet column name".into());
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
            if projection.is_some_and(|ids| ids.is_empty()) {
                let rows =
                    u64::try_from(reader.metadata().file_metadata().num_rows()).map_err(err)?;
                let group_rows =
                    reader
                        .metadata()
                        .row_groups()
                        .iter()
                        .try_fold(0u64, |total, group| {
                            total
                                .checked_add(u64::try_from(group.num_rows()).map_err(err)?)
                                .ok_or_else(|| "Parquet row count overflow".to_owned())
                        })?;
                if rows != group_rows {
                    return Err("inconsistent Parquet row counts".into());
                }
                Source::ParquetCount(rows)
            } else {
                Source::Parquet(
                    RowIter::from_file_into(Box::new(reader))
                        .project(projected)
                        .map_err(err)?,
                )
            }
        }
        _ => return Err("unsupported file format".into()),
    };
    Ok(OpenedSource {
        source,
        handle: Some(handle),
        names,
        identity: Some(identity),
    })
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
        let result = self.read_row(options);
        result.map_err(|error| {
            let location = match self {
                Self::Json(_, line) => format!("JSONL line {line}"),
                Self::Csv(reader, _) => format!("CSV record {}", reader.position().record() + 1),
                Self::Parquet(_) | Self::ParquetCount(_) => "Parquet".into(),
                Self::Glob(_) => "glob".into(),
            };
            format!("{}: {location}: {error}", options.path)
        })
    }
    fn read_row(&mut self, options: &Options) -> Result<Option<Vec<(String, Value)>>> {
        let row: Vec<(String, Value)> = match self {
            Self::Csv(reader, names) => {
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
            Self::Json(reader, line_number) => {
                let mut line = Vec::new();
                loop {
                    line.clear();
                    if (&mut *reader)
                        .take(MAX_ROW as u64 + 3)
                        .read_until(b'\n', &mut line)
                        .map_err(err)?
                        == 0
                    {
                        return Ok(None);
                    }
                    *line_number += 1;
                    if line.last() == Some(&b'\n') {
                        line.pop();
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                    }
                    if line.len() > MAX_ROW {
                        return Err("JSON row exceeds resource limit".into());
                    }
                    if !line.iter().all(u8::is_ascii_whitespace) {
                        break;
                    }
                }
                serde_json::from_slice::<JsonRow>(&line).map_err(err)?.0
            }
            Self::Parquet(reader) => match reader.next() {
                None => return Ok(None),
                Some(row) => row
                    .map_err(err)?
                    .get_column_iter()
                    .map(|(k, v)| parquet_value(v).map(|v| (k.clone(), v)))
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::ParquetCount(rows) => {
                if *rows == 0 {
                    return Ok(None);
                }
                *rows -= 1;
                Vec::new()
            }
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

// Deserialize keys before building a Value map: Value silently keeps the last
// duplicate key, and may round an out-of-range integer into a floating point value.
struct JsonRow(Vec<(String, Value)>);
impl<'de> Deserialize<'de> for JsonRow {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = JsonRow;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a flat JSONL object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<JsonRow, M::Error> {
                use serde::de::Error;
                let mut fields = Vec::new();
                let mut seen = HashSet::new();
                while let Some(name) = map.next_key::<String>()? {
                    if name.is_empty() || !seen.insert(name.to_ascii_lowercase()) {
                        return Err(M::Error::custom(format!(
                            "empty or duplicate JSON field {name:?}"
                        )));
                    }
                    if fields.len() >= MAX_COLUMNS {
                        return Err(M::Error::custom("too many JSON fields"));
                    }
                    let raw = map.next_value::<Box<serde_json::value::RawValue>>()?;
                    let text = raw.get();
                    if integer_text(text) && text.parse::<i64>().is_err() {
                        return Err(M::Error::custom(format!(
                            "field {name:?}: integer exceeds BIGINT range"
                        )));
                    }
                    let value: Value = serde_json::from_str(text).map_err(M::Error::custom)?;
                    if value.is_object() || value.is_array() {
                        return Err(M::Error::custom(format!(
                            "field {name:?}: nested JSON is unsupported"
                        )));
                    }
                    fields.push((name, value));
                }
                Ok(JsonRow(fields))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

fn integer_text(text: &str) -> bool {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}
fn date(text: &str) -> Result<NaiveDate> {
    let bytes = text.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
        || &bytes[..4] == b"0000"
    {
        return Err("invalid date format".into());
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(err)
}
fn datetime(text: &str) -> Result<NaiveDateTime> {
    if text.len() == 10 {
        return date(text)?
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid date".into());
    }
    let b = text.as_bytes();
    if b.len() < 19
        || !matches!(b[10], b' ' | b'T')
        || b[13] != b':'
        || b[16] != b':'
        || !b[11..19]
            .iter()
            .enumerate()
            .all(|(i, c)| i == 2 || i == 5 || c.is_ascii_digit())
        || b[17] >= b'6'
        || (b.len() > 19
            && (b.len() < 21
                || b.len() > 26
                || b[19] != b'.'
                || !b[20..].iter().all(u8::is_ascii_digit)))
    {
        return Err("invalid datetime format".into());
    }
    date(&text[..10])?;
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
            } else if csv && integer_text(s) {
                if s.parse::<i64>().is_ok() {
                    Kind::Bigint
                } else {
                    Kind::Varchar
                }
            } else if csv && s.parse::<f64>().map(f64::is_finite).unwrap_or(false) {
                Kind::Double
            } else if date(s).is_ok() {
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
    bind_file_impl(options, secure_root, &|| Ok(()))
}
fn bind_file_with_status(
    options: &str,
    secure_root: &str,
    status: &ffi::FileSqlStatus,
) -> Result<String> {
    bind_file_impl(options, secure_root, &|| check_status(status))
}
fn bind_file_impl(
    options: &str,
    secure_root: &str,
    check: &dyn Fn() -> Result<()>,
) -> Result<String> {
    check()?;
    let mut options: Options = serde_json::from_str(options).map_err(err)?;
    secure_path(Path::new(secure_root), secure_root)?;
    if options.sample_size != -1 {
        return Err("only sample_size=-1 is supported".into());
    }
    let mut files = Vec::new();
    if options.format == "glob" {
        files = file_discovery::discover(&options.path, secure_root, check)?;
    } else {
        options.path = secure_path(Path::new(&options.path), secure_root)?
            .to_str()
            .ok_or("path is not UTF-8")?
            .to_owned();
    }
    let OpenedSource {
        mut source,
        handle,
        names,
        identity,
    } = source(&options, secure_root, &files, None, false, check)?;
    let mut columns: Vec<_> = names
        .into_iter()
        .map(|name| Column {
            name,
            kind: if options.format == "glob" {
                Kind::Varchar
            } else {
                Kind::Null
            },
            nullable: false,
            precision: 0,
        })
        .collect();
    // Parquet's declared schema is authoritative, including files with no rows.
    // Inferring types from values would incorrectly turn date-looking strings into DATE.
    if options.format == "parquet" {
        use parquet::basic::{ConvertedType as C, LogicalType as L, Type as P};
        let reader = parquet_reader(
            handle.as_ref().unwrap().try_clone().map_err(err)?,
            None,
            false,
            check,
        )?;
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
    let mut indices: HashMap<String, usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect();
    let mut non_null = vec![0u64; columns.len()];
    if options.format != "parquet" && options.format != "glob" {
        while let Some(row) = source.row(&options)? {
            if rows % 128 == 0 {
                check()?;
            }
            for (name, value) in row {
                let value_kind = kind(&value, options.format == "csv");
                let precision = value
                    .as_str()
                    .filter(|_| value_kind == Kind::Datetime)
                    .and_then(|s| s.split_once('.'))
                    .map(|(_, fraction)| fraction.len() as u8)
                    .unwrap_or(0);
                let index = if let Some(&index) = indices.get(&name) {
                    let column = &mut columns[index];
                    column.kind = widen(column.kind, value_kind);
                    column.precision = column.precision.max(precision);
                    index
                } else {
                    if columns.len() >= MAX_COLUMNS {
                        return Err("too many inferred columns".into());
                    }
                    let index = columns.len();
                    indices.insert(name.clone(), index);
                    non_null.push(0);
                    columns.push(Column {
                        name,
                        kind: value_kind,
                        nullable: rows > 0 || value.is_null(),
                        precision,
                    });
                    index
                };
                if !value.is_null() {
                    non_null[index] += 1;
                }
            }
            rows += 1;
        }
        for (column, count) in columns.iter_mut().zip(non_null) {
            column.nullable = count < rows || rows == 0;
        }
    }
    if options.format == "json" {
        columns.sort_by(|a, b| a.name.cmp(&b.name));
        let mut seen = HashSet::new();
        for column in &columns {
            if !seen.insert(column.name.to_ascii_lowercase()) {
                return Err(format!(
                    "{}: ambiguous JSON field {:?}",
                    options.path, column.name
                ));
            }
        }
    }
    if rows == 0 && options.format == "csv" {
        for column in &mut columns {
            column.nullable = true;
        }
    }
    if let Some(file) = handle {
        if Some(Identity::of(&file.metadata().map_err(err)?)?) != identity
            || Some(Identity::of(
                &std::fs::metadata(&options.path).map_err(err)?,
            )?) != identity
        {
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
    open_projected_file_impl(binding, secure_root, projection, &|| Ok(()))
}
fn open_projected_file_with_status(
    binding: &str,
    secure_root: &str,
    projection: &str,
    status: &ffi::FileSqlStatus,
) -> Result<Box<FileScan>> {
    open_projected_file_impl(binding, secure_root, projection, &|| check_status(status))
}
fn open_projected_file_impl(
    binding: &str,
    secure_root: &str,
    projection: &str,
    check: &dyn Fn() -> Result<()>,
) -> Result<Box<FileScan>> {
    let binding: Binding = serde_json::from_str(binding).map_err(err)?;
    let mut projection: Option<Vec<usize>> = serde_json::from_str(projection).map_err(err)?;
    if let Some(ids) = &mut projection {
        if ids.iter().any(|&i| i >= binding.columns.len()) {
            return Err("invalid file column projection".into());
        }
        ids.sort_unstable();
        ids.dedup();
    }
    let OpenedSource { source, handle, .. } = source(
        &binding.options,
        secure_root,
        &binding.files,
        projection.as_deref(),
        true,
        check,
    )?;
    let column_indices = binding
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect();
    let scan = Box::new(FileScan {
        binding,
        root: secure_root.into(),
        source,
        handle,
        projection,
        column_indices,
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
        let row = self.source.row(&self.binding.options)?;
        self.check_identity()?;
        let Some(row) = row else {
            return Ok(String::new());
        };
        let mut values = vec![Value::Null; self.binding.columns.len()];
        for (name, value) in row {
            let index = *self
                .column_indices
                .get(&name)
                .ok_or("file schema changed since binding")?;
            let column = &self.binding.columns[index];
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
            values[index] = value;
        }
        serde_json::to_string(&values).map_err(err)
    }
    fn rescan(&mut self) -> Result<()> {
        self.rescan_impl(&|| Ok(()))
    }
    fn rescan_with_status(&mut self, status: &ffi::FileSqlStatus) -> Result<()> {
        self.rescan_impl(&|| check_status(status))
    }
    fn rescan_impl(&mut self, check: &dyn Fn() -> Result<()>) -> Result<()> {
        self.check_identity()?;
        let OpenedSource { source, handle, .. } = source(
            &self.binding.options,
            &self.root,
            &self.binding.files,
            self.projection.as_deref(),
            true,
            check,
        )?;
        self.source = source;
        self.handle = handle;
        self.check_identity()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::json;

    fn parquet_matrix_file(path: &Path, dictionary: bool) {
        parquet_matrix_file_version(path, dictionary, false);
    }
    fn parquet_matrix_file_version(path: &Path, dictionary: bool, v2: bool) {
        use parquet::basic::Compression;
        use parquet::data_type::{
            BoolType, ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type,
        };
        use parquet::file::properties::{WriterProperties, WriterVersion};
        use parquet::file::writer::SerializedFileWriter;
        use std::sync::Arc;
        let schema = parquet::schema::parser::parse_message_type(
            "message root { required int64 id; optional double score; optional binary text (UTF8); optional int32 day (DATE); optional int64 stamp (TIMESTAMP_MICROS); optional boolean flag; }",
        ).unwrap();
        let properties = WriterProperties::builder()
            .set_writer_version(if v2 {
                WriterVersion::PARQUET_2_0
            } else {
                WriterVersion::PARQUET_1_0
            })
            .set_encoding(parquet::basic::Encoding::PLAIN)
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(dictionary)
            .build();
        let mut writer = SerializedFileWriter::new(
            File::create(path).unwrap(),
            Arc::new(schema),
            Arc::new(properties),
        )
        .unwrap();
        for group in 0..2 {
            let mut rows = writer.next_row_group().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<Int64Type>()
                .write_batch(&[group * 3 + 1, group * 3 + 2, group * 3 + 3], None, None)
                .unwrap();
            column.close().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<DoubleType>()
                .write_batch(&[3.25, -2.5], Some(&[1, 0, 1]), None)
                .unwrap();
            column.close().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<ByteArrayType>()
                .write_batch(
                    &[
                        ByteArray::from("2026-01-01"),
                        ByteArray::from("中文\n\"text\""),
                    ],
                    Some(&[1, 0, 1]),
                    None,
                )
                .unwrap();
            column.close().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<Int32Type>()
                .write_batch(&[0, 1], Some(&[1, 0, 1]), None)
                .unwrap();
            column.close().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<Int64Type>()
                .write_batch(&[1_123_456, -1], Some(&[1, 0, 1]), None)
                .unwrap();
            column.close().unwrap();
            let mut column = rows.next_column().unwrap().unwrap();
            column
                .typed::<BoolType>()
                .write_batch(&[true, false], Some(&[1, 0, 1]), None)
                .unwrap();
            column.close().unwrap();
            rows.close().unwrap();
        }
        writer.close().unwrap();
    }

    #[test]
    fn parquet_multi_group_types_nulls_dictionary_and_projection() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        for (dictionary, v2) in [(false, false), (true, false), (false, true), (true, true)] {
            let path = directory
                .path()
                .join(format!("matrix-{dictionary}.parquet"));
            parquet_matrix_file_version(&path, dictionary, v2);
            let binding =
                bind_file(&json!({"format":"parquet","path":path}).to_string(), root).unwrap();
            let full = all(&mut open_file(&binding, root).unwrap());
            assert_eq!(full.len(), 6);
            assert_eq!(
                full[0],
                json!([
                    1,
                    3.25,
                    "2026-01-01",
                    "1970-01-01",
                    "1970-01-01 00:00:01.123456",
                    true
                ])
            );
            assert_eq!(full[1], json!([2, null, null, null, null, null]));
            assert_eq!(full[2][4], "1969-12-31 23:59:59.999999");
            for projection in ["[5,0,2]", "[1]", "[]"] {
                let mut scan = open_projected_file(&binding, root, projection).unwrap();
                let rows = all(&mut scan);
                assert_eq!(rows.len(), full.len());
                let ids: Vec<usize> = serde_json::from_str(projection).unwrap();
                for (row, expected) in rows.iter().zip(&full) {
                    for &index in &ids {
                        assert_eq!(row[index], expected[index]);
                    }
                }
                scan.rescan().unwrap();
                assert_eq!(all(&mut scan), rows);
            }
        }
    }

    #[test]
    fn malformed_parquet_footer_returns_error_without_panicking() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bad.parquet");
        // Unknown field 99 of type DOUBLE with no 8-byte payload. The generated
        // metadata decoder skips it through a slice reader's unchecked double.
        let footer = [0x07u8, 0xc6, 0x01];
        let mut bytes = b"PAR1".to_vec();
        bytes.extend_from_slice(&footer);
        bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        std::fs::write(&path, bytes).unwrap();
        let result = std::panic::catch_unwind(|| {
            bind_file(
                &json!({"format":"parquet","path":path}).to_string(),
                directory.path().to_str().unwrap(),
            )
        });
        assert!(result.is_ok(), "malformed input must not unwind into C++");
        assert!(result.unwrap().is_err());
    }
    #[test]
    fn parquet_projection_and_count_do_not_decode_unread_columns() {
        use std::io::{Seek, SeekFrom, Write};
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        for damaged in [0, 1] {
            let path = directory.path().join("projection.parquet");
            parquet_matrix_file(&path, true);
            let reader =
                parquet_reader(File::open(&path).unwrap(), None, false, &|| Ok(())).unwrap();
            let (offset, _) = reader.metadata().row_group(0).column(damaged).byte_range();
            drop(reader);
            let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[0]).unwrap();
            drop(file);
            let binding =
                bind_file(&json!({"format":"parquet","path":path}).to_string(), root).unwrap();
            assert_eq!(
                all(&mut open_projected_file(&binding, root, "[2]").unwrap()).len(),
                6
            );
            assert_eq!(
                all(&mut open_projected_file(&binding, root, "[]").unwrap()).len(),
                6
            );
            assert!(open_file(&binding, root).is_err());
        }
    }
    #[test]
    fn parquet_footer_mutations_are_bounded_errors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mutations.parquet");
        parquet_matrix_file(&path, true);
        let original = std::fs::read(&path).unwrap();
        let length = original.len();
        let footer_size =
            u32::from_le_bytes(original[length - 8..length - 4].try_into().unwrap()) as usize;
        let first = length - 8 - footer_size;
        for iteration in 0..256usize {
            let mut bytes = original.clone();
            let index = first + (iteration * 37 % footer_size);
            bytes[index] ^= ((iteration * 13 % 255) + 1) as u8;
            std::fs::write(&path, bytes).unwrap();
            let result = std::panic::catch_unwind(|| {
                bind_file(
                    &json!({"format":"parquet","path":path}).to_string(),
                    directory.path().to_str().unwrap(),
                )
            });
            assert!(result.is_ok(), "Footer mutation {iteration} panicked");
        }
    }
    #[test]
    fn malformed_parquet_schema_and_statistics_are_rejected_before_decoding() {
        use parquet::thrift::TSerializable;
        use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("semantic-footer.parquet");
        parquet_matrix_file(&path, true);
        let original = std::fs::read(&path).unwrap();
        let length = original.len();
        let size =
            u32::from_le_bytes(original[length - 8..length - 4].try_into().unwrap()) as usize;
        let first = length - 8 - size;
        for mutation in 0..6 {
            let mut protocol =
                TCompactInputProtocol::new(std::io::Cursor::new(&original[first..length - 8]));
            let mut metadata =
                parquet::format::FileMetaData::read_from_in_protocol(&mut protocol).unwrap();
            match mutation {
                0 => metadata.schema[0].type_ = Some(parquet::format::Type::INT64),
                1 => metadata.schema[1].type_ = None,
                2 => metadata.schema[0].num_children = Some(i32::MAX),
                3 => metadata.num_rows = -1,
                4 => {
                    metadata.row_groups[0].columns[0]
                        .meta_data
                        .as_mut()
                        .unwrap()
                        .statistics
                        .as_mut()
                        .unwrap()
                        .min_value = Some(vec![0])
                }
                _ => metadata.row_groups[0].num_rows += 1,
            }
            let mut footer = Vec::new();
            metadata
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut footer))
                .unwrap();
            let mut bytes = original[..first].to_vec();
            bytes.extend_from_slice(&footer);
            bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
            bytes.extend_from_slice(b"PAR1");
            std::fs::write(&path, bytes).unwrap();
            let result = std::panic::catch_unwind(|| {
                bind_file(
                    &json!({"format":"parquet","path":path}).to_string(),
                    directory.path().to_str().unwrap(),
                )
            });
            assert!(result.is_ok(), "semantic mutation {mutation} panicked");
            assert!(
                result.unwrap().is_err(),
                "semantic mutation {mutation} was accepted"
            );
        }
    }
    #[test]
    fn parquet_page_mutations_do_not_panic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("page-mutations.parquet");
        for (dictionary, v2) in [(false, false), (true, false), (false, true), (true, true)] {
            parquet_matrix_file_version(&path, dictionary, v2);
            let original = std::fs::read(&path).unwrap();
            let length = original.len();
            let footer_size =
                u32::from_le_bytes(original[length - 8..length - 4].try_into().unwrap()) as usize;
            let end = length - 8 - footer_size;
            for iteration in 0..96usize {
                let mut bytes = original.clone();
                bytes[4 + (iteration * 37 % (end - 4))] ^= ((iteration * 13 % 255) + 1) as u8;
                std::fs::write(&path, bytes).unwrap();
                let result = std::panic::catch_unwind(|| -> Result<()> {
                    let root = directory.path().to_str().unwrap();
                    let binding =
                        bind_file(&json!({"format":"parquet","path":path}).to_string(), root)?;
                    let mut scan = open_file(&binding, root)?;
                    for _ in 0..16 {
                        if scan.next_row()?.is_empty() {
                            return Ok(());
                        }
                    }
                    Err("corrupt scan did not terminate".into())
                });
                assert!(
                    result.is_ok(),
                    "page mutation {iteration} dictionary={dictionary} panicked"
                );
            }
        }
    }
    #[test]
    fn inference_checks_cancellation_and_releases_its_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancel.csv");
        std::fs::write(&path, "id\n".to_owned() + &"1\n".repeat(1024)).unwrap();
        let calls = std::cell::Cell::new(0);
        let result = bind_file_impl(
            &json!({"format":"csv","path":path}).to_string(),
            directory.path().to_str().unwrap(),
            &|| {
                calls.set(calls.get() + 1);
                if calls.get() == 4 {
                    Err("cancelled".into())
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.unwrap_err().contains("cancelled"));
        assert_eq!(calls.get(), 4);
        std::fs::remove_file(&path).unwrap();
    }
    #[test]
    fn all_type_merges_are_order_independent() {
        let kinds = [
            Kind::Null,
            Kind::Boolean,
            Kind::Bigint,
            Kind::Double,
            Kind::Varchar,
            Kind::Date,
            Kind::Datetime,
        ];
        for a in kinds {
            for b in kinds {
                for c in kinds {
                    assert_eq!(widen(a, b), widen(b, a));
                    assert_eq!(widen(widen(a, b), c), widen(a, widen(b, c)));
                }
            }
        }
    }
    #[test]
    fn file_mutations_fail_fetch_and_rescan() {
        use std::io::Write;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let path = directory.path().join("changing.csv");
        for action in ["append", "truncate", "replace", "delete"] {
            std::fs::write(&path, "id\n1\n2\n").unwrap();
            let binding =
                bind_file(&json!({"format":"csv","path":path}).to_string(), root).unwrap();
            let mut scan = open_file(&binding, root).unwrap();
            assert_eq!(scan.next_row().unwrap(), "[1]");
            match action {
                "append" => std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"3\n")
                    .unwrap(),
                "truncate" => File::create(&path).unwrap().set_len(0).unwrap(),
                "replace" => {
                    let replacement = directory.path().join("new.csv");
                    std::fs::write(&replacement, "id\n1\n2\n").unwrap();
                    std::fs::rename(replacement, &path).unwrap();
                }
                _ => std::fs::remove_file(&path).unwrap(),
            }
            assert!(scan.next_row().is_err(), "{action}");
            assert!(scan.rescan().is_err(), "{action}");
        }
    }

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
    fn default_inference_reads_past_2048_rows() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        for (format, initial, tail) in [
            ("csv", "id\n".to_owned() + &"1\n".repeat(2048), "text\n"),
            (
                "json",
                "{\"id\":1}\n".repeat(2048),
                "{\"id\":null,\"late\":true}\n",
            ),
        ] {
            let path = directory.path().join(format);
            std::fs::write(&path, initial + tail).unwrap();
            let options = json!({"format":format,"path":path});
            let binding = bind_file(&options.to_string(), root).unwrap();
            let mut explicit_options = options.clone();
            explicit_options["sample_size"] = json!(-1);
            assert_eq!(
                binding,
                bind_file(&explicit_options.to_string(), root).unwrap()
            );
            let mut scan = open_file(&binding, root).unwrap();
            let rows = all(&mut scan);
            assert_eq!(rows.len(), 2049);
            let schema: Binding = serde_json::from_str(&binding).unwrap();
            if format == "csv" {
                assert_eq!(schema.columns[0].kind, Kind::Varchar);
                assert_eq!(rows[2048][0], "text");
            } else {
                assert!(schema.columns[0].nullable);
                assert_eq!(schema.columns[1].name, "late");
            }
            for sample in [0, 10, 2048] {
                let mut invalid = options.clone();
                invalid["sample_size"] = json!(sample);
                assert!(bind_file(&invalid.to_string(), root).is_err());
            }
        }
    }
    #[test]
    fn schema_names_are_unambiguous_and_independent_of_row_order() {
        assert_eq!(
            unique_names(["id", "ID", "id__2", ""].into_iter().map(str::to_owned)).unwrap(),
            ["id", "ID__2", "id__2__2", "column4"]
        );
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let path = directory.path().join("rows.jsonl");
        for contents in ["{\"z\":1}\n{\"a\":true}\n", "{\"a\":true}\n{\"z\":1}\n"] {
            std::fs::write(&path, contents).unwrap();
            let binding =
                bind_file(&json!({"format":"json","path":path}).to_string(), root).unwrap();
            let schema: Binding = serde_json::from_str(&binding).unwrap();
            assert_eq!(
                schema
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>(),
                ["a", "z"]
            );
            assert!(schema.columns.iter().all(|c| c.nullable));
        }
    }
    #[test]
    fn json_rejects_duplicate_fields_and_out_of_range_integers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bad.jsonl");
        let root = directory.path().to_str().unwrap();
        for text in [
            "{\"id\":1,\"id\":2}\n",
            "{\"id\":9223372036854775808}\n",
            "{\"id\":-9223372036854775809}\n",
            "{\"\":1}\n",
            "{\"id\":1}\n{\"ID\":2}\n",
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(
                bind_file(&json!({"format":"json","path":path}).to_string(), root).is_err(),
                "{text}"
            );
        }
    }
    #[test]
    fn numeric_and_datetime_boundaries_preserve_values() {
        for text in [
            "9223372036854775808",
            "-9223372036854775809",
            "2026-01-01 12:13:14.1234567",
            "2026-1-01",
            "2026-01-01 12:13:60",
            "0000-01-01",
        ] {
            assert_eq!(kind(&json!(text), true), Kind::Varchar, "{text}");
        }
        assert_eq!(kind(&json!("9223372036854775807"), true), Kind::Bigint);
        assert_eq!(kind(&json!("1e3"), true), Kind::Double);
        assert_eq!(
            kind(&json!("2024-02-29T12:13:14.123456"), true),
            Kind::Datetime
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dates.csv");
        std::fs::write(&path, "stamp\n2026-01-01 12:13:14.120000\n").unwrap();
        let binding = bind_file(
            &json!({"format":"csv","path":path}).to_string(),
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let schema: Binding = serde_json::from_str(&binding).unwrap();
        assert_eq!(schema.columns[0].precision, 6);
    }
    #[test]
    fn csv_row_limit_is_independent_of_reader_buffering() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.csv");
        let root = directory.path().to_str().unwrap();
        // The CSV parser has already prefetched part of the next record when the
        // caller starts it; resetting a byte allowance there permits oversized rows.
        std::fs::write(
            &path,
            "value\n".to_owned() + &"x".repeat(MAX_ROW + 1) + "\n",
        )
        .unwrap();
        assert!(bind_file(
            &json!({"format":"csv","path":path,"sample_size":-1}).to_string(),
            root
        )
        .is_err());
        std::fs::write(
            &path,
            "value\r\n".to_owned() + &"x".repeat(MAX_ROW) + "\r\n",
        )
        .unwrap();
        let binding = bind_file(&json!({"format":"csv","path":path}).to_string(), root).unwrap();
        assert_eq!(
            all(&mut open_file(&binding, root).unwrap())[0][0]
                .as_str()
                .unwrap()
                .len(),
            MAX_ROW
        );
    }
    #[test]
    fn errors_include_file_and_record_location() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bad.jsonl");
        std::fs::write(&path, "\n{\"id\":1}\nnot json\n").unwrap();
        let error = bind_file(
            &json!({"format":"json","path":path}).to_string(),
            directory.path().to_str().unwrap(),
        )
        .unwrap_err();
        assert!(
            error.contains("bad.jsonl") && error.contains("line 3"),
            "{error}"
        );
    }
    #[test]
    fn empty_glob_retains_its_declared_schema() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let binding = bind_file(
            &json!({"format":"glob","path":directory.path().join("*.csv")}).to_string(),
            root,
        )
        .unwrap();
        let schema: Binding = serde_json::from_str(&binding).unwrap();
        assert_eq!(schema.columns[0].name, "file");
        assert_eq!(schema.columns[0].kind, Kind::Varchar);
        assert!(!schema.columns[0].nullable);
        assert!(all(&mut open_file(&binding, root).unwrap()).is_empty());
    }
    #[test]
    fn glob_is_streamed_deduplicated_and_validates_all_results() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let file = directory.path().join("data.csv");
        std::fs::write(&file, "id\n1\n").unwrap();
        for index in 0..10001 {
            symlink(&file, directory.path().join(format!("alias-{index}.csv"))).unwrap();
        }
        let pattern = directory.path().join("*.csv").to_str().unwrap().to_owned();
        let found = file_discovery::discover(&pattern, root, &|| Ok(())).unwrap();
        assert_eq!(found, [file.canonicalize().unwrap().to_str().unwrap()]);
        assert!(file_discovery::discover(
            directory.path().join("missing/*.csv").to_str().unwrap(),
            root,
            &|| Ok(())
        )
        .unwrap()
        .is_empty());
        let child = directory.path().join("dir");
        std::fs::create_dir(&child).unwrap();
        for name in ["a1.csv", "b2.csv", "c.txt"] {
            std::fs::write(child.join(name), "id\n1\n").unwrap();
        }
        assert_eq!(
            file_discovery::discover(
                directory.path().join("d*/[ab]?.csv").to_str().unwrap(),
                root,
                &|| Ok(())
            )
            .unwrap()
            .len(),
            2
        );
        assert_eq!(
            file_discovery::discover(
                directory.path().join("d*/../*.csv").to_str().unwrap(),
                root,
                &|| Ok(())
            )
            .unwrap(),
            found
        );
        let calls = std::cell::Cell::new(0);
        assert!(file_discovery::discover(&pattern, root, &|| {
            calls.set(calls.get() + 1);
            if calls.get() > 5 {
                Err("cancelled".into())
            } else {
                Ok(())
            }
        })
        .is_err());
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("outside.csv"), "secret").unwrap();
        symlink(
            outside.path().join("outside.csv"),
            directory.path().join("escape.csv"),
        )
        .unwrap();
        assert!(file_discovery::discover(&pattern, root, &|| Ok(())).is_err());
    }
    #[test]
    fn glob_limits_unique_results() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("source");
        std::fs::write(&file, "id\n1\n").unwrap();
        for index in 0..10001 {
            std::fs::hard_link(&file, directory.path().join(format!("file-{index}.csv"))).unwrap();
        }
        assert!(file_discovery::discover(
            directory.path().join("*.csv").to_str().unwrap(),
            directory.path().to_str().unwrap(),
            &|| Ok(())
        )
        .unwrap_err()
        .contains("file limit"));
    }
    #[cfg(unix)]
    #[test]
    fn secure_open_rejects_symlink_substitution_and_non_regular_files() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("rows.csv"), "id\n99\n").unwrap();
        let parent = directory.path().join("source");
        std::fs::create_dir(&parent).unwrap();
        let path = parent.join("rows.csv");
        std::fs::write(&path, "id\n1\n").unwrap();
        let canonical = secure_path(&path, root).unwrap();
        // Simulate a directory replacement between canonicalize and open.
        std::fs::rename(&parent, directory.path().join("old-source")).unwrap();
        symlink(outside.path(), &parent).unwrap();
        assert!(open_regular_file(&canonical).is_err());
        let file = directory.path().join("old-source/rows.csv");
        let canonical = secure_path(&file, root).unwrap();
        std::fs::remove_file(&file).unwrap();
        symlink(outside.path().join("rows.csv"), &file).unwrap();
        assert!(open_regular_file(&canonical).is_err());
        assert!(open_regular_file(&directory.path().canonicalize().unwrap()).is_err());
        let fifo = directory.path().join("fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        assert!(open_regular_file(&fifo.canonicalize().unwrap()).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn copy_directory_replacement_cannot_redirect_publication_or_cleanup() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let parent = directory.path().join("export");
        std::fs::create_dir(&parent).unwrap();
        let canonical = parent.canonicalize().unwrap();
        let temporary = TemporaryOutput::new_in(&canonical).unwrap();
        let name = temporary.name.clone();
        std::fs::rename(&parent, directory.path().join("moved")).unwrap();
        symlink(outside.path(), &parent).unwrap();
        let decoy = outside.path().join(&name);
        std::fs::write(&decoy, "unrelated outside file").unwrap();
        // Simulate replacement immediately after the caller's last path check.
        temporary
            .persist_noclobber(&parent.join("result.csv"))
            .unwrap();
        assert!(directory.path().join("moved/result.csv").is_file());
        assert!(!outside.path().join("result.csv").exists());
        assert_eq!(
            std::fs::read_to_string(&decoy).unwrap(),
            "unrelated outside file"
        );
        assert!(!directory.path().join("moved").join(&name).exists());
        assert!(TemporaryOutput::new_in(&canonical).is_err());
    }
    #[test]
    fn copy_rejects_replaced_temporary_file_without_deleting_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("result.csv");
        let mut writer = create_writer(
            &json!({"path":target,"columns":["id"]}).to_string(),
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        writer.write_row("[\"1\"]").unwrap();
        let path = directory
            .path()
            .join(&writer.temporary.as_ref().unwrap().name);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        assert!(writer.finish().is_err());
        drop(writer);
        assert!(!target.exists());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "replacement");
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

#[cfg(all(test, not(unix)))]
mod unsupported_platform_tests {
    #[test]
    fn file_access_fails_before_touching_the_filesystem() {
        assert!(
            super::secure_path(std::path::Path::new("untrusted"), "untrusted")
                .unwrap_err()
                .contains("unsupported")
        );
    }
}
