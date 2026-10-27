//! Bounded, streaming glob discovery. The glob crate's directory expansion
//! collects a whole directory before our result limit can be checked.
use super::Result;
#[cfg(unix)]
use super::{err, secure_path};
#[cfg(unix)]
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
pub(super) fn discover(
    pattern: &str,
    root: &str,
    check: &dyn Fn() -> Result<()>,
) -> Result<Vec<String>> {
    use std::collections::BTreeSet;
    use std::os::unix::ffi::OsStrExt;
    check()?;
    let allowed = secure_path(Path::new(root), root)?;
    let path = Path::new(pattern);
    if !path.is_absolute() {
        return Err("glob requires an absolute path".into());
    }
    let parts: Vec<_> = path.components().collect();
    if parts.len() > 128 || parts.iter().any(|part| part.as_os_str() == "**") {
        return Err("recursive or excessive-depth glob is unsupported".into());
    }
    let first = parts
        .iter()
        .position(|part| part.as_os_str().to_string_lossy().contains(['*', '?', '[']))
        .unwrap_or(parts.len());
    let prefix: PathBuf = parts[..first].iter().collect();
    if !prefix.try_exists().map_err(err)? {
        // Validate missing literal suffixes against the nearest existing ancestor;
        // no matches under an allowed, missing directory is an empty result.
        for end in (1..=first).rev() {
            let ancestor: PathBuf = parts[..end].iter().collect();
            if ancestor.try_exists().map_err(err)? {
                let mut candidate = secure_path(&ancestor, root)?;
                for part in &parts[end..first] {
                    match part {
                        Component::ParentDir => {
                            candidate.pop();
                        }
                        Component::CurDir => {}
                        _ => candidate.push(part.as_os_str()),
                    }
                    if !candidate.starts_with(&allowed) {
                        return Err("glob is outside secure_file_priv".into());
                    }
                }
                return Ok(Vec::new());
            }
        }
        return Err("glob prefix cannot be resolved".into());
    }
    let prefix = secure_path(&prefix, root)?;
    let suffix: Vec<_> = parts[first..]
        .iter()
        .map(|part| {
            let text = part.as_os_str().to_str().ok_or("glob path is not UTF-8")?;
            glob::Pattern::new(text).map_err(err)
        })
        .collect::<Result<_>>()?;
    struct Walk<'a> {
        root: &'a str,
        check: &'a dyn Fn() -> Result<()>,
        examined: usize,
        files: BTreeSet<String>,
    }
    impl Walk<'_> {
        fn visit(&mut self, path: &Path, suffix: &[glob::Pattern]) -> Result<()> {
            (self.check)()?;
            if !path.try_exists().map_err(err)? {
                return Ok(());
            }
            let path = secure_path(path, self.root)?;
            if suffix.is_empty() {
                if path.is_file() {
                    self.files
                        .insert(path.to_str().ok_or("glob result is not UTF-8")?.to_owned());
                    if self.files.len() > 10000 {
                        return Err("glob exceeds file limit".into());
                    }
                }
                return Ok(());
            }
            if !path.is_dir() {
                return Ok(());
            }
            if !suffix[0].as_str().contains(['*', '?', '[']) {
                return self.visit(&path.join(suffix[0].as_str()), &suffix[1..]);
            }
            let directory = super::open_canonical_path(&path, true)?;
            for entry in rustix::fs::Dir::new(directory).map_err(err)? {
                (self.check)()?;
                let entry = entry.map_err(err)?;
                let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
                if name == "." || name == ".." {
                    continue;
                }
                self.examined += 1;
                if self.examined > 100000 {
                    return Err("glob exceeds directory traversal limit".into());
                }
                if suffix[0].matches_path(Path::new(name)) {
                    self.visit(&path.join(name), &suffix[1..])?;
                }
            }
            Ok(())
        }
    }
    let mut walk = Walk {
        root,
        check,
        examined: 0,
        files: BTreeSet::new(),
    };
    walk.visit(&prefix, &suffix)?;
    Ok(walk.files.into_iter().collect())
}

#[cfg(not(unix))]
pub(super) fn discover(_: &str, _: &str, _: &dyn Fn() -> Result<()>) -> Result<Vec<String>> {
    Err("secure file discovery is unsupported on this platform".into())
}
