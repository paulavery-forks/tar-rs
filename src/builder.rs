use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str;

use crate::header::path2bytes;
use crate::{other, EntryType, Header};

fn prepare_longlink_header(size: u64, entry_type: u8) -> Header {
    let mut header = Header::new_gnu();
    let name = b"././@LongLink";
    header.as_gnu_mut().unwrap().name[..name.len()].clone_from_slice(&name[..]);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    // + 1 to be compliant with GNU tar
    header.set_size(size + 1);
    header.set_entry_type(EntryType::new(entry_type));
    header.set_cksum();
    header
}

/// A structure for building archives
///
/// This structure has methods for building up an archive from scratch into any
/// arbitrary writer.
pub struct Builder<W: Write> {
    follow: bool,
    finished: bool,
    obj: Option<W>,
}

impl<W: Write> Builder<W> {
    /// Create a new archive builder with the underlying object as the
    /// destination of all data written.
    pub fn new(obj: W) -> Builder<W> {
        Builder {
            follow: false,
            finished: false,
            obj: Some(obj),
        }
    }

    /// Follow symlinks, archiving the contents of the file they point to rather
    /// than adding a symlink to the archive. Defaults to false.
    pub fn follow_symlinks(&mut self, follow: bool) {
        self.follow = follow;
    }

    /// Gets mutable reference to the underlying object.
    ///
    /// Note that care must be taken while writing to the underlying
    /// object. But, e.g. `get_mut().flush()` is claimed to be safe and
    /// useful in the situations when one needs to be ensured that
    /// tar entry was flushed to the disk.
    pub fn get_mut(&mut self) -> &mut W {
        self.obj.as_mut().unwrap()
    }

    /// Unwrap this archive, returning the underlying object.
    ///
    /// This function will finish writing the archive if the `finish` function
    /// hasn't yet been called, returning any I/O error which happens during
    /// that operation.
    pub fn into_inner(mut self) -> io::Result<W> {
        if !self.finished {
            self.finish()?;
        }
        Ok(self.obj.take().unwrap())
    }

    pub fn append(&mut self, entry: impl WritableEntry) -> io::Result<()> {
        entry.write(self)
    }

    pub fn append_header(&mut self, header: &Header) -> io::Result<()> {
        self.get_mut().write_all(header.as_bytes())
    }

    pub fn append_content(&mut self, mut source: impl Read) -> io::Result<()> {
        let len = io::copy(&mut source, self.get_mut())?;

        // Pad with zeros if necessary.
        let buf = [0; 512];
        let remaining = 512 - (len % 512);
        if remaining < 512 {
            self.get_mut().write_all(&buf[..remaining as usize])?;
        }

        Ok(())
    }

    pub fn append_longpath(
        &mut self,
        path: impl AsRef<Path>,
        header: &mut Header,
    ) -> io::Result<()> {
        // Try to encode the path directly in the header, but if it ends up not
        // working (probably because it's too long) then try to use the GNU-specific
        // long name extension by emitting an entry which indicates that it's the
        // filename.
        if let Err(e) = header.set_path(&path) {
            let data = path2bytes(path.as_ref())?;
            let max = header.as_old().name.len();

            // Since `e` isn't specific enough to let us know the path is indeed too
            // long, verify it first before using the extension.
            if data.len() < max {
                return Err(e);
            }

            self.append_header(&mut prepare_longlink_header(
                data.len() as u64,
                EntryType::GNULongName.as_byte(),
            ))?;
            // write null-terminated string
            self.append_content(&mut data.chain(io::repeat(0).take(1)))?;

            // Truncate the path to store in the header we're about to emit to
            // ensure we've got something at least mentioned. Note that we use
            // `str`-encoding to be compatible with Windows, but in general the
            // entry in the header itself shouldn't matter too much since extraction
            // doesn't look at it.
            let truncated = match str::from_utf8(&data[..max]) {
                Ok(s) => s,
                Err(e) => str::from_utf8(&data[..e.valid_up_to()]).unwrap(),
            };

            header.set_path(truncated)?;
        }

        header.set_cksum();

        Ok(())
    }

    /// Finish writing this archive, emitting the termination sections.
    ///
    /// This function should only be called when the archive has been written
    /// entirely and if an I/O error happens the underlying object still needs
    /// to be acquired.
    ///
    /// In most situations the `into_inner` method should be preferred.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.get_mut().write_all(&[0; 1024])
    }
}

impl<W: Write> Drop for Builder<W> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

pub trait WritableEntry {
    fn write(self, destination: &mut Builder<impl io::Write>) -> io::Result<()>;
}

pub struct FileEntry<R> {
    pub path: PathBuf,
    pub header: Header,
    pub content: R,
}

impl<R> FileEntry<R> {
    pub fn set_path(self, path: PathBuf) -> Self {
        FileEntry { path, ..self }
    }

    pub fn set_content<R2: io::Read>(self, content: R2) -> FileEntry<R2> {
        FileEntry {
            content,
            header: self.header,
            path: self.path,
        }
    }
}

impl FileEntry<fs::File> {
    pub fn from_fs(path: PathBuf, metadata: fs::Metadata) -> io::Result<Self> {
        let mut header = Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_cksum();

        Ok(FileEntry {
            content: fs::File::open(&path)?,
            header,
            path,
        })
    }
}

impl<R: io::Read> WritableEntry for FileEntry<R> {
    fn write(mut self, builder: &mut Builder<impl io::Write>) -> io::Result<()> {
        builder.append_longpath(&self.path, &mut self.header)?;
        builder.append_header(&self.header)?;
        builder.append_content(&mut self.content)
    }
}

pub struct DirEntry {
    pub path: PathBuf,
    pub header: Header,
}

impl DirEntry {
    pub fn set_path(self, path: PathBuf) -> Self {
        DirEntry { path, ..self }
    }

    pub fn from_fs(path: PathBuf, metadata: fs::Metadata) -> io::Result<Self> {
        let mut header = Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_cksum();

        Ok(DirEntry { path, header })
    }
}

impl WritableEntry for DirEntry {
    fn write(mut self, builder: &mut Builder<impl io::Write>) -> io::Result<()> {
        builder.append_longpath(&self.path, &mut self.header)?;
        builder.append_header(&self.header)
    }
}

pub struct LinkEntry {
    pub path: PathBuf,
    pub target: PathBuf,
    pub header: Header,
}

impl LinkEntry {
    pub fn set_path(self, path: PathBuf) -> Self {
        LinkEntry { path, ..self }
    }

    pub fn from_fs(path: PathBuf, metadata: fs::Metadata) -> io::Result<Self> {
        let mut header = Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_cksum();

        Ok(LinkEntry {
            target: fs::read_link(&path)?,
            header,
            path,
        })
    }
}

impl WritableEntry for LinkEntry {
    fn write(mut self, builder: &mut Builder<impl io::Write>) -> io::Result<()> {
        builder.append_longpath(&self.path, &mut self.header)?;

        if let Err(e) = self.header.set_link_name(&self.target) {
            let data = path2bytes(&self.target)?;

            // Since `e` isn't specific enough to let us know the path is indeed too
            // long, verify it first before using the extension.
            if data.len() < self.header.as_old().linkname.len() {
                return Err(e);
            }

            builder.append_header(&mut prepare_longlink_header(
                data.len() as u64,
                EntryType::GNULongLink.as_byte(),
            ))?;

            // write null-terminated string
            builder.append_content(&mut data.chain(io::repeat(0).take(1)))?;
        }

        self.header.set_cksum();

        builder.append_header(&mut self.header)
    }
}

pub struct SpecialEntry {
    pub path: PathBuf,
    pub header: Header,
}

#[cfg(unix)]
impl SpecialEntry {
    pub fn set_path(self, path: PathBuf) -> Self {
        SpecialEntry { path, ..self }
    }

    pub fn from_fs(path: PathBuf, metadata: fs::Metadata) -> io::Result<Self> {
        use ::std::os::unix::fs::{FileTypeExt, MetadataExt};

        let mut header = Header::new_gnu();
        header.set_metadata(&metadata);

        let file_type = metadata.file_type();
        if file_type.is_fifo() {
            header.set_entry_type(EntryType::Fifo);
        } else if file_type.is_char_device() {
            header.set_entry_type(EntryType::Char);
        } else if file_type.is_block_device() {
            header.set_entry_type(EntryType::Block);
        } else if file_type.is_socket() {
            return Err(other(&format!(
                "{}: socket can not be archived",
                path.display()
            )));
        } else {
            return Err(other(&format!("{} has unknown file type", path.display())));
        }

        let dev_id = metadata.rdev();
        let dev_major = ((dev_id >> 32) & 0xffff_f000) | ((dev_id >> 8) & 0x0000_0fff);
        let dev_minor = ((dev_id >> 12) & 0xffff_ff00) | ((dev_id) & 0x0000_00ff);
        header.set_device_major(dev_major as u32)?;
        header.set_device_minor(dev_minor as u32)?;
        header.set_cksum();

        Ok(SpecialEntry { path, header })
    }
}

#[cfg(unix)]
impl WritableEntry for SpecialEntry {
    fn write(mut self, builder: &mut Builder<impl io::Write>) -> io::Result<()> {
        builder.append_longpath(self.path, &mut self.header)?;
        builder.append_header(&self.header)
    }
}

pub enum FsEntry<R> {
    Dir(DirEntry),
    File(FileEntry<R>),
    Link(LinkEntry),
    #[cfg(unix)]
    Special(SpecialEntry),
}

impl<R> FsEntry<R> {
    pub fn path(&self) -> &Path {
        match self {
            Self::Dir(entry) => &entry.path,
            Self::File(entry) => &entry.path,
            Self::Link(entry) => &entry.path,
            #[cfg(unix)]
            Self::Special(entry) => &entry.path,
        }
    }

    pub fn prefix(self, path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().join(self.path());

        self.set_path(path)
    }

    pub fn set_path(self, path: PathBuf) -> Self {
        match self {
            Self::Dir(entry) => Self::Dir(entry.set_path(path)),
            Self::File(entry) => Self::File(entry.set_path(path)),
            Self::Link(entry) => Self::Link(entry.set_path(path)),
            #[cfg(unix)]
            Self::Special(entry) => Self::Special(entry.set_path(path)),
        }
    }
}

impl FsEntry<fs::File> {
    pub fn from_fs(path: PathBuf, follow_symlinks: bool) -> io::Result<Self> {
        let metadata = if follow_symlinks {
            fs::metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("{} when getting metadata for {}", err, path.display()),
                )
            })?
        } else {
            fs::symlink_metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("{} when getting metadata for {}", err, path.display()),
                )
            })?
        };

        if metadata.is_dir() {
            return Ok(FsEntry::Dir(DirEntry::from_fs(path, metadata)?));
        } else if metadata.is_file() {
            return Ok(FsEntry::File(FileEntry::from_fs(path, metadata)?));
        } else if metadata.file_type().is_symlink() {
            return Ok(FsEntry::Link(LinkEntry::from_fs(path, metadata)?));
        } else {
            #[cfg(unix)]
            {
                return Ok(FsEntry::Special(SpecialEntry::from_fs(path, metadata)?));
            }
            #[cfg(not(unix))]
            {
                Err(other(&format!("{} has unknown file type", path.display())))
            }
        }
    }
}

impl<R: io::Read> WritableEntry for FsEntry<R> {
    fn write(self, builder: &mut Builder<impl io::Write>) -> io::Result<()> {
        match self {
            Self::Dir(entry) => entry.write(builder),
            Self::File(entry) => entry.write(builder),
            Self::Link(entry) => entry.write(builder),
            #[cfg(unix)]
            Self::Special(entry) => entry.write(builder),
        }
    }
}

pub struct DirWalker {
    root: PathBuf,
    stack: Vec<(PathBuf, bool, bool)>,
    follow_symlinks: bool,
}

impl DirWalker {
    pub fn new(root: PathBuf, follow_symlinks: bool) -> Self {
        DirWalker {
            stack: vec![(root.clone(), true, false)],
            root,
            follow_symlinks,
        }
    }

    fn extend_from_directory(&mut self, pth: &Path) -> io::Result<()> {
        for entry in fs::read_dir(&pth)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            self.stack
                .push((entry.path(), file_type.is_dir(), file_type.is_symlink()));
        }

        Ok(())
    }

    fn stack_entry_to_fs_entry(
        &mut self,
        (src, is_dir, is_symlink): (PathBuf, bool, bool),
    ) -> io::Result<Option<FsEntry<fs::File>>> {
        // In case of a symlink pointing to a directory, is_dir is false, but src.is_dir() will return true
        if is_dir || (is_symlink && self.follow_symlinks && src.is_dir()) {
            self.extend_from_directory(&src)?;
        }

        let dest = src.strip_prefix(&self.root).unwrap().to_owned();
        if dest == Path::new("") {
            return Ok(None);
        }

        Ok(Some(
            FsEntry::from_fs(src, self.follow_symlinks)?.set_path(dest),
        ))
    }
}

impl Iterator for DirWalker {
    type Item = io::Result<FsEntry<fs::File>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.stack.pop() {
            match self.stack_entry_to_fs_entry(entry) {
                Err(err) => return Some(Err(err)),
                Ok(Some(fs_entry)) => return Some(Ok(fs_entry)),
                Ok(None) => continue,
            }
        }

        return None;
    }
}
