use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str;

use crate::header::{path2bytes, HeaderMode};
use crate::{other, EntryType, Header};

/// A structure for building archives
///
/// This structure has methods for building up an archive from scratch into any
/// arbitrary writer.
pub struct Builder<W: Write> {
    mode: HeaderMode,
    follow: bool,
    finished: bool,
    obj: Option<W>,
}

impl<W: Write> Builder<W> {
    /// Create a new archive builder with the underlying object as the
    /// destination of all data written. The builder will use
    /// `HeaderMode::Complete` by default.
    pub fn new(obj: W) -> Builder<W> {
        Builder {
            mode: HeaderMode::Complete,
            follow: true,
            finished: false,
            obj: Some(obj),
        }
    }

    /// Changes the HeaderMode that will be used when reading fs Metadata for
    /// methods that implicitly read metadata for an input Path. Notably, this
    /// does _not_ apply to `append(Header)`.
    pub fn mode(&mut self, mode: HeaderMode) {
        self.mode = mode;
    }

    /// Follow symlinks, archiving the contents of the file they point to rather
    /// than adding a symlink to the archive. Defaults to true.
    pub fn follow_symlinks(&mut self, follow: bool) {
        self.follow = follow;
    }

    /// Gets shared reference to the underlying object.
    pub fn get_ref(&self) -> &W {
        self.obj.as_ref().unwrap()
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

    /// Adds a new entry to this archive.
    ///
    /// This function will append the header specified, followed by contents of
    /// the stream specified by `data`. To produce a valid archive the `size`
    /// field of `header` must be the same as the length of the stream that's
    /// being written. Additionally the checksum for the header should have been
    /// set via the `set_cksum` method.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all entries have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Errors
    ///
    /// This function will return an error for any intermittent I/O error which
    /// occurs when either reading or writing.
    ///
    /// # Examples
    ///
    /// ```
    /// use tar::{Builder, Header};
    ///
    /// let mut header = Header::new_gnu();
    /// header.set_path("foo").unwrap();
    /// header.set_size(4);
    /// header.set_cksum();
    ///
    /// let mut data: &[u8] = &[1, 2, 3, 4];
    ///
    /// let mut ar = Builder::new(Vec::new());
    /// ar.append(&header, data).unwrap();
    /// let data = ar.into_inner().unwrap();
    /// ```
    pub fn append<R: Read>(&mut self, header: &Header, mut data: R) -> io::Result<()> {
        write_header(header, self.get_mut())?;
        write_content(&mut data, self.get_mut())
    }

    /// Adds a new entry to this archive with the specified path.
    ///
    /// This function will set the specified path in the given header, which may
    /// require appending a GNU long-name extension entry to the archive first.
    /// The checksum for the header will be automatically updated via the
    /// `set_cksum` method after setting the path. No other metadata in the
    /// header will be modified.
    ///
    /// Then it will append the header, followed by contents of the stream
    /// specified by `data`. To produce a valid archive the `size` field of
    /// `header` must be the same as the length of the stream that's being
    /// written.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all entries have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Errors
    ///
    /// This function will return an error for any intermittent I/O error which
    /// occurs when either reading or writing.
    ///
    /// # Examples
    ///
    /// ```
    /// use tar::{Builder, Header};
    ///
    /// let mut header = Header::new_gnu();
    /// header.set_size(4);
    /// header.set_cksum();
    ///
    /// let mut data: &[u8] = &[1, 2, 3, 4];
    ///
    /// let mut ar = Builder::new(Vec::new());
    /// ar.append_data(&mut header, "really/long/path/to/foo", data).unwrap();
    /// let data = ar.into_inner().unwrap();
    /// ```
    pub fn append_data<P: AsRef<Path>, R: Read>(
        &mut self,
        header: &mut Header,
        path: P,
        data: R,
    ) -> io::Result<()> {
        write_header_longpath(path, header, self.get_mut())?;

        self.append(header, data)
    }

    /// Adds a file on the local filesystem to this archive.
    ///
    /// This function will open the file specified by `path` and insert the file
    /// into the archive with the appropriate metadata set, returning any I/O
    /// error which occurs while writing. The path name for the file inside of
    /// this archive will be the same as `path`, and it is required that the
    /// path is a relative path.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// ar.append_path("foo/bar.txt").unwrap();
    /// ```
    pub fn append_path<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
        let mode = self.mode.clone();

        FsEntry::from_fs(path, self.follow)?.write(self.get_mut(), mode)
    }

    /// Adds a file on the local filesystem to this archive under another name.
    ///
    /// This function will open the file specified by `path` and insert the file
    /// into the archive as `name` with appropriate metadata set, returning any
    /// I/O error which occurs while writing. The path name for the file inside
    /// of this archive will be `name` is required to be a relative path.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Note if the `path` is a directory. This will just add an entry to the archive,
    /// rather than contents of the directory.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Insert the local file "foo/bar.txt" in the archive but with the name
    /// // "bar/foo.txt".
    /// ar.append_path_with_name("foo/bar.txt", "bar/foo.txt").unwrap();
    /// ```
    pub fn append_path_with_name<P: AsRef<Path>, N: AsRef<Path>>(
        &mut self,
        path: P,
        name: N,
    ) -> io::Result<()> {
        let mode = self.mode.clone();

        FsEntry::from_fs(path, self.follow)?
            .set_path(name.as_ref().to_owned())
            .write(self.get_mut(), mode)
    }

    /// Adds a file to this archive with the given path as the name of the file
    /// in the archive.
    ///
    /// This will use the metadata of `file` to populate a `Header`, and it will
    /// then append the file to the archive with the name `path`.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Open the file at one location, but insert it into the archive with a
    /// // different name.
    /// let mut f = File::open("foo/bar/baz.txt").unwrap();
    /// ar.append_file("bar/baz.txt", &mut f).unwrap();
    /// ```
    pub fn append_file<P: AsRef<Path>>(&mut self, path: P, file: &mut fs::File) -> io::Result<()> {
        let mode = self.mode.clone();

        FsEntry {
            stat: file.metadata()?,
            // This path is only used in error messages and to read link-content, so
            // using the archive-path here has no impact
            fs_path: path.as_ref().to_owned(),
            path: path.as_ref().to_owned(),
            content: Some(file),
        }
        .write(self.get_mut(), mode)
    }

    /// Adds a directory to this archive with the given path as the name of the
    /// directory in the archive.
    ///
    /// This will use `stat` to populate a `Header`, and it will then append the
    /// directory to the archive with the name `path`.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Note this will not add the contents of the directory to the archive.
    /// See `append_dir_all` for recusively adding the contents of the directory.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fs;
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Use the directory at one location, but insert it into the archive
    /// // with a different name.
    /// ar.append_dir("bardir", ".").unwrap();
    /// ```
    pub fn append_dir<P, Q>(&mut self, path: P, src_path: Q) -> io::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let mode = self.mode.clone();

        FsEntry::from_fs(src_path, self.follow)?
            .set_path(path.as_ref().to_owned())
            .write(self.get_mut(), mode)
    }

    /// Adds a directory and all of its contents (recursively) to this archive
    /// with the given path as the name of the directory in the archive.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fs;
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Use the directory at one location, but insert it into the archive
    /// // with a different name.
    /// ar.append_dir_all("bardir", ".").unwrap();
    /// ```
    pub fn append_dir_all<P, Q>(&mut self, path: P, src_path: Q) -> io::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let mode = self.mode.clone();
        let follow = self.follow;
        append_dir_all(
            self.get_mut(),
            path.as_ref(),
            src_path.as_ref(),
            mode,
            follow,
        )
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

pub struct FsEntry<R> {
    stat: fs::Metadata,
    content: Option<R>,
    path: PathBuf,
    fs_path: PathBuf,
}

impl FsEntry<fs::File> {
    pub fn from_fs(path: impl AsRef<Path>, follow_symlinks: bool) -> io::Result<FsEntry<fs::File>> {
        let stat = if follow_symlinks {
            fs::metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "{} when getting metadata for {}",
                        err,
                        path.as_ref().display()
                    ),
                )
            })?
        } else {
            fs::symlink_metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "{} when getting metadata for {}",
                        err,
                        path.as_ref().display()
                    ),
                )
            })?
        };

        Ok(FsEntry {
            content: if stat.is_file() {
                Some(fs::File::open(&path)?)
            } else {
                None
            },
            path: path.as_ref().to_owned(),
            fs_path: path.as_ref().to_owned(),
            stat,
        })
    }
}

impl<R> FsEntry<R> {
    pub fn set_path(self, path: PathBuf) -> Self {
        FsEntry { path, ..self }
    }

    pub fn set_stat(self, stat: fs::Metadata) -> Self {
        FsEntry { stat, ..self }
    }

    pub fn set_content<Content: Read>(self, content: Content) -> FsEntry<Content> {
        FsEntry {
            content: Some(content),
            path: self.path,
            fs_path: self.fs_path,
            stat: self.stat,
        }
    }
}

impl<R: Read> FsEntry<R> {
    fn write_link_header(&self, mut header: Header, dst: &mut impl Write) -> io::Result<()> {
        let link_name = fs::read_link(&self.fs_path)?;

        if let Err(e) = header.set_link_name(&link_name) {
            let data = path2bytes(&link_name)?;

            // Since `e` isn't specific enough to let us know the path is indeed too
            // long, verify it first before using the extension.
            if data.len() < header.as_old().linkname.len() {
                return Err(e);
            }

            write_header(
                &mut prepare_longlink_header(data.len() as u64, EntryType::GNULongLink.as_byte()),
                dst,
            )?;
            // write null-terminated string
            write_content(&mut data.chain(io::repeat(0).take(1)), dst)?;
        }

        header.set_cksum();

        write_header(&mut header, dst)
    }

    #[cfg(unix)]
    fn write_special_header(&self, mut header: Header, dst: &mut impl Write) -> io::Result<()> {
        use ::std::os::unix::fs::{FileTypeExt, MetadataExt};

        let file_type = self.stat.file_type();

        if file_type.is_fifo() {
            header.set_entry_type(EntryType::Fifo);
        } else if file_type.is_char_device() {
            header.set_entry_type(EntryType::Char);
        } else if file_type.is_block_device() {
            header.set_entry_type(EntryType::Block);
        } else if file_type.is_socket() {
            return Err(other(&format!(
                "{}: socket can not be archived",
                self.fs_path.display()
            )));
        } else {
            return Err(other(&format!(
                "{} has unknown file type",
                self.fs_path.display()
            )));
        }

        let dev_id = self.stat.rdev();
        let dev_major = ((dev_id >> 32) & 0xffff_f000) | ((dev_id >> 8) & 0x0000_0fff);
        let dev_minor = ((dev_id >> 12) & 0xffff_ff00) | ((dev_id) & 0x0000_00ff);
        header.set_device_major(dev_major as u32)?;
        header.set_device_minor(dev_minor as u32)?;
        header.set_cksum();

        write_header(&mut header, dst)
    }

    pub fn write(self, dst: &mut impl Write, mode: HeaderMode) -> io::Result<()> {
        let mut header = Header::new_gnu();
        header.set_metadata_in_mode(&self.stat, mode);
        header.set_cksum();

        write_header_longpath(&self.path, &mut header, dst)?;

        if self.stat.is_file() || self.stat.is_dir() {
            write_header(&mut header, dst)?;
        } else if self.stat.file_type().is_symlink() {
            self.write_link_header(header, dst)?;
        } else {
            #[cfg(unix)]
            {
                self.write_special_header(header, dst)?;
            }
            #[cfg(not(unix))]
            {
                Err(other(&format!("{} has unknown file type", path.display())))
            }
        }

        if let Some(mut reader) = self.content {
            write_content(&mut reader, dst)?;
        }

        Ok(())
    }
}

fn write_header_longpath(
    path: impl AsRef<Path>,
    header: &mut Header,
    dst: &mut impl Write,
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

        write_header(
            &mut prepare_longlink_header(data.len() as u64, EntryType::GNULongName.as_byte()),
            dst,
        )?;
        // write null-terminated string
        write_content(&mut data.chain(io::repeat(0).take(1)), dst)?;

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

fn write_header(header: &Header, dst: &mut impl Write) -> io::Result<()> {
    dst.write_all(header.as_bytes())
}

fn write_content(src: &mut impl Read, dst: &mut impl Write) -> io::Result<()> {
    let len = io::copy(src, dst)?;

    // Pad with zeros if necessary.
    let buf = [0; 512];
    let remaining = 512 - (len % 512);
    if remaining < 512 {
        dst.write_all(&buf[..remaining as usize])?;
    }

    Ok(())
}

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

fn append_dir_all(
    dst: &mut impl Write,
    path: &Path,
    src_path: &Path,
    mode: HeaderMode,
    follow: bool,
) -> io::Result<()> {
    let mut stack = vec![(src_path.to_path_buf(), true, false)];
    while let Some((src, is_dir, is_symlink)) = stack.pop() {
        // In case of a symlink pointing to a directory, is_dir is false, but src.is_dir() will return true
        if is_dir || (is_symlink && follow && src.is_dir()) {
            for entry in fs::read_dir(&src)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                stack.push((entry.path(), file_type.is_dir(), file_type.is_symlink()));
            }
        }

        let dest = path.join(src.strip_prefix(&src_path).unwrap());
        if dest != Path::new("") {
            FsEntry::from_fs(src, follow)?
                .set_path(dest)
                .write(dst, mode)?;
        }
    }

    Ok(())
}

impl<W: Write> Drop for Builder<W> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
