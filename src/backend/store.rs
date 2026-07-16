//! This module provides the `KeithStore` struct, which manages the storage and transfer
//! of blobs (files and collections) using the iroh-blobs crate. It handles importing
//! local files into the blob store, exporting blobs to the local filesystem, and managing
//! temporary storage directories.

use anyhow::{Context, Result};
use data_encoding::HEXLOWER;
use futures_buffered::BufferedStreamExt;
use iroh_blobs::{
    BlobFormat,
    api::{
        TempTag,
        blobs::{AddPathOptions, AddProgressItem, ExportMode, ExportOptions, ImportMode},
    },
    format::collection::Collection,
    store::fs::FsStore,
};
use n0_future::StreamExt;
use rand::RngExt;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

/// A wrapper around `iroh_blobs::store::fs::FsStore` that manages a temporary directory
/// for blob storage and provides high-level methods for importing and exporting files.
#[derive(Clone)]
pub struct KeithStore {
    /// The path to the temporary directory where the `FsStore` is initialized.
    pub tmp_dir: PathBuf,
    /// The underlying Iroh [`FsStore`] instance.
    pub db: FsStore,
}

impl KeithStore {
    /// Creates a new `KeithStore` instance.
    ///
    /// This function creates a temporary directory for the underlying `FsStore` and initializes it.
    /// The temporary directory will be automatically removed when the `KeithStore` instance is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `KeithStore` instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary directory cannot be created or if the `FsStore` fails to load.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use keith_shareit::backend::store::KeithStore;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// let store = KeithStore::new().await?;
    /// println!("Store temporary directory: {:?}", store.tmp_dir);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new() -> Result<Self> {
        let tmp_dir = Self::create_tmp_store_dir().await?;
        let db = FsStore::load(&tmp_dir).await?;
        Ok(Self { tmp_dir, db })
    }

    /// Imports a file or directory into the Iroh blob store.
    ///
    /// This function walks the provided path, adds all files as blobs to the store,
    /// and then creates a [`Collection`] representing the imported content. The collection
    /// hash is returned as a [`TempTag`].
    ///
    /// # Arguments
    ///
    /// * `self` - A reference to the `KeithStore` instance.
    /// * `path` - The path to the file or directory to import.
    ///
    /// # Returns
    ///
    /// A `Result` containing a [`TempTag`] representing the root hash of the imported collection.
    ///
    /// # Errors
    ///
    /// - [`anyhow::Error`] if the provided `path` does not exist.
    /// - [`anyhow::Error`] if there are issues canonicalizing the path or getting its parent directory.
    /// - [`anyhow::Error`] if walking the directory fails.
    /// - [`anyhow::Error`] if importing a file into the blob store fails.
    /// - [`anyhow::Error`] if storing the collection fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use keith_shareit::backend::store::KeithStore;
    /// # use std::path::PathBuf;
    /// # use tokio::fs;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let store = KeithStore::new().await?;
    /// // Create a dummy file for import
    /// let test_dir = store.tmp_dir.join("test_import_dir");
    /// fs::create_dir_all(&test_dir).await?;
    /// let file_path = test_dir.join("test_file.txt");
    /// fs::write(&file_path, b"Hello Iroh!").await?;
    ///
    /// let tag = store.import(file_path.clone()).await?;
    /// println!("Imported file under tag: {:?}", tag);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import(self: &KeithStore, path: PathBuf) -> Result<TempTag> {
        let path = path.canonicalize()?;
        anyhow::ensure!(path.exists(), "path {} does not exist", path.display());

        let root = path.parent().context("couldn't get parent of path")?;

        // WalkDir also works for files, so we don't need to special case them
        let files = WalkDir::new(path.clone()).into_iter();

        // Flatten the directory structure into a list of (name, path) pairs.
        let data_sources: Vec<(String, PathBuf)> = files
            .map(|entry| {
                let entry = entry?;
                if !entry.file_type().is_file() {
                    // Skip symlinks. Directories are handled by WalkDir.
                    return Ok(None);
                }
                let path = entry.into_path();
                let relative = path.strip_prefix(root)?;
                let name = Self::canonicalized_path_to_string(relative, true)?;
                anyhow::Ok(Some((name, path)))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>>>()?;

        // Import all the files, using num_cpus workers, return names and temp tags
        let mut names_and_tags = n0_future::stream::iter(data_sources)
            .map(|(name, path)| {
                // let db = db.clone();
                async move {
                    let import = self.db.add_path_with_opts(AddPathOptions {
                        path,
                        mode: ImportMode::TryReference,
                        format: BlobFormat::Raw,
                    });
                    let mut stream = import.stream().await;
                    let temp_tag = loop {
                        let item = stream
                            .next()
                            .await
                            .context("import stream ended without a tag")?;
                        match item {
                            AddProgressItem::Error(cause) => {
                                anyhow::bail!("error importing {}: {}", name, cause);
                            }
                            AddProgressItem::Done(tag) => {
                                break tag;
                            }
                            _ => (),
                        }
                    };
                    anyhow::Ok((name, temp_tag))
                }
            })
            .buffered_unordered(num_cpus::get())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        names_and_tags.sort_by(|(a, _), (b, _)| a.cmp(b));

        // collect the (name, hash) tuples into a collection
        // we must also keep the tags around so the data does not get gced.
        let (collection, tags) = names_and_tags
            .into_iter()
            .map(|(name, tag)| ((name, tag.hash()), tag))
            .unzip::<_, _, Collection, Vec<_>>();

        let root_hash = collection.clone().store(&self.db).await?;

        // now that the collection is stored, we can drop the tags
        // data is protected by the collection
        drop(tags);

        Ok(root_hash)
    }

    /// Exports a [`Collection`] of blobs to the local filesystem.
    ///
    /// This function iterates through the blobs in the provided `collection`, exports each
    /// blob to the specified `dst_dir`, and reconstructs the original file structure.
    /// Existing files at the target path will be removed before export.
    ///
    /// # Arguments
    ///
    /// * `self` - A reference to the `KeithStore` instance.
    /// * `collection` - The [`Collection`] to export.
    /// * `relative_path` - The relative path within the `dst_dir` where the collection should be exported.
    /// * `dst_dir` - The base destination directory for the export.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the success or failure of the export process.
    ///
    /// # Errors
    ///
    /// - [`anyhow::Error`] if the full destination path has no parent directory.
    /// - [`std::io::Error`] if removing existing files fails.
    /// - [`anyhow::Error`] if exporting a blob fails.
    /// - [`anyhow::Error`] propagated from `get_export_path`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use keith_shareit::backend::store::KeithStore;
    /// # use std::path::PathBuf;
    /// # use iroh_blobs::format::collection::Collection;
    /// # use iroh_blobs::Hash;
    /// # use tokio::fs;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let store = KeithStore::new().await?;
    /// // Create a dummy collection and a dummy file in the store
    /// let test_file_path = store.tmp_dir.join("test_export_file.txt");
    /// fs::write(&test_file_path, b"Exportable content").await?;
    /// let tag = store.db.add_path(test_file_path.clone()).await?.pop().unwrap();
    /// let mut collection = Collection::new();
    /// collection.insert("exported_file.txt".to_string(), tag.hash());
    /// collection.store(&store.db).await?;
    ///
    /// let export_to_dir = store.tmp_dir.join("exported_content");
    /// fs::create_dir_all(&export_to_dir).await?;
    ///
    /// store.export(collection, PathBuf::from("sub_dir"), export_to_dir.clone()).await?;
    /// println!("Exported collection to: {:?}", export_to_dir.join("sub_dir/exported_file.txt"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn export(
        self: &KeithStore,
        collection: Collection,
        relative_path: PathBuf,
        dst_dir: PathBuf,
    ) -> Result<()> {
        // let cwd = std::env::current_dir()?;
        let dst_path = dst_dir.join(relative_path);
        let Some(parent) = dst_path.parent() else {
            anyhow::bail!("Full destination path has no parent: {:?}", dst_path);
        };

        let mut removed_existing = false;
        for (name, hash) in collection.iter() {
            let target = Self::get_export_path(parent, name)?;
            if target.exists() {
                if !removed_existing {
                    println!("Removing existing file(s)");
                    removed_existing = true;
                }
                tokio::fs::remove_file(target.clone()).await?;
            }
            let _ = self
                .db
                .export_with_opts(ExportOptions {
                    hash: *hash,
                    target,
                    mode: ExportMode::Copy,
                })
                .await?;
        }
        Ok(())
    }

    /// This function converts an already canonicalized path to a string.
    ///
    /// If `must_be_relative` is true, the function will fail if any component of the path is
    /// `Component::RootDir`
    ///
    /// This function will also fail if the path is non canonical, i.e. contains
    /// `..` or `.`, or if the path components contain any windows or unix path
    /// separators.
    fn canonicalized_path_to_string(
        path: impl AsRef<Path>,
        must_be_relative: bool,
    ) -> Result<String> {
        let mut path_str = String::new();
        let parts = path
            .as_ref()
            .components()
            .filter_map(|c| match c {
                Component::Normal(x) => {
                    let c = match x.to_str() {
                        Some(c) => c,
                        None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                    };

                    if !c.contains('/') && !c.contains('\\') {
                        Some(Ok(c))
                    } else {
                        Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                    }
                }
                Component::RootDir => {
                    if must_be_relative {
                        Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                    } else {
                        path_str.push('/');
                        None
                    }
                }
                _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
            })
            .collect::<Result<Vec<_>>>()?;
        let parts = parts.join("/");
        path_str.push_str(&parts);
        Ok(path_str)
    }

    /// Creates a unique temporary directory for the `KeithStore`.
    ///
    /// The directory is named `.keith-shareit-<random_hex_suffix>` within the current working directory.
    /// It ensures that the directory does not already exist to prevent conflicts.
    ///
    /// # Returns
    ///
    /// A `Result` containing the path to the newly created temporary directory.
    ///
    /// # Errors
    ///
    /// - [`anyhow::Error`] if a directory with the generated name already exists (indicating a conflict).
    /// - [`std::io::Error`] if creating the directory fails.
    async fn create_tmp_store_dir() -> Result<PathBuf> {
        let suffix = rand::rng().random::<[u8; 16]>();
        let cwd = std::env::current_dir()?;
        let dir = cwd.join(format!(".keith-shareit-{}", HEXLOWER.encode(&suffix)));

        anyhow::ensure!(
            !dir.exists(),
            "can not share twice from the same directory: {}",
            cwd.display()
        );

        tokio::fs::create_dir_all(&dir).await?;

        Ok(dir)
    }

    /// Constructs the full export path for a given file name within a root directory.
    ///
    /// This function takes a root path and a file name (potentially with subdirectories)
    /// and safely constructs a `PathBuf`, validating each component to prevent path traversal issues.
    ///
    /// # Arguments
    ///
    /// * `root` - The base directory where the file will be exported.
    /// * `name` - The name of the file, which may include `/` for subdirectories (e.g., "subdir/file.txt").
    ///
    /// # Returns
    ///
    /// A `Result` containing the full `PathBuf` for the export target.
    ///
    /// # Errors
    ///
    /// - [`anyhow::Error`] if any path component in `name` is invalid (e.g., contains `/` or `\`).
    fn get_export_path(root: &Path, name: &str) -> Result<PathBuf> {
        let parts = name.split('/');
        let mut path = root.to_path_buf();
        for part in parts {
            Self::validate_path_component(part)?;
            path.push(part);
        }
        Ok(path)
    }

    /// Validates a single path component to ensure it does not contain invalid characters.
    ///
    /// Specifically, it checks that the component does not contain the path separator `/`.
    ///
    /// # Arguments
    ///
    /// * `component` - The path component to validate.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success if the component is valid, or an error otherwise.
    ///
    /// # Errors
    ///
    /// - [`anyhow::Error`] if the component contains the `/` character.
    fn validate_path_component(component: &str) -> Result<()> {
        anyhow::ensure!(
            !component.contains('/'),
            "path components must not contain the only correct path separator, /"
        );
        Ok(())
    }
}

impl Drop for KeithStore {
    /// Cleans up the temporary directory created by the `KeithStore` when it goes out of scope.
    ///
    /// If the temporary directory cannot be removed, an error message is printed to `stderr`.
    fn drop(&mut self) {
        if std::fs::remove_dir_all(self.tmp_dir.clone()).is_err() {
            eprintln!("Failed to delete dir {:?}", self.tmp_dir);
        }
    }
}
