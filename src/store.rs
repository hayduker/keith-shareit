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
use std::{
    ffi::OsStr,
    path::{Component, Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Clone)]
pub struct KeithStore {
    pub tmp_dir: PathBuf,
    pub db: FsStore,
}

impl KeithStore {
    pub async fn new() -> Result<Self> {
        let tmp_dir = Self::create_tmp_store_dir().await?;
        let db = FsStore::load(&tmp_dir).await?;
        Ok(Self { tmp_dir, db })
    }

    /// Import from a file or directory into the database.
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

    fn get_export_path(root: &Path, name: &str) -> Result<PathBuf> {
        let parts = name.split('/');
        let mut path = root.to_path_buf();
        for part in parts {
            Self::validate_path_component(part)?;
            path.push(part);
        }
        Ok(path)
    }

    fn validate_path_component(component: &str) -> Result<()> {
        anyhow::ensure!(
            !component.contains('/'),
            "path components must not contain the only correct path separator, /"
        );
        Ok(())
    }
}

impl Drop for KeithStore {
    fn drop(&mut self) {
        if std::fs::remove_dir_all(self.tmp_dir.clone()).is_err() {
            eprintln!("Failed to delete dir {:?}", self.tmp_dir);
        }
    }
}
