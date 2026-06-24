use anyhow::{Context, Result};
use data_encoding::HEXLOWER;
use futures_buffered::BufferedStreamExt;
use iroh::{Endpoint, endpoint::presets};
use iroh_blobs::{
    BlobFormat, BlobsProtocol,
    api::{
        Store, TempTag,
        blobs::{AddPathOptions, AddProgressItem, ImportMode},
    },
    format::collection::Collection,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::StreamExt;
use rand::RngExt;
use std::{
    path::{Component, Path, PathBuf},
    time::Duration,
};
use walkdir::WalkDir;

use crate::secret::get_or_create_secret;

pub async fn create_store(path: &PathBuf) -> Result<(FsStore, PathBuf)> {
    let store_dir = create_tmp_send_dir(&path).await?;
    let store = FsStore::load(&store_dir).await?;
    Ok((store, store_dir))
}

pub async fn send(path: PathBuf) -> Result<()> {
    // set up store and endpoint
    let secret_key = get_or_create_secret()?;
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .secret_key(secret_key)
        .bind()
        .await?;

    println!("Endpoint id: {}", endpoint.id());
    println!("Endpoint addr: {:?}", endpoint.addr());

    let (store, store_dir) = create_store(&path).await?;

    println!("Importing {}...", path.display());
    let blobs = BlobsProtocol::new(&store, None);
    let tag = import(path.clone(), blobs.store()).await?;
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    println!("Bringing up endpoint...");
    tokio::time::timeout(Duration::from_secs(30), async {
        router.endpoint().online().await;
    })
    .await?;

    let addr = router.endpoint().addr();
    let ticket = BlobTicket::new(addr, tag.hash(), BlobFormat::HashSeq);
    println!("Blob imported, to receive use: {ticket}");

    // wait until interrupt
    tokio::signal::ctrl_c().await?;
    println!("\nShutting down.");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    tokio::fs::remove_dir_all(store_dir).await?;

    Ok(())
}

/// Import from a file or directory into the database.
pub async fn import(path: PathBuf, db: &Store) -> Result<TempTag> {
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
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;

    // Import all the files, using num_cpus workers, return names and temp tags
    let mut names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            async move {
                let import = db.add_path_with_opts(AddPathOptions {
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

    let root_hash = collection.clone().store(db).await?;

    // now that the collection is stored, we can drop the tags
    // data is protected by the collection
    drop(tags);

    Ok(root_hash)
}

/// This function converts an already canonicalized path to a string.
///
/// If `must_be_relative` is true, the function will fail if any component of the path is
/// `Component::RootDir`
///
/// This function will also fail if the path is non canonical, i.e. contains
/// `..` or `.`, or if the path components contain any windows or unix path
/// separators.
fn canonicalized_path_to_string(path: impl AsRef<Path>, must_be_relative: bool) -> Result<String> {
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

async fn create_tmp_send_dir(path: &PathBuf) -> Result<PathBuf> {
    let suffix = rand::rng().random::<[u8; 16]>();
    let cwd = std::env::current_dir()?;
    let dir = cwd.join(format!(".send-{}", HEXLOWER.encode(&suffix)));

    anyhow::ensure!(
        !dir.exists(),
        "can not share twice from the same directory: {}",
        cwd.display()
    );

    anyhow::ensure!(
        cwd.join(path) != cwd,
        "can not share from the current directory"
    );

    tokio::fs::create_dir_all(&dir).await?;

    Ok(dir)
}
