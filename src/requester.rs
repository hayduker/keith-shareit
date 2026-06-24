use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, endpoint::presets};
use iroh_blobs::{
    Hash, HashAndFormat,
    api::{
        Store,
        blobs::{ExportMode, ExportOptions},
        remote::GetProgressItem,
    },
    format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::StreamExt;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio::select;

use crate::secret::get_or_create_secret;

pub async fn receive(
    endpoint: &Endpoint,
    hash_and_format: HashAndFormat,
    target_addr: EndpointAddr,
) -> Result<()> {
    let store_dir = format!(".recv-{}", hash_and_format.hash.to_hex());
    let store_dir = std::env::current_dir()?.join(store_dir);
    let db = FsStore::load(&store_dir).await?;

    let download_future = async {
        println!("Downloading blob...");
        let local = db.remote().local(hash_and_format).await?;
        if !local.is_complete() {
            let connection = endpoint
                .connect(target_addr, iroh_blobs::protocol::ALPN)
                .await?;

            println!("Made blob connection back to sender");

            get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                .await?;

            let get = db.remote().execute_get(connection, local.missing());
            let mut stream = get.stream();

            while let Some(item) = stream.next().await {
                match item {
                    GetProgressItem::Done(_) => {
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!("iroh get error {:?}", cause);
                    }
                    _ => (),
                }
            }
        };

        let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;

        if let Some((name, _)) = collection.iter().next()
            && let Some(first) = name.split('/').next()
        {
            println!("Exporting to {first}...");
        }
        export(&db, collection).await?;
        println!("Done.");

        Ok(())
    };

    select! {
        x = download_future => match x {
            Ok(_) => {
                // endpoint.close().await;
                tokio::fs::remove_dir_all(store_dir).await?;
            }
            Err(e) => {
                // endpoint.close().await;
                db.shutdown().await?;
                eprintln!("Error: {e}");
                tokio::fs::remove_dir_all(store_dir).await?;
                std::process::exit(1);
            }
        },
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down.");
            // endpoint.close().await;
            db.shutdown().await?;
            tokio::fs::remove_dir_all(store_dir).await?;
            std::process::exit(130);
        }
    };

    Ok(())
}

pub async fn receive_legacy(ticket: BlobTicket) -> Result<()> {
    let secret_key = get_or_create_secret()?;
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![])
        .secret_key(secret_key)
        .bind()
        .await?;

    println!("Endpoint id: {}", endpoint.id());
    println!("Endpoint addr: {:?}", endpoint.addr());

    let store_dir = format!(".recv-{}", ticket.hash().to_hex());
    let store_dir = std::env::current_dir()?.join(store_dir);
    let db = FsStore::load(&store_dir).await?;

    let download_future = async {
        println!("Downloading blob...");
        let hash_and_format = ticket.hash_and_format();
        let local = db.remote().local(hash_and_format).await?;
        if !local.is_complete() {
            let connection = endpoint
                .connect(ticket.addr().clone(), iroh_blobs::protocol::ALPN)
                .await?;

            get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                .await?;

            let get = db.remote().execute_get(connection, local.missing());
            let mut stream = get.stream();

            while let Some(item) = stream.next().await {
                match item {
                    GetProgressItem::Done(_) => {
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!("iroh get error {:?}", cause);
                    }
                    _ => (),
                }
            }
        };

        let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;

        if let Some((name, _)) = collection.iter().next()
            && let Some(first) = name.split('/').next()
        {
            println!("Exporting to {first}...");
        }
        export(&db, collection).await?;
        println!("Done.");

        Ok(())
    };

    select! {
        x = download_future => match x {
            Ok(_) => {
                endpoint.close().await;
                tokio::fs::remove_dir_all(store_dir).await?;
            }
            Err(e) => {
                endpoint.close().await;
                db.shutdown().await?;
                eprintln!("Error: {e}");
                tokio::fs::remove_dir_all(store_dir).await?;
                std::process::exit(1);
            }
        },
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down.");
            endpoint.close().await;
            db.shutdown().await?;
            tokio::fs::remove_dir_all(store_dir).await?;
            std::process::exit(130);
        }
    };

    Ok(())
}

async fn export(db: &Store, collection: Collection) -> Result<()> {
    let root = std::env::current_dir()?;

    for (name, hash) in collection.iter() {
        let target = get_export_path(&root, name)?;
        if target.exists() {
            if target.is_dir() {
                println!(
                    "Removing existing directory at export location: {:?}",
                    target
                );
                fs::remove_dir_all(target.clone())?;
            } else if target.is_file() {
                println!("Removing existing file at export location: {:?}", target);
                fs::remove_file(target.clone())?;
            }
        }
        let _ = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::Copy,
            })
            .await?;
    }
    Ok(())
}

fn get_export_path(root: &Path, name: &str) -> Result<PathBuf> {
    let parts = name.split('/');
    let mut path = root.to_path_buf();
    for part in parts {
        validate_path_component(part)?;
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
