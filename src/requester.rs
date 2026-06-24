use anyhow::Result;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::{
    HashAndFormat, api::remote::GetProgressItem, format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
};
use n0_future::StreamExt;
use tokio::select;

use crate::store::KeithStore;

pub async fn receive(
    endpoint: &Endpoint,
    hash_and_format: HashAndFormat,
    target_addr: EndpointAddr,
    store: &KeithStore,
) -> Result<()> {
    let download_future = async {
        println!("Downloading blob...");
        let local = store.db.remote().local(hash_and_format).await?;
        if !local.is_complete() {
            let connection = endpoint
                .connect(target_addr, iroh_blobs::protocol::ALPN)
                .await?;

            println!("Made blob connection back to sender");

            get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                .await?;

            let get = store.db.remote().execute_get(connection, local.missing());
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

        let collection = Collection::load(hash_and_format.hash, store.db.as_ref()).await?;

        if let Some((name, _)) = collection.iter().next()
            && let Some(first) = name.split('/').next()
        {
            println!("Exporting to {first}...");
        }
        store.export(collection).await?;
        println!("Done.");

        Ok(())
    };

    select! {
        x = download_future => match x {
            Ok(_) => {
                // endpoint.close().await;
                // tokio::fs::remove_dir_all(&store.tmp_dir).await?;
            }
            Err(e) => {
                // endpoint.close().await;
                store.db.shutdown().await?;
                eprintln!("Error: {e}");
                tokio::fs::remove_dir_all(&store.tmp_dir).await?;
                std::process::exit(1);
            }
        },
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down.");
            // endpoint.close().await;
            store.db.shutdown().await?;
            tokio::fs::remove_dir_all(&store.tmp_dir).await?;
            std::process::exit(130);
        }
    };

    Ok(())
}
