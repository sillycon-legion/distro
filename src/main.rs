use std::{
    collections::HashMap,
    io::{Cursor, ErrorKind},
    path::{self, PathBuf},
    sync::Arc,
};

use async_stream::stream;
use axum::{
    Json, RequestPartsExt, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequestParts, Path, State},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{ErrorResponse, IntoResponse, Response},
    routing::{get, options, post},
};
use blake2::{Digest, digest::consts::U32};
use chrono::{DateTime, Utc};
use clap::Parser;
use color_eyre::eyre::{Context, Result, eyre};
use futures_core::Stream;
use nom::{
    Parser as _,
    bytes::complete::{tag, take, take_until},
    combinator::eof,
    multi::many0,
    sequence::delimited,
};
use positioned_io::RandomAccessFile;
use rc_zip_tokio::{ReadZip, rc_zip::parse::EntryKind};
use rocksdb::{
    BlockBasedOptions, Cache, CompactionPri, DB, DBCompressionType, FlushOptions, Options,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tracing::{error, warn};
use zerocopy::FromBytes;
use zip::{ZipWriter, write::SimpleFileOptions};

const PREFETCH_SIZE: usize = 16;

#[derive(Clone, Debug, Deserialize)]
struct Config {
    base_url: String,
    bind_addr: String,
    storage_dir: PathBuf,
    forks: HashMap<String, ForkConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct ForkConfig {
    publish_token: String,
    watchdogs: Vec<WatchdogConfig>,
    display_name: String,
    builds_page_link: String,
    builds_page_link_text: String,
}

#[derive(Clone, Debug, Deserialize)]
struct WatchdogConfig {
    url: String,
    instance: String,
    token: String,
}

#[derive(Parser, Debug)]
struct Args {
    /// Config file location
    config_file: PathBuf,
}

#[derive(Clone)]
struct AppState<'a> {
    base_url: &'a str,
    storage_dir: &'a path::Path,
    db: &'a DB,
    forks: &'a HashMap<String, ForkConfig>,
    http_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = toml::from_str(&tokio::fs::read_to_string(args.config_file).await?)?;

    let cache = Cache::new_lru_cache(128 << 20);
    let mut table_opts = BlockBasedOptions::default();
    table_opts.set_block_cache(&cache);
    table_opts.set_bloom_filter(10., false);
    table_opts.set_optimize_filters_for_memory(true);
    table_opts.set_block_size(16 * 1024);
    table_opts.set_cache_index_and_filter_blocks(true);
    table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    table_opts.set_format_version(7);
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_block_based_table_factory(&table_opts);
    opts.set_compression_type(DBCompressionType::Zstd);
    opts.set_compression_options(0, 3, 0, 16 << 10);
    opts.set_zstd_max_train_bytes(1600 << 10);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_compression_options(0, 3, 0, 16 << 10, true);
    opts.set_bottommost_zstd_max_train_bytes(1600 << 10, true);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_max_background_jobs(6);
    opts.set_bytes_per_sync(1048576);
    opts.set_compaction_pri(CompactionPri::MinOverlappingRatio);
    let db = DB::open(&opts, config.storage_dir.join("blobs"))?;

    let app = Router::new()
        .route("/fork/{fork}/manifest", get(fork_manifest))
        .route(
            "/fork/{fork}/version/{version}/manifest",
            get(version_manifest),
        )
        .route("/fork/{fork}/publish/start", post(fork_publish_start))
        .route("/fork/{fork}/publish/file", post(fork_publish_file))
        .route("/fork/{fork}/publish/finish", post(fork_publish_finish))
        .route(
            "/fork/{fork}/version/{version}/download",
            options(download_options),
        )
        .route(
            "/fork/{fork}/version/{version}/download",
            post(download_post),
        )
        .route(
            "/fork/{fork}/version/{version}/file/{file}",
            get(version_file),
        )
        .layer(DefaultBodyLimit::max(512 << 20))
        .with_state(AppState {
            base_url: config.base_url.leak(),
            storage_dir: Box::leak(Box::new(config.storage_dir)),
            db: Box::leak(Box::new(db)),
            forks: Box::leak(Box::new(config.forks)),
            http_client: reqwest::Client::new(),
        });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishStartRequest {
    version: String,
    engine_version: String,
}

async fn fork_publish_start(
    RequireAuthentication: RequireAuthentication,
    Path(fork): Path<String>,
    State(state): State<AppState<'_>>,
    Json(PublishStartRequest {
        version,
        engine_version,
    }): Json<PublishStartRequest>,
) -> Result<(), ErrorResponse> {
    if let Err(e) =
        tokio::fs::remove_dir_all(state.storage_dir.join("pending").join(&fork).join(&version))
            .await
    {
        if e.kind() != ErrorKind::NotFound {
            return Err(log_500(e));
        }
    }
    tokio::fs::create_dir_all(state.storage_dir.join("pending").join(&fork).join(&version))
        .await
        .map_err(log_500)?;
    tokio::fs::write(
        state
            .storage_dir
            .join("pending")
            .join(fork)
            .join(version)
            .join("started"),
        engine_version,
    )
    .await
    .map_err(log_500)?;
    Ok(())
}

async fn fork_publish_file(
    RequireAuthentication: RequireAuthentication,
    Path(fork): Path<String>,
    State(state): State<AppState<'_>>,
    headers: HeaderMap,
    body: Body,
) -> Result<(), ErrorResponse> {
    let Some(file) = headers
        .get("Robust-Cdn-Publish-File")
        .and_then(|v| v.to_str().ok())
    else {
        return Err((StatusCode::BAD_REQUEST).into());
    };
    let Some(version) = headers
        .get("Robust-Cdn-Publish-Version")
        .and_then(|v| v.to_str().ok())
    else {
        return Err((StatusCode::BAD_REQUEST).into());
    };
    let publish_inpr = tokio::fs::try_exists(
        state
            .storage_dir
            .join("pending")
            .join(&fork)
            .join(&version)
            .join("started"),
    )
    .await
    .map_err(log_500)?;
    if !publish_inpr {
        return Err((StatusCode::BAD_REQUEST).into());
    }
    let mut file = tokio::fs::File::create(
        state
            .storage_dir
            .join("pending")
            .join(fork)
            .join(version)
            .join(file),
    )
    .await
    .map_err(log_500)?;
    let mut body_stream = body.into_data_stream();
    while let Some(chunk) = body_stream.next().await {
        match chunk {
            Ok(chunk) => file.write_all(&chunk).await.map_err(log_500)?,
            Err(e) => return Err(log_500(e)),
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
struct PublishFinishRequest {
    version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct ForkManifest {
    builds: HashMap<String, ForkVersionManifest>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ForkVersionManifest {
    time: DateTime<Utc>,
    client: ForkDownloadManifest,
    server: HashMap<String, ForkDownloadManifest>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ForkDownloadManifest {
    url: String,
    sha256: String,
}

async fn fork_publish_finish(
    RequireAuthentication: RequireAuthentication,
    Path(fork): Path<String>,
    State(state): State<AppState<'static>>,
    Json(PublishFinishRequest { version }): Json<PublishFinishRequest>,
) -> Result<(), ErrorResponse> {
    let pending_dir = state.storage_dir.join("pending").join(&fork).join(&version);
    let result_dir = state
        .storage_dir
        .join("versions")
        .join(&fork)
        .join(&version);
    tokio::fs::create_dir_all(&result_dir)
        .await
        .map_err(log_500)?;
    let engine_version = tokio::fs::read_to_string(pending_dir.join("started")).await;
    let engine_version = match engine_version {
        Ok(resp) => resp,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err((StatusCode::BAD_REQUEST).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let mut has_client = false;
    let mut server_platforms = vec![];

    let mut files = tokio::fs::read_dir(&pending_dir).await.map_err(log_500)?;
    while let Some(entry) = files.next_entry().await.map_err(log_500)? {
        let filename = entry
            .file_name()
            .into_string()
            .map_err(|_| eyre!("File name is not UTF-8"))
            .map_err(log_500)?;
        if let Some(artifact) = filename.strip_suffix(".zip") {
            if let Some(platform) = artifact.strip_prefix("SS14.Server_") {
                server_platforms.push(platform.to_string());
            } else if artifact == "SS14.Client" {
                has_client = true;
            }
        }
    }
    if !has_client {
        tokio::fs::remove_dir_all(&pending_dir)
            .await
            .map_err(log_500)?;
        return Err((StatusCode::BAD_REQUEST).into());
    }
    let client =
        Arc::new(RandomAccessFile::open(pending_dir.join("SS14.Client.zip")).map_err(log_500)?);
    let client_zip = client
        .read_zip()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut client_manifest = "Robust Content Manifest 1\n".to_string();
    for file in client_zip.entries() {
        if file.kind() != EntryKind::File {
            continue;
        }
        let buf = file.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        let hash = blake2::Blake2b::<U32>::digest(&buf);
        client_manifest += &format!("{} {}\n", hex::encode_upper(hash), file.name);
        tokio::task::spawn_blocking(move || state.db.put(hash, buf))
            .await
            .map_err(log_500)?
            .map_err(log_500)?;
    }
    tokio::task::spawn_blocking(move || state.db.flush())
        .await
        .map_err(log_500)?
        .map_err(log_500)?;
    let manifest_hash = blake2::Blake2b::<U32>::digest(client_manifest.as_bytes());
    let mut client = tokio::fs::File::open(pending_dir.join("SS14.Client.zip"))
        .await
        .map_err(log_500)?;
    let mut client_zip_hasher = sha2::Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = client.read(&mut buf).await.map_err(log_500)?;
        if read == 0 {
            break;
        }
        client_zip_hasher.update(&buf[0..read]);
    }
    let client_zip_hash = client_zip_hasher.finalize();
    tokio::fs::write(result_dir.join("manifest"), client_manifest)
        .await
        .map_err(log_500)?;
    tokio::fs::rename(
        pending_dir.join("SS14.Client.zip"),
        result_dir.join("SS14.Client.zip"),
    )
    .await
    .map_err(log_500)?;
    let build_json = serde_json::json!({
        "download": format!("{}/fork/{{FORK_ID}}/version/{{FORK_VERSION}}/file/SS14.Client.zip", state.base_url),
        "version": &version,
        "hash": hex::encode_upper(client_zip_hash),
        "fork_id": &fork,
        "engine_version": engine_version,
        "manifest_url": format!("{}/fork/{{FORK_ID}}/version/{{FORK_VERSION}}/manifest", state.base_url),
        "manifest_download_url": format!("{}/fork/{{FORK_ID}}/version/{{FORK_VERSION}}/download", state.base_url),
        "manifest_hash": hex::encode_upper(manifest_hash),
    });
    let mut servers = HashMap::new();
    for platform in server_platforms {
        let server = tokio::fs::read(pending_dir.join(format!("SS14.Server_{platform}.zip")))
            .await
            .map_err(log_500)?;
        let mut server =
            ZipWriter::new_append(Cursor::new(server)).map_err(|_| StatusCode::BAD_REQUEST)?;
        server
            .start_file("build.json", SimpleFileOptions::default())
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        serde_json::to_writer(&mut server, &build_json).map_err(|_| StatusCode::BAD_REQUEST)?;
        let server = server
            .finish()
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .into_inner();
        servers.insert(
            platform.clone(),
            ForkDownloadManifest {
                url: format!(
                    "{}/{fork}/version/{version}/SS14.Server_{platform}.zip",
                    state.base_url
                ),
                sha256: hex::encode_upper(sha2::Sha256::digest(&server)),
            },
        );
        tokio::fs::write(
            result_dir.join(format!("SS14.Server_{platform}.zip")),
            server,
        )
        .await
        .map_err(log_500)?;
    }
    tokio::fs::remove_dir_all(pending_dir)
        .await
        .map_err(log_500)?;

    tokio::fs::create_dir_all(state.storage_dir.join("manifests"))
        .await
        .map_err(log_500)?;
    let manifest = tokio::fs::read(state.storage_dir.join("manifests").join(&fork)).await;
    let mut manifest: ForkManifest = match manifest {
        Ok(manifest) => serde_json::from_slice(&manifest).map_err(log_500)?,
        Err(e) if e.kind() == ErrorKind::NotFound => Default::default(),
        Err(e) => return Err(log_500(e)),
    };
    manifest.builds.insert(
        version.clone(),
        ForkVersionManifest {
            time: Utc::now(),
            client: ForkDownloadManifest {
                url: format!(
                    "{}/{fork}/version/{version}/file/SS14.Client.zip",
                    state.base_url
                ),
                sha256: hex::encode_upper(client_zip_hash),
            },
            server: servers,
        },
    );
    tokio::fs::write(
        state.storage_dir.join("manifests").join(&fork),
        serde_json::to_string(&manifest).map_err(log_500)?,
    )
    .await
    .map_err(log_500)?;
    for watchdog in &state.forks[&fork].watchdogs {
        if let Err(e) = state
            .http_client
            .post(format!(
                "{}/instances/{}/update",
                watchdog.url, watchdog.instance
            ))
            .basic_auth(&watchdog.instance, Some(&watchdog.token))
            .send()
            .await
            .map(reqwest::Response::error_for_status)
        {
            warn!("Failed to notify watchdog: {e:?}");
        };
    }
    Ok(())
}

async fn fork_manifest(
    Path(fork): Path<String>,
    State(state): State<AppState<'_>>,
) -> Result<([(&'static str, &'static str); 1], Vec<u8>), ErrorResponse> {
    let manifest = tokio::fs::read(state.storage_dir.join("manifests").join(&fork)).await;
    let manifest = match manifest {
        Ok(manifest) => manifest,
        Err(e) if e.kind() == ErrorKind::NotFound => return Err((StatusCode::NOT_FOUND).into()),
        Err(e) => return Err(log_500(e)),
    };
    Ok(([("Content-Type", "application/json")], manifest))
}

async fn version_manifest(
    Path((fork, version)): Path<(String, String)>,
    State(state): State<AppState<'_>>,
) -> Result<([(&'static str, &'static str); 1], Vec<u8>), ErrorResponse> {
    let body = tokio::fs::read(
        state
            .storage_dir
            .join("versions")
            .join(&fork)
            .join(&version)
            .join("manifest"),
    )
    .await;
    let body = match body {
        Ok(body) => body,
        Err(e) if e.kind() == ErrorKind::NotFound => return Err((StatusCode::NOT_FOUND).into()),
        Err(e) => return Err(log_500(e)),
    };
    Ok(([("Content-Type", "text/plain")], body))
}

async fn version_file(
    Path((fork, version, file)): Path<(String, String, String)>,
    State(state): State<AppState<'_>>,
) -> Result<Vec<u8>, ErrorResponse> {
    let body = tokio::fs::read(
        state
            .storage_dir
            .join("versions")
            .join(&fork)
            .join(&version)
            .join(&file),
    )
    .await;
    let body = match body {
        Ok(body) => body,
        Err(e) if e.kind() == ErrorKind::NotFound => return Err((StatusCode::NOT_FOUND).into()),
        Err(e) => return Err(log_500(e)),
    };
    Ok(body)
}

async fn download_options() -> [(&'static str, &'static str); 2] {
    [
        ("X-Robust-Download-Min-Protocol", "1"),
        ("X-Robust-Download-Max-Protocol", "1"),
    ]
}

async fn download_post(
    Path((fork, version)): Path<(String, String)>,
    State(state): State<AppState<'static>>,
    body: Bytes,
) -> Result<Body, ErrorResponse> {
    let Ok(body) = <[[u8; 4]]>::ref_from_bytes(&body) else {
        return Err((StatusCode::BAD_REQUEST).into());
    };

    let resp = tokio::fs::read(
        state
            .storage_dir
            .join("versions")
            .join(&fork)
            .join(&version)
            .join("manifest"),
    )
    .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err((StatusCode::NOT_FOUND).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let manifest = str::from_utf8(&resp).map_err(log_500)?;
    let manifest = parse_manifest(manifest).map_err(log_500)?;
    let Some(blobs) = body
        .iter()
        .copied()
        .map(u32::from_le_bytes)
        .map(|e| manifest.0.get(e as usize).map(|e| e.0))
        .collect::<Option<Vec<[u8; 32]>>>()
    else {
        return Err((StatusCode::BAD_REQUEST).into());
    };
    Ok(Body::from_stream(get_blobs(state, blobs)))
}

fn get_blobs(state: AppState<'static>, blobs: Vec<[u8; 32]>) -> impl Stream<Item = Result<Bytes>> {
    stream! {
        let blobs = blobs.chunks(PREFETCH_SIZE);
        yield Ok(Bytes::from_static(&[0, 0, 0, 0]));
        for blobchunk in blobs {
            let blobchunk = blobchunk.to_vec();
            let resps = tokio::task::spawn_blocking(move || state.db.multi_get(blobchunk)).await?;
            for resp in resps {
                let resp = resp.unwrap_or_default().unwrap_or_default();
                yield Ok(Bytes::from((resp.len() as u32).to_le_bytes().to_vec()));
                yield Ok(Bytes::from(resp));
            }
        }
    }
}

struct ContentManifest<'a>(Vec<ContentManifestEntry<'a>>);
struct ContentManifestEntry<'a>([u8; 32], &'a str);

fn from_hex<const LEN: usize>(hex: &str) -> Result<[u8; LEN]> {
    let mut buf = [0u8; LEN];
    hex::decode_to_slice(hex, &mut buf)?;
    Ok(buf)
}

fn parse_manifest(input: &str) -> Result<ContentManifest> {
    let (_, manifest) = delimited::<_, _, nom::error::Error<&str>, _, _, _>(
        tag("Robust Content Manifest 1\n"),
        many0(
            (
                take(64usize).map_res(from_hex::<32>),
                delimited(tag(" "), take_until("\n"), tag("\n")),
            )
                .map(|(hash, path)| ContentManifestEntry(hash, path)),
        ),
        eof,
    )
    .parse_complete(input)
    .map_err(|e| e.to_owned())
    .context("Failed to parse manifest")?;
    Ok(ContentManifest(manifest))
}

struct RequireAuthentication;

impl FromRequestParts<AppState<'_>> for RequireAuthentication {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState<'_>,
    ) -> Result<Self, Self::Rejection> {
        let Path(fork) = parts
            .extract::<Path<String>>()
            .await
            .map_err(|err| err.into_response())?;
        let Some(fork) = state.forks.get(&fork) else {
            return Err(StatusCode::NOT_FOUND.into_response());
        };
        let Some(token) = parts.headers.get("Authorization") else {
            return Err(StatusCode::UNAUTHORIZED.into_response());
        };
        let target = format!("Bearer {}", fork.publish_token);
        if !constant_time_eq::constant_time_eq(target.as_bytes(), token.as_bytes()) {
            return Err(StatusCode::UNAUTHORIZED.into_response());
        }
        Ok(Self)
    }
}

fn log_500(e: impl Into<color_eyre::eyre::Report>) -> ErrorResponse {
    error!("Error handling request: {:?}", e.into());
    StatusCode::INTERNAL_SERVER_ERROR.into()
}
