use std::{
    collections::HashMap,
    convert::Infallible,
    io::{Cursor, Read},
    path::PathBuf,
};

use async_stream::stream;
use aws_config::Region;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Credentials, SharedCredentialsProvider},
    error::SdkError,
    operation::get_object::GetObjectError,
};
use axum::{
    body::{Body, Bytes}, extract::{DefaultBodyLimit, FromRequestParts, Path, State}, http::{request::Parts, HeaderMap, StatusCode}, response::{ErrorResponse, IntoResponse, Response}, routing::{get, options, post}, Json, RequestPartsExt, Router
};
use blake2::{Digest, digest::consts::U32};
use clap::Parser;
use color_eyre::eyre::{self, Context, Result};
use futures_core::Stream;
use nom::{
    Parser as _,
    bytes::complete::{tag, take, take_until},
    combinator::eof,
    multi::many0,
    sequence::delimited,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{error, warn};
use zerocopy::FromBytes;
use zip::{ZipWriter, write::SimpleFileOptions};

const PREFETCH_SIZE: usize = 16;

#[derive(Clone, Debug, Deserialize)]
struct Config {
    base_url: String,
    bind_addr: String,
    storage: StorageConfig,
    forks: HashMap<String, ForkConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct StorageConfig {
    endpoint_url: Option<String>,
    bucket: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
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
    bucket: &'a str,
    s3_client: S3Client,
    forks: &'a HashMap<String, ForkConfig>,
    http_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = toml::from_str(&tokio::fs::read_to_string(args.config_file).await?)?;
    let sdk_config = aws_config::load_from_env().await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
    s3_config.set_endpoint_url(config.storage.endpoint_url);
    s3_config.set_region(Some(Region::new(config.storage.region)));
    s3_config.set_credentials_provider(Some(SharedCredentialsProvider::new(Credentials::new(
        config.storage.access_key_id,
        config.storage.secret_access_key,
        None,
        None,
        "config",
    ))));
    let s3_client = S3Client::from_conf(s3_config.build());

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
            bucket: config.storage.bucket.leak(),
            s3_client,
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
    let publish_inpr = state
        .s3_client
        .head_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/started"))
        .send()
        .await
        .is_ok();
    if publish_inpr {
        let mut files = state
            .s3_client
            .list_objects_v2()
            .bucket(state.bucket)
            .prefix(format!("pending/{fork}/{version}/"))
            .into_paginator()
            .send();
        while let Some(page) = files.next().await {
            let page = page.map_err(log_500)?;
            for file in page.contents.ok_or(eyre::eyre!("Failed to list pending files")).map_err(log_500)? {
                state
                    .s3_client
                    .delete_object()
                    .bucket(state.bucket)
                    .key(file.key.ok_or(eyre::eyre!("Failed to list pending files")).map_err(log_500)?)
                    .send()
                    .await
                    .map_err(log_500)?;
            }
        }
    }
    state
        .s3_client
        .put_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/started"))
        .body(engine_version.into_bytes().into())
        .send()
        .await
        .map_err(log_500)?;
    Ok(())
}

async fn fork_publish_file(
    RequireAuthentication: RequireAuthentication,
    Path(fork): Path<String>,
    State(state): State<AppState<'_>>,
    headers: HeaderMap,
    body: Bytes,
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
    let publish_inpr = state
        .s3_client
        .head_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/started"))
        .send()
        .await
        .is_ok();
    if !publish_inpr {
        return Err((StatusCode::BAD_REQUEST).into());
    }
    state
        .s3_client
        .put_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/{file}"))
        .body(body.into())
        .send()
        .await
        .map_err(log_500)?;
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
    #[serde(with = "time::serde::iso8601")]
    time: OffsetDateTime,
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
    State(state): State<AppState<'_>>,
    Json(PublishFinishRequest { version }): Json<PublishFinishRequest>,
) -> Result<(), ErrorResponse> {
    let engine_version = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/started"))
        .send()
        .await;
    let engine_version = match engine_version {
        Ok(resp) => resp,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            return Err((StatusCode::BAD_REQUEST).into());
        }
        Err(e) => return Err(log_500(e)),
    };
    let engine_version = engine_version
        .body
        .collect()
        .await
        .map_err(log_500)?
        .into_bytes();
    let engine_version =
        str::from_utf8(&engine_version).map_err(log_500)?;

    let mut has_client = false;
    let mut server_platforms = vec![];
    let mut all_files = vec![];

    let prefix = format!("pending/{fork}/{version}/");
    let mut files = state
        .s3_client
        .list_objects_v2()
        .bucket(state.bucket)
        .prefix(&prefix)
        .into_paginator()
        .send();
    while let Some(page) = files.next().await {
        let page = page.map_err(log_500)?;
        for file in page.contents.ok_or(eyre::eyre!("Failed to list pending files")).map_err(log_500)? {
            let key = file.key.ok_or(eyre::eyre!("Failed to list pending files")).map_err(log_500)?;
            let filename = key
                .strip_prefix(&prefix)
                .ok_or(eyre::eyre!("Failed to list pending files")).map_err(log_500)?;
            if let Some(artifact) = filename.strip_suffix(".zip") {
                if let Some(platform) = artifact.strip_prefix("SS14.Server_") {
                    server_platforms.push(platform.to_string());
                } else if artifact == "SS14.Client" {
                    has_client = true;
                }
            }
            all_files.push(key);
        }
    }
    if !has_client {
        for file in all_files {
            state
                .s3_client
                .delete_object()
                .bucket(state.bucket)
                .key(file)
                .send()
                .await
                .map_err(log_500)?;
        }
        return Err((StatusCode::BAD_REQUEST).into());
    }
    let client = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("pending/{fork}/{version}/SS14.Client.zip"))
        .send()
        .await
        .map_err(log_500)?
        .body
        .collect()
        .await
        .map_err(log_500)?
        .into_bytes();
    let mut client_zip =
        zip::ZipArchive::new(Cursor::new(&client)).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut client_manifest = "Robust Content Manifest 1\n".to_string();
    for filename in client_zip
        .file_names()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>()
    {
        let mut file = client_zip
            .by_name(&filename)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        if file.is_dir() {
            continue;
        }
        let mut buf = Vec::with_capacity(file.size().min(64 << 20) as usize);
        file.read_to_end(&mut buf)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let hash = blake2::Blake2b::<U32>::digest(&buf);
        client_manifest += &format!("{} {}\n", hex::encode_upper(hash), filename);
        let resp = state
            .s3_client
            .put_object()
            .if_none_match("*")
            .bucket(state.bucket)
            .key(format!("content/{}", hex::encode(hash)))
            .body(buf.into())
            .send()
            .await;
        if let Err(e) = resp {
            if let Some(e) = e.raw_response()
                && e.status() == StatusCode::PRECONDITION_FAILED.into()
            {
                // 412 Precondition Failed is okay, we're deduping
            } else {
                return Err(log_500(e));
            }
        }
    }
    let manifest_hash = blake2::Blake2b::<U32>::digest(client_manifest.as_bytes());
    let client_zip_hash = sha2::Sha256::digest(&client);
    state
        .s3_client
        .put_object()
        .bucket(state.bucket)
        .key(format!("versions/{fork}/{version}/manifest"))
        .body(client_manifest.into_bytes().into())
        .send()
        .await
        .map_err(log_500)?;
    state
        .s3_client
        .put_object()
        .bucket(state.bucket)
        .key(format!("versions/{fork}/{version}/SS14.Client.zip"))
        .body(client.into())
        .send()
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
        let server = state
            .s3_client
            .get_object()
            .bucket(state.bucket)
            .key(format!(
                "pending/{fork}/{version}/SS14.Server_{platform}.zip"
            ))
            .send()
            .await
            .map_err(log_500)?
            .body
            .collect()
            .await
            .map_err(log_500)?
            .to_vec();
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
        state
            .s3_client
            .put_object()
            .bucket(state.bucket)
            .key(format!(
                "versions/{fork}/{version}/SS14.Server_{platform}.zip"
            ))
            .body(server.into())
            .send()
            .await
            .map_err(log_500)?;
    }
    for file in all_files {
        state
            .s3_client
            .delete_object()
            .bucket(state.bucket)
            .key(file)
            .send()
            .await
            .map_err(log_500)?;
    }

    let manifest = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("manifests/{fork}"))
        .send()
        .await;
    let mut manifest: ForkManifest = match manifest {
        Ok(resp) => serde_json::from_slice(
            &resp
                .body
                .collect()
                .await
                .map_err(log_500)?
                .into_bytes(),
        )
        .map_err(log_500)?,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            Default::default()
        }
        Err(e) => return Err(log_500(e)),
    };
    manifest.builds.insert(
        version.clone(),
        ForkVersionManifest {
            time: OffsetDateTime::now_utc(),
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
    state
        .s3_client
        .put_object()
        .bucket(state.bucket)
        .key(format!("manifests/{fork}"))
        .body(
            serde_json::to_vec(&manifest)
                .map_err(log_500)?
                .into(),
        )
        .send()
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
                    .map(reqwest::Response::error_for_status) {
            warn!("Failed to notify watchdog: {e:?}");
        };
    }
    Ok(())
}

async fn fork_manifest(
    Path(fork): Path<String>,
    State(state): State<AppState<'_>>,
) -> Result<([(&'static str, &'static str); 1], Bytes), ErrorResponse> {
    let resp = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("manifests/{fork}"))
        .send()
        .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            return Err((StatusCode::NOT_FOUND).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let body = resp
        .body
        .collect()
        .await
        .map_err(log_500)?;
    Ok(([("Content-Type", "application/json")], body.into_bytes()))
}

async fn version_manifest(
    Path((fork, version)): Path<(String, String)>,
    State(state): State<AppState<'_>>,
) -> Result<([(&'static str, &'static str); 1], Bytes), ErrorResponse> {
    let resp = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("versions/{fork}/{version}/manifest"))
        .send()
        .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            return Err((StatusCode::NOT_FOUND).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let body = resp.body.collect().await.map_err(log_500)?;
    Ok(([("Content-Type", "text/plain")], body.into_bytes()))
}

async fn version_file(
    Path((fork, version, file)): Path<(String, String, String)>,
    State(state): State<AppState<'_>>,
) -> Result<([(&'static str, String); 1], Bytes), ErrorResponse> {
    let resp = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("versions/{fork}/{version}/{file}"))
        .send()
        .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            return Err((StatusCode::NOT_FOUND).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let body = resp.body.collect().await.map_err(log_500)?;
    Ok((
        [(
            "Content-Type",
            resp.content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        )],
        body.into_bytes(),
    ))
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

    let resp = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("versions/{fork}/{version}/manifest"))
        .send()
        .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), &GetObjectError::NoSuchKey(_)) => {
            return Err((StatusCode::NOT_FOUND).into());
        }
        Err(e) => return Err(log_500(e)),
    };

    let resp = resp.body.collect().await.map_err(log_500)?;
    let resp = resp.into_bytes();

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

fn get_blobs(
    state: AppState<'static>,
    blobs: Vec<[u8; 32]>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let mut blobs = blobs.into_iter();
    let mut next: usize = 0;
    let mut prefetch = [const { None }; PREFETCH_SIZE];
    for entry in prefetch.iter_mut() {
        *entry = blobs
            .next()
            .map(|blob| tokio::spawn(get_blob(state.clone(), blob)));
    }
    stream! {
        yield Ok(Bytes::from_static(&[0, 0, 0, 0]));
        while let Some(fut) = std::mem::take(&mut prefetch[next]) {
            let resp = fut.await.ok().flatten().unwrap_or_else(Bytes::new);
            yield Ok(Bytes::from((resp.len() as u32).to_le_bytes().to_vec()));
            yield Ok(resp);
            prefetch[next] = blobs.next().map(|blob| tokio::spawn(get_blob(state.clone(), blob)));
            next = (next + 1) % PREFETCH_SIZE;
        }
    }
}

async fn get_blob(state: AppState<'_>, blob: [u8; 32]) -> Option<Bytes> {
    let resp = state
        .s3_client
        .get_object()
        .bucket(state.bucket)
        .key(format!("content/{}", hex::encode(blob)))
        .send()
        .await
        .ok()?;

    Some(resp.body.collect().await.ok()?.into_bytes())
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