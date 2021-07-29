use anyhow::Result;
use std::{convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc};
use tantivy::schema::NamedFieldDocument;
use tokio::sync::RwLock;
use warp::{hyper::StatusCode, Filter};

use serde::{Deserialize, Serialize};
use serde_json::value::Value;

use crate::indexer::SearchService;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    query: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

#[derive(Serialize, Deserialize)]
struct Hit {
    score: f32,
    doc: NamedFieldDocument,
}

impl From<(f32, NamedFieldDocument)> for Hit {
    fn from(item: (f32, NamedFieldDocument)) -> Self {
        Self {
            score: item.0,
            doc: item.1,
        }
    }
}

//use once_cell::sync::{Lazy, OnceCell};
//static SEARCH_SERVICE: Lazy<Arc<SearchService>> = Lazy::new(|| Arc::new(SearchService::new(Path::new("./data").to_path_buf()).unwrap()));

pub async fn start_server(index_path: PathBuf, addr: SocketAddr) -> Result<()> {
    let search_service = Arc::new(RwLock::new(SearchService::new(index_path)?));
    let service_filter =
        |service: Arc<RwLock<SearchService>>| warp::any().map(move || service.clone());

    // GET / => 200 OK
    let info = warp::path::end().map(|| "Welcome to movie searcher");

    // POST /documents
    let post_documents = warp::path!("documents")
        .and(service_filter(search_service.clone()))
        .and(warp::post())
        .and(warp::body::json())
        .and_then(add_documents_handler);

    // GET /documents?query=morpheus
    let query_documents = warp::path!("documents")
        .and(service_filter(search_service.clone()))
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and_then(query_documents_handler);

    let routes = info.or(post_documents).or(query_documents);

    warp::serve(routes).run(addr).await;
    Ok(())
}

async fn add_documents_handler(
    service: Arc<RwLock<SearchService>>,
    documents: Vec<Value>,
) -> Result<impl warp::Reply, Infallible> {
    let mut lock_guard = service.write().await;
    for document in documents {
        let result = lock_guard.add_doc(&document.to_string());
        if let Err(_) = result {
            let response = "An error occured while adding documents.";
            return Ok(warp::reply::with_status(
                warp::reply::json(&response),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }

    let response = "Document added successfully!";
    Ok(warp::reply::with_status(
        warp::reply::json(&response),
        StatusCode::ACCEPTED,
    ))
}

async fn query_documents_handler(
    service: Arc<RwLock<SearchService>>,
    query: SearchQuery,
) -> Result<impl warp::Reply, Infallible> {
    let lock_guard = service.read().await;
    let result = lock_guard.search(&query.query, query.offset, query.limit);
    if let Err(_) = result {
        let response = "An error occured while searching.";
        return Ok(warp::reply::with_status(
            warp::reply::json(&response),
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }
    let response: Vec<Hit> = result
        .unwrap()
        .into_iter()
        .map(|item| Hit::from(item))
        .collect();
    Ok(warp::reply::with_status(
        warp::reply::json(&response),
        StatusCode::OK,
    ))
}
