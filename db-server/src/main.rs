use std::io;
use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tarpc::context::Context;
use tokio::sync::Mutex;

use db::{DbType, Row, SavedDatabase};

#[derive(Clone)]
struct Server(pub Arc<Mutex<Option<SavedDatabase>>>);

#[tarpc::service]
pub trait Service {
    async fn create(name: String, path: String);
    async fn open(path: String);
    async fn get_name() -> Option<String>;
    async fn get_table_names() -> Option<Vec<String>>;
    async fn save();
    async fn remove_table(name: String);
    async fn create_table(name: String, schema: Vec<DbType>);
    async fn remove_row(table: String, index: usize);
    async fn insert_row(table: String, row: Row);
    async fn get_table_schema(table: String) -> Option<Vec<DbType>>;
    async fn get_rows(table: String) -> Option<Vec<Row>>;
    async fn table_projection(table: String, rows: Vec<bool>, new_table: String);
}

#[tarpc::server]
impl Service for Server {
    async fn create(self, _: tarpc::context::Context, name: String, path: String) {
        let mut lock = self.0.lock().await;
        let new_db = SavedDatabase::create(name, path).unwrap();
        lock.replace(new_db);
    }

    async fn open(self, _: tarpc::context::Context, path: String) {
        let mut lock = self.0.lock().await;
        let new_db = SavedDatabase::load_from_disk(path).unwrap();
        lock.replace(new_db);
    }

    async fn get_name(self, _: tarpc::context::Context) -> Option<String> {
        let lock = self.0.lock().await;
        lock.as_ref().map(|db| db.get_name().to_string())
    }

    async fn get_table_names(self, _: tarpc::context::Context) -> Option<Vec<String>> {
        let lock = self.0.lock().await;
        lock.as_ref().map(|db| db.get_table_names())
    }

    async fn save(self, _: tarpc::context::Context) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.save().unwrap();
        }
    }

    async fn remove_table(self, _: tarpc::context::Context, name: String) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.remove_table(name).unwrap();
        }
    }

    async fn create_table(self, _: tarpc::context::Context, name: String, schema: Vec<DbType>) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.create_table(name, schema).unwrap();
        }
    }

    async fn remove_row(self, _: tarpc::context::Context, table: String, index: usize) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(table) {
                table.remove_row(index);
            }
        }
    }

    async fn insert_row(self, _: tarpc::context::Context, table: String, row: Row) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(table) {
                let _ = table.insert_row(row);
            }
        }
    }

    async fn get_table_schema(
        self,
        _: tarpc::context::Context,
        table: String,
    ) -> Option<Vec<DbType>> {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(table) {
                return Some(table.schema().to_vec());
            }
        }
        None
    }

    async fn get_rows(
        self,
        _: tarpc::context::Context,
        table: String,
    ) -> Option<Vec<Row>> {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(table) {
                return Some(table.rows().to_vec());
            }
        }
        None
    }

    async fn table_projection(self, context: Context, table: String, rows: Vec<bool>, new_table: String) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            let _ = db.projection(table, rows, new_table);
        }
    }
}

const PATH: &str = "/Users/antond/Desktop/ITLab1/database";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Mutex::new(None));
    Arc::new(Mutex::new(
        SavedDatabase::load_from_disk(PATH.to_string()).unwrap(),
    ));

    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 8080);

    // JSON transport is provided by the json_transport tarpc module. It makes it easy
    // to start up a serde-powered json serialization strategy over TCP.
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let server = Server(db.clone());
            channel.execute(server.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}