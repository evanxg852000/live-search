use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{NamedFieldDocument, Schema, FAST, STORED, TEXT},
    Index, IndexWriter,
};

const MAX_IN_MEMORY_DOC: usize = 5_000;
const STORE_NAME: &str = "documents.db";
const DB_NAME: &str = "documents";

struct SearchIndex {
    schema: Schema,
    index: Index,
    writer: IndexWriter,
    num_docs: usize,
    is_in_memory: bool,
}

impl SearchIndex {
    pub fn new(path_opt: Option<PathBuf>) -> Result<Self> {
        let is_in_memory = path_opt.is_none();
        let schema = build_schema();
        let index = if let Some(path) = path_opt {
            if path.exists() {
                Index::open_in_dir(path)?
            } else {
                fs::create_dir_all(path.clone())?;
                Index::create_in_dir(path, schema.clone())?
            }
        } else {
            Index::create_in_ram(schema.clone())
        };

        let writer = index.writer(20_000_000)?;
        Ok(Self {
            schema,
            index,
            writer,
            num_docs: 0,
            is_in_memory,
        })
    }

    pub fn add_doc(&mut self, doc: &str) -> Result<()> {
        let parsed_doc = self.schema.parse_document(doc)?;
        self.writer.add_document(parsed_doc);
        self.num_docs += 1;
        if self.is_in_memory {
            self.writer.commit()?;
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.writer.delete_all_documents()?;
        self.writer.commit()?;
        self.num_docs += 0;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        _offset: usize,
        limit: usize,
    ) -> Result<Vec<(f32, NamedFieldDocument)>> {
        let field_names = default_search_field_names();
        let mut fields = vec![];
        for name in field_names {
            let field = self.schema.get_field(&name).unwrap();
            fields.push(field);
        }
        let query_parser = QueryParser::for_index(&self.index, fields);
        let query = query_parser.parse_query(query_str)?;

        let searcher = self.index.reader()?.searcher();
        let hits = searcher.search(&query, &TopDocs::with_limit(limit))?;
        let mut search_result = vec![];
        for (score, doc_id) in hits {
            let doc = searcher.doc(doc_id)?;
            let named_doc = searcher.schema().to_named_doc(&doc);
            search_result.push((score, named_doc));
        }
        Ok(search_result)
    }
}

fn build_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("id", TEXT | STORED);
    builder.add_text_field("title", TEXT | STORED);
    builder.add_text_field("poster", STORED);
    builder.add_text_field("overview", TEXT | STORED);
    builder.add_i64_field("release_date", STORED | FAST);
    builder.build()
}

fn default_search_field_names() -> Vec<String> {
    vec!["title".to_string(), "overview".to_string()]
}

pub struct SearchService {
    disk_index: SearchIndex,
    memory_index: SearchIndex,
    store: PickleDb,
}

impl SearchService {
    pub fn new(path: PathBuf) -> Result<Self> {
        let disk_index = SearchIndex::new(Some(path.clone()))?;
        let mut memory_index = SearchIndex::new(None)?;
        let mut store = if path.join(STORE_NAME).exists() {
            PickleDb::load(
                path.join(STORE_NAME),
                PickleDbDumpPolicy::AutoDump,
                SerializationMethod::Bin,
            )?
        } else {
            PickleDb::new(
                path.join(STORE_NAME),
                PickleDbDumpPolicy::AutoDump,
                SerializationMethod::Bin,
            )
        };
        if !store.lexists(DB_NAME) {
            store.lcreate(DB_NAME)?;
        }
        for item in store.liter(DB_NAME) {
            let doc_str: String = item.get_item().expect("should be valid string.");
            memory_index.add_doc(&doc_str)?;
        }
        Ok(Self {
            disk_index,
            memory_index,
            store,
        })
    }

    pub fn add_doc(&mut self, doc: &str) -> Result<()> {
        self.memory_index.add_doc(doc)?;
        self.store.ladd(DB_NAME, &String::from(doc));

        if self.memory_index.num_docs >= MAX_IN_MEMORY_DOC {
            for item in self.store.liter(DB_NAME) {
                let doc_str: String = item.get_item().expect("should be valid string.");
                self.disk_index.add_doc(&doc_str)?;
            }
            self.disk_index.commit()?;

            self.memory_index.clear()?;
            self.store.lrem_list(DB_NAME)?;
            self.store.lcreate(DB_NAME)?;
        }
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(f32, NamedFieldDocument)>> {
        let mut mem_result = self.memory_index.search(query_str, offset, limit)?;
        let disk_result = self.disk_index.search(query_str, offset, limit)?;
        mem_result.extend(disk_result);
        mem_result.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        mem_result.truncate(limit);
        Ok(mem_result)
    }
}
