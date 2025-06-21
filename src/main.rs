use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use crossbeam;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

// Macro for creating Arrow schema fields
macro_rules! field {
    ($name:expr, $data_type:expr) => {
        Field::new($name, $data_type, true)
    };
    ($name:expr, $data_type:expr, $nullable:expr) => {
        Field::new($name, $data_type, $nullable)
    };
}

// Macro for creating schemas with less boilerplate
macro_rules! schema {
    ($($name:expr => $data_type:expr $(, $nullable:expr)?);* $(;)?) => {
        Schema::new(vec![
            $(field!($name, $data_type $(, $nullable)?),)*
        ])
    };
}

// Macro for creating string arrays from record fields
macro_rules! string_array_required {
    ($records:expr, $field:ident) => {
        Arc::new(StringArray::from_iter_values(
            $records.iter().map(|r| &r.$field),
        ))
    };
}

// Macro for creating optional string arrays
macro_rules! string_array_optional {
    ($records:expr, $field:ident) => {
        Arc::new(StringArray::from_iter(
            $records.iter().map(|r| r.$field.as_deref()),
        ))
    };
}

// Macro for creating numeric arrays
macro_rules! int64_array {
    ($records:expr, $field:ident) => {
        Arc::new(Int64Array::from_iter_values(
            $records.iter().map(|r| r.$field),
        ))
    };
}

// Macro for creating int32 arrays
macro_rules! int32_array_optional {
    ($records:expr, $field:ident) => {
        Arc::new(Int32Array::from_iter($records.iter().map(|r| r.$field)))
    };
}

// Macro for creating boolean arrays
macro_rules! bool_array_optional {
    ($records:expr, $field:ident) => {
        Arc::new(BooleanArray::from_iter($records.iter().map(|r| r.$field)))
    };
}

// Macro for creating required boolean arrays
macro_rules! bool_array_required {
    ($records:expr, $field:ident) => {
        Arc::new(BooleanArray::from_iter(
            $records.iter().map(|r| Some(r.$field)),
        ))
    };
}

// Macro for creating large string arrays (required)
macro_rules! large_string_array_required {
    ($records:expr, $field:ident) => {
        Arc::new(LargeStringArray::from_iter_values(
            $records.iter().map(|r| &r.$field),
        ))
    };
}

// Macro for creating large string arrays (optional)
macro_rules! large_string_array_optional {
    ($records:expr, $field:ident) => {
        Arc::new(LargeStringArray::from_iter(
            $records.iter().map(|r| r.$field.as_deref()),
        ))
    };
}

// Macro for creating float arrays
macro_rules! float64_array_optional {
    ($records:expr, $field:ident) => {
        Arc::new(Float64Array::from_iter($records.iter().map(|r| r.$field)))
    };
}

// Macro for batch writing with error handling
macro_rules! write_batch_if_needed {
    ($batch:expr, $writer:expr, $batch_size:expr, $converter:expr, $batch_type:expr) => {
        if $batch.len() >= $batch_size {
            if let Err(e) =
                write_parquet_batch(&mut $writer, std::mem::take(&mut $batch), $converter)
            {
                eprintln!("Error writing {} batch: {}", $batch_type, e);
                std::process::exit(1);
            }
        }
    };
}

// Macro for buffer flushing pattern used throughout processing functions
macro_rules! flush_local_buffer {
    ($local_buffer:expr, $global_buffer:expr, $writer:expr, $batch_size:expr, $converter:expr, $local_threshold:expr) => {
        if $local_buffer.len() >= $local_threshold {
            let mut buffer = $global_buffer.lock().unwrap();
            buffer.extend($local_buffer.drain(..));
            if buffer.len() >= $batch_size {
                let mut writer = $writer.lock().unwrap();
                write_parquet_batch(&mut *writer, buffer.drain(..).collect(), $converter)?;
            }
        }
    };
}

// Macro for final buffer flush at end of processing
macro_rules! final_flush {
    ($local_buffer:expr, $global_buffer:expr, $writer:expr, $converter:expr) => {
        if !$local_buffer.is_empty() {
            let mut buffer = $global_buffer.lock().unwrap();
            buffer.extend($local_buffer);
        }
        {
            let mut buffer = $global_buffer.lock().unwrap();
            if !buffer.is_empty() {
                let mut writer = $writer.lock().unwrap();
                write_parquet_batch(&mut *writer, buffer.drain(..).collect(), $converter)?;
            }
        }
    };
}

// Macro for extracting string fields from JSON with fallback
macro_rules! extract_string {
    ($json:expr, $field:expr) => {
        $json
            .get($field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
}

// Macro for extracting required string fields from JSON
macro_rules! extract_string_required {
    ($json:expr, $field:expr, $default:expr) => {
        $json
            .get($field)
            .and_then(|v| v.as_str())
            .unwrap_or($default)
            .to_string()
    };
}

// Macro for extracting integer fields from JSON
macro_rules! extract_i64 {
    ($json:expr, $field:expr, $default:expr) => {
        $json
            .get($field)
            .and_then(|v| v.as_i64())
            .unwrap_or($default)
    };
}

// Macro for extracting optional integer fields from JSON
macro_rules! extract_i64_optional {
    ($json:expr, $field:expr) => {
        $json.get($field).and_then(|v| v.as_i64())
    };
}

// Macro for extracting and serializing array fields from JSON
macro_rules! extract_array_as_string {
    ($json:expr, $field:expr) => {
        $json
            .get($field)
            .and_then(|v| serde_json::to_string(v).ok())
    };
}

// Macro for extracting nested string fields from JSON objects
macro_rules! extract_nested_string {
    ($json:expr, $outer_field:expr, $inner_field:expr) => {
        $json
            .get($outer_field)
            .and_then(|v| v.get($inner_field))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
}

// Macro for creating thread-safe parquet writers
macro_rules! create_writer {
    ($output_path:expr, $schema:expr) => {
        Arc::new(Mutex::new(create_parquet_writer($output_path, $schema)?))
    };
}

// Macro for creating record batches with less boilerplate
macro_rules! record_batch {
    ($schema:expr, $($array:expr),* $(,)?) => {
        RecordBatch::try_new(Arc::new($schema), vec![$($array,)*])
    };
}

#[derive(Parser)]
#[command(name = "openalex_processor")]
#[command(about = "OpenAlex Academic Data Processor - Creates PySpark-ready Parquet datasets")]
struct Cli {
    /// Input directory containing OpenAlex snapshot
    #[arg(short, long, default_value = "../download/openalex-snapshot/data")]
    input_dir: String,

    /// Output directory for Parquet datasets
    #[arg(short, long, default_value = "./pyspark_datasets")]
    output_dir: String,

    /// Number of parallel workers (default: 80% of cores)
    #[arg(short, long)]
    workers: Option<usize>,

    /// Batch size for Parquet writing
    #[arg(short, long, default_value = "1000000")]
    batch_size: usize,

    /// Entity types to process (comma-separated: authors,institutions,publishers,works,topics)
    #[arg(
        short,
        long,
        default_value = "authors,institutions,publishers,works,topics"
    )]
    entities: String,

    /// Limit files per entity for testing (0 = no limit)
    #[arg(short, long, default_value = "0")]
    files_per_entity: usize,
}

// ====== PROCESSING STATISTICS ======
#[derive(Debug, Default)]
pub struct ProcessingStats {
    pub authors_processed: AtomicU64,
    pub institutions_processed: AtomicU64,
    pub publishers_processed: AtomicU64,
    pub works_processed: AtomicU64,
    pub topics_processed: AtomicU64,
    pub citation_edges: AtomicU64,
    pub files_processed: AtomicU64,
}

impl ProcessingStats {
    pub fn new() -> Self {
        Default::default()
    }
}

// ====== MEMORY MONITORING ======
fn get_memory_usage() -> String {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                return line.to_string();
            }
        }
    }
    "Memory info unavailable".to_string()
}

// ====== UTILITY FUNCTIONS ======
fn create_parquet_writer(output_path: &Path, schema: Schema) -> Result<ArrowWriter<File>> {
    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_write_batch_size(5_000_000) // MASSIVE batch size for your 150GB RAM beast!
        .set_max_row_group_size(50_000_000) // Huge row groups for maximum compression
        .set_data_page_size_limit(2_000_000) // Large data pages
        .set_dictionary_page_size_limit(2_000_000) // Large dictionary pages
        .build();

    let writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    Ok(writer)
}

fn find_entity_files(input_dir: &str, entity: &str) -> Result<Vec<PathBuf>> {
    let pattern = format!("{}/{}/updated_date*/part_*.gz", input_dir, entity);
    info!("Searching for {} files with pattern: {}", entity, pattern);

    let mut files = Vec::new();
    for entry in glob(&pattern)? {
        match entry {
            Ok(path) => {
                if path.metadata()?.len() > 0 {
                    files.push(path);
                }
            }
            Err(e) => warn!("Error reading glob entry: {}", e),
        }
    }

    files.sort();
    info!("Found {} {} files", files.len(), entity);
    Ok(files)
}

fn safe_truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Find a safe UTF-8 boundary near the limit
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

#[derive(Debug, Clone)]
pub struct AuthorRecord {
    pub id: String,
    pub orcid: Option<String>,
    pub display_name: String,
    pub display_name_alternatives: Option<String>,
    pub works_count: i64,
    pub cited_by_count: i64,
    pub last_known_institution: Option<String>,
    pub works_api_url: Option<String>,
    pub updated_date: Option<String>,
}
// ====== DATA STRUCTURES FOR CITATION ANALYSIS ======

#[derive(Debug, Clone)]
pub struct AuthorIdsRecord {
    pub author_id: String,
    pub openalex: Option<String>,
    pub orcid: Option<String>,
    pub scopus: Option<String>,
    pub twitter: Option<String>,
    pub wikipedia: Option<String>,
    pub mag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthorCountsByYearRecord {
    pub author_id: String,
    pub year: i32,
    pub works_count: i64,
    pub cited_by_count: i64,
    pub oa_works_count: i64,
}

#[derive(Debug, Clone)]
pub struct InstitutionRecord {
    pub id: String,
    pub ror: Option<String>,
    pub display_name: String,
    pub country_code: Option<String>,
    pub type_: Option<String>,
    pub homepage_url: Option<String>,
    pub image_url: Option<String>,
    pub image_thumbnail_url: Option<String>,
    pub display_name_acronyms: Option<String>,
    pub display_name_alternatives: Option<String>,
    pub works_count: i64,
    pub cited_by_count: i64,
    pub works_api_url: Option<String>,
    pub updated_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InstitutionIdsRecord {
    pub institution_id: String,
    pub openalex: Option<String>,
    pub ror: Option<String>,
    pub grid: Option<String>,
    pub wikipedia: Option<String>,
    pub wikidata: Option<String>,
    pub mag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InstitutionGeoRecord {
    pub institution_id: String,
    pub city: Option<String>,
    pub geonames_city_id: Option<String>,
    pub region: Option<String>,
    pub country_code: Option<String>,
    pub country: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct PublisherRecord {
    pub id: String,
    pub display_name: String,
    pub alternate_titles: Option<String>,
    pub country_codes: Option<String>,
    pub hierarchy_level: Option<i32>,
    pub parent_publisher: Option<String>,
    pub works_count: i64,
    pub cited_by_count: i64,
    pub sources_api_url: Option<String>,
    pub updated_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopicRecord {
    pub id: String,
    pub display_name: String,
    pub subfield_id: String,
    pub subfield_display_name: String,
    pub field_id: String,
    pub field_display_name: String,
    pub domain_id: String,
    pub domain_display_name: String,
    pub description: Option<String>,
    pub keywords: Option<String>,
    pub works_api_url: Option<String>,
    pub wikipedia_id: Option<String>,
    pub works_count: i64,
    pub cited_by_count: i64,
    pub updated_date: Option<String>,
    pub siblings: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkRecord {
    pub id: String,
    pub doi: Option<String>,
    pub title: Option<String>,
    pub display_name: String,
    pub publication_year: Option<i32>,
    pub publication_date: Option<String>,
    pub type_: Option<String>,
    pub cited_by_count: i64,
    pub is_retracted: bool,
    pub is_paratext: Option<bool>,
    pub cited_by_api_url: Option<String>,
    pub abstract_inverted_index: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkLocationRecord {
    pub work_id: String,
    pub source_id: Option<String>,
    pub landing_page_url: Option<String>,
    pub pdf_url: Option<String>,
    pub is_oa: Option<bool>,
    pub version: Option<String>,
    pub license: Option<String>,
    pub location_type: String, // "primary", "location", "best_oa"
}

#[derive(Debug, Clone)]
pub struct WorkAuthorshipRecord {
    pub work_id: String,
    pub author_position: String,
    pub author_id: String,
    pub institution_id: Option<String>,
    pub raw_affiliation_string: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkBiblioRecord {
    pub work_id: String,
    pub volume: Option<String>,
    pub issue: Option<String>,
    pub first_page: Option<String>,
    pub last_page: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkTopicRecord {
    pub work_id: String,
    pub topic_id: String,
    pub score: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct WorkOpenAccessRecord {
    pub work_id: String,
    pub is_oa: Option<bool>,
    pub oa_status: Option<String>,
    pub oa_url: Option<String>,
    pub any_repository_has_fulltext: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CitationRecord {
    pub work_id: String,
    pub referenced_work_id: String,
}

// ====== RECORD BATCH CREATORS ======
fn authors_to_record_batch(records: Vec<AuthorRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "id" => DataType::Utf8, false;
        "orcid" => DataType::Utf8;
        "display_name" => DataType::Utf8, false;
        "display_name_alternatives" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "last_known_institution" => DataType::Utf8;
        "works_api_url" => DataType::Utf8;
        "updated_date" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, id),
        string_array_optional!(records, orcid),
        string_array_required!(records, display_name),
        string_array_optional!(records, display_name_alternatives),
        int64_array!(records, works_count),
        int64_array!(records, cited_by_count),
        string_array_optional!(records, last_known_institution),
        string_array_optional!(records, works_api_url),
        string_array_optional!(records, updated_date),
    )?;

    Ok(batch)
}

fn author_ids_to_record_batch(records: Vec<AuthorIdsRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "author_id" => DataType::Utf8, false;
        "openalex" => DataType::Utf8;
        "orcid" => DataType::Utf8;
        "scopus" => DataType::Utf8;
        "twitter" => DataType::Utf8;
        "wikipedia" => DataType::Utf8;
        "mag" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, author_id),
        string_array_optional!(records, openalex),
        string_array_optional!(records, orcid),
        string_array_optional!(records, scopus),
        string_array_optional!(records, twitter),
        string_array_optional!(records, wikipedia),
        string_array_optional!(records, mag),
    )?;

    Ok(batch)
}

fn author_counts_by_year_to_record_batch(
    records: Vec<AuthorCountsByYearRecord>,
) -> Result<RecordBatch> {
    let schema = schema! {
        "author_id" => DataType::Utf8, false;
        "year" => DataType::Int32, false;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "oa_works_count" => DataType::Int64, false;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, author_id),
        Arc::new(Int32Array::from_iter_values(records.iter().map(|r| r.year))),
        int64_array!(records, works_count),
        int64_array!(records, cited_by_count),
        int64_array!(records, oa_works_count),
    )?;

    Ok(batch)
}

fn institutions_to_record_batch(records: Vec<InstitutionRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "id" => DataType::Utf8, false;
        "ror" => DataType::Utf8;
        "display_name" => DataType::Utf8, false;
        "country_code" => DataType::Utf8;
        "type_" => DataType::Utf8;
        "homepage_url" => DataType::Utf8;
        "image_url" => DataType::Utf8;
        "image_thumbnail_url" => DataType::Utf8;
        "display_name_acronyms" => DataType::Utf8;
        "display_name_alternatives" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "works_api_url" => DataType::Utf8;
        "updated_date" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, id),
        string_array_optional!(records, ror),
        string_array_required!(records, display_name),
        string_array_optional!(records, country_code),
        string_array_optional!(records, type_),
        string_array_optional!(records, homepage_url),
        string_array_optional!(records, image_url),
        string_array_optional!(records, image_thumbnail_url),
        string_array_optional!(records, display_name_acronyms),
        string_array_optional!(records, display_name_alternatives),
        int64_array!(records, works_count),
        int64_array!(records, cited_by_count),
        string_array_optional!(records, works_api_url),
        string_array_optional!(records, updated_date),
    )?;

    Ok(batch)
}

fn institution_geo_to_record_batch(records: Vec<InstitutionGeoRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "institution_id" => DataType::Utf8, false;
        "city" => DataType::Utf8;
        "geonames_city_id" => DataType::Utf8;
        "region" => DataType::Utf8;
        "country_code" => DataType::Utf8;
        "country" => DataType::Utf8;
        "latitude" => DataType::Float64;
        "longitude" => DataType::Float64;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, institution_id),
        string_array_optional!(records, city),
        string_array_optional!(records, geonames_city_id),
        string_array_optional!(records, region),
        string_array_optional!(records, country_code),
        string_array_optional!(records, country),
        float64_array_optional!(records, latitude),
        float64_array_optional!(records, longitude),
    )?;

    Ok(batch)
}

fn publishers_to_record_batch(records: Vec<PublisherRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "id" => DataType::Utf8, false;
        "display_name" => DataType::Utf8, false;
        "alternate_titles" => DataType::Utf8;
        "country_codes" => DataType::Utf8;
        "hierarchy_level" => DataType::Int32;
        "parent_publisher" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "sources_api_url" => DataType::Utf8;
        "updated_date" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, id),
        string_array_required!(records, display_name),
        string_array_optional!(records, alternate_titles),
        string_array_optional!(records, country_codes),
        int32_array_optional!(records, hierarchy_level),
        string_array_optional!(records, parent_publisher),
        int64_array!(records, works_count),
        int64_array!(records, cited_by_count),
        string_array_optional!(records, sources_api_url),
        string_array_optional!(records, updated_date),
    )?;

    Ok(batch)
}

fn topics_to_record_batch(records: Vec<TopicRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "id" => DataType::Utf8, false;
        "display_name" => DataType::Utf8, false;
        "subfield_id" => DataType::Utf8, false;
        "subfield_display_name" => DataType::Utf8, false;
        "field_id" => DataType::Utf8, false;
        "field_display_name" => DataType::Utf8, false;
        "domain_id" => DataType::Utf8, false;
        "domain_display_name" => DataType::Utf8, false;
        "description" => DataType::Utf8;
        "keywords" => DataType::Utf8;
        "works_api_url" => DataType::Utf8;
        "wikipedia_id" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "updated_date" => DataType::Utf8;
        "siblings" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, id),
        string_array_required!(records, display_name),
        string_array_required!(records, subfield_id),
        string_array_required!(records, subfield_display_name),
        string_array_required!(records, field_id),
        string_array_required!(records, field_display_name),
        string_array_required!(records, domain_id),
        string_array_required!(records, domain_display_name),
        string_array_optional!(records, description),
        string_array_optional!(records, keywords),
        string_array_optional!(records, works_api_url),
        string_array_optional!(records, wikipedia_id),
        int64_array!(records, works_count),
        int64_array!(records, cited_by_count),
        string_array_optional!(records, updated_date),
        string_array_optional!(records, siblings),
    )?;

    Ok(batch)
}

fn works_to_record_batch(records: Vec<WorkRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "id" => DataType::Utf8, false;
        "doi" => DataType::Utf8;
        "title" => DataType::LargeUtf8;
        "display_name" => DataType::LargeUtf8, false;
        "publication_year" => DataType::Int32;
        "publication_date" => DataType::Utf8;
        "type" => DataType::Utf8;
        "cited_by_count" => DataType::Int64, false;
        "is_retracted" => DataType::Boolean, false;
        "is_paratext" => DataType::Boolean;
        "cited_by_api_url" => DataType::Utf8;
        "abstract_inverted_index" => DataType::LargeUtf8;
        "language" => DataType::Utf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, id),
        string_array_optional!(records, doi),
        large_string_array_optional!(records, title),
        large_string_array_required!(records, display_name),
        int32_array_optional!(records, publication_year),
        string_array_optional!(records, publication_date),
        string_array_optional!(records, type_),
        int64_array!(records, cited_by_count),
        bool_array_required!(records, is_retracted),
        bool_array_optional!(records, is_paratext),
        string_array_optional!(records, cited_by_api_url),
        large_string_array_optional!(records, abstract_inverted_index),
        string_array_optional!(records, language),
    )?;

    Ok(batch)
}

fn work_locations_to_record_batch(records: Vec<WorkLocationRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "work_id" => DataType::Utf8, false;
        "source_id" => DataType::Utf8;
        "landing_page_url" => DataType::LargeUtf8;
        "pdf_url" => DataType::LargeUtf8;
        "is_oa" => DataType::Boolean;
        "version" => DataType::Utf8;
        "license" => DataType::Utf8;
        "location_type" => DataType::Utf8, false;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, work_id),
        string_array_optional!(records, source_id),
        large_string_array_optional!(records, landing_page_url),
        large_string_array_optional!(records, pdf_url),
        bool_array_optional!(records, is_oa),
        string_array_optional!(records, version),
        string_array_optional!(records, license),
        string_array_required!(records, location_type),
    )?;

    Ok(batch)
}

fn citations_to_record_batch(records: Vec<CitationRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "work_id" => DataType::Utf8, false;
        "referenced_work_id" => DataType::Utf8, false;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, work_id),
        string_array_required!(records, referenced_work_id),
    )?;

    Ok(batch)
}

fn authorships_to_record_batch(records: Vec<WorkAuthorshipRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "work_id" => DataType::Utf8, false;
        "author_position" => DataType::Utf8, false;
        "author_id" => DataType::Utf8, false;
        "institution_id" => DataType::Utf8;
        "raw_affiliation_string" => DataType::LargeUtf8;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, work_id),
        string_array_required!(records, author_position),
        string_array_required!(records, author_id),
        string_array_optional!(records, institution_id),
        large_string_array_optional!(records, raw_affiliation_string),
    )?;

    Ok(batch)
}

fn work_topics_to_record_batch(records: Vec<WorkTopicRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "work_id" => DataType::Utf8, false;
        "topic_id" => DataType::Utf8, false;
        "score" => DataType::Float64;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, work_id),
        string_array_required!(records, topic_id),
        float64_array_optional!(records, score),
    )?;

    Ok(batch)
}

fn work_open_access_to_record_batch(records: Vec<WorkOpenAccessRecord>) -> Result<RecordBatch> {
    let schema = schema! {
        "work_id" => DataType::Utf8, false;
        "is_oa" => DataType::Boolean;
        "oa_status" => DataType::Utf8;
        "oa_url" => DataType::Utf8;
        "any_repository_has_fulltext" => DataType::Boolean;
    };

    let batch = record_batch!(
        schema,
        string_array_required!(records, work_id),
        bool_array_optional!(records, is_oa),
        string_array_optional!(records, oa_status),
        string_array_optional!(records, oa_url),
        bool_array_optional!(records, any_repository_has_fulltext),
    )?;

    Ok(batch)
}

fn write_parquet_batch<T: 'static + Send + Sync>(
    writer: &mut ArrowWriter<File>,
    records: Vec<T>,
    to_record_batch: fn(Vec<T>) -> Result<RecordBatch>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let batch = to_record_batch(records)?;
    writer.write(&batch)?;
    Ok(())
}

// ====== AUTHORS PROCESSING (PARALLEL VERSION FOR 250-CORE MACHINE) ======
pub fn process_authors(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("Processing authors to Parquet with parallelization");

    // Create schemas
    let authors_schema = schema! {
        "id" => DataType::Utf8, false;
        "orcid" => DataType::Utf8;
        "display_name" => DataType::Utf8, false;
        "display_name_alternatives" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "last_known_institution" => DataType::Utf8;
        "works_api_url" => DataType::Utf8;
        "updated_date" => DataType::Utf8;
    };

    let ids_schema = schema! {
        "author_id" => DataType::Utf8, false;
        "openalex" => DataType::Utf8;
        "orcid" => DataType::Utf8;
        "scopus" => DataType::Utf8;
        "twitter" => DataType::Utf8;
        "wikipedia" => DataType::Utf8;
        "mag" => DataType::Utf8;
    };

    let counts_schema = schema! {
        "author_id" => DataType::Utf8, false;
        "year" => DataType::Int32, false;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "oa_works_count" => DataType::Int64, false;
    };

    // Create writers (thread-safe)
    let authors_writer = create_writer!(&output_dir.join("authors.parquet"), authors_schema);
    let ids_writer = create_writer!(&output_dir.join("authors_ids.parquet"), ids_schema);
    let counts_writer = create_writer!(
        &output_dir.join("authors_counts_by_year.parquet"),
        counts_schema
    );

    let files = find_entity_files(input_dir, "authors")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} authors files ({eta})",
    )?);

    // Use your 150GB RAM! Create ABSOLUTELY MASSIVE buffers
    let authors_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size * 10)));
    let ids_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size * 10)));
    let counts_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size * 50)));

    let seen_ids = Arc::new(Mutex::new(HashSet::with_capacity(50_000_000))); // 50M capacity with your massive RAM
    let processed_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

    info!(
        "Processing {} files with {} cores",
        files_to_process.len(),
        num_cpus::get()
    );

    // Parallel file processing
    files_to_process
        .par_iter()
        .try_for_each(|file_path| -> Result<()> {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            let reader = BufReader::with_capacity(2 * 1024 * 1024, decoder); // 2MB buffer per thread

            let mut local_authors = Vec::with_capacity(100000); // Much larger local buffers
            let mut local_ids = Vec::with_capacity(100000);
            let mut local_counts = Vec::with_capacity(500000);
            let mut local_processed = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let author: Value = serde_json::from_str(&line)?;

                if let Some(author_id) = author.get("id").and_then(|v| v.as_str()) {
                    // Quick local check, then global check only if needed
                    let should_process = {
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.contains(author_id) {
                            false
                        } else {
                            seen.insert(author_id.to_string());
                            true
                        }
                    };

                    if !should_process {
                        continue;
                    }

                    // Main author record
                    let display_name_alternatives =
                        extract_array_as_string!(author, "display_name_alternatives");
                    let last_known_institution =
                        extract_nested_string!(author, "last_known_institution", "id");

                    let author_record = AuthorRecord {
                        id: author_id.to_string(),
                        orcid: extract_string!(author, "orcid"),
                        display_name: extract_string_required!(author, "display_name", "Unknown"),
                        display_name_alternatives,
                        works_count: extract_i64!(author, "works_count", 0),
                        cited_by_count: extract_i64!(author, "cited_by_count", 0),
                        last_known_institution,
                        works_api_url: extract_string!(author, "works_api_url"),
                        updated_date: extract_string!(author, "updated_date"),
                    };

                    local_authors.push(author_record);

                    // IDs record
                    if let Some(ids) = author.get("ids") {
                        let ids_record = AuthorIdsRecord {
                            author_id: author_id.to_string(),
                            openalex: ids
                                .get("openalex")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            orcid: ids
                                .get("orcid")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            scopus: ids
                                .get("scopus")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            twitter: ids
                                .get("twitter")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            wikipedia: ids
                                .get("wikipedia")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            mag: ids
                                .get("mag")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                        };
                        local_ids.push(ids_record);
                    }

                    // Counts by year records
                    if let Some(counts_by_year) =
                        author.get("counts_by_year").and_then(|v| v.as_array())
                    {
                        for count_by_year in counts_by_year {
                            if let Some(year) = extract_i64_optional!(count_by_year, "year") {
                                let count_record = AuthorCountsByYearRecord {
                                    author_id: author_id.to_string(),
                                    year: year as i32,
                                    works_count: extract_i64!(count_by_year, "works_count", 0),
                                    cited_by_count: extract_i64!(
                                        count_by_year,
                                        "cited_by_count",
                                        0
                                    ),
                                    oa_works_count: extract_i64!(
                                        count_by_year,
                                        "oa_works_count",
                                        0
                                    ),
                                };
                                local_counts.push(count_record);
                            }
                        }
                    }

                    local_processed += 1;

                    // Batch writes when local buffers get large (use your RAM!)
                    flush_local_buffer!(
                        local_authors,
                        authors_buffer,
                        authors_writer,
                        batch_size,
                        authors_to_record_batch,
                        50000
                    );
                    flush_local_buffer!(
                        local_ids,
                        ids_buffer,
                        ids_writer,
                        batch_size,
                        author_ids_to_record_batch,
                        50000
                    );
                    flush_local_buffer!(
                        local_counts,
                        counts_buffer,
                        counts_writer,
                        batch_size * 10,
                        author_counts_by_year_to_record_batch,
                        100000
                    );
                }
            }

            // Flush remaining local data
            final_flush!(
                local_authors,
                authors_buffer,
                authors_writer,
                authors_to_record_batch
            );
            final_flush!(
                local_ids,
                ids_buffer,
                ids_writer,
                author_ids_to_record_batch
            );
            final_flush!(
                local_counts,
                counts_buffer,
                counts_writer,
                author_counts_by_year_to_record_batch
            );

            let total_processed =
                processed_count.fetch_add(local_processed, Ordering::Relaxed) + local_processed;
            progress.inc(1);

            if total_processed % 1_000_000 == 0 {
                info!(
                    "🔥 Processed {} authors across all threads! Memory: {}",
                    total_processed,
                    get_memory_usage()
                );
            }

            stats
                .authors_processed
                .fetch_add(local_processed, Ordering::Relaxed);
            stats.files_processed.fetch_add(1, Ordering::Relaxed);

            Ok(())
        })
        .unwrap();

    // Write remaining batches - use empty vec as placeholder since final_flush handles buffer checks
    final_flush!(
        Vec::<AuthorRecord>::new(),
        authors_buffer,
        authors_writer,
        authors_to_record_batch
    );
    final_flush!(
        Vec::<AuthorIdsRecord>::new(),
        ids_buffer,
        ids_writer,
        author_ids_to_record_batch
    );
    final_flush!(
        Vec::<AuthorCountsByYearRecord>::new(),
        counts_buffer,
        counts_writer,
        author_counts_by_year_to_record_batch
    );

    // Close writers by taking them out of Arc<Mutex<>>
    let authors_writer = Arc::try_unwrap(authors_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap authors_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock authors_writer: {:?}", e))?;
    authors_writer.close()?;

    let ids_writer = Arc::try_unwrap(ids_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap ids_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock ids_writer: {:?}", e))?;
    ids_writer.close()?;

    let counts_writer = Arc::try_unwrap(counts_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap counts_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock counts_writer: {:?}", e))?;
    counts_writer.close()?;

    progress.finish_with_message("Authors processing complete");

    let authors_count = stats.authors_processed.load(Ordering::Relaxed);
    info!(
        "Processed {} authors using {} cores",
        authors_count,
        num_cpus::get()
    );

    Ok(())
}

// ====== INSTITUTIONS PROCESSING ======
pub fn process_institutions(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("Processing institutions to Parquet");

    // Create schemas
    let institutions_schema = schema! {
        "id" => DataType::Utf8, false;
        "ror" => DataType::Utf8;
        "display_name" => DataType::Utf8, false;
        "country_code" => DataType::Utf8;
        "type" => DataType::Utf8;
        "homepage_url" => DataType::Utf8;
        "image_url" => DataType::Utf8;
        "image_thumbnail_url" => DataType::Utf8;
        "display_name_acronyms" => DataType::Utf8;
        "display_name_alternatives" => DataType::Utf8;
        "works_count" => DataType::Int64, false;
        "cited_by_count" => DataType::Int64, false;
        "works_api_url" => DataType::Utf8;
        "updated_date" => DataType::Utf8;
    };

    let geo_schema = Schema::new(vec![
        Field::new("institution_id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, true),
        Field::new("geonames_city_id", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("country_code", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
    ]);

    // Create writers
    let mut institutions_writer = create_parquet_writer(
        &output_dir.join("institutions.parquet"),
        institutions_schema,
    )?;
    let mut geo_writer =
        create_parquet_writer(&output_dir.join("institutions_geo.parquet"), geo_schema)?;

    // Create batches
    let mut institutions_batch = Vec::with_capacity(batch_size);
    let mut geo_batch = Vec::with_capacity(batch_size);

    let files = find_entity_files(input_dir, "institutions")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} institutions files ({eta})",
    )?);

    let mut seen_ids = HashSet::new();

    for file_path in files_to_process {
        let file = File::open(file_path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let institution: Value = serde_json::from_str(&line)?;

            if let Some(institution_id) = institution.get("id").and_then(|v| v.as_str()) {
                if seen_ids.contains(institution_id) {
                    continue;
                }
                seen_ids.insert(institution_id.to_string());

                // Main institution record
                let display_name_acronyms = institution
                    .get("display_name_acronyms")
                    .map(|v| serde_json::to_string(v).unwrap_or_default());
                let display_name_alternatives = institution
                    .get("display_name_alternatives")
                    .map(|v| serde_json::to_string(v).unwrap_or_default());

                let record = InstitutionRecord {
                    id: institution_id.to_string(),
                    ror: institution
                        .get("ror")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    display_name: institution
                        .get("display_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    country_code: institution
                        .get("country_code")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    type_: institution
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    homepage_url: institution
                        .get("homepage_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    image_url: institution
                        .get("image_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    image_thumbnail_url: institution
                        .get("image_thumbnail_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    display_name_acronyms,
                    display_name_alternatives,
                    works_count: institution
                        .get("works_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    cited_by_count: institution
                        .get("cited_by_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    works_api_url: institution
                        .get("works_api_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    updated_date: institution
                        .get("updated_date")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                };

                institutions_batch.push(record);

                // Geo record
                if let Some(geo) = institution.get("geo") {
                    let geo_record = InstitutionGeoRecord {
                        institution_id: institution_id.to_string(),
                        city: geo
                            .get("city")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        geonames_city_id: geo
                            .get("geonames_city_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        region: geo
                            .get("region")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        country_code: geo
                            .get("country_code")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        country: geo
                            .get("country")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        latitude: geo.get("latitude").and_then(|v| v.as_f64()),
                        longitude: geo.get("longitude").and_then(|v| v.as_f64()),
                    };
                    geo_batch.push(geo_record);
                }

                // Write batches when they get large enough
                if institutions_batch.len() >= batch_size {
                    write_parquet_batch(
                        &mut institutions_writer,
                        std::mem::take(&mut institutions_batch),
                        institutions_to_record_batch,
                    )?;
                }
                if geo_batch.len() >= batch_size {
                    write_parquet_batch(
                        &mut geo_writer,
                        std::mem::take(&mut geo_batch),
                        institution_geo_to_record_batch,
                    )?;
                }

                stats.institutions_processed.fetch_add(1, Ordering::Relaxed);
            }
        }

        progress.inc(1);
        stats.files_processed.fetch_add(1, Ordering::Relaxed);
    }

    // Write remaining batches
    if !institutions_batch.is_empty() {
        write_parquet_batch(
            &mut institutions_writer,
            institutions_batch,
            institutions_to_record_batch,
        )?;
    }
    if !geo_batch.is_empty() {
        write_parquet_batch(&mut geo_writer, geo_batch, institution_geo_to_record_batch)?;
    }

    // Close writers
    institutions_writer.close()?;
    geo_writer.close()?;

    progress.finish_with_message("Institutions processing complete");

    let institutions_count = stats.institutions_processed.load(Ordering::Relaxed);
    info!("Processed {} institutions to Parquet", institutions_count);

    Ok(())
}

// ====== PUBLISHERS PROCESSING ======
pub fn process_publishers(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("📰 Processing Publishers to Parquet...");

    let publishers_schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("display_name", DataType::Utf8, false),
        Field::new("alternate_titles", DataType::Utf8, true),
        Field::new("country_codes", DataType::Utf8, true),
        Field::new("hierarchy_level", DataType::Int32, true),
        Field::new("parent_publisher", DataType::Utf8, true),
        Field::new("works_count", DataType::Int64, false),
        Field::new("cited_by_count", DataType::Int64, false),
        Field::new("sources_api_url", DataType::Utf8, true),
        Field::new("updated_date", DataType::Utf8, true),
    ]);

    let mut publishers_writer =
        create_parquet_writer(&output_dir.join("publishers.parquet"), publishers_schema)?;
    let mut publishers_batch = Vec::with_capacity(batch_size);

    let files = find_entity_files(input_dir, "publishers")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} publishers files ({eta})",
    )?);

    let mut seen_ids = HashSet::new();

    for file_path in files_to_process {
        let file = File::open(file_path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let publisher: Value = serde_json::from_str(&line)?;

            if let Some(publisher_id) = publisher.get("id").and_then(|v| v.as_str()) {
                if seen_ids.contains(publisher_id) {
                    continue;
                }
                seen_ids.insert(publisher_id.to_string());

                let alternate_titles = publisher
                    .get("alternate_titles")
                    .map(|v| serde_json::to_string(v).unwrap_or_default());
                let country_codes = publisher
                    .get("country_codes")
                    .map(|v| serde_json::to_string(v).unwrap_or_default());

                let record = PublisherRecord {
                    id: publisher_id.to_string(),
                    display_name: publisher
                        .get("display_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    alternate_titles,
                    country_codes,
                    hierarchy_level: publisher
                        .get("hierarchy_level")
                        .and_then(|v| v.as_i64())
                        .map(|i| i as i32),
                    parent_publisher: publisher
                        .get("parent_publisher")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    works_count: publisher
                        .get("works_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    cited_by_count: publisher
                        .get("cited_by_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    sources_api_url: publisher
                        .get("sources_api_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    updated_date: publisher
                        .get("updated_date")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                };

                publishers_batch.push(record);

                if publishers_batch.len() >= batch_size {
                    write_parquet_batch(
                        &mut publishers_writer,
                        std::mem::take(&mut publishers_batch),
                        publishers_to_record_batch,
                    )?;
                }

                stats.publishers_processed.fetch_add(1, Ordering::Relaxed);
            }
        }

        progress.inc(1);
        stats.files_processed.fetch_add(1, Ordering::Relaxed);
    }

    // Write remaining batch
    if !publishers_batch.is_empty() {
        write_parquet_batch(
            &mut publishers_writer,
            publishers_batch,
            publishers_to_record_batch,
        )?;
    }

    publishers_writer.close()?;
    progress.finish_with_message("Publishers processing complete");

    let publishers_count = stats.publishers_processed.load(Ordering::Relaxed);
    info!("Processed {} publishers to Parquet", publishers_count);

    Ok(())
}

// ====== TOPICS PROCESSING ======
pub fn process_topics(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("🏷️ Processing Topics to Parquet...");

    let topics_schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("display_name", DataType::Utf8, false),
        Field::new("subfield_id", DataType::Utf8, false),
        Field::new("subfield_display_name", DataType::Utf8, false),
        Field::new("field_id", DataType::Utf8, false),
        Field::new("field_display_name", DataType::Utf8, false),
        Field::new("domain_id", DataType::Utf8, false),
        Field::new("domain_display_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("keywords", DataType::Utf8, true),
        Field::new("works_api_url", DataType::Utf8, true),
        Field::new("wikipedia_id", DataType::Utf8, true),
        Field::new("works_count", DataType::Int64, false),
        Field::new("cited_by_count", DataType::Int64, false),
        Field::new("updated_date", DataType::Utf8, true),
        Field::new("siblings", DataType::Utf8, true),
    ]);

    let mut topics_writer =
        create_parquet_writer(&output_dir.join("topics.parquet"), topics_schema)?;
    let mut topics_batch = Vec::with_capacity(batch_size);

    let files = find_entity_files(input_dir, "topics")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} topics files ({eta})",
    )?);

    let mut seen_ids = HashSet::new();

    for file_path in files_to_process {
        let file = File::open(file_path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let topic: Value = serde_json::from_str(&line)?;

            if let Some(topic_id) = topic.get("id").and_then(|v| v.as_str()) {
                if seen_ids.contains(topic_id) {
                    continue;
                }
                seen_ids.insert(topic_id.to_string());

                // Extract keywords and join them
                let keywords = topic.get("keywords").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                });

                let siblings = topic
                    .get("siblings")
                    .map(|v| serde_json::to_string(v).unwrap_or_default());

                let record = TopicRecord {
                    id: topic_id.to_string(),
                    display_name: topic
                        .get("display_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    subfield_id: topic
                        .get("subfield")
                        .and_then(|v| v.get("id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    subfield_display_name: topic
                        .get("subfield")
                        .and_then(|v| v.get("display_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    field_id: topic
                        .get("field")
                        .and_then(|v| v.get("id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    field_display_name: topic
                        .get("field")
                        .and_then(|v| v.get("display_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    domain_id: topic
                        .get("domain")
                        .and_then(|v| v.get("id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    domain_display_name: topic
                        .get("domain")
                        .and_then(|v| v.get("display_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    description: topic
                        .get("description")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    keywords,
                    works_api_url: topic
                        .get("works_api_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    wikipedia_id: topic
                        .get("ids")
                        .and_then(|v| v.get("wikipedia"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    works_count: topic
                        .get("works_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    cited_by_count: topic
                        .get("cited_by_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    updated_date: topic
                        .get("updated")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    siblings,
                };

                topics_batch.push(record);

                if topics_batch.len() >= batch_size {
                    write_parquet_batch(
                        &mut topics_writer,
                        std::mem::take(&mut topics_batch),
                        topics_to_record_batch,
                    )?;
                }

                stats.topics_processed.fetch_add(1, Ordering::Relaxed);
            }
        }

        progress.inc(1);
        stats.files_processed.fetch_add(1, Ordering::Relaxed);
    }

    // Write remaining batch
    if !topics_batch.is_empty() {
        write_parquet_batch(&mut topics_writer, topics_batch, topics_to_record_batch)?;
    }

    topics_writer.close()?;
    progress.finish_with_message("Topics processing complete");

    let topics_count = stats.topics_processed.load(Ordering::Relaxed);
    info!("Processed {} topics to Parquet", topics_count);

    Ok(())
}

// ====== WORKS PROCESSING (Optimized for Citation Analysis) ======
pub fn process_works(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("📚 Processing Works to Parquet...");

    // Create schemas
    let works_schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("doi", DataType::Utf8, true),
        Field::new("title", DataType::LargeUtf8, true), // Use LargeUtf8 for potentially large titles
        Field::new("display_name", DataType::LargeUtf8, false), // Use LargeUtf8 for display names
        Field::new("publication_year", DataType::Int32, true),
        Field::new("publication_date", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("cited_by_count", DataType::Int64, false),
        Field::new("is_retracted", DataType::Boolean, false),
        Field::new("is_paratext", DataType::Boolean, true),
        Field::new("cited_by_api_url", DataType::Utf8, true),
        Field::new("abstract_inverted_index", DataType::LargeUtf8, true), // Use LargeUtf8 for abstracts
        Field::new("language", DataType::Utf8, true),
    ]);

    let locations_schema = Schema::new(vec![
        Field::new("work_id", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, true),
        Field::new("landing_page_url", DataType::LargeUtf8, true), // URLs can be very long
        Field::new("pdf_url", DataType::LargeUtf8, true),          // URLs can be very long
        Field::new("is_oa", DataType::Boolean, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("license", DataType::Utf8, true),
        Field::new("location_type", DataType::Utf8, false),
    ]);

    let authorships_schema = Schema::new(vec![
        Field::new("work_id", DataType::Utf8, false),
        Field::new("author_position", DataType::Utf8, false),
        Field::new("author_id", DataType::Utf8, false),
        Field::new("institution_id", DataType::Utf8, true),
        Field::new("raw_affiliation_string", DataType::LargeUtf8, true), // Can be very long
    ]);

    let topics_schema = Schema::new(vec![
        Field::new("work_id", DataType::Utf8, false),
        Field::new("topic_id", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]);

    let open_access_schema = Schema::new(vec![
        Field::new("work_id", DataType::Utf8, false),
        Field::new("is_oa", DataType::Boolean, true),
        Field::new("oa_status", DataType::Utf8, true),
        Field::new("oa_url", DataType::Utf8, true),
        Field::new("any_repository_has_fulltext", DataType::Boolean, true),
    ]);

    let citations_schema = Schema::new(vec![
        Field::new("work_id", DataType::Utf8, false),
        Field::new("referenced_work_id", DataType::Utf8, false),
    ]);

    // Create writers
    let mut works_writer = create_parquet_writer(&output_dir.join("works.parquet"), works_schema)?;
    let mut locations_writer = create_parquet_writer(
        &output_dir.join("works_locations.parquet"),
        locations_schema,
    )?;
    let mut authorships_writer = create_parquet_writer(
        &output_dir.join("works_authorships.parquet"),
        authorships_schema,
    )?;
    let mut topics_writer =
        create_parquet_writer(&output_dir.join("works_topics.parquet"), topics_schema)?;
    let mut open_access_writer = create_parquet_writer(
        &output_dir.join("works_open_access.parquet"),
        open_access_schema,
    )?;
    let mut citations_writer = create_parquet_writer(
        &output_dir.join("works_citations.parquet"),
        citations_schema,
    )?;

    // Create batches - Use much smaller batch sizes for works due to large text fields
    let works_batch_size = std::cmp::min(batch_size / 10, 50000); // Much smaller batch for works
    let mut works_batch = Vec::with_capacity(works_batch_size);
    let mut locations_batch = Vec::with_capacity(works_batch_size * 3);
    let mut authorships_batch = Vec::with_capacity(works_batch_size * 3);
    let mut topics_batch = Vec::with_capacity(works_batch_size * 5);
    let mut open_access_batch = Vec::with_capacity(works_batch_size);
    let mut citations_batch = Vec::with_capacity(works_batch_size * 10);

    let files = find_entity_files(input_dir, "works")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let num_cpus = num_cpus::get();

    info!(
        "Processing works to Parquet - {} files with {} threads",
        files_to_process.len(),
        num_cpus
    );

    let progress = Arc::new(Mutex::new(ProgressBar::new(files_to_process.len() as u64)));
    progress
        .lock()
        .unwrap()
        .set_style(ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} works files ({eta}) [{msg}]",
        )?);

    // Process files in parallel chunks
    let chunk_size = std::cmp::max(1, files_to_process.len() / num_cpus);
    let file_chunks: Vec<_> = files_to_process.chunks(chunk_size).collect();

    // Use crossbeam for parallel processing
    crossbeam::scope(|s| {
        let mut handles = Vec::new();

        for (_chunk_idx, file_chunk) in file_chunks.into_iter().enumerate() {
            let progress_clone = Arc::clone(&progress);

            let handle = s.spawn(
                move |_| -> Result<(
                    Vec<WorkRecord>,
                    Vec<WorkLocationRecord>,
                    Vec<WorkAuthorshipRecord>,
                    Vec<WorkTopicRecord>,
                    Vec<WorkOpenAccessRecord>,
                    Vec<CitationRecord>,
                )> {
                    let mut local_works = Vec::new();
                    let mut local_locations = Vec::new();
                    let mut local_authorships = Vec::new();
                    let mut local_topics = Vec::new();
                    let mut local_open_access = Vec::new();
                    let mut local_citations = Vec::new();

                    for file_path in file_chunk {
                        let file = File::open(file_path)?;
                        let decoder = GzDecoder::new(file);
                        let reader = BufReader::new(decoder);

                        for line in reader.lines() {
                            let line = line?;
                            if line.trim().is_empty() {
                                continue;
                            }

                            let work: Value = serde_json::from_str(&line)?;

                            if let Some(work_id) = work.get("id").and_then(|v| v.as_str()) {
                                // Truncate very long titles and abstracts to prevent memory issues
                                let title = work
                                    .get("title")
                                    .and_then(|v| v.as_str())
                                    .map(|s| safe_truncate(s, 10000));

                                let display_name = work
                                    .get("display_name")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let display_name = safe_truncate(display_name, 5000);

                                let abstract_inverted_index =
                                    work.get("abstract_inverted_index").map(|v| {
                                        let s = serde_json::to_string(v).unwrap_or_default();
                                        safe_truncate(&s, 50000)
                                    });

                                // Main work record
                                let work_record = WorkRecord {
                                    id: work_id.to_string(),
                                    doi: work
                                        .get("doi")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                    title,
                                    display_name,
                                    publication_year: work
                                        .get("publication_year")
                                        .and_then(|v| v.as_i64())
                                        .map(|y| y as i32),
                                    publication_date: work
                                        .get("publication_date")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                    type_: work
                                        .get("type")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                    cited_by_count: work
                                        .get("cited_by_count")
                                        .and_then(|v| v.as_i64())
                                        .unwrap_or(0),
                                    is_retracted: work
                                        .get("is_retracted")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(false),
                                    is_paratext: work.get("is_paratext").and_then(|v| v.as_bool()),
                                    cited_by_api_url: work
                                        .get("cited_by_api_url")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                    abstract_inverted_index,
                                    language: work
                                        .get("language")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                };

                                local_works.push(work_record);

                                // Process primary location
                                if let Some(primary_location) = work.get("primary_location") {
                                    if let Some(source_id) = primary_location
                                        .get("source")
                                        .and_then(|s| s.get("id"))
                                        .and_then(|v| v.as_str())
                                    {
                                        let location_record = WorkLocationRecord {
                                            work_id: work_id.to_string(),
                                            source_id: Some(source_id.to_string()),
                                            landing_page_url: primary_location
                                                .get("landing_page_url")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            pdf_url: primary_location
                                                .get("pdf_url")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            is_oa: primary_location
                                                .get("is_oa")
                                                .and_then(|v| v.as_bool()),
                                            version: primary_location
                                                .get("version")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            license: primary_location
                                                .get("license")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            location_type: "primary".to_string(),
                                        };
                                        local_locations.push(location_record);
                                    }
                                }

                                // Process all locations
                                if let Some(locations) =
                                    work.get("locations").and_then(|v| v.as_array())
                                {
                                    for location in locations {
                                        if let Some(source_id) = location
                                            .get("source")
                                            .and_then(|s| s.get("id"))
                                            .and_then(|v| v.as_str())
                                        {
                                            let location_record = WorkLocationRecord {
                                                work_id: work_id.to_string(),
                                                source_id: Some(source_id.to_string()),
                                                landing_page_url: location
                                                    .get("landing_page_url")
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string()),
                                                pdf_url: location
                                                    .get("pdf_url")
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string()),
                                                is_oa: location
                                                    .get("is_oa")
                                                    .and_then(|v| v.as_bool()),
                                                version: location
                                                    .get("version")
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string()),
                                                license: location
                                                    .get("license")
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string()),
                                                location_type: "location".to_string(),
                                            };
                                            local_locations.push(location_record);
                                        }
                                    }
                                }

                                // Process best OA location
                                if let Some(best_oa_location) = work.get("best_oa_location") {
                                    if let Some(source_id) = best_oa_location
                                        .get("source")
                                        .and_then(|s| s.get("id"))
                                        .and_then(|v| v.as_str())
                                    {
                                        let location_record = WorkLocationRecord {
                                            work_id: work_id.to_string(),
                                            source_id: Some(source_id.to_string()),
                                            landing_page_url: best_oa_location
                                                .get("landing_page_url")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            pdf_url: best_oa_location
                                                .get("pdf_url")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            is_oa: best_oa_location
                                                .get("is_oa")
                                                .and_then(|v| v.as_bool()),
                                            version: best_oa_location
                                                .get("version")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            license: best_oa_location
                                                .get("license")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string()),
                                            location_type: "best_oa".to_string(),
                                        };
                                        local_locations.push(location_record);
                                    }
                                }

                                // Process authorships (crucial for citation fairness analysis)
                                if let Some(authorships) =
                                    work.get("authorships").and_then(|v| v.as_array())
                                {
                                    for authorship in authorships {
                                        if let Some(author_id) = authorship
                                            .get("author")
                                            .and_then(|a| a.get("id"))
                                            .and_then(|v| v.as_str())
                                        {
                                            let empty_vec = vec![];
                                            let institutions = authorship
                                                .get("institutions")
                                                .and_then(|v| v.as_array())
                                                .unwrap_or(&empty_vec);
                                            let institution_ids: Vec<String> = institutions
                                                .iter()
                                                .filter_map(|i| {
                                                    i.get("id")
                                                        .and_then(|v| v.as_str())
                                                        .map(|s| s.to_string())
                                                })
                                                .collect();

                                            // If no institutions, create one record with null institution
                                            if institution_ids.is_empty() {
                                                let authorship_record = WorkAuthorshipRecord {
                                                    work_id: work_id.to_string(),
                                                    author_position: authorship
                                                        .get("author_position")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("unknown")
                                                        .to_string(),
                                                    author_id: author_id.to_string(),
                                                    institution_id: None,
                                                    raw_affiliation_string: authorship
                                                        .get("raw_affiliation_string")
                                                        .and_then(|v| v.as_str())
                                                        .map(|s| s.to_string()),
                                                };
                                                local_authorships.push(authorship_record);
                                            } else {
                                                // Create record for each institution
                                                for institution_id in institution_ids {
                                                    let authorship_record = WorkAuthorshipRecord {
                                                        work_id: work_id.to_string(),
                                                        author_position: authorship
                                                            .get("author_position")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("unknown")
                                                            .to_string(),
                                                        author_id: author_id.to_string(),
                                                        institution_id: Some(institution_id),
                                                        raw_affiliation_string: authorship
                                                            .get("raw_affiliation_string")
                                                            .and_then(|v| v.as_str())
                                                            .map(|s| s.to_string()),
                                                    };
                                                    local_authorships.push(authorship_record);
                                                }
                                            }
                                        }
                                    }
                                }

                                // Process topics
                                if let Some(topics) = work.get("topics").and_then(|v| v.as_array())
                                {
                                    for topic in topics {
                                        if let Some(topic_id) =
                                            topic.get("id").and_then(|v| v.as_str())
                                        {
                                            let topic_record = WorkTopicRecord {
                                                work_id: work_id.to_string(),
                                                topic_id: topic_id.to_string(),
                                                score: topic.get("score").and_then(|v| v.as_f64()),
                                            };
                                            local_topics.push(topic_record);
                                        }
                                    }
                                }

                                // Process open access information
                                if let Some(open_access) = work.get("open_access") {
                                    let oa_record = WorkOpenAccessRecord {
                                        work_id: work_id.to_string(),
                                        is_oa: open_access.get("is_oa").and_then(|v| v.as_bool()),
                                        oa_status: open_access
                                            .get("oa_status")
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.to_string()),
                                        oa_url: open_access
                                            .get("oa_url")
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.to_string()),
                                        any_repository_has_fulltext: open_access
                                            .get("any_repository_has_fulltext")
                                            .and_then(|v| v.as_bool()),
                                    };
                                    local_open_access.push(oa_record);
                                }

                                // Process citations (core of citation fairness analysis)
                                if let Some(referenced_works) =
                                    work.get("referenced_works").and_then(|v| v.as_array())
                                {
                                    for referenced_work in referenced_works {
                                        if let Some(referenced_work_id) = referenced_work.as_str() {
                                            let citation_record = CitationRecord {
                                                work_id: work_id.to_string(),
                                                referenced_work_id: referenced_work_id.to_string(),
                                            };
                                            local_citations.push(citation_record);
                                        }
                                    }
                                }
                            }
                        }

                        // Update progress
                        progress_clone.lock().unwrap().inc(1);
                    }

                    Ok((
                        local_works,
                        local_locations,
                        local_authorships,
                        local_topics,
                        local_open_access,
                        local_citations,
                    ))
                },
            );

            handles.push(handle);
        }

        // Collect results from all threads
        for handle in handles {
            match handle.join() {
                Ok(Ok((works, locations, authorships, topics, open_access, citations))) => {
                    // Capture lengths before moving
                    let works_len = works.len() as u64;
                    let citations_len = citations.len() as u64;

                    // Add to main batches
                    works_batch.extend(works);
                    locations_batch.extend(locations);
                    authorships_batch.extend(authorships);
                    topics_batch.extend(topics);
                    open_access_batch.extend(open_access);
                    citations_batch.extend(citations);

                    // Write batches when they get large
                    write_batch_if_needed!(
                        works_batch,
                        works_writer,
                        works_batch_size * 4,
                        works_to_record_batch,
                        "works"
                    );
                    write_batch_if_needed!(
                        locations_batch,
                        locations_writer,
                        works_batch_size * 12,
                        work_locations_to_record_batch,
                        "locations"
                    );
                    write_batch_if_needed!(
                        authorships_batch,
                        authorships_writer,
                        works_batch_size * 12,
                        authorships_to_record_batch,
                        "authorships"
                    );
                    write_batch_if_needed!(
                        topics_batch,
                        topics_writer,
                        works_batch_size * 20,
                        work_topics_to_record_batch,
                        "topics"
                    );
                    write_batch_if_needed!(
                        open_access_batch,
                        open_access_writer,
                        works_batch_size * 4,
                        work_open_access_to_record_batch,
                        "open access"
                    );
                    write_batch_if_needed!(
                        citations_batch,
                        citations_writer,
                        works_batch_size * 40,
                        citations_to_record_batch,
                        "citations"
                    );

                    // Update stats
                    stats
                        .works_processed
                        .fetch_add(works_len, Ordering::Relaxed);
                    stats
                        .citation_edges
                        .fetch_add(citations_len, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    eprintln!("Error in thread: {}", e);
                    std::process::exit(1);
                }
                Err(_) => {
                    eprintln!("Thread panicked during works processing");
                    std::process::exit(1);
                }
            }
        }
    })
    .unwrap();

    // Write remaining batches
    if !works_batch.is_empty() {
        write_parquet_batch(&mut works_writer, works_batch, works_to_record_batch)?;
    }
    if !locations_batch.is_empty() {
        write_parquet_batch(
            &mut locations_writer,
            locations_batch,
            work_locations_to_record_batch,
        )?;
    }
    if !authorships_batch.is_empty() {
        write_parquet_batch(
            &mut authorships_writer,
            authorships_batch,
            authorships_to_record_batch,
        )?;
    }
    if !topics_batch.is_empty() {
        write_parquet_batch(
            &mut topics_writer,
            topics_batch,
            work_topics_to_record_batch,
        )?;
    }
    if !open_access_batch.is_empty() {
        write_parquet_batch(
            &mut open_access_writer,
            open_access_batch,
            work_open_access_to_record_batch,
        )?;
    }
    if !citations_batch.is_empty() {
        write_parquet_batch(
            &mut citations_writer,
            citations_batch,
            citations_to_record_batch,
        )?;
    }

    // Close writers
    works_writer.close()?;
    locations_writer.close()?;
    authorships_writer.close()?;
    topics_writer.close()?;
    open_access_writer.close()?;
    citations_writer.close()?;

    progress
        .lock()
        .unwrap()
        .finish_with_message("Works processing complete");

    let works_count = stats.works_processed.load(Ordering::Relaxed);
    let citations_count = stats.citation_edges.load(Ordering::Relaxed);
    info!(
        "Processed {} works and {} citations to Parquet",
        works_count, citations_count
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    // Set up massive thread pool for your 126-core beast!
    let num_workers = args.workers.unwrap_or_else(|| {
        let cores = num_cpus::get();
        cores // Use all available cores - you've got 126!
    });

    info!(
        "Starting OpenAlex Academic Data Processor with {} workers",
        num_workers
    );
    info!("Batch size: {} records per batch", args.batch_size);
    info!(
        "Processing entities simultaneously across {} cores",
        num_workers
    );

    // Build thread pool optimized for your monster machine
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .stack_size(16 * 1024 * 1024) // 16MB stack per thread for massive data processing
        .thread_name(|i| format!("openalex-worker-{}", i))
        .build_global()?;

    // Create output directory
    let output_path = Path::new(&args.output_dir);
    create_dir_all(output_path)?;
    info!("Output directory: {}", output_path.display());

    let stats = ProcessingStats::new();
    let entities: Vec<&str> = args.entities.split(',').map(|s| s.trim()).collect();

    info!("Processing entities: {:?}", entities);
    info!("Output format: Parquet (optimized for PySpark analytics)");
    info!("Processing all {} entities simultaneously", entities.len());

    // Process all entities simultaneously with parallel processing
    let results: Result<Vec<_>> = entities
        .par_iter()
        .map(|&entity| -> Result<()> {
            match entity {
                "authors" => {
                    info!("Starting authors processing with {} cores", num_workers);
                    process_authors(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "institutions" => {
                    info!(
                        "🚀 Starting INSTITUTIONS processing with shared {} cores...",
                        num_workers
                    );
                    process_institutions(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "publishers" => {
                    info!(
                        "🚀 Starting PUBLISHERS processing with shared {} cores...",
                        num_workers
                    );
                    process_publishers(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "topics" => {
                    info!(
                        "🚀 Starting TOPICS processing with shared {} cores...",
                        num_workers
                    );
                    process_topics(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "works" => {
                    info!(
                        "🚀 Starting WORKS processing with shared {} cores...",
                        num_workers
                    );
                    process_works(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                _ => {
                    warn!("Unknown entity type: {}", entity);
                    Ok(())
                }
            }
        })
        .collect();

    // Check if all processing completed successfully
    results?;

    // Print final statistics
    info!("Final Processing Statistics:");
    info!(
        "  Authors processed: {}",
        stats.authors_processed.load(Ordering::Relaxed)
    );
    info!(
        "  Works processed: {}",
        stats.works_processed.load(Ordering::Relaxed)
    );
    info!(
        "  Institutions processed: {}",
        stats.institutions_processed.load(Ordering::Relaxed)
    );
    info!(
        "  Publishers processed: {}",
        stats.publishers_processed.load(Ordering::Relaxed)
    );
    info!(
        "  Topics processed: {}",
        stats.topics_processed.load(Ordering::Relaxed)
    );
    info!(
        "  Citation edges: {}",
        stats.citation_edges.load(Ordering::Relaxed)
    );
    info!(
        "  Files processed: {}",
        stats.files_processed.load(Ordering::Relaxed)
    );

    info!(
        "PySpark-ready Parquet datasets created successfully in: {}",
        args.output_dir
    );
    info!("Load in PySpark with:");
    info!("    spark.read.parquet('{}/*.parquet')", args.output_dir);
    info!("    # Example for citation analysis:");
    info!(
        "    works = spark.read.parquet('{}/works.parquet')",
        args.output_dir
    );
    info!(
        "    citations = spark.read.parquet('{}/works_citations.parquet')",
        args.output_dir
    );
    info!(
        "    authorships = spark.read.parquet('{}/works_authorships.parquet')",
        args.output_dir
    );

    Ok(())
}
