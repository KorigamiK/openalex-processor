use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clap::Parser;

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

// Macro for buffer flushing pattern used throughout processing functions
macro_rules! flush_local_buffer {
    ($local_buffer:expr, $global_buffer:expr, $writer:expr, $batch_size:expr, $converter:expr, $local_threshold:expr) => {
        if $local_buffer.len() >= $local_threshold {
            let mut buffer = $global_buffer.lock().unwrap();
            buffer.extend($local_buffer.drain(..)); // drain(..) clears the local buffer
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
#[command(about = "OpenAlex Academic Data Processor")]
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

// Extract memory in GB for consistent reporting
fn get_memory_gb() -> String {
    let memory_info = get_memory_usage();
    if memory_info.contains("VmRSS:") {
        // Split the line and look for the number
        let parts: Vec<&str> = memory_info.split_whitespace().collect();
        for (i, part) in parts.iter().enumerate() {
            if part == &"VmRSS:" && i + 1 < parts.len() {
                if let Ok(kb) = parts[i + 1].parse::<u64>() {
                    return format!("{:.1}GB", kb as f64 / 1_000_000.0);
                }
            }
        }
    }
    "?GB".to_string()
}

// Standardized progress update function
fn update_progress_with_memory(
    progress: &ProgressBar,
    entity_name: &str,
    processed_count: u64,
    files_done: u64,
    total_files: u64,
) {
    let memory_gb = get_memory_gb();
    let count_display = if processed_count >= 1_000_000 {
        format!("{:.1}M", processed_count as f64 / 1_000_000.0)
    } else if processed_count >= 1_000 {
        format!("{:.1}K", processed_count as f64 / 1_000.0)
    } else {
        processed_count.to_string()
    };
    
    progress.set_message(format!(
        "File {}/{} | {} {} | {} RAM",
        files_done, total_files, count_display, entity_name, memory_gb
    ));
}

// ====== UTILITY FUNCTIONS ======
fn create_parquet_writer(output_path: &Path, schema: Schema) -> Result<ArrowWriter<File>> {
    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_write_batch_size(5_000_000) // Large batch size for efficient processing
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

// ====== AUTHORS PROCESSING (PARALLEL VERSION) ======
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
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} files | {msg}",
    )?);
    progress.set_message("Starting authors processing...");
    
    // Initial progress update
    update_progress_with_memory(&progress, "authors", 0, 0, files_to_process.len() as u64);

    // Memory-efficient buffers - smaller but more frequent flushes
    let authors_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let ids_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let counts_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size * 2)));

    let seen_ids = Arc::new(Mutex::new(HashSet::with_capacity(1_000_000))); // Much smaller capacity
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

            let mut local_authors = Vec::with_capacity(5000); // Smaller local buffers
            let mut local_ids = Vec::with_capacity(5000);
            let mut local_counts = Vec::with_capacity(10000);
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

                    // More frequent batch writes with immediate buffer clearing
                    flush_local_buffer!(
                        local_authors,
                        authors_buffer,
                        authors_writer,
                        batch_size,
                        authors_to_record_batch,
                        2000
                    );
                    flush_local_buffer!(
                        local_ids,
                        ids_buffer,
                        ids_writer,
                        batch_size,
                        author_ids_to_record_batch,
                        2000
                    );
                    flush_local_buffer!(
                        local_counts,
                        counts_buffer,
                        counts_writer,
                        batch_size * 2,
                        author_counts_by_year_to_record_batch,
                        5000
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
            
            stats
                .authors_processed
                .fetch_add(local_processed, Ordering::Relaxed);
            stats.files_processed.fetch_add(1, Ordering::Relaxed);
            
            progress.inc(1);
            
            // Update progress bar for every file processed
            let files_done = stats.files_processed.load(Ordering::Relaxed);
            update_progress_with_memory(&progress, "authors", total_processed, files_done, files_to_process.len() as u64);

            if total_processed % 100_000 == 0 {
                info!(
                    "Processed {} authors across all threads! Memory: {}",
                    total_processed,
                    get_memory_gb()
                );
                
                // Periodic cleanup of seen_ids to prevent unbounded growth
                if total_processed % 5_000_000 == 0 {
                    let mut seen = seen_ids.lock().unwrap();
                    let old_size = seen.len();
                    seen.shrink_to_fit();
                    info!("Cleaned seen_ids: {} -> {} entries", old_size, seen.len());
                }
            }

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

    let geo_schema = schema! {
        "institution_id" => DataType::Utf8, false;
        "city" => DataType::Utf8;
        "geonames_city_id" => DataType::Utf8;
        "region" => DataType::Utf8;
        "country_code" => DataType::Utf8;
        "country" => DataType::Utf8;
        "latitude" => DataType::Float64;
        "longitude" => DataType::Float64;
    };

    // Create thread-safe writers
    let institutions_writer = create_writer!(&output_dir.join("institutions.parquet"), institutions_schema);
    let geo_writer = create_writer!(&output_dir.join("institutions_geo.parquet"), geo_schema);

    let files = find_entity_files(input_dir, "institutions")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} files | {msg}",
    )?);
    progress.set_message("Starting institutions processing...");
    
    // Initial progress update
    update_progress_with_memory(&progress, "institutions", 0, 0, files_to_process.len() as u64);

    // Thread-safe buffers and deduplication
    let institutions_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let geo_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let seen_ids = Arc::new(Mutex::new(HashSet::with_capacity(100_000)));
    let processed_count = Arc::new(AtomicU64::new(0));

    // Parallel file processing
    files_to_process
        .par_iter()
        .try_for_each(|file_path| -> Result<()> {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            let reader = BufReader::with_capacity(1024 * 1024, decoder);

            let mut local_institutions = Vec::with_capacity(2000);
            let mut local_geo = Vec::with_capacity(2000);
            let mut local_processed = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let institution: Value = serde_json::from_str(&line)?;

                if let Some(institution_id) = institution.get("id").and_then(|v| v.as_str()) {
                    // Thread-safe duplicate check
                    let should_process = {
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.contains(institution_id) {
                            false
                        } else {
                            seen.insert(institution_id.to_string());
                            true
                        }
                    };

                    if !should_process {
                        continue;
                    }

                    // Main institution record
                    let display_name_acronyms = extract_array_as_string!(institution, "display_name_acronyms");
                    let display_name_alternatives = extract_array_as_string!(institution, "display_name_alternatives");

                    let record = InstitutionRecord {
                        id: institution_id.to_string(),
                        ror: extract_string!(institution, "ror"),
                        display_name: extract_string_required!(institution, "display_name", "Unknown"),
                        country_code: extract_string!(institution, "country_code"),
                        type_: extract_string!(institution, "type"),
                        homepage_url: extract_string!(institution, "homepage_url"),
                        image_url: extract_string!(institution, "image_url"),
                        image_thumbnail_url: extract_string!(institution, "image_thumbnail_url"),
                        display_name_acronyms,
                        display_name_alternatives,
                        works_count: extract_i64!(institution, "works_count", 0),
                        cited_by_count: extract_i64!(institution, "cited_by_count", 0),
                        works_api_url: extract_string!(institution, "works_api_url"),
                        updated_date: extract_string!(institution, "updated_date"),
                    };

                    local_institutions.push(record);

                    // Geo record
                    if let Some(geo) = institution.get("geo") {
                        let geo_record = InstitutionGeoRecord {
                            institution_id: institution_id.to_string(),
                            city: extract_string!(geo, "city"),
                            geonames_city_id: extract_string!(geo, "geonames_city_id"),
                            region: extract_string!(geo, "region"),
                            country_code: extract_string!(geo, "country_code"),
                            country: extract_string!(geo, "country"),
                            latitude: geo.get("latitude").and_then(|v| v.as_f64()),
                            longitude: geo.get("longitude").and_then(|v| v.as_f64()),
                        };
                        local_geo.push(geo_record);
                    }

                    local_processed += 1;

                    // Flush local buffers when they get large enough
                    flush_local_buffer!(
                        local_institutions,
                        institutions_buffer,
                        institutions_writer,
                        batch_size,
                        institutions_to_record_batch,
                        2000
                    );
                    flush_local_buffer!(
                        local_geo,
                        geo_buffer,
                        geo_writer,
                        batch_size,
                        institution_geo_to_record_batch,
                        2000
                    );
                }
            }

            // Flush remaining local data
            final_flush!(
                local_institutions,
                institutions_buffer,
                institutions_writer,
                institutions_to_record_batch
            );
            final_flush!(
                local_geo,
                geo_buffer,
                geo_writer,
                institution_geo_to_record_batch
            );

            let total_processed = processed_count.fetch_add(local_processed, Ordering::Relaxed) + local_processed;
            
            stats.institutions_processed.fetch_add(local_processed, Ordering::Relaxed);
            stats.files_processed.fetch_add(1, Ordering::Relaxed);
            
            progress.inc(1);
            
            // Update progress bar for every file processed
            let files_done = stats.files_processed.load(Ordering::Relaxed);
            update_progress_with_memory(&progress, "institutions", total_processed, files_done, files_to_process.len() as u64);

            if total_processed % 10_000 == 0 {
                info!(
                    "Processed {} institutions across all threads! Memory: {}",
                    total_processed,
                    get_memory_gb()
                );
            }

            Ok(())
        })?;

    // Write remaining batches
    final_flush!(
        Vec::<InstitutionRecord>::new(),
        institutions_buffer,
        institutions_writer,
        institutions_to_record_batch
    );
    final_flush!(
        Vec::<InstitutionGeoRecord>::new(),
        geo_buffer,
        geo_writer,
        institution_geo_to_record_batch
    );

    // Close writers
    let institutions_writer = Arc::try_unwrap(institutions_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap institutions_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock institutions_writer: {:?}", e))?;
    institutions_writer.close()?;

    let geo_writer = Arc::try_unwrap(geo_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap geo_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock geo_writer: {:?}", e))?;
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
    info!("Processing publishers to Parquet with parallel processing");

    let publishers_schema = schema! {
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

    let publishers_writer = create_writer!(&output_dir.join("publishers.parquet"), publishers_schema);

    let files = find_entity_files(input_dir, "publishers")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} files | {msg}",
    )?);
    progress.set_message("Starting publishers processing...");
    
    // Initial progress update
    update_progress_with_memory(&progress, "publishers", 0, 0, files_to_process.len() as u64);

    // Thread-safe buffers and deduplication
    let publishers_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let seen_ids = Arc::new(Mutex::new(HashSet::with_capacity(50_000)));
    let processed_count = Arc::new(AtomicU64::new(0));

    // Parallel file processing
    files_to_process
        .par_iter()
        .try_for_each(|file_path| -> Result<()> {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            let reader = BufReader::with_capacity(1024 * 1024, decoder);

            let mut local_publishers = Vec::with_capacity(2000);
            let mut local_processed = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let publisher: Value = serde_json::from_str(&line)?;

                if let Some(publisher_id) = publisher.get("id").and_then(|v| v.as_str()) {
                    // Thread-safe duplicate check
                    let should_process = {
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.contains(publisher_id) {
                            false
                        } else {
                            seen.insert(publisher_id.to_string());
                            true
                        }
                    };

                    if !should_process {
                        continue;
                    }

                    let alternate_titles = extract_array_as_string!(publisher, "alternate_titles");
                    let country_codes = extract_array_as_string!(publisher, "country_codes");

                    let record = PublisherRecord {
                        id: publisher_id.to_string(),
                        display_name: extract_string_required!(publisher, "display_name", "Unknown"),
                        alternate_titles,
                        country_codes,
                        hierarchy_level: extract_i64_optional!(publisher, "hierarchy_level").map(|i| i as i32),
                        parent_publisher: extract_string!(publisher, "parent_publisher"),
                        works_count: extract_i64!(publisher, "works_count", 0),
                        cited_by_count: extract_i64!(publisher, "cited_by_count", 0),
                        sources_api_url: extract_string!(publisher, "sources_api_url"),
                        updated_date: extract_string!(publisher, "updated_date"),
                    };

                    local_publishers.push(record);
                    local_processed += 1;

                    // Flush local buffer when it gets large enough
                    flush_local_buffer!(
                        local_publishers,
                        publishers_buffer,
                        publishers_writer,
                        batch_size,
                        publishers_to_record_batch,
                        2000
                    );
                }
            }

            // Flush remaining local data
            final_flush!(
                local_publishers,
                publishers_buffer,
                publishers_writer,
                publishers_to_record_batch
            );

            let total_processed = processed_count.fetch_add(local_processed, Ordering::Relaxed) + local_processed;
            
            stats.publishers_processed.fetch_add(local_processed, Ordering::Relaxed);
            stats.files_processed.fetch_add(1, Ordering::Relaxed);
            
            progress.inc(1);
            
            // Update progress bar for every file processed
            let files_done = stats.files_processed.load(Ordering::Relaxed);
            update_progress_with_memory(&progress, "publishers", total_processed, files_done, files_to_process.len() as u64);

            if total_processed % 5_000 == 0 {
                info!(
                    "Processed {} publishers across all threads! Memory: {}",
                    total_processed,
                    get_memory_gb()
                );
            }

            Ok(())
        })?;

    // Write remaining batch
    final_flush!(
        Vec::<PublisherRecord>::new(),
        publishers_buffer,
        publishers_writer,
        publishers_to_record_batch
    );

    let publishers_writer = Arc::try_unwrap(publishers_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap publishers_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock publishers_writer: {:?}", e))?;
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
    info!("Processing topics to Parquet with parallel processing");

    let topics_schema = schema! {
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

    let topics_writer = create_writer!(&output_dir.join("topics.parquet"), topics_schema);

    let files = find_entity_files(input_dir, "topics")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} files | {msg}",
    )?);
    progress.set_message("Starting topics processing...");
    
    // Initial progress update
    update_progress_with_memory(&progress, "topics", 0, 0, files_to_process.len() as u64);

    // Thread-safe buffers and deduplication
    let topics_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    let seen_ids = Arc::new(Mutex::new(HashSet::with_capacity(100_000)));
    let processed_count = Arc::new(AtomicU64::new(0));

    // Parallel file processing
    files_to_process
        .par_iter()
        .try_for_each(|file_path| -> Result<()> {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            let reader = BufReader::with_capacity(1024 * 1024, decoder);

            let mut local_topics = Vec::with_capacity(2000);
            let mut local_processed = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let topic: Value = serde_json::from_str(&line)?;

                if let Some(topic_id) = topic.get("id").and_then(|v| v.as_str()) {
                    // Thread-safe duplicate check
                    let should_process = {
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.contains(topic_id) {
                            false
                        } else {
                            seen.insert(topic_id.to_string());
                            true
                        }
                    };

                    if !should_process {
                        continue;
                    }

                    // Extract keywords and join them
                    let keywords = topic.get("keywords").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join("; ")
                    });

                    let siblings = extract_array_as_string!(topic, "siblings");

                    let record = TopicRecord {
                        id: topic_id.to_string(),
                        display_name: extract_string_required!(topic, "display_name", "Unknown"),
                        subfield_id: extract_nested_string!(topic, "subfield", "id").unwrap_or_else(|| "Unknown".to_string()),
                        subfield_display_name: extract_nested_string!(topic, "subfield", "display_name").unwrap_or_else(|| "Unknown".to_string()),
                        field_id: extract_nested_string!(topic, "field", "id").unwrap_or_else(|| "Unknown".to_string()),
                        field_display_name: extract_nested_string!(topic, "field", "display_name").unwrap_or_else(|| "Unknown".to_string()),
                        domain_id: extract_nested_string!(topic, "domain", "id").unwrap_or_else(|| "Unknown".to_string()),
                        domain_display_name: extract_nested_string!(topic, "domain", "display_name").unwrap_or_else(|| "Unknown".to_string()),
                        description: extract_string!(topic, "description"),
                        keywords,
                        works_api_url: extract_string!(topic, "works_api_url"),
                        wikipedia_id: topic
                            .get("ids")
                            .and_then(|v| v.get("wikipedia"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        works_count: extract_i64!(topic, "works_count", 0),
                        cited_by_count: extract_i64!(topic, "cited_by_count", 0),
                        updated_date: extract_string!(topic, "updated"),
                        siblings,
                    };

                    local_topics.push(record);
                    local_processed += 1;

                    // Flush local buffer when it gets large enough
                    flush_local_buffer!(
                        local_topics,
                        topics_buffer,
                        topics_writer,
                        batch_size,
                        topics_to_record_batch,
                        2000
                    );
                }
            }

            // Flush remaining local data
            final_flush!(
                local_topics,
                topics_buffer,
                topics_writer,
                topics_to_record_batch
            );

            let total_processed = processed_count.fetch_add(local_processed, Ordering::Relaxed) + local_processed;
            
            stats.topics_processed.fetch_add(local_processed, Ordering::Relaxed);
            stats.files_processed.fetch_add(1, Ordering::Relaxed);
            
            progress.inc(1);
            
            // Update progress bar for every file processed
            let files_done = stats.files_processed.load(Ordering::Relaxed);
            update_progress_with_memory(&progress, "topics", total_processed, files_done, files_to_process.len() as u64);

            if total_processed % 5_000 == 0 {
                info!(
                    "Processed {} topics across all threads! Memory: {}",
                    total_processed,
                    get_memory_gb()
                );
            }

            Ok(())
        })?;

    // Write remaining batch
    final_flush!(
        Vec::<TopicRecord>::new(),
        topics_buffer,
        topics_writer,
        topics_to_record_batch
    );

    let topics_writer = Arc::try_unwrap(topics_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap topics_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock topics_writer: {:?}", e))?;
    topics_writer.close()?;
    progress.finish_with_message("Topics processing complete");

    let topics_count = stats.topics_processed.load(Ordering::Relaxed);
    info!("Processed {} topics to Parquet", topics_count);

    Ok(())
}

// ====== WORKS PROCESSING (PARALLEL VERSION) ======
pub fn process_works(
    input_dir: &str,
    output_dir: &Path,
    batch_size: usize,
    files_per_entity: usize,
    stats: &ProcessingStats,
) -> Result<()> {
    info!("Processing Works to Parquet with memory-optimized parallelization!");

    // Create schemas with consistent string types
    let works_schema = schema! {
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

    let locations_schema = schema! {
        "work_id" => DataType::Utf8, false;
        "source_id" => DataType::Utf8;
        "landing_page_url" => DataType::LargeUtf8;
        "pdf_url" => DataType::LargeUtf8;
        "is_oa" => DataType::Boolean;
        "version" => DataType::Utf8;
        "license" => DataType::Utf8;
        "location_type" => DataType::Utf8, false;
    };

    let authorships_schema = schema! {
        "work_id" => DataType::Utf8, false;
        "author_position" => DataType::Utf8, false;
        "author_id" => DataType::Utf8, false;
        "institution_id" => DataType::Utf8;
        "raw_affiliation_string" => DataType::LargeUtf8;
    };

    let topics_schema = schema! {
        "work_id" => DataType::Utf8, false;
        "topic_id" => DataType::Utf8, false;
        "score" => DataType::Float64;
    };

    let open_access_schema = schema! {
        "work_id" => DataType::Utf8, false;
        "is_oa" => DataType::Boolean;
        "oa_status" => DataType::Utf8;
        "oa_url" => DataType::LargeUtf8;
        "any_repository_has_fulltext" => DataType::Boolean;
    };

    let citations_schema = schema! {
        "work_id" => DataType::Utf8, false;
        "referenced_work_id" => DataType::Utf8, false;
    };

    // Create thread-safe writers (thread-safe)
    let works_writer = create_writer!(&output_dir.join("works.parquet"), works_schema);
    let locations_writer = create_writer!(&output_dir.join("works_locations.parquet"), locations_schema);
    let authorships_writer = create_writer!(&output_dir.join("works_authorships.parquet"), authorships_schema);
    let topics_writer = create_writer!(&output_dir.join("works_topics.parquet"), topics_schema);
    let open_access_writer = create_writer!(&output_dir.join("works_open_access.parquet"), open_access_schema);
    let citations_writer = create_writer!(&output_dir.join("works_citations.parquet"), citations_schema);

    let files = find_entity_files(input_dir, "works")?;
    let files_to_process = if files_per_entity > 0 && files_per_entity < files.len() {
        &files[..files_per_entity]
    } else {
        &files
    };

    let progress = ProgressBar::new(files_to_process.len() as u64);
    progress.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:50.cyan/blue} {pos:>7}/{len:7} files | {msg}",
    )?);
    
    // Set initial progress message
    progress.set_message("Starting works processing...");
    
    // Initial progress update
    update_progress_with_memory(&progress, "works", 0, 0, files_to_process.len() as u64);

    // Conservative shared buffers - prioritize frequent writes over memory
    let works_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 8)));       // Even smaller
    let locations_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 8)));   // Even smaller
    let authorships_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 4))); // Even smaller
    let topics_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 8)));      // Even smaller
    let open_access_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 8))); // Even smaller
    let citations_buffer = Arc::new(Mutex::new(Vec::with_capacity(batch_size / 2)));   // Even smaller

    let processed_count = Arc::new(AtomicU64::new(0));
    
    // CRITICAL FIX: Add duplicate detection for works (was missing!)
    let seen_work_ids = Arc::new(Mutex::new(HashSet::with_capacity(10_000_000))); // Large capacity for works

    info!(
        "Processing {} files with {} cores optimized for memory efficiency",
        files_to_process.len(),
        num_cpus::get()
    );

    // PARALLEL FILE PROCESSING
    files_to_process
        .par_iter()
        .try_for_each(|file_path| -> Result<()> {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            let reader = BufReader::with_capacity(256 * 1024, decoder);

            let mut local_works = Vec::with_capacity(50);        
            let mut local_locations = Vec::with_capacity(100);   
            let mut local_authorships = Vec::with_capacity(250); 
            let mut local_topics = Vec::with_capacity(100);      
            let mut local_open_access = Vec::with_capacity(50);  
            let mut local_citations = Vec::with_capacity(500);   
            let mut local_processed = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let work: Value = serde_json::from_str(&line)?;

                if let Some(work_id) = work.get("id").and_then(|v| v.as_str()) {
                    // CRITICAL FIX: Add duplicate checking for works
                    let should_process = {
                        let mut seen = seen_work_ids.lock().unwrap();
                        if seen.contains(work_id) {
                            false
                        } else {
                            seen.insert(work_id.to_string());
                            true
                        }
                    };

                    if !should_process {
                        continue;
                    }

                    // Process ALL data - preserve full content for research evaluation
                    let title = work
                        .get("title")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()); // Keep full titles

                    let display_name = work
                        .get("display_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(); // Keep full display names

                    let abstract_inverted_index = work.get("abstract_inverted_index").map(|v| {
                        serde_json::to_string(v).unwrap_or_default() // Keep full abstracts
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

                    // Process ALL primary location data
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
                                    .map(|s| s.to_string()), // Keep full URLs
                                pdf_url: primary_location
                                    .get("pdf_url")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()), // Keep full URLs
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

                    // FIXED: Process authorships with consistent logic to prevent double-counting
                    if let Some(authorships) = work.get("authorships").and_then(|v| v.as_array()) {
                        for authorship in authorships {
                            if let Some(author_id) = authorship
                                .get("author")
                                .and_then(|a| a.get("id"))
                                .and_then(|v| v.as_str())
                            {
                                // FIXED: Create ONE record per author per work, aggregating institutions
                                let institutions = authorship
                                    .get("institutions")
                                    .and_then(|v| v.as_array());
                                
                                // Aggregate all institution IDs for this author on this work
                                let institution_ids: Vec<String> = if let Some(institutions) = institutions {
                                    institutions
                                        .iter()
                                        .filter_map(|inst| inst.get("id").and_then(|v| v.as_str()))
                                        .map(|s| s.to_string())
                                        .collect()
                                } else {
                                    Vec::new()
                                };

                                // Create a single record with primary institution (first one) or None
                                let primary_institution = institution_ids.first().cloned();
                                
                                let authorship_record = WorkAuthorshipRecord {
                                    work_id: work_id.to_string(),
                                    author_position: authorship
                                        .get("author_position")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown")
                                        .to_string(),
                                    author_id: author_id.to_string(),
                                    institution_id: primary_institution,
                                    raw_affiliation_string: authorship
                                        .get("raw_affiliation_string")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string()),
                                };
                                local_authorships.push(authorship_record);
                            }
                        }
                    }

                    // Process ALL topics (don't limit)
                    if let Some(topics) = work.get("topics").and_then(|v| v.as_array()) {
                        for topic in topics {
                            if let Some(topic_id) = topic.get("id").and_then(|v| v.as_str()) {
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
                                .map(|s| s.to_string()), // Keep full URLs
                            any_repository_has_fulltext: open_access
                                .get("any_repository_has_fulltext")
                                .and_then(|v| v.as_bool()),
                        };
                        local_open_access.push(oa_record);
                    }

                    // Process ALL citations (this is crucial for citation analysis!)
                    if let Some(referenced_works) = work.get("referenced_works").and_then(|v| v.as_array()) {
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

                    local_processed += 1;

                    flush_local_buffer!(local_works, works_buffer, works_writer, batch_size, works_to_record_batch, 50);   // Even smaller
                    flush_local_buffer!(local_locations, locations_buffer, locations_writer, batch_size, work_locations_to_record_batch, 100);  // Even smaller
                    flush_local_buffer!(local_authorships, authorships_buffer, authorships_writer, batch_size * 2, authorships_to_record_batch, 250);  // Even smaller
                    flush_local_buffer!(local_topics, topics_buffer, topics_writer, batch_size, work_topics_to_record_batch, 100);  // Even smaller
                    flush_local_buffer!(local_open_access, open_access_buffer, open_access_writer, batch_size, work_open_access_to_record_batch, 50);   // Even smaller
                    flush_local_buffer!(local_citations, citations_buffer, citations_writer, batch_size * 3, citations_to_record_batch, 500);  // Even smaller

                    // EMERGENCY memory cleanup every 500 records (MUCH more frequent)
                    if local_processed % 500 == 0 {
                        // Force flush ALL local buffers every 500 records to prevent ANY memory buildup
                        flush_local_buffer!(local_works, works_buffer, works_writer, batch_size, works_to_record_batch, 1);
                        flush_local_buffer!(local_locations, locations_buffer, locations_writer, batch_size, work_locations_to_record_batch, 1);
                        flush_local_buffer!(local_authorships, authorships_buffer, authorships_writer, batch_size * 2, authorships_to_record_batch, 1);
                        flush_local_buffer!(local_topics, topics_buffer, topics_writer, batch_size, work_topics_to_record_batch, 1);
                        flush_local_buffer!(local_open_access, open_access_buffer, open_access_writer, batch_size, work_open_access_to_record_batch, 1);
                        flush_local_buffer!(local_citations, citations_buffer, citations_writer, batch_size * 3, citations_to_record_batch, 1);
                    }
                    
                    // Much more frequent progress updates (every 2.5k instead of 5k)
                    if local_processed % 2500 == 0 {
                        let total_processed = processed_count.fetch_add(2500, Ordering::Relaxed) + 2500;
                        
                        // Update progress much more frequently for better monitoring
                        if total_processed % 25_000 == 0 { // Update every 25K instead of 50K
                            let files_done = stats.files_processed.load(Ordering::Relaxed);
                            update_progress_with_memory(&progress, "works", total_processed, files_done, files_to_process.len() as u64);
                            
                            info!(
                                "Processed {} works across all threads! Memory: {}",
                                total_processed,
                                get_memory_gb()
                            );
                        }
                        
                        // CRITICAL: Periodic cleanup of seen_work_ids to prevent unbounded growth
                        if total_processed % 10_000_000 == 0 {
                            let mut seen = seen_work_ids.lock().unwrap();
                            let old_size = seen.len();
                            seen.shrink_to_fit();
                            info!("Cleaned seen_work_ids: {} -> {} entries", old_size, seen.len());
                        }
                    }

                    // Memory pressure relief system
                    if local_processed % 1000 == 0 { // Check more frequently
                        // Check memory usage and force global flush if needed
                        let memory_info = get_memory_usage();
                        if memory_info.contains("VmRSS:") {
                            // Extract RSS memory in kB
                            if let Some(rss_start) = memory_info.find("VmRSS:") {
                                if let Some(rss_str) = memory_info[rss_start..].split_whitespace().nth(1) {
                                    if let Ok(rss_kb) = rss_str.parse::<u64>() {
                                        // Much more aggressive: If using more than 30GB RSS, force emergency flush
                                        if rss_kb > 30_000_000 {
                                            info!("EMERGENCY FLUSH: Memory usage {}GB exceeds 30GB limit", 
                                                rss_kb as f64 / 1_000_000.0);
                                            
                                            // EMERGENCY: Force flush ALL buffers immediately
                                            flush_local_buffer!(local_works, works_buffer, works_writer, batch_size, works_to_record_batch, 1);
                                            flush_local_buffer!(local_locations, locations_buffer, locations_writer, batch_size, work_locations_to_record_batch, 1);
                                            flush_local_buffer!(local_authorships, authorships_buffer, authorships_writer, batch_size * 2, authorships_to_record_batch, 1);
                                            flush_local_buffer!(local_topics, topics_buffer, topics_writer, batch_size, work_topics_to_record_batch, 1);
                                            flush_local_buffer!(local_open_access, open_access_buffer, open_access_writer, batch_size, work_open_access_to_record_batch, 1);
                                            flush_local_buffer!(local_citations, citations_buffer, citations_writer, batch_size * 3, citations_to_record_batch, 1);
                                            
                                            // Clear all local buffers to minimum capacity
                                            local_works.shrink_to_fit();
                                            local_locations.shrink_to_fit();
                                            local_authorships.shrink_to_fit();
                                            local_topics.shrink_to_fit();
                                            local_open_access.shrink_to_fit();
                                            local_citations.shrink_to_fit();
                                        }
                                    }
                                }
                            }
                        }
                    }

                    stats.works_processed.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Final flush for this thread
            final_flush!(local_works, works_buffer, works_writer, works_to_record_batch);
            final_flush!(local_locations, locations_buffer, locations_writer, work_locations_to_record_batch);
            final_flush!(local_authorships, authorships_buffer, authorships_writer, authorships_to_record_batch);
            final_flush!(local_topics, topics_buffer, topics_writer, work_topics_to_record_batch);
            final_flush!(local_open_access, open_access_buffer, open_access_writer, work_open_access_to_record_batch);
            final_flush!(local_citations, citations_buffer, citations_writer, citations_to_record_batch);

            stats.files_processed.fetch_add(1, Ordering::Relaxed);
            progress.inc(1);
            
            // Update progress bar for every file processed
            let files_done = stats.files_processed.load(Ordering::Relaxed);
            let works_processed = stats.works_processed.load(Ordering::Relaxed);
            update_progress_with_memory(&progress, "works", works_processed, files_done, files_to_process.len() as u64);
            
            // Show file progress for better monitoring with more frequent updates
            if files_done % 5 == 0 || files_done == 1 { // Update every 5 files or on first file
                info!("Processing file {}/{} - {} works total - {} RAM", 
                    files_done, files_to_process.len(), works_processed, get_memory_gb());
            }

            Ok(())
        })?;

    // Final flush of any remaining global buffers
    final_flush!(Vec::<WorkRecord>::new(), works_buffer, works_writer, works_to_record_batch);
    final_flush!(Vec::<WorkLocationRecord>::new(), locations_buffer, locations_writer, work_locations_to_record_batch);
    final_flush!(Vec::<WorkAuthorshipRecord>::new(), authorships_buffer, authorships_writer, authorships_to_record_batch);
    final_flush!(Vec::<WorkTopicRecord>::new(), topics_buffer, topics_writer, work_topics_to_record_batch);
    final_flush!(Vec::<WorkOpenAccessRecord>::new(), open_access_buffer, open_access_writer, work_open_access_to_record_batch);
    final_flush!(Vec::<CitationRecord>::new(), citations_buffer, citations_writer, citations_to_record_batch);

    // Close writers by taking them out of Arc<Mutex<>>
    let works_writer = Arc::try_unwrap(works_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap works_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock works_writer: {:?}", e))?;
    works_writer.close()?;

    let locations_writer = Arc::try_unwrap(locations_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap locations_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock locations_writer: {:?}", e))?;
    locations_writer.close()?;

    let authorships_writer = Arc::try_unwrap(authorships_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap authorships_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock authorships_writer: {:?}", e))?;
    authorships_writer.close()?;

    let topics_writer = Arc::try_unwrap(topics_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap topics_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock topics_writer: {:?}", e))?;
    topics_writer.close()?;

    let open_access_writer = Arc::try_unwrap(open_access_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap open_access_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock open_access_writer: {:?}", e))?;
    open_access_writer.close()?;

    let citations_writer = Arc::try_unwrap(citations_writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap citations_writer"))?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to lock citations_writer: {:?}", e))?;
    citations_writer.close()?;

    progress.finish_with_message("Works processing complete");

    let works_count = stats.works_processed.load(Ordering::Relaxed);
    info!("Processed {} works using {} cores!", works_count, num_cpus::get());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    // Set up thread pool - conservative memory management
    let max_workers = num_cpus::get();
    // For works processing, use even fewer threads to prevent memory explosion
    let conservative_workers = std::cmp::min(max_workers, 32); // Cap at 32 threads for memory efficiency
    let num_workers = args.workers.unwrap_or(conservative_workers);

    info!("Using {} workers (max available: {})", num_workers, max_workers);
    info!("Memory-optimized processing with consistent tracking across all entities");
    info!("Processing with batch size: {} records per batch", args.batch_size);

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .stack_size(8 * 1024 * 1024) // 8MB stack per thread 
        .thread_name(|i| format!("openalex-worker-{}", i))
        .build_global()?;

    // Create output directory
    let output_path = Path::new(&args.output_dir);
    create_dir_all(output_path)?;
    info!("Output directory: {}", output_path.display());

    let stats = ProcessingStats::new();
    let entities: Vec<&str> = args.entities.split(',').map(|s| s.trim()).collect();

    info!("Processing entities: {:?}", entities);

    // Process all entities
    let results: Result<Vec<_>> = entities
        .par_iter()
        .map(|&entity| -> Result<()> {
            match entity {
                "authors" => {
                    process_authors(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "institutions" => {
                    process_institutions(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "publishers" => {
                    process_publishers(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "topics" => {
                    process_topics(
                        &args.input_dir,
                        output_path,
                        args.batch_size,
                        args.files_per_entity,
                        &stats,
                    )
                }
                "works" => {
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

    results?;

    // Print final statistics
    info!("Final Processing Statistics:");
    info!("  Authors processed: {}", stats.authors_processed.load(Ordering::Relaxed));
    info!("  Works processed: {}", stats.works_processed.load(Ordering::Relaxed));
    info!("  Institutions processed: {}", stats.institutions_processed.load(Ordering::Relaxed));
    info!("  Publishers processed: {}", stats.publishers_processed.load(Ordering::Relaxed));
    info!("  Topics processed: {}", stats.topics_processed.load(Ordering::Relaxed));
    info!("  Citation edges: {}", stats.citation_edges.load(Ordering::Relaxed));
    info!("  Files processed: {}", stats.files_processed.load(Ordering::Relaxed));

    info!("Parquet datasets created successfully in: {}", args.output_dir);

    Ok(())
}
