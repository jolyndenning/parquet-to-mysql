use std::{fs::File, path::PathBuf, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use clap::Parser;
use color_eyre::eyre::{bail, Context, Result};
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::convert::to_sql_value;

mod convert;

#[derive(Parser)]
struct Opts {
    /// SQL Table name, if not present, filename without the extension will be used instead
    #[arg(short, long)]
    table_name: Option<String>,

    /// Number of rows to group into a single INSERT INTO statement
    #[arg(short = 'r', long, default_value = "100")]
    rows_batch_size: usize,

    input_file: String,
}

/// Header setting common mysql variables (charset, tz) to sane defaults
pub const MYSQLDUMP_HEADER: &str = r#"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;"#;

/// Footer resetting mysql variables to their original value
pub const MYSQLDUMP_FOOTER: &str = r#"/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;"#;

/// Extract column names of a Arrow schema and join them with `,`
pub fn column_names(schema: SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|field| format!("`{}`", field.name()))
        .join(",")
}

/// Ouptut an arrow `RecordBatch` as INSERT INTO statement.
///
/// The order of values are the column order, if the destination table have different column order,
/// column names can be specified to generate correct column order. The `column_names` argument should be
/// generated from the `column_names` function. (comma separated list of column names
pub fn record_batch_to_sql_inserts(
    batch: RecordBatch,
    table_name: &str,
    column_names: Option<&str>,
    rows_batch_size: usize,
) -> String {
    (0..batch.num_rows())
        .map(|i| {
            batch
                .columns()
                .iter()
                .cloned()
                .map(|array| to_sql_value(array, i))
                .join(",")
        })
        .map(|values| format!("({values})"))
        .chunks(rows_batch_size)
        .into_iter()
        .map(|mut values| values.join(","))
        .map(|values| match column_names {
            Some(columns_names) => {
                format!("INSERT INTO `{table_name}` ({columns_names}) VALUES {values};")
            }
            None => format!("INSERT INTO `{table_name}` VALUES {values};"),
        })
        .join("\n")
}
