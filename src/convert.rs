use std::borrow::Cow;

use arrow::array::*;
use chrono::{DateTime, Utc};
use format_sql_query::QuotedData;

pub(crate) fn to_sql_value(column: ArrayRef, index: usize) -> Cow<'static, str> {
    if column.is_null(index) {
        "NULL".into()
    } else {
        match column.data_type() {
            arrow::datatypes::DataType::Null => todo!(),
            arrow::datatypes::DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.value(index) {
                    "TRUE".into()
                } else {
                    "FALSE".into()
                }
            }
            arrow::datatypes::DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Float16 => {
                let array = column.as_any().downcast_ref::<Float16Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                format!("{}", array.value(index)).into()
            }
            arrow::datatypes::DataType::Timestamp(timeunit, _) => match timeunit {
                arrow::datatypes::TimeUnit::Second => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    let ts = array.value(index);
                    let dt: DateTime<Utc> = DateTime::from_timestamp(ts, 0).unwrap();
                    date_time_to_sql(dt).into()
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let ts = array.value(index);
                    let dt: DateTime<Utc> =
                        DateTime::from_timestamp(ts / 1000, (ts % 1000) as u32 * 1_000_000)
                            .unwrap();
                    date_time_to_sql(dt).into()
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let ts = array.value(index);
                    let dt: DateTime<Utc> =
                        DateTime::from_timestamp(ts / 1_000_000, (ts % 1_000_000) as u32 * 1_000)
                            .unwrap();
                    date_time_to_sql(dt).into()
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let ts = array.value(index);
                    let dt: DateTime<Utc> =
                        DateTime::from_timestamp(ts / 1_000_000_000, (ts % 1_000_000_000) as u32)
                            .unwrap();
                    date_time_to_sql(dt).into()
                }
            },
            arrow::datatypes::DataType::Date32 => todo!(),
            arrow::datatypes::DataType::Date64 => todo!(),
            arrow::datatypes::DataType::Time32(_) => todo!(),
            arrow::datatypes::DataType::Time64(_) => todo!(),
            arrow::datatypes::DataType::Duration(_) => todo!(),
            arrow::datatypes::DataType::Interval(_) => todo!(),
            arrow::datatypes::DataType::Binary => todo!(),
            arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
            arrow::datatypes::DataType::LargeBinary => todo!(),
            arrow::datatypes::DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                format!("{}", QuotedData(array.value(index))).into()
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                format!("{}", QuotedData(array.value(index))).into()
            }
            arrow::datatypes::DataType::List(_) => todo!(),
            arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
            arrow::datatypes::DataType::LargeList(_) => todo!(),
            arrow::datatypes::DataType::Struct(_) => todo!(),
            arrow::datatypes::DataType::Union(_, _) => todo!(),
            arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
            arrow::datatypes::DataType::Map(_, _) => todo!(),
            arrow::datatypes::DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}

fn date_time_to_sql(dt: DateTime<Utc>) -> String {
    dt.format("'%Y-%m-%d %H:%M:%S'").to_string()
}
