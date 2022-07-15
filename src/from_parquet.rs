use bytes::Bytes;
use parquet::record::{Field, Row};
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::reader::FileReader;
use nu_protocol::{Value, Span, ShellError};
use chrono::{Duration, FixedOffset, TimeZone, DateTime};
use std::convert::TryInto;
use std::ops::Add;

fn convert_to_nu(field: &Field, span: Span) -> Value {
    let epoch: DateTime<FixedOffset> = FixedOffset::west(0)
        .ymd(1970, 1, 1)
        .and_hms(0, 0, 0);
    match field {
        Field::Null => Value::nothing(span),
        Field::Bool(b) => Value::boolean(*b, span),
        Field::Byte(b) => Value::binary(vec![*b as u8], span),
        Field::UByte(b) => Value::binary(vec![*b], span),
        Field::Short(s) => Value::int((*s).into(), span),
        Field::UShort(s) => Value::int((*s).into(), span),
        Field::Int(i) => Value::int((*i).into(), span),
        Field::UInt(i) => Value::int((*i).into(), span),
        Field::Long(l) => Value::int(*l, span),
        Field::ULong(l) => {
            (*l).try_into()
                .map(|l| Value::int(l, span))
                .unwrap_or_else(|e| Value::Error { error: ShellError::CantConvert("i64".into(), "u64".into(), span, Some(e.to_string())) })
        }
        Field::Float(f) => Value::float((*f).into(), span),
        Field::Double(f) => Value::float(*f, span),
        Field::Str(s) => Value::string(s, span),
        Field::Bytes(bytes) => Value::binary(bytes.data().to_vec(), span),
        Field::Date(days_since_epoch) => {
            let val = epoch.add(Duration::days(*days_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMillis(millis_since_epoch) => {
            let val = epoch.add(Duration::milliseconds(*millis_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMicros(micros_since_epoch) => {
            let val = epoch.add(Duration::microseconds(*micros_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::Decimal(_d) => { unimplemented!("Parquet DECIMAL is not handled yet") }
        Field::Group(_row) => { unimplemented!("Nested structs not supported yet") }
        Field::ListInternal(_list) => { unimplemented!("Lists not supported yet") }
        Field::MapInternal(_map) => { unimplemented!("Maps not supported yet") }
    }
}

fn convert_parquet_row(row: Row, span: Span) -> Value {
    let mut cols = vec![];
    let mut vals = vec![];
    for (name, field) in row.get_column_iter() {
        cols.push(name.clone());
        vals.push(convert_to_nu(field, span.clone()));
    }
    Value::Record { cols, vals, span }
}

pub fn from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Value {
    let cursor = Bytes::from(bytes);
    let reader = SerializedFileReader::new(cursor).unwrap();
        let mut iter = reader.get_row_iter(None)
            .unwrap();
    let mut vals = Vec::new();
    while let Some(record) = iter.next() {
        let row = convert_parquet_row(record, span);
        vals.push(row);
    }
    Value::List { vals, span }
}