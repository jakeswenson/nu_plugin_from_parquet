use parquet::record::{Field, Row};
use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};
use parquet::file::reader::FileReader;
use nu_protocol::{UntaggedValue, Value, ReturnValue};
use nu_source::Tag;
use chrono::{Duration, FixedOffset, TimeZone, DateTime};
use std::ops::Add;
use bigdecimal::{BigDecimal, FromPrimitive};
use indexmap::IndexMap;

fn convert_to_nu(field: &Field, tag: impl Into<Tag>) -> Value {
    let epoch: DateTime<FixedOffset> = FixedOffset::west(0)
        .ymd(1970, 1, 1)
        .and_hms(0, 0, 0);
    match field {
        Field::Null => UntaggedValue::nothing().into_value(tag),
        Field::Bool(b) => UntaggedValue::boolean(*b).into_value(tag),
        Field::Byte(b) => UntaggedValue::binary(vec![*b as u8]).into_value(tag),
        Field::UByte(b) => UntaggedValue::binary(vec![*b]).into_value(tag),
        Field::Short(s) => UntaggedValue::int(*s).into_value(tag),
        Field::UShort(s) => UntaggedValue::int(*s).into_value(tag),
        Field::Int(i) => UntaggedValue::int(*i).into_value(tag),
        Field::UInt(i) => UntaggedValue::int(*i).into_value(tag),
        Field::Long(l) => UntaggedValue::int(*l).into_value(tag),
        Field::ULong(l) => UntaggedValue::int(*l).into_value(tag),
        Field::Float(float) => {
            if let Some(f) = BigDecimal::from_f32(*float) {
                UntaggedValue::decimal(f).into_value(tag)
            } else {
                unreachable!("Internal error: protocol did not use f32-compatible decimal")
            }
        },
        Field::Double(double) => {
            if let Some(d) = BigDecimal::from_f64(*double) {
                UntaggedValue::decimal(d).into_value(tag)
            } else {
                unreachable!("Internal error: protocol did not use f64-compatible decimal")
            }
        },
        Field::Str(s) => UntaggedValue::string(s).into_value(tag),
        Field::Bytes(bytes) => UntaggedValue::binary(bytes.data().to_vec()).into_value(tag),
        Field::Date(days_since_epoch) => {
            let value = epoch.add(Duration::days(*days_since_epoch as i64));
            UntaggedValue::date(value).into_value(tag)
        }
        Field::TimestampMillis(millis_since_epoch) => {
            let value = epoch.add(Duration::milliseconds(*millis_since_epoch as i64));
            UntaggedValue::date(value).into_value(tag)
        }
        Field::TimestampMicros(micros_since_epoch) => {
            let value = epoch.add(Duration::microseconds(*micros_since_epoch as i64));
            UntaggedValue::date(value).into_value(tag)
        }
        Field::Decimal(_d) => unimplemented!("Parquet DECIMAL is not handled yet"),
        Field::Group(_row) => { unimplemented!("Nested structs not supported yet") }
        Field::ListInternal(_list) => { unimplemented!("Lists not supported yet") }
        Field::MapInternal(_map) => { unimplemented!("Maps not supported yet") }
    }
}

fn convert_parquet_row(row: Row, tag: impl Into<Tag>) -> Value {
    let mut map: IndexMap<String, Value> = IndexMap::with_capacity(row.len());
    let mut row_iter = row.get_column_iter();
    let tag = tag.into();
    while let Some(col) = row_iter.next() {
        map.insert(col.0.clone(), convert_to_nu(col.1, tag.clone()));
    }

    UntaggedValue::row(map).into_value(tag)
}

pub fn from_parquet_bytes(bytes: Vec<u8>) -> ReturnValue {
    let cursor = SliceableCursor::new(bytes);
    let reader = SerializedFileReader::new(cursor).unwrap();
        let mut iter = reader.get_row_iter(None)
            .unwrap();
    let mut result = Vec::new();
    while let Some(record) = iter.next() {
        let row = convert_parquet_row(record, Tag::unknown());
        result.push(row);
    }

    let value: Value = UntaggedValue::Table(result).into_value(Tag::unknown());
    ReturnValue::from(value)
}