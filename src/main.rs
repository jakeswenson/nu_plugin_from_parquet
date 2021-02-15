mod from_parquet;

use nu_source::{Tag};

use nu_errors::ShellError;
use nu_plugin::{serve_plugin, Plugin};
use nu_protocol::{
    CallInfo, Primitive, ReturnValue, Signature, UntaggedValue, Value,
};

struct FromParquet {
    bytes: Vec<u8>,
    name_tag: Tag
}

impl FromParquet {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            name_tag: Tag::unknown()
        }
    }
}

impl Plugin for FromParquet {
    fn config(&mut self) -> Result<Signature, ShellError> {
        Ok(Signature::build("from parquet")
            .desc("Convert from .parquet binary into table")
            .filter())
    }

    fn begin_filter(&mut self, call_info: CallInfo) -> Result<Vec<ReturnValue>, ShellError> {
        self.name_tag = call_info.name_tag;
        Ok(vec![])
    }

    fn filter(&mut self, input: Value) -> Result<Vec<ReturnValue>, ShellError> {
        match input {
            Value {
                value: UntaggedValue::Primitive(Primitive::Binary(b)),
                ..
            } => {
                self.bytes.extend_from_slice(&b);
            }
            Value { tag, .. } => {
                return Err(ShellError::labeled_error_with_secondary(
                    "Expected binary from pipeline",
                    "requires binary input",
                    self.name_tag.clone(),
                    "value originates from here",
                    tag,
                ));
            }
        }
        Ok(vec![])
    }

    fn end_filter(&mut self) -> Result<Vec<ReturnValue>, ShellError> {
        let result = vec![
            crate::from_parquet::from_parquet_bytes(self.bytes.clone())
        ];
        self.bytes.clear();
        Ok(result)
    }
}

fn main() {
    serve_plugin(&mut FromParquet::new());
}
