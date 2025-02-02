# Crate flarion-task

String manipulation functions for Apache Arrow arrays using regular expressions.

This module provides functionality similar to Apache Spark's string manipulation functions,
implemented for use with Apache Arrow arrays via DataFusion.

## Functionality  

* [`regexp_extract`] - Extracts matching groups from strings using regular expressions.
* [`create_regexp_extract`] - Creates a DataFusion UDF wrapper for regexp_extract functionality.

## Usage

### Direct Function Usage

```rust
use flarion_task::regexp_extract;
use datafusion::arrow::array::{Array, StringArray};

let array = StringArray::from(vec![Some("hello123")]);
let result = regexp_extract(&array, r"([a-z]+)(\d+)", 1).unwrap();
let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
assert_eq!(result_array.value(0), "hello");
```