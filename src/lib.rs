#![doc = include_str!("../README.md")]

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{create_udf, ScalarFunctionImplementation, ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use regex::Regex;
use std::sync::Arc;

/// Extracts a capture group from strings using a regular expression pattern.
///
/// # Arguments
/// * `input` - Input string array to process
/// * `pattern` - Regular expression pattern to match
/// * `group_index` - Index of the capture group to extract (0 for full match)
///
/// # Returns
/// * `Result<ArrayRef>` - Arrow array containing extracted strings or nulls
///
/// # Example
/// ```
/// use flarion_task::{regexp_extract};
/// use datafusion::arrow::array::{Array, StringArray};
///
/// let input = StringArray::from(vec![Some("hello123"), Some("world456"), None]);
/// let pattern = r"([a-z]+)(\d+)";
/// let group_index = 1;
///
/// let result = regexp_extract(&input, pattern, group_index).unwrap();
/// let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(result_array.value(0), "hello");
/// assert_eq!(result_array.value(1), "world");
/// assert!(result_array.is_null(2));
/// ```
pub fn regexp_extract(input: &StringArray, pattern: &str, group_index: usize) -> Result<ArrayRef> {
    let re = Regex::new(pattern)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let array: StringArray = input
        .iter()
        .map(|optional_data| {
            optional_data.map(|data| {
                re.captures(data)
                    .and_then(|captures| captures.get(group_index))
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default()
            })
        })
        .collect();

    Ok(Arc::new(array))
}

/// Creates a DataFusion UDF that extracts a capture group from strings using a regular expression pattern.
///
/// # Returns
/// * `ScalarUDF` - User Defined Function that accepts:
///   - input: string array to process
///   - pattern: regex pattern string
///   - group_index: capture group index (as UInt32).
pub fn create_regexp_extract() -> ScalarUDF {
    // Create the UDF signature
    let input_types = vec![
        DataType::Utf8,   // First input type: StringArray (Utf8)
        DataType::Utf8,   // Second input type: String (Pattern)
        DataType::UInt32, // Third input type: UInt32 (group_index)
    ];

    let return_type = DataType::Utf8; // The return type will be StringArray (Utf8)

    let volatility = Volatility::Immutable; // Mark as immutable (does not depend on the data)

    // Define the implementation of the function
    let fun: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
            let input = match &args[0] {
                ColumnarValue::Array(arr) => {
                    arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        DataFusionError::Execution("Expected StringArray".to_string())
                    })?
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "Expected StringArray".to_string(),
                    ))
                }
            };

            let pattern = match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
                _ => {
                    return Err(DataFusionError::Execution(
                        "Expected pattern string".to_string(),
                    ))
                }
            };

            let group_index = match &args[2] {
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(i))) => *i as usize,
                _ => return Err(DataFusionError::Execution("Expected UInt32".to_string())),
            };

            Ok(ColumnarValue::Array(regexp_extract(
                input,
                pattern,
                group_index,
            )?))
        });

    // Create the UDF and return it
    create_udf("regexp_extract", input_types, return_type, volatility, fun)
}

#[cfg(test)]
mod tests {
    use super::regexp_extract;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn test_regexp_extract_basic() {
        let input = StringArray::from(vec![Some("hello123"), Some("world456"), None]);
        let pattern = r"([a-z]+)(\d+)";

        // Test group 0 (full match)
        let result = regexp_extract(&input, pattern, 0).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "hello123");
        assert_eq!(result_array.value(1), "world456");
        assert!(result_array.is_null(2));

        // Test group 1 (letters)
        let result = regexp_extract(&input, pattern, 1).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "hello");
        assert_eq!(result_array.value(1), "world");
        assert!(result_array.is_null(2));

        // Test group 2 (numbers)
        let result = regexp_extract(&input, pattern, 2).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "456");
        assert!(result_array.is_null(2));
    }

    #[test]
    fn test_regexp_extract_no_match() {
        let input = StringArray::from(vec![Some("abc"), Some("def")]);
        let pattern = r"\d+";
        let result = regexp_extract(&input, pattern, 0).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        // No match should return empty string (Same as Spark behavior).
        assert_eq!(result_array.value(0), "");
        assert_eq!(result_array.value(1), "");
    }

    #[test]
    fn test_regexp_extract_invalid_group() {
        let input = StringArray::from(vec![Some("test123")]);
        let pattern = r"(\w+)";
        let result = regexp_extract(&input, pattern, 99).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), ""); // Invalid group index returns empty string (Same as Spark behavior).
    }

    #[test]
    fn test_regexp_extract_optional_groups() {
        let input = StringArray::from(vec![Some("abc123"), Some("def"), Some("456")]);
        let pattern = r"([a-z]+)?(\d+)?";

        // Test group 1 (letters)
        let result = regexp_extract(&input, pattern, 1).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "abc");
        assert_eq!(result_array.value(1), "def");
        assert_eq!(result_array.value(2), "");

        // Test group 2 (numbers)
        let result = regexp_extract(&input, pattern, 2).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "");
        assert_eq!(result_array.value(2), "456");
    }

    #[test]
    fn test_regexp_extract_special_chars() {
        let input = StringArray::from(vec![Some("test.123"), Some("test-456"), Some("test_789")]);
        let pattern = r"test(.)(\d+)";

        // Test group 1 (special char)
        let result = regexp_extract(&input, pattern, 1).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), ".");
        assert_eq!(result_array.value(1), "-");
        assert_eq!(result_array.value(2), "_");
    }

    #[test]
    fn test_regexp_extract_empty_input() {
        let input = StringArray::from(vec![Some("")]);
        let pattern = r".*";
        let result = regexp_extract(&input, pattern, 0).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
    }

    #[test]
    fn test_regexp_extract_number_range() {
        let input = StringArray::from(vec![Some("100-200")]);
        let result = regexp_extract(&input, r"(\d+)-(\d+)", 1).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "100");
    }

    #[test]
    fn test_regexp_extract_no_digits() {
        let input = StringArray::from(vec![Some("foo")]);
        let result = regexp_extract(&input, r"(\d+)", 1).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
    }

    #[test]
    fn test_regexp_extract_missing_optional_group() {
        let input = StringArray::from(vec![Some("aaaac")]);
        let result = regexp_extract(&input, "(a+)(b)?(c)", 2).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
    }
}
