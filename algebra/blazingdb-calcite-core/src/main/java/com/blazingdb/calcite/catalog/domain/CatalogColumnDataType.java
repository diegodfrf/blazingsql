package com.blazingdb.calcite.catalog.domain;

//// TODO: handle situations where our column type is timestamp of not the default millisecond resolution
//GDF_invalid,
//GDF_INT8,
//GDF_INT16,
//GDF_INT32,
//GDF_INT64,
//GDF_FLOAT32,
//GDF_FLOAT64,
//GDF_BOOL8,
//GDF_DATE32,	/**< int32_t days since the UNIX epoch */
//GDF_DATE64,	/**< int64_t milliseconds since the UNIX epoch */
//GDF_TIMESTAMP, /**< Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond) */
//GDF_CATEGORY,
//GDF_STRING,
//GDF_STRING_CATEGORY;

public enum CatalogColumnDataType {
	// See arrow/type_fwwd.h type enum
	NA(0, "NA"),
	BOOL(1, "BOOL"), /// Boolean as 1 bit  (As BOOL8 in cudf)
	UINT8(2, "UINT8"), /// Unsigned 8-bit little-endian integer
	INT8(3, "INT8"), /// Signed 8-bit little-endian integer
	UINT16(4, "UINT16"), /// Unsigned 16-bit little-endian integer
	INT16(5, "INT16"), /// Signed 16-bit little-endian integer
	UINT32(6, "UINT32"), /// Unsigned 32-bit little-endian integer
	INT32(7, "INT32"), /// Signed 32-bit little-endian integer
	UINT64(8, "UINT64"), /// Unsigned 64-bit little-endian integer
	INT64(9, "INT64"),/// Signed 64-bit little-endian integer
	HALF_FLOAT(10, "HALF_FLOAT"), /// 2-byte floating point value
	FLOAT(11, "FLOAT"), /// 4-byte floating point value
	DOUBLE(12, "DOUBLE"), /// 8-byte floating point value
	STRING(13, "STRING"), ///< String elements
	BINARY(14, "BINARY"),
	FIXED_SIZE_BINARY(15, "FIXED_SIZE_BINARY"),
	DATE32(16, "DATE32"),
	DATE64(17, "DATE64"),
	TIMESTAMP(18, "TIMESTAMP"),
	TIME32(19, "TIME32"),
    TIME64(20, "TIME64"),		/// Time as signed 64-bit integer, representing either microseconds or nanoseconds since midnight
    INTERVAL_MONTHS(21, "INTERVAL_MONTHS"),	/// YEAR_MONTH interval in SQL style
    INTERVAL_DAY_TIME(22, "INTERVAL_DAY_TIME"),  	/// DAY_TIME interval in SQL style
    DECIMAL128(23, "DECIMAL128"),		/// Precision- and scale-based decimal type with 128 bits.
    DECIMAL(24, "DECIMAL"),	/// Defined for backward-compatibility.
    DECIMAL256(25, "DECIMAL256"),    /// Precision- and scale-based decimal type with 256 bits.
    LIST(26, "LIST"),	/// A list of some logical data type
    STRUCT(27, "STRUCT"),	/// Struct of logical types
    SPARSE_UNION(28, "SPARSE_UNION"),	/// Sparse unions of logical types
    DENSE_UNION(29, "DENSE_UNION"),	/// Dense unions of logical types
    /// Dictionary-encoded type, also called "categorical" or "factor"
    /// in other programming languages. Holds the dictionary value
    /// type but not the dictionary itself, which is part of the
    /// ArrayData struct
    DICTIONARY(29, "DICTIONARY"),
    MAP(29, "MAP"),	/// Map, a repeated struct logical type
    EXTENSION(29, "EXTENSION"), /// Custom data type, implemented by user
    FIXED_SIZE_LIST(29, "FIXED_SIZE_LIST"), /// Fixed size list of some logical type
    DURATION(29, "DURATION"),    /// Measure of elapsed time in either seconds, milliseconds, microseconds  or nanoseconds.
    LARGE_STRING(29, "LARGE_STRING"),  /// Like STRING, but with 64-bit offsets
    LARGE_BINARY(29, "LARGE_BINARY"),  /// Like BINARY, but with 64-bit offsets
    LARGE_LIST(29, "LARGE_LIST"),   /// Like LIST, but with 64-bit offsets
    MAX_ID(29, "MAX_ID");     // Leave this at the end

	private final int type_id;
	private final String type_id_name;

	private CatalogColumnDataType(int type_id, String type_id_name) {
		this.type_id = type_id;
		this.type_id_name = type_id_name;
	}

	public final int getTypeId() {
		return this.type_id;
	}

	public final String getTypeIdName() {
		return this.type_id_name;
	}

	public static CatalogColumnDataType fromTypeId(int type_id) {
		for (CatalogColumnDataType verbosity : CatalogColumnDataType.values()) {
			if (verbosity.getTypeId() == type_id)
				return verbosity;
		}

		return NA;
	}

	public static CatalogColumnDataType fromString(final String type_id_name) {
		CatalogColumnDataType dataType = null;
		switch (type_id_name) {
			case "NA": return NA;
			case "BOOL": return BOOL;
			case "UINT8": return UINT8;
			case "INT8": return INT8;
			case "UINT16": return UINT16;
			case "INT16": return INT16;
			case "UINT32": return UINT32;
			case "INT32": return INT32;
			case "UINT64": return UINT64;
			case "INT64": return INT64;
			case "HALF_FLOAT": return HALF_FLOAT;
			case "FLOAT": return FLOAT;
			case "DOUBLE": return DOUBLE;
			case "STRING": return STRING;
			case "BINARY": return BINARY;
			case "FIXED_SIZE_BINARY": return FIXED_SIZE_BINARY;
			case "DATE32": return DATE32;
			case "DATE64": return DATE64;
			case "TIMESTAMP": return TIMESTAMP;
			case "TIME32": return TIME32;
			case "TIME64": return TIME64;
			case "INTERVAL_MONTHS": return INTERVAL_MONTHS;
			case "INTERVAL_DAY_TIME": return INTERVAL_DAY_TIME;
			case "DECIMAL128": return DECIMAL128;
			case "DECIMAL": return DECIMAL;
			case "DECIMAL256": return DECIMAL256;
			case "LIST": return LIST;
			case "STRUCT": return STRUCT;
			case "SPARSE_UNION": return SPARSE_UNION;
			case "DENSE_UNION": return DENSE_UNION;
			case "DICTIONARY": return DICTIONARY;
			case "MAP": return MAP;
			case "EXTENSION": return EXTENSION;
			case "FIXED_SIZE_LIST": return FIXED_SIZE_LIST;
			case "DURATION": return DURATION;
			case "LARGE_STRING": return LARGE_STRING;
			case "LARGE_BINARY": return LARGE_BINARY;
			case "LARGE_LIST": return LARGE_LIST;
			case "MAX_ID": return MAX_ID;
		}
		return dataType;
	}
}
