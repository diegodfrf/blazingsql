#ifndef BLAZING_RAL_DATA_TYPE_H_
#define BLAZING_RAL_DATA_TYPE_H_

namespace ral {
namespace io {

typedef enum {
  UNDEFINED = 999,
  PARQUET = 0,
  ORC = 1,
  CSV = 2,
  JSON = 3,
  CUDF = 4,
  DASK_CUDF = 5,
  ARROW = 6,
  MYSQL = 7,
  POSTGRESQL = 8,
  SQLITE = 9,
  SNOWFLAKE = 10,
  PANDAS_DF = 11,
} DataType;

/**
 * @brief Compression algorithms
 */
typedef enum CompressionType {
  NONE = 0,    ///< No compression
  AUTO = 1,    ///< Automatically detect or select compression format
  SNAPPY = 2,  ///< Snappy format, using byte-oriented LZ77
  GZIP = 3,    ///< GZIP format, using DEFLATE algorithm
  BZIP2 = 4,   ///< BZIP2 format, using Burrows-Wheeler transform
  BROTLI = 5,  ///< BROTLI format, using LZ77 + Huffman + 2nd order context modeling
  ZIP = 6,     ///< ZIP format, using DEFLATE algorithm
  XZ = 7       ///< XZ format, using LZMA(2) algorithm
};

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_DATA_TYPE_H_ */
