#pragma once

namespace voltron{
namespace compute{

/// \brief Indicates the kind of aggregation to perform.
enum class AggregateKind {
	SUM,
	SUM0,
	MEAN,
	MIN,
	MAX,
	COUNT_VALID,
	COUNT_ALL,
	COUNT_DISTINCT,
	ROW_NUMBER,
	LAG,
	LEAD,
	NTH_ELEMENT
};

/// \brief Indicates the order in which elements should be sorted.
enum class SortOrder : bool { //TODO: Rommel Replace by arrow::compute::SortOrder for Arrow 4.x
  ASCENDING,
  DESCENDING
};

/// \brief Indicates whether the values are already sorted.
enum class sorted : bool {
  NO,
  YES
};

/// \brief Indicates how null values compare against all other values.
enum class NullOrder : bool {
  AFTER,  ///< NULL values are ordered after all other values
  BEFORE  ///< NULL values are ordered before all other values
};

} //namespace compute
} //namespace voltron
