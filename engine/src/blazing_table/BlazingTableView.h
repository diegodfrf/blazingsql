#pragma once

#include "compute/backend.h"
#include "cudf/types.hpp"
#include <vector>
#include <string>
#include <memory>

namespace ral {
namespace frame {

class BlazingTable;
class BlazingCudfTable;

class BlazingTableView : public execution::BlazingDispatchable {
public:
  BlazingTableView(execution::backend_id execution_backend_id);

  virtual std::size_t num_columns() const = 0;
  virtual std::size_t num_rows() const = 0;
  virtual std::vector<std::string> column_names() const = 0;
  virtual std::vector<cudf::data_type> column_types() const = 0; // TODO percy rommel arrow use srd_ptr arrow::Type
  virtual void set_column_names(const std::vector<std::string> & column_names) = 0;
  virtual unsigned long long size_in_bytes() const = 0;
  virtual std::unique_ptr<BlazingTable> clone() const = 0;
};

}  // namespace frame
}  // namespace ral
