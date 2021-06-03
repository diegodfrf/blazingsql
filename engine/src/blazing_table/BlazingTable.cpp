#include "BlazingTable.h"
#include <cudf/detail/interop.hpp>

namespace ral {
namespace frame {

BlazingTable::BlazingTable(execution::backend_id execution_backend_id, const bool & valid)
  : BlazingTableView(execution_backend_id), valid(valid) {
}

}  // namespace frame
}  // namespace ral