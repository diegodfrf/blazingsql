#include "BlazingTableView.h"
#include <cudf/detail/interop.hpp>

namespace ral {
namespace frame {

BlazingTableView::BlazingTableView(execution::backend_id execution_backend_id)
  : BlazingDispatchable(execution_backend_id) {
}

}  // namespace frame
}  // namespace ral