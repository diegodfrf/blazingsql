#pragma once

#include "BlazingTableView.h"

namespace ral {
namespace frame {

class BlazingTable: public BlazingTableView {
public:
  BlazingTable(execution::backend_id execution_backend_id, const bool & valid);
  BlazingTable(BlazingTable &&) = default;
  virtual ~BlazingTable() {};

  bool is_valid() const { return valid; }
  operator bool() const { return this->is_valid(); }

  virtual void ensureOwnership() {}
  virtual std::shared_ptr<ral::frame::BlazingTableView> to_table_view() const = 0;

protected:
  bool valid = true;
};

}  // namespace frame
}  // namespace ral