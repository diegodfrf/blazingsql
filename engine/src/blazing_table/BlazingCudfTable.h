#pragma once

#include "BlazingTable.h"
#include "BlazingCudfTableView.h"
#include "BlazingArrowTable.h"
#include "blazing_table/BlazingColumn.h"

namespace ral {
namespace frame {

class BlazingCudfTable : public BlazingTable {
public:
    BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
    BlazingCudfTable(std::unique_ptr<cudf::table> table, const std::vector<std::string> & columnNames);
    BlazingCudfTable(const cudf::table_view & table, const std::vector<std::string> & columnNames);
    BlazingCudfTable(std::unique_ptr<BlazingArrowTable> blazing_arrow_table);
    BlazingCudfTable(BlazingCudfTable &&other);

    BlazingCudfTable & operator=(BlazingCudfTable const &) = delete;
    BlazingCudfTable & operator=(BlazingCudfTable &&) = delete;

    size_t num_columns() const override;
    size_t num_rows() const override;
    std::vector<std::string> column_names() const override;
    std::vector<cudf::data_type> column_types() const override;
    void set_column_names(const std::vector<std::string> & column_names) override;
    unsigned long long size_in_bytes() const override;
    std::unique_ptr<BlazingTable> clone() const override;
    std::unique_ptr<BlazingCudfTable> clone();
    cudf::table_view view() const;
    std::shared_ptr<ral::frame::BlazingTableView> to_table_view() const override;
    std::shared_ptr<BlazingCudfTableView> to_table_view();
    std::unique_ptr<cudf::table> releaseCudfTable();
    std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();
    void ensureOwnership() override;

private:
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<BlazingColumn>> columns;
};


std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::data_type> column_types,
                                    std::vector<std::string> column_names);

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::type_id> column_types,
                                    std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const cudf::table_view & table);

}  // namespace frame
}  // namespace ral