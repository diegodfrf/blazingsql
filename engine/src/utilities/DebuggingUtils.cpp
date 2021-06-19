#include <sstream>
#include "DebuggingUtils.h"
#include <iostream>
#include <assert.h>

#ifdef CUDF_SUPPORT
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/strings/string_view.cuh>

#ifdef BSQLDBGUTILS
#include <cudf_test/column_utilities.hpp> 
#endif // BSQLDBGUTILS

#pragma GCC diagnostic pop
#include "blazing_table/BlazingCudfTable.h"
#endif

namespace ral {
namespace utilities {

std::string type_string_arrow(arrow::Type::type dtype) {
	switch (dtype) {
		case arrow::Type::BOOL: return "BOOL";
		case arrow::Type::INT8:  return "INT8";
		case arrow::Type::INT16: return "INT16";
		case arrow::Type::INT32: return "INT32";
		case arrow::Type::INT64: return "INT64";
		case arrow::Type::UINT8:  return "UINT8";
		case arrow::Type::UINT16: return "UINT16";
		case arrow::Type::UINT32: return "UINT32";
		case arrow::Type::UINT64: return "UINT64";
		case arrow::Type::FLOAT: return "FLOAT";
		case arrow::Type::DOUBLE: return "DOUBLE";
		// TODO: enables more types
		/*
		case type_id::TIMESTAMP_DAYS: return "TIMESTAMP_DAYS";
		case type_id::TIMESTAMP_SECONDS: return "TIMESTAMP_SECONDS";
		case type_id::TIMESTAMP_MILLISECONDS: return "TIMESTAMP_MILLISECONDS";
		case type_id::TIMESTAMP_MICROSECONDS: return "TIMESTAMP_MICROSECONDS";
		case type_id::TIMESTAMP_NANOSECONDS: return "TIMESTAMP_NANOSECONDS";
		case type_id::DURATION_DAYS: return "DURATION_DAYS";
		case type_id::DURATION_SECONDS: return "DURATION_SECONDS";
		case type_id::DURATION_MILLISECONDS: return "DURATION_MILLISECONDS";
		case type_id::DURATION_MICROSECONDS: return "DURATION_MICROSECONDS";
		case type_id::DURATION_NANOSECONDS: return "DURATION_NANOSECONDS";
		case type_id::DICTIONARY32:  return "DICTIONARY32";
		*/
		case arrow::Type::STRING:  return "STRING";
		case arrow::Type::LIST:  return "LIST";
		case arrow::Type::STRUCT: return "STRUCT";
		default: return "Unsupported type_id";
	}
}

#ifdef CUDF_SUPPORT
void print_blazing_cudf_table_view(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name){
	auto *table_view_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table_view.get());
	std::cout<<"Table: "<<table_name<<std::endl;
	std::cout<<"\t"<<"Num Rows: "<<table_view_ptr->num_rows()<<std::endl;
	std::cout<<"\t"<<"Num Columns: "<<table_view_ptr->num_columns()<<std::endl;
	assert(table_view_ptr->num_columns() == table_view_ptr->column_names().size());
	for(int col_idx=0; col_idx<table_view_ptr->num_columns(); col_idx++){
		std::string col_string;
		if (table_view_ptr->num_rows() > 0){
#ifdef BSQLDBGUTILS
			col_string = cudf::test::to_string(table_view_ptr->column(col_idx), "|");
#endif // BSQLDBGUTILS
		}
		std::cout<<"\t"<<table_view_ptr->column_names().at(col_idx)<<" ("<<"type: "<<type_string_arrow(table_view_ptr->column_types()[col_idx]->id())<<"): "<<col_string<<std::endl;
	}
}
#endif

void print_blazing_table_view_schema(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name){
	std::cout<<blazing_table_view_schema_to_string(table_view, table_name);	
}

std::string blazing_table_view_schema_to_string(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name){
	std::ostringstream ostream;
	ostream <<"Table: "<<table_name<<std::endl;
	ostream<<"\t"<<"Num Rows: "<<table_view->num_rows()<<std::endl;
	ostream<<"\t"<<"Num Columns: "<<table_view->num_columns()<<std::endl;
	assert(table_view->num_columns() == table_view->column_names().size());
	for(int col_idx=0; col_idx<table_view->num_columns(); col_idx++){
		ostream<<"\t"<<table_view->column_names().at(col_idx)<<" ("<<"type: "<<type_string_arrow(table_view->column_types()[col_idx]->id())<<")"<<std::endl;
	}
	return ostream.str();
}

// std::string cache_data_schema_to_string(ral::cache::CacheData * cache_data){
// 	std::ostringstream ostream;
// 	std::vector<std::string> cache_data_names = cache_data->column_names();
// 	std::vector<cudf::data_type> cache_data_types = cache_data->get_schema();
// 	ostream<<"Num Rows: "<<cache_data->num_rows()<<std::endl;
// 	ostream<<"\t"<<"Num Columns: "<<cache_data_names.size()<<std::endl;
// 	for(size_t col_idx=0; col_idx<cache_data_names.size(); col_idx++){
// 		ostream<<"\t"<<cache_data_names[col_idx]<<" ("<<"type: "<<type_string(cache_data_types[col_idx])<<")"<<std::endl;
// 	}
// 	return ostream.str();
// }

}  // namespace utilities
}  // namespace ral
