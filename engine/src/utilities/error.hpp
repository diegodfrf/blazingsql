#pragma once

#include <iostream>
#include <vector>
#include <exception>

#define STRINGIFY_DETAIL(x) #x
#define RAL_STRINGIFY(x) STRINGIFY_DETAIL(x)

#ifdef CUDF_SUPPORT

#include <cudf/utilities/error.hpp>
#define error_wrapper cudf::logic_error
#define FINAL_STRINGIFY(x) CUDF_STRINGIFY(x)

#else

#define error_wrapper std::runtime_error
#define FINAL_STRINGIFY(x) RAL_STRINGIFY(x)

#endif


#define RAL_EXPECTS(cond, reason)                            \
  (!!(cond))                                                 \
      ? static_cast<void>(0)                                 \
      : throw error_wrapper("Ral failure at: " __FILE__ \
                                ":" FINAL_STRINGIFY(__LINE__) ": " reason)

#define RAL_FAIL(reason)                              \
  throw error_wrapper("Ral failure at: " __FILE__ \
                          ":" FINAL_STRINGIFY(__LINE__) ": " reason)

struct BlazingMissingMetadataException : public std::exception
{

  BlazingMissingMetadataException(std::string key) : key{key} {}
  virtual ~BlazingMissingMetadataException() {}
  const char * what () const throw ()
    {
    	return ("Missing metadata, could not find key " + key).c_str();
    }
  private:
    std::string key;
};

enum _error
{
  E_SUCCESS = 0,
  E_EXCEPTION
};

typedef int error_code_t;
