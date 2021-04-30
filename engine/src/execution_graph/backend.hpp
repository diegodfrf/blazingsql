#pragma once

#include <cstdint>

namespace ral{
namespace execution {

enum class backend_id : int32_t {
  NONE,
  ARROW,
  CUDF,
  NUM_TYPE_IDS  ///< number of backends
};

/**
 * @brief The backend type being used.
 *
 * Allows us to be able to think of backends as an enum that we can use a dispatcher
 * to perform a specific type of compute
 */
class execution_backend {
 public:
  execution_backend() = default;
  ~execution_backend() = default;
  execution_backend(execution_backend const&) = default;
  execution_backend(execution_backend&&) = default;
  execution_backend& operator=(execution_backend const&) = default;
  execution_backend& operator=(execution_backend&&) = default;

  /**
   * @brief Construct a new `execution_backend` object
   *
   * @param id The exectuion backend's identifier
   */
  explicit constexpr execution_backend(backend_id id) : _id{id} {}

  /**
   * @brief Returns the execution backend identifier
   */
  constexpr backend_id id() const noexcept { return _id; }

 private:
  backend_id _id{backend_id::NONE};
};

/**
 * @brief Compares two `execution_backend` objects for equality.
 *
 * @param lhs The first `execution_backend` to compare
 * @param rhs The second `execution_backend` to compare
 * @return true `lhs` is equal to `rhs`
 * @return false `lhs` is not equal to `rhs`
 */
constexpr bool operator==(execution_backend const& lhs, execution_backend const& rhs)
{
  return lhs.id() == rhs.id();
}

/**
 * @brief Compares two `execution_backend` objects for inequality.
 *
 *
 * @param lhs The first `execution_backend` to compare
 * @param rhs The second `execution_backend` to compare
 * @return true `lhs` is not equal to `rhs`
 * @return false `lhs` is equal to `rhs`
 */
inline bool operator!=(execution_backend const& lhs, execution_backend const& rhs)
{
    return !(lhs == rhs);
}

class BlazingDispatchable {
public:
  BlazingDispatchable(execution::backend_id execution_backend_id) : execution_backend(execution_backend_id) {}
  BlazingDispatchable(BlazingDispatchable &&) = default;
  virtual ~BlazingDispatchable() {}

  execution::execution_backend get_execution_backend() const { return this->execution_backend; }

private:
  execution::execution_backend execution_backend;
};

} //namespace execution
} //namespace ral
