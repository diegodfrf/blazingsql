#include "compute/backend_register.h"
#include "CacheMachine.h"

namespace ral {
namespace cache {

using make_single_machine_fn = std::shared_ptr<CacheMachine> (*)(std::shared_ptr<Context> context, std::string cache_machine_name, bool log_timeout, int cache_level_override, bool is_array_access);

using make_concatenating_machine_fn = std::shared_ptr<CacheMachine> (*)(std::shared_ptr<Context> context, std::size_t concat_cache_num_bytes, int num_bytes_timeout, bool concat_all, std::string cache_machine_name);

DECLARE_DISPATCH(make_single_machine_fn, make_single_machine_stub);
DECLARE_DISPATCH(make_concatenating_machine_fn, make_concatenating_machine_stub);

}  // namespace cache
}  // namespace ral

