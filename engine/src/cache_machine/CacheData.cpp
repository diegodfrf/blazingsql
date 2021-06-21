#include "CacheData.h"

namespace ral {
namespace cache {

std::string MetadataDictionary::get_value(std::string key) {
  if (!this->has_value(key)) {
    return std::string();
  }
  return this->values.at(key);
}

void MetadataDictionary::set_value(std::string key, std::string value) {
  this->values[key] = value;
}

}  // namespace cache
}  // namespace ral
