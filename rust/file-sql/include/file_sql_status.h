#pragma once
#include <cstdint>

namespace oceanbase { namespace file_sql {
class FileSqlStatus final {
public:
  using Check = std::int32_t (*)(void *);
  FileSqlStatus(void *context, Check check) : context_(context), check_(check), status_(0) {}
  std::int32_t check() const noexcept { return status_ = check_(context_); }
  std::int32_t last_status() const noexcept { return status_; }
private:
  void *context_;
  Check check_;
  mutable std::int32_t status_;
};
}}
