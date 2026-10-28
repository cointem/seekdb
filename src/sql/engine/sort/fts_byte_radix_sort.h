/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <utility>

namespace oceanbase
{
namespace sql
{
// Keys must already encode SQL ordering as unsigned lexicographic bytes.
// Reorder only the caller-owned pointers; keep auxiliary memory independent
// of the posting count. Zero from check_status means success.
template <typename Row, typename KeyOf, typename CheckStatus>
int fts_byte_radix_sort(Row **rows, int64_t count, const KeyOf &key_of,
                        const CheckStatus &check_status, int64_t prefix = 0, int depth = 0)
{
  int ret = 0;
  while (ret == 0 && count > 1) {
    if ((ret = check_status()) != 0) { break; }
    // Bound recursion and long-prefix work. Equal byte keys need no stable
    // tie order, matching the existing encoded sorter.
    if (count < 64 || depth >= 16 || prefix >= 256) {
      std::sort(rows, rows + count, [&](const Row *left, const Row *right) {
        const auto &a = key_of(left);
        const auto &b = key_of(right);
        const auto length = std::min(a.len_, b.len_);
        const int cmp = length == 0 ? 0 : std::memcmp(a.ptr_, b.ptr_, length);
        return cmp == 0 ? a.len_ < b.len_ : cmp < 0;
      });
      break;
    }
    int64_t sizes[257] = {};
    int64_t ends[257];
    int64_t next[257];
    auto bucket = [&](const Row *row) -> int {
      const auto &key = key_of(row);
      return prefix >= key.len_ ? 0 : 1 + static_cast<unsigned char>(key.ptr_[prefix]);
    };
    for (int64_t i = 0; i < count; ++i) {
      if ((i & 8191) == 0 && (ret = check_status()) != 0) { break; }
      ++sizes[bucket(rows[i])];
    }
    if (ret != 0) { break; }
    int live = 0;
    int only = 0;
    int64_t offset = 0;
    for (int i = 0; i < 257; ++i) {
      next[i] = offset;
      offset += sizes[i];
      ends[i] = offset;
      if (sizes[i] != 0) { ++live; only = i; }
    }
    if (live == 1) {
      if (only == 0) { break; }
      ++prefix;
      continue;
    }
    int64_t moves = 0;
    for (int i = 0; ret == 0 && i < 257; ++i) {
      while (ret == 0 && next[i] < ends[i]) {
        if ((moves++ & 8191) == 0 && (ret = check_status()) != 0) { break; }
        const int destination = bucket(rows[next[i]]);
        if (destination == i) {
          ++next[i];
        } else {
          std::swap(rows[next[i]], rows[next[destination]++]);
        }
      }
    }
    // Bucket zero contains terminal keys and is already in final order.
    for (int i = 1; ret == 0 && i < 257; ++i) {
      if (sizes[i] > 1) {
        ret = fts_byte_radix_sort(rows + ends[i] - sizes[i], sizes[i], key_of,
                                 check_status, prefix + 1, depth + 1);
      }
    }
    break;
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
