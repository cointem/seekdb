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

#include "data_plane/fts/ob_fts_struct.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase { namespace storage {
// DEFAULT TOKENIZE only. Input views are borrowed until expression packing;
// transient normalized spellings are retained in document-owned slabs.
class FTUniqueTerms
{
public:
  static constexpr int64_t MAX_TERMS = 16384;
  static constexpr int64_t MAX_OWNED_BYTES = 1024 * 1024;
  struct Entry { ObFTWord first; int32_t next_; };
  class Iterator
  {
  public:
    Iterator(const FTUniqueTerms *owner, int64_t bucket, int32_t index)
        : owner_(owner), bucket_(bucket), index_(index) {}
    const Entry *operator->() const { return &owner_->entries_.at(index_); }
    bool operator!=(const Iterator &other) const { return index_ != other.index_; }
    Iterator &operator++()
    {
      index_ = owner_->entries_.at(index_).next_;
      while (index_ < 0 && ++bucket_ < owner_->heads_.count()) {
        index_ = owner_->heads_.at(bucket_);
      }
      return *this;
    }
  private:
    const FTUniqueTerms *owner_;
    int64_t bucket_;
    int32_t index_;
  };
  FTUniqueTerms() : strings_("TokenizeUnique") {}
  int start(int64_t legacy_bucket_count, const char *input, int64_t length);
  int add(const ObFTWord &word, const common::ObObjMeta &meta);
  void reuse();
  int64_t size() const { return entries_.count(); }
  bool overflow() const { return overflow_; }
  Iterator begin() const
  {
    int64_t bucket = 0;
    while (bucket < heads_.count() && heads_.at(bucket) < 0) { ++bucket; }
    return Iterator(this, bucket, bucket < heads_.count() ? heads_.at(bucket) : -1);
  }
  Iterator end() const { return Iterator(this, heads_.count(), -1); }
private:
  common::ObSEArray<Entry, 64> entries_;
  common::ObSEArray<int32_t, 128> heads_;
  common::ObArenaAllocator strings_;
  const char *input_ = nullptr;
  int64_t input_length_ = 0;
  char *slab_ = nullptr;
  int64_t remaining_ = 0;
  int64_t owned_bytes_ = 0;
  bool overflow_ = false;
};
} }
