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

#include <cstddef>
#include <cstdint>
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

// A word map is private to one parser invocation or DDL iterator. Keep small
// maps off the heap, but use the existing allocator for unbounded documents.
template <class T, int64_t INLINE_COUNT = 64>
class FTWordNodeAllocator final
{
  union Slot
  {
    Slot() {}
    ~Slot() {}
    T value_;
    Slot *next_;
  };
  using OverflowAllocator = common::hash::SimpleAllocer<T,
      common::hash::NodeNumTraits<T>::NODE_NUM, common::hash::NoPthreadDefendMode>;
public:
  FTWordNodeAllocator() : next_unused_(0), free_list_(nullptr) {}
  void set_attr(const common::ObMemAttr &attr) { overflow_.set_attr(attr); }
  void set_label(const lib::ObLabel &label) { overflow_.set_label(label); }
  int reserve(int64_t count)
  {
    return overflow_.reserve(count > INLINE_COUNT ? count - INLINE_COUNT : 0);
  }
  template <class... Args>
  T *alloc(Args &...args)
  {
    Slot *slot = nullptr;
    if (nullptr != free_list_) {
      slot = free_list_;
      free_list_ = slot->next_;
    } else if (next_unused_ < INLINE_COUNT) {
      slot = &slots_[next_unused_++];
    }
    return nullptr == slot ? overflow_.alloc(args...) : new (&slot->value_) T(args...);
  }
  void free(T *value)
  {
    const uintptr_t addr = reinterpret_cast<uintptr_t>(value);
    const uintptr_t first = reinterpret_cast<uintptr_t>(&slots_[0]);
    if (addr >= first && addr < first + sizeof(slots_)) {
      Slot *slot = reinterpret_cast<Slot *>(value);
      value->~T();
      slot->next_ = free_list_;
      free_list_ = slot;
    } else {
      overflow_.free(value);
    }
  }
private:
  Slot slots_[INLINE_COUNT];
  int64_t next_unused_;
  Slot *free_list_;
  OverflowAllocator overflow_;
  DISALLOW_COPY_AND_ASSIGN(FTWordNodeAllocator);
};

// The backing buffer belongs to the word map, rather than this allocator:
// ObHashTable also contains an unused default bucket allocator.
class FTWordBucketAllocator final
{
public:
  explicit FTWordBucketAllocator(const lib::ObLabel &label = common::ObModIds::OB_HASH_BUCKET)
      : overflow_(label), buffer_(nullptr), capacity_(0), used_(false) {}
  void set_attr(const common::ObMemAttr &attr) { overflow_.set_attr(attr); }
  void set_buffer(void *buffer, int64_t capacity)
  {
    buffer_ = buffer;
    capacity_ = capacity;
  }
  void *alloc(int64_t size)
  {
    void *result = nullptr;
    if (!used_ && nullptr != buffer_ && size > 0 && size <= capacity_) {
      used_ = true;
      result = buffer_;
    } else {
      result = overflow_.alloc(size);
    }
    return result;
  }
  void free(void *ptr)
  {
    if (nullptr != buffer_ && ptr == buffer_) {
      used_ = false;
    } else {
      overflow_.free(ptr);
    }
  }
private:
  common::hash::DefaultSimpleAllocerAllocator overflow_;
  void *buffer_;
  int64_t capacity_;
  bool used_;
  DISALLOW_COPY_AND_ASSIGN(FTWordBucketAllocator);
};

} // namespace storage
} // namespace oceanbase
