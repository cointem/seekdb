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

#include <atomic>
#include <cstdint>
#include "lib/charset/ob_charset.h"

namespace oceanbase { namespace storage {
class TokenizeContext;
// Only the compiled builtin main dictionary. Readers hold a strong generation
// lease; the shared automaton contains no input text, cursor, or query results.
class IKACMatcher
{
public:
  class Lease
  {
  public:
    Lease() = default;
    Lease(const Lease &other) : matcher_(other.matcher_) { retain(); }
    Lease &operator=(const Lease &other)
    {
      if (this != &other) {
        const IKACMatcher *next = other.matcher_;
        if (nullptr != next) { next->references_.fetch_add(1, std::memory_order_relaxed); }
        reset();
        matcher_ = next;
      }
      return *this;
    }
    ~Lease() { reset(); }
    void reset()
    {
      if (nullptr != matcher_ && matcher_->references_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete matcher_;
      }
      matcher_ = nullptr;
    }
    explicit operator bool() const { return nullptr != matcher_; }
    const IKACMatcher *operator->() const { return matcher_; }
  private:
    friend class IKACMatcher;
    explicit Lease(const IKACMatcher *matcher) : matcher_(matcher) {}
    void retain() { if (nullptr != matcher_) { matcher_->references_.fetch_add(1, std::memory_order_relaxed); } }
    const IKACMatcher *matcher_ = nullptr;
  };
  static Lease acquire(uint64_t generation);
  ~IKACMatcher();
  uint32_t advance(uint32_t state, ob_wc_t unicode) const;
  int output(uint32_t state, TokenizeContext &ctx) const;
private:
  IKACMatcher() = default;
  int build();
  uint32_t child(uint32_t state, ob_wc_t unicode) const;
  struct Impl;
  Impl *impl_ = nullptr;
  mutable std::atomic<uint32_t> references_{1};
};
} }
