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

#include <cstdint>
#include <cstring>

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(__x86_64__)
#include <tmmintrin.h>
#endif

namespace oceanbase
{
namespace blocksstable
{
// Expand a packed difference string into one SIMD-width result. The template
// has zeroes at difference positions; a byte permutation scatters the packed
// bytes into those positions. This plan is shared by every row in a batch.
class ShortStringDiffDecoder
{
public:
  static constexpr int64_t WIDTH = 16;

  ShortStringDiffDecoder(const char *common_template, const uint8_t *diff_positions,
                        const int64_t diff_count, const bool use_x86_simd = false)
    : diff_count_(diff_count), use_x86_simd_(use_x86_simd)
  {
    std::memcpy(common_, common_template, WIDTH);
    std::memset(shuffle_, 0x80, WIDTH);
    for (int64_t i = 0; i < diff_count_; ++i) {
      positions_[i] = diff_positions[i];
      shuffle_[positions_[i]] = static_cast<uint8_t>(i);
    }
#if defined(__aarch64__) && defined(__ARM_NEON)
    common_vector_ = vld1q_u8(common_);
    shuffle_vector_ = vld1q_u8(shuffle_);
#endif
  }

  // Exactly WIDTH bytes are written into the caller's padded slot. A source
  // load must stay inside the fixed column, including its final physical row.
  void decode(const char *packed, const int64_t available, char *result) const
  {
#if defined(__aarch64__) && defined(__ARM_NEON)
    alignas(16) char tail[WIDTH] = {};
    if (available < WIDTH) {
      std::memcpy(tail, packed, diff_count_);
      packed = tail;
    }
    const uint8x16_t differences = vqtbl1q_u8(
        vld1q_u8(reinterpret_cast<const uint8_t *>(packed)), shuffle_vector_);
    vst1q_u8(reinterpret_cast<uint8_t *>(result), vorrq_u8(common_vector_, differences));
#elif defined(__x86_64__)
    if (use_x86_simd_) {
      decode_ssse3(packed, available, result);
    } else {
      decode_scalar(packed, result);
    }
#else
    (void)available;
    decode_scalar(packed, result);
#endif
  }

  void decode_scalar(const char *packed, char *result) const
  {
    std::memcpy(result, common_, WIDTH);
    for (int64_t i = 0; i < diff_count_; ++i) {
      result[positions_[i]] = packed[i];
    }
  }

private:
#if defined(__x86_64__)
  // The caller uses the engine's CPU feature dispatch before enabling this.
  __attribute__((target("ssse3")))
  void decode_ssse3(const char *packed, const int64_t available, char *result) const
  {
    alignas(16) char tail[WIDTH] = {};
    if (available < WIDTH) {
      std::memcpy(tail, packed, diff_count_);
      packed = tail;
    }
    const __m128i differences = _mm_shuffle_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(packed)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(shuffle_)));
    _mm_storeu_si128(reinterpret_cast<__m128i *>(result), _mm_or_si128(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(common_)), differences));
  }
#endif
  int64_t diff_count_;
  [[maybe_unused]] bool use_x86_simd_;
  uint8_t common_[WIDTH];
  uint8_t shuffle_[WIDTH];
  uint8_t positions_[WIDTH];
#if defined(__aarch64__) && defined(__ARM_NEON)
  uint8x16_t common_vector_;
  uint8x16_t shuffle_vector_;
#endif
};
} // namespace blocksstable
} // namespace oceanbase
