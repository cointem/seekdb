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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_ac_matcher.h"
#include "storage/fts/dict/ob_ik_dic.h"
#include "lib/allocator/ob_malloc.h"
#include <atomic>
#include <new>
#include <mutex>
#include <chrono>
#include <cstdio>
#include <cstdlib>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
namespace {
// Process-wide cap includes all live generations AND temporary BFS memory.
// Allocations reserve before malloc; failed/new generations simply use the DAT.
std::atomic<uint64_t> ik_ac_charged_bytes{0};
uint64_t ik_ac_budget()
{
  static const uint64_t value = []() -> uint64_t {
    constexpr uint64_t cap = 64ULL * 1024 * 1024;
    const char *input = std::getenv("FTS_IK_AC_MAX_BYTES");
    if (nullptr == input) { return cap; }
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(input, &end, 10);
    return end != input && *end == 0 && parsed <= cap ? parsed : cap;
  }();
  return value;
}
void *ik_ac_alloc(uint64_t bytes)
{
  uint64_t current = ik_ac_charged_bytes.load(std::memory_order_relaxed);
  do {
    if (bytes > ik_ac_budget() || current > ik_ac_budget() - bytes) { return nullptr; }
  } while (!ik_ac_charged_bytes.compare_exchange_weak(current, current + bytes,
                                                     std::memory_order_relaxed));
  void *buffer = common::ob_malloc(bytes, common::ObMemAttr("IKAC"));
  if (nullptr == buffer) { ik_ac_charged_bytes.fetch_sub(bytes, std::memory_order_relaxed); }
  return buffer;
}
void ik_ac_free(void *buffer, uint64_t bytes)
{
  if (nullptr != buffer) {
    common::ob_free(buffer);
    ik_ac_charged_bytes.fetch_sub(bytes, std::memory_order_relaxed);
  }
}
}

struct IKACMatcher::Impl
{
  static constexpr uint32_t MAX_NODES = 1024 * 1024;
  static constexpr uint32_t MAX_DEPTH = 256;
  struct Node {
    uint32_t child_ = 0, next_ = 0, fail_ = 0, output_ = 0;
    uint32_t depth_ = 0, unicode_ = 0, edge_count_ = 0, terminal_ = 0;
  };
  struct Edge { uint32_t unicode_, node_; };
  Node *nodes_ = nullptr;
  Edge *edges_ = nullptr;
  uint32_t *root_ = nullptr;
  uint32_t count_ = 0;
  uint64_t node_bytes_ = 0, edge_bytes_ = 0;
  static constexpr uint64_t ROOT_BYTES = 65536 * sizeof(uint32_t);
  ~Impl()
  {
    ik_ac_free(nodes_, node_bytes_);
    ik_ac_free(edges_, edge_bytes_);
    ik_ac_free(root_, ROOT_BYTES);
  }
};

IKACMatcher::~IKACMatcher() { delete impl_; }

IKACMatcher::Lease IKACMatcher::acquire(uint64_t generation)
{
  static std::mutex lock;
  static uint64_t cached_generation = 0;
  static bool attempted = false;
  static Lease cached;
  std::lock_guard<std::mutex> guard(lock);
  if (!attempted || cached_generation != generation) {
    cached.reset(); // Leases pin any older generation still being read.
    cached_generation = generation;
    attempted = true;
    const auto start = std::chrono::steady_clock::now();
    IKACMatcher *candidate = new (std::nothrow) IKACMatcher;
    const int ret = nullptr == candidate ? OB_ALLOCATE_MEMORY_FAILED : candidate->build();
    const uint64_t nodes = nullptr != candidate && nullptr != candidate->impl_
        ? candidate->impl_->count_ : 0;
    if (OB_SUCCESS == ret) { cached = Lease(candidate); }
    else { delete candidate; }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
    std::fprintf(stderr, "FTS_IK_AC_BUILD {\"generation\":%llu,\"ready\":%s,\"ret\":%d,"
        "\"elapsed_us\":%llu,\"nodes\":%llu,\"charged_bytes\":%llu,\"cap_bytes\":%llu}\n",
        static_cast<unsigned long long>(generation), cached ? "true" : "false", ret,
        static_cast<unsigned long long>(elapsed), static_cast<unsigned long long>(nodes),
        static_cast<unsigned long long>(ik_ac_charged_bytes.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(ik_ac_budget()));
    std::fflush(stderr);
  }
  return cached;
}

int IKACMatcher::build()
{
  if (ik_ac_budget() < sizeof(Impl::Node) + Impl::ROOT_BYTES) { return OB_SIZE_OVERFLOW; }
  impl_ = new (std::nothrow) Impl;
  if (nullptr == impl_) { return OB_ALLOCATE_MEMORY_FAILED; }
  Impl &data = *impl_;
  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_BIN);
  if (nullptr == cs || nullptr == cs->cset) { return OB_NOT_INIT; }
  // Count exact trie nodes before allocating. No vector growth/throwing
  // allocation and no reserve of an unused MAX_NODES-sized backing block.
  ObIKDictIterator sizing(ObIKDictLoader::dict_text());
  int sizing_ret = sizing.init();
  uint32_t previous_codes[Impl::MAX_DEPTH] = {0};
  uint32_t previous_size = 0;
  uint64_t node_count = 1;
  ObString previous_word;
  while (OB_SUCCESS == sizing_ret) {
    ObString word;
    if (OB_SUCCESS != sizing.get_key(word)) { return OB_ERR_UNEXPECTED; }
    if (word.empty() || (!previous_word.empty() && previous_word.compare(word) > 0)) { return OB_NOT_SUPPORTED; }
    uint32_t depth = 0;
    bool shared = true;
    const auto *cursor = reinterpret_cast<const unsigned char *>(word.ptr());
    const auto *end = cursor + word.length();
    while (cursor < end) {
      ob_wc_t code = 0;
      const int bytes = common::ob_charset_decode_unicode<CHARSET_UTF8MB4>(cursor, end, code);
      if (bytes <= 0 || depth >= Impl::MAX_DEPTH) { return OB_NOT_SUPPORTED; }
      if (!shared || depth >= previous_size || previous_codes[depth] != code) {
        shared = false;
        if (++node_count > Impl::MAX_NODES) { return OB_SIZE_OVERFLOW; }
      }
      previous_codes[depth++] = code;
      cursor += bytes;
    }
    previous_size = depth;
    previous_word = word;
    sizing_ret = sizing.next();
  }
  if (sizing_ret != OB_ITER_END) { return sizing_ret; }
  data.node_bytes_ = sizeof(Impl::Node) * node_count;
  data.nodes_ = static_cast<Impl::Node *>(ik_ac_alloc(data.node_bytes_));
  if (nullptr == data.nodes_) { return OB_ALLOCATE_MEMORY_FAILED; }
  new (&data.nodes_[0]) Impl::Node;
  data.count_ = 1;
  uint32_t path[Impl::MAX_DEPTH + 1] = {0};
  uint32_t previous_depth = 0;
  ObString previous;
  ObIKDictIterator iterator(ObIKDictLoader::dict_text());
  int ret = iterator.init();
  while (OB_SUCC(ret)) {
    ObString word;
    if (OB_FAIL(iterator.get_key(word))) { break; }
    // The very same iterator feeds the legacy DAT. Its sorted order permits a
    // linear prefix-stack builder, checked rather than assumed on new corpora.
    if (word.empty() || (!previous.empty() && previous.compare(word) > 0)) {
      return OB_NOT_SUPPORTED;
    }
    uint32_t depth = 0;
    bool shared_prefix = true;
    const unsigned char *cursor = reinterpret_cast<const unsigned char *>(word.ptr());
    const unsigned char *end = cursor + word.length();
    while (cursor < end) {
      ob_wc_t unicode = 0;
      const int bytes = common::ob_charset_decode_unicode<CHARSET_UTF8MB4>(cursor, end, unicode);
      if (bytes <= 0 || ++depth > Impl::MAX_DEPTH) { return OB_NOT_SUPPORTED; }
      if (shared_prefix && depth <= previous_depth
          && data.nodes_[path[depth]].unicode_ == unicode) {
        // Reuse the common prefix of consecutive dictionary entries.
      } else {
        shared_prefix = false;
        if (data.count_ >= node_count) { return OB_SIZE_OVERFLOW; }
        const uint32_t parent = path[depth - 1];
        Impl::Node &node = data.nodes_[data.count_];
        new (&node) Impl::Node;
        node.unicode_ = unicode;
        node.depth_ = depth;
        Impl::Node &parent_node = data.nodes_[parent];
        // output_ is temporarily the last-child pointer during construction.
        if (parent_node.edge_count_ == 0) { parent_node.child_ = data.count_; }
        else {
          if (data.nodes_[parent_node.output_].unicode_ >= unicode) { return OB_NOT_SUPPORTED; }
          data.nodes_[parent_node.output_].next_ = data.count_;
        }
        parent_node.output_ = data.count_;
        ++parent_node.edge_count_;
        path[depth] = data.count_++;
      }
      cursor += bytes;
    }
    data.nodes_[path[depth]].terminal_ = 1;
    previous = word;
    previous_depth = depth;
    ret = iterator.next();
  }
  if (ret != OB_ITER_END) { return ret; }
  data.edge_bytes_ = (data.count_ - 1) * sizeof(Impl::Edge);
  data.edges_ = static_cast<Impl::Edge *>(ik_ac_alloc(data.edge_bytes_));
  data.root_ = static_cast<uint32_t *>(ik_ac_alloc(Impl::ROOT_BYTES));
  if (nullptr == data.edges_ || nullptr == data.root_) { return OB_ALLOCATE_MEMORY_FAILED; }
  MEMSET(data.root_, 0, Impl::ROOT_BYTES);
  uint32_t edge_count = 0;
  for (uint32_t i = 0; i < data.count_; ++i) {
    Impl::Node &node = data.nodes_[i];
    uint32_t child_index = node.child_;
    node.child_ = edge_count;
    node.output_ = 0;
    for (uint32_t j = 0; j < node.edge_count_; ++j) {
      const Impl::Node &child_node = data.nodes_[child_index];
      data.edges_[edge_count++] = {child_node.unicode_, child_index};
      if (i == 0 && child_node.unicode_ < 65536) { data.root_[child_node.unicode_] = child_index; }
      child_index = child_node.next_;
    }
  }
  const uint64_t queue_bytes = data.count_ * sizeof(uint32_t);
  uint32_t *queue = static_cast<uint32_t *>(ik_ac_alloc(queue_bytes));
  if (nullptr == queue) { return OB_ALLOCATE_MEMORY_FAILED; }
  uint32_t head = 0, tail = 0;
  const Impl::Node &root = data.nodes_[0];
  for (uint32_t i = 0; i < root.edge_count_; ++i) { queue[tail++] = data.edges_[root.child_ + i].node_; }
  while (head < tail) {
    const uint32_t parent = queue[head++];
    const Impl::Node &node = data.nodes_[parent];
    for (uint32_t j = 0; j < node.edge_count_; ++j) {
      const Impl::Edge &edge = data.edges_[node.child_ + j];
      uint32_t failure = node.fail_;
      uint32_t target = child(failure, edge.unicode_);
      while (failure != 0 && target == 0) {
        failure = data.nodes_[failure].fail_;
        target = child(failure, edge.unicode_);
      }
      Impl::Node &next = data.nodes_[edge.node_];
      next.fail_ = target;
      next.output_ = data.nodes_[target].terminal_ ? target : data.nodes_[target].output_;
      queue[tail++] = edge.node_;
    }
  }
  ik_ac_free(queue, queue_bytes);
  return OB_SUCCESS;
}

uint32_t IKACMatcher::child(uint32_t state, ob_wc_t unicode) const
{
  const Impl &data = *impl_;
  if (state == 0 && unicode < 65536) { return data.root_[unicode]; }
  const Impl::Node &node = data.nodes_[state];
  uint32_t low = node.child_, high = low + node.edge_count_;
  while (low < high) {
    const uint32_t middle = low + (high - low) / 2;
    const Impl::Edge &edge = data.edges_[middle];
    if (edge.unicode_ < unicode) { low = middle + 1; }
    else if (edge.unicode_ > unicode) { high = middle; }
    else { return edge.node_; }
  }
  return 0;
}

uint32_t IKACMatcher::advance(uint32_t state, ob_wc_t unicode) const
{
  uint32_t next = child(state, unicode);
  while (state != 0 && next == 0) {
    state = impl_->nodes_[state].fail_;
    next = child(state, unicode);
  }
  return next;
}

int IKACMatcher::output(uint32_t state, TokenizeContext &ctx) const
{
  int ret = OB_SUCCESS;
  uint32_t current = impl_->nodes_[state].terminal_ ? state : impl_->nodes_[state].output_;
  // Longest first == legacy active hits in ascending start-position order.
  while (OB_SUCC(ret) && current != 0) {
    const uint32_t depth = impl_->nodes_[current].depth_;
    const int64_t offset = ctx.char_offset(ctx.current_char_index() + 1 - depth);
    ret = ctx.add_token(ctx.fulltext(), offset, ctx.get_end_cursor() - offset,
                       depth, ObIKTokenType::IK_CHINESE_TOKEN);
    current = impl_->nodes_[current].output_;
  }
  return ret;
}

int ObIKCJKProcessor::do_process(TokenizeContext &ctx,
                                 const char *ch,
                                 const uint8_t char_len,
                                 const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  if (ctx.default_pipeline_enabled()) { return ctx.match_main_dictionary(); }

  if (ObFTCharUtil::CharType::USELESS != type) {
    // handle previous hits_ first and then check from this char
    for (ObList<ObDATrieHit, ObIAllocator>::iterator iter = hits_.begin();
         OB_SUCC(ret) && iter != hits_.end();
         iter++) {
      ObDATrieHit &hit = *iter;
      if (OB_FAIL(dict_main_.match_with_hit({char_len, ch}, hit, hit))) {
      } else if (hit.is_match()) {
        if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                  hit.start_pos_,
                                  hit.end_pos_ - hit.start_pos_,
                                  hit.char_cnt_,
                                  ObIKTokenType::IK_CHINESE_TOKEN))) {
        } else if (hit.is_prefix()) {
          // match will record the start_cursor
        }
      } else if (hit.is_prefix()) {
        // nothing
      } else if (hit.is_unmatch()) {
        hits_.erase(hit);
      } else {
        ret = OB_UNEXPECT_INTERNAL_ERROR;
      }
    }
    if (OB_SUCC(ret)) {
      // Start from this char
      // find range of dict
      ObDATrieHit hit(&dict_main_, ctx.get_cursor());
      if (OB_FAIL(dict_main_.match({char_len, ch}, hit))) {
      } else if (hit.is_match()) {
        // output token
        hits_.push_back(hit);
        ctx.add_token(ctx.fulltext(),
                      ctx.get_cursor(),
                      char_len,
                      1,
                      ObIKTokenType::IK_CHINESE_TOKEN);
      } else if (hit.is_prefix()) {
        hits_.push_back(hit);
      } else {
        // ignore mismatch
      }
    }
  } else {
    // stop previous match
    hits_.clear();
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_last()) {
      hits_.clear();
    }
  } else {
    hits_.clear();
  }

  return ret;
}

} //  namespace storage
} //  namespace oceanbase
