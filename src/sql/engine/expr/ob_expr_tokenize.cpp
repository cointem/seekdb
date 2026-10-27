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

#include "sql/engine/expr/ob_expr_tokenize.h"
#include <mutex>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <ctime>

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_bin.h"
#include "common/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "object/ob_object.h"
#include "sql/resolver/ddl/ob_fts_index_builder_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_json_access_utils.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "data_plane/fts/dict/ob_gen_dic_loader.h"
#include "data_plane/fts/ob_fts_parser_property.h"
#include "data_plane/fts/ob_fts_literal.h"

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_json_func_helper.h" // file not self-contained, there're logs inside.

namespace oceanbase
{
namespace sql
{
namespace
{
// Diagnostic only: flags are immutable for a process and ignored unless profiling
// is explicitly enabled. Disabled calls never read the thread CPU clock.
struct TokenizeCpuConfig
{
  static bool env_enabled(const char *name)
  {
    const char *value = std::getenv(name);
    return nullptr != value && 0 == std::strcmp(value, "1");
  }
  const bool enabled_ = env_enabled("FTS_TOKENIZE_PROFILE");
  const bool force_legacy_ = enabled_ && env_enabled("FTS_TOKENIZE_PROFILE_FORCE_LEGACY");
  const bool no_ac_ = enabled_ && env_enabled("FTS_TOKENIZE_PROFILE_NO_AC");
};

class TokenizeCpuProfile
{
public:
  explicit TokenizeCpuProfile(const int &status) : config_(config()), status_(status)
  {
    start_ = stamp();
  }
  ~TokenizeCpuProfile()
  {
    if (!config_.enabled_) { return; }
    const uint64_t total = elapsed(start_);
    static Totals totals[6];
    Totals &sum = totals[parser_ + (all_ ? 3 : 0)];
    sum.total_.fetch_add(total, std::memory_order_relaxed);
    sum.parse_.fetch_add(parse_ns_, std::memory_order_relaxed);
    sum.pack_.fetch_add(pack_ns_, std::memory_order_relaxed);
    sum.release_.fetch_add(release_ns_, std::memory_order_relaxed);
    if (fallback_) { sum.fallback_.fetch_add(1, std::memory_order_relaxed); }
    if (OB_SUCCESS != status_) { sum.errors_.fetch_add(1, std::memory_order_relaxed); }
    const uint64_t count = sum.calls_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (count % 3000 == 0) {
      const char *name = parser_ == 0 ? "ik" : parser_ == 1 ? "beng" : "other";
      const char *mode = config_.force_legacy_ ? "C_legacy"
          : config_.no_ac_ ? "B_dense_dat" : "A_dense_ac";
      // One write per 3000 completed docs, independent of native log filtering.
      // Concurrent snapshots are cumulative relaxed-atomic diagnostics; adjacent
      // fields can include another document finishing during this read.
      std::fprintf(stderr, "FTS_TOKENIZE_CPU {\"parser\":\"%s\",\"output\":\"%s\","
          "\"mode\":\"%s\",\"calls\":%llu,\"total_cpu_ns\":%llu,"
          "\"parse_cpu_ns\":%llu,\"pack_cpu_ns\":%llu,\"release_cpu_ns\":%llu,"
          "\"fallback\":%llu,\"errors\":%llu}\n",
          name, all_ ? "all" : "default", mode, static_cast<unsigned long long>(count),
          static_cast<unsigned long long>(sum.total_.load(std::memory_order_relaxed)),
          static_cast<unsigned long long>(sum.parse_.load(std::memory_order_relaxed)),
          static_cast<unsigned long long>(sum.pack_.load(std::memory_order_relaxed)),
          static_cast<unsigned long long>(sum.release_.load(std::memory_order_relaxed)),
          static_cast<unsigned long long>(sum.fallback_.load(std::memory_order_relaxed)),
          static_cast<unsigned long long>(sum.errors_.load(std::memory_order_relaxed)));
      std::fflush(stderr);
    }
  }
  uint64_t stamp() const
  {
    if (!config_.enabled_) { return 0; }
    timespec value{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL + value.tv_nsec;
  }
  uint64_t elapsed(uint64_t start) const { return config_.enabled_ ? stamp() - start : 0; }
  bool force_legacy() const { return config_.force_legacy_; }
  void set_parser(const ObString &name, bool all)
  {
    if (config_.enabled_) {
      parser_ = name.length() >= 3 && 0 == MEMCMP(name.ptr(), "ik.", 3) ? 0
          : name.length() >= 5 && 0 == MEMCMP(name.ptr(), "beng.", 5) ? 1 : 2;
      all_ = all;
    }
  }
  uint64_t parse_ns_ = 0;
  uint64_t pack_ns_ = 0;
  uint64_t release_ns_ = 0;
  bool fallback_ = false;
private:
  struct Totals
  {
    std::atomic<uint64_t> total_{0}, parse_{0}, pack_{0}, release_{0};
    std::atomic<uint64_t> calls_{0}, fallback_{0}, errors_{0};
  };
  static const TokenizeCpuConfig &config() { static const TokenizeCpuConfig value; return value; }
  const TokenizeCpuConfig &config_;
  const int &status_;
  uint64_t start_ = 0;
  int parser_ = 2;
  bool all_ = false;
};

// DEFAULT output contains only strings. Serialize that flat array into the
// expression-owned payload, without creating a JSON tree or a temporary binary.
// Keep the generic tree/serializer path for ALL and for nonconstant config.
template <typename Terms>
int pack_token_array_binary(const ObExpr &expr, ObEvalCtx &ctx,
                            const Terms &words, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const uint64_t count = words.size();
  uint64_t payload_size = 0;
  for (auto it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it) {
    const ObString word = it->first.get_word().get_string();
    const uint64_t size = sizeof(uint8_t)
        + serialization::encoded_length_vi64(word.length()) + word.length();
    if (payload_size > static_cast<uint64_t>(INT64_MAX) - size) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      payload_size += size;
    }
  }

  ObJsonBinMeta meta;
  meta.set_type(ObJsonBin::get_array_vertype(), false);
  meta.set_element_count(count);
  meta.set_element_count_var_type(ObJsonVar::get_var_type(count));
  meta.set_is_continuous(true);
  uint8_t size_type = ObJsonVar::get_var_type(static_cast<uint64_t>(0));
  uint64_t array_size = 0;
  uint64_t header_size = 0;
  // Widening the size/offset fields also grows the entry table. Find its fixed
  // point before reserving the output, so even width boundaries need no rewrite.
  while (OB_SUCC(ret)) {
    const uint64_t width = ObJsonVar::get_var_size(size_type);
    const uint64_t fixed = sizeof(ObJsonBinArrHeader)
        + meta.element_count_var_size() + width;
    if (count > static_cast<uint64_t>(INT32_MAX)
        || payload_size > static_cast<uint64_t>(INT64_MAX) - fixed
        || count > (static_cast<uint64_t>(INT64_MAX) - fixed - payload_size)
                       / (width + OB_JSON_BIN_VALUE_TYPE_LEN)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      header_size = fixed + count * (width + OB_JSON_BIN_VALUE_TYPE_LEN);
      array_size = header_size + payload_size;
      const uint8_t required_type = ObJsonVar::get_var_type(array_size);
      if (required_type <= size_type) {
        break;
      } else {
        size_type = required_type;
      }
    }
  }

  ObTextStringDatumResult result(expr.datum_meta_.type_, &expr, &ctx, &datum);
  char *output = nullptr;
  int64_t capacity = 0;
  const uint64_t total_size = sizeof(ObJsonBinDocHeader) + array_size;
  // The document header stores its end offset in 42 bits. The LOB result helper
  // performs the same result-size and LOB-header checks as pack_json_str_res.
  if (OB_FAIL(ret)) {
  } else if (total_size >= (1ULL << 42)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(result.init(total_size))) {
  } else if (OB_FAIL(result.get_reserved_buffer(output, capacity))) {
  } else if (static_cast<uint64_t>(capacity) < total_size) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObJsonBinDocHeader doc_header;
    doc_header.extend_seg_offset_ = total_size;
    MEMCPY(output, &doc_header, sizeof(doc_header));
    char *array = output + sizeof(doc_header);
    meta.set_entry_var_type(size_type);
    meta.set_obj_size_var_type(size_type);
    meta.set_obj_size(array_size);
    ObJsonBinArrHeader array_header;
    int64_t pos = header_size;
    int index = 0;
    if (OB_FAIL(meta.calc_entry_array())) {
    } else if (OB_FAIL(meta.to_header(array_header))) {
    } else {
      MEMCPY(array, &array_header, sizeof(array_header));
      if (OB_FAIL(ObJsonVar::set_var(count, meta.element_count_var_type(),
                                   array + meta.get_element_count_offset()))) {
      } else if (OB_FAIL(ObJsonVar::set_var(array_size, size_type,
                                          array + meta.get_obj_size_offset()))) {
      }
    }
    for (auto it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it, ++index) {
      const ObString word = it->first.get_word().get_string();
      const uint8_t string_type = ObJsonBin::get_string_vertype();
      if (OB_FAIL(ObJsonBinSerializer::set_value_entry(meta, array, index, pos, string_type))) {
      } else {
        array[pos++] = static_cast<char>(string_type);
        if (OB_FAIL(serialization::encode_vi64(array, array_size, pos, word.length()))) {
        } else {
          MEMCPY(array + pos, word.ptr(), word.length());
          pos += word.length();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (static_cast<uint64_t>(pos) != array_size) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result.lseek(total_size, 0))) {
    } else {
      result.set_result();
    }
  }
  return ret;
}
} // namespace

ObExprTokenize::ObExprTokenize(common::ObIAllocator &alloc)
    : ObStringExprOperator(alloc,
                           T_FUN_TOKENIZE,
                           N_TOKENIZE,
                           MORE_THAN_ZERO,
                           VALID_FOR_GENERATED_COL)
{
}

ObExprTokenize::~ObExprTokenize() {}

int ObExprTokenize::eval_tokenize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  if (expr.extra_ != 0) {
    return eval_reusable(expr, ctx, expr_datum);
  }
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  ObIJsonBase *json_result = nullptr;
  TokenizeParam param;

  // check param num, which is checked in ObExprOperator::calc_result_typeN.
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_param(expr, ctx, temp_allocator, param))) {
  } else if (OB_FAIL(tokenize_fulltext(param, param.output_mode_, temp_allocator, json_result))) {
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr,
                                                     ctx,
                                                     temp_allocator,
                                                     json_result,
                                                     expr_datum))) {
  }

  return ret;
}

int ObExprTokenize::TokenizeContext::make_token_array_json(ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  const int64_t count = words_.size();
  if (count > token_capacity_) {
    const int64_t capacity = MAX(count, MAX(16, token_capacity_ * 2));
    void *buf = json_allocator_.alloc(capacity * sizeof(ObJsonString));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      token_nodes_ = static_cast<ObJsonString *>(buf);
      token_capacity_ = capacity;
      for (int64_t i = 0; i < capacity; ++i) {
        new (&token_nodes_[i]) ObJsonString(nullptr, 0);
      }
    }
  }
  for (auto it = words_.begin(); OB_SUCC(ret) && it != words_.end(); ++it) {
    const ObString word = it->first.get_word().get_string();
    ObJsonString &node = token_nodes_[token_count_++];
    node.set_value(word.ptr(), word.length());
    ret = token_array_.append(&node);
  }
  if (OB_SUCC(ret)) {
    token_array_.update_serialize_size();
    result = &token_array_;
  }
  return ret;
}

// Bounded leases reuse parser configuration and scratch across independent SQL
// executions. A slot is never visible to two callers. No document results live
// in the pool, and allocation, dictionary IO and parsing happen outside its lock.
class ObExprTokenize::TokenizePool
{
public:
  static constexpr int64_t CAPACITY = 16;
  struct Slot { TokenizeContext *state_ = nullptr; bool busy_ = false; };
  ~TokenizePool()
  {
    for (Slot &slot : slots_) {
      if (nullptr != slot.state_) { OB_DELETE(TokenizeContext, "TokenizePool", slot.state_); }
    }
  }
  TokenizeContext *acquire(const ObString &parser, const ObString &options,
                          int64_t mask, ObCollationType collation, uint64_t generation, int64_t word_bucket_count,
                          int64_t &slot_id)
  {
    slot_id = -1;
    if (parser.length() + options.length() > 4096) { return nullptr; }
    TokenizeContext *old = nullptr;
    {
      std::lock_guard<std::mutex> guard(lock_);
      int64_t idle = -1;
      for (int64_t n = 0; n < CAPACITY; ++n) {
        const int64_t i = (next_victim_ + n) % CAPACITY;
        Slot &slot = slots_[i];
        if (!slot.busy_) {
          if (idle < 0) { idle = i; }
          TokenizeContext *s = slot.state_;
          if (nullptr != s && s->configured_ && s->generation_ == generation
              && s->collation_ == collation && s->null_mask_ == mask
              && s->word_bucket_count_ == word_bucket_count
              && s->parser_key_ == parser && s->options_key_ == options) {
            slot.busy_ = true;
            slot_id = i;
            return s;
          }
        }
      }
      if (idle < 0) { return nullptr; }
      slot_id = idle;
      next_victim_ = (idle + 1) % CAPACITY;
      slots_[idle].busy_ = true;
      old = slots_[idle].state_;
      slots_[idle].state_ = nullptr;
    }
    if (nullptr != old) { OB_DELETE(TokenizeContext, "TokenizePool", old); }
    TokenizeContext *s = OB_NEW(TokenizeContext, "TokenizePool");
    if (nullptr != s) {
      if (OB_SUCCESS != ob_write_string(s->param_.allocator_, parser, s->parser_key_)
          || OB_SUCCESS != ob_write_string(s->param_.allocator_, options, s->options_key_)) {
        OB_DELETE(TokenizeContext, "TokenizePool", s);
        s = nullptr;
      } else {
        s->null_mask_ = mask;
        s->collation_ = collation;
        s->generation_ = generation;
        s->word_bucket_count_ = word_bucket_count;
      }
    }
    {
      std::lock_guard<std::mutex> guard(lock_);
      slots_[slot_id].state_ = s;
      if (nullptr == s) { slots_[slot_id].busy_ = false; slot_id = -1; }
    }
    return s;
  }
  void release(int64_t slot_id, TokenizeContext *state)
  {
    state->clear_document();
    if (state->helper_.retained_workspace_bytes() > 2 * 1024 * 1024
        || state->json_allocator_.total() > 1024 * 1024) {
      state->configured_ = false;
    }
    // Failed initialization may have partial parser/config state: evict it.
    TokenizeContext *discard = state->configured_ ? nullptr : state;
    {
      std::lock_guard<std::mutex> guard(lock_);
      slots_[slot_id].state_ = discard == nullptr ? state : nullptr;
      slots_[slot_id].busy_ = false;
    }
    if (nullptr != discard) { OB_DELETE(TokenizeContext, "TokenizePool", discard); }
  }
private:
  std::mutex lock_;
  Slot slots_[CAPACITY];
  int64_t next_victim_ = 0;
};

int ObExprTokenize::eval_reusable(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  // Construct before the lease so total CPU includes its final cleanup.
  TokenizeCpuProfile profile(ret);
  ObEvalCtx::TempAllocGuard key_guard(ctx);
  MultimodeAlloctor key_allocator(key_guard.get_allocator());
  ObDatum *fulltext = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, fulltext))) { return ret; }
  const int64_t text_length = fulltext->is_null() ? 0 : fulltext->get_string().length();
  const int64_t bucket_count = MIN(MAX(text_length / 2, 2), 997);
  ObString parser_key;
  ObString options_key;
  int64_t mask = expr.arg_cnt_ << 2;
  for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum *arg = nullptr;
    if (OB_FAIL(expr.args_[i]->eval(ctx, arg))) {
    } else if (arg->is_null()) {
      mask |= 1 << (i - 1);
    } else if (i == 1) {
      parser_key = arg->get_string();
    } else {
      bool is_null = false;
      // Key the actual JSON/text bytes, never an out-of-row LOB locator.
      ret = ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx,
          key_allocator, options_key, is_null);
      if (ob_is_json(expr.args_[i]->datum_meta_.type_)) { mask |= 1 << 8; }
    }
  }
  if (OB_FAIL(ret)) { return ret; }
  static TokenizePool pool;
  int64_t slot_id = -1;
  TokenizeContext *state = pool.acquire(parser_key, options_key, mask,
      expr.args_[0]->obj_meta_.get_collation_type(),
      storage::ObFTParseData::instance().generation(), bucket_count, slot_id);
  if (nullptr == state) { state = OB_NEW(TokenizeContext, "TokenizeLocal"); }
  if (OB_ISNULL(state)) { return OB_ALLOCATE_MEMORY_FAILED; }
  struct Lease
  {
    TokenizePool &pool_;
    int64_t slot_id_;
    TokenizeContext *state_;
    TokenizeCpuProfile &profile_;
    ~Lease()
    {
      const uint64_t start = profile_.stamp();
      if (slot_id_ >= 0) { pool_.release(slot_id_, state_); }
      else { OB_DELETE(TokenizeContext, "TokenizeLocal", state_); }
      profile_.release_ns_ += profile_.elapsed(start);
    }
  } lease{pool, slot_id, state, profile};
  {
    ObEvalCtx::TempAllocGuard guard(ctx);
    ObArenaAllocator &tmp = guard.get_allocator();
    TokenizeParam &param = state->param_;
    if (!state->configured_) {
      // Keep configuration storage separate from the arena reused per document.
      // Only publish the initialized state after every step succeeds.
      if (OB_FAIL(parse_param(expr, ctx, tmp, param))) {
      } else if (OB_FAIL(state->helper_.init(&state->document_allocator_,
                                           param.parser_name_, param.parsed_properties_, true))) {
      } else if (param.output_mode_ == TokenizeParam::ALL
          && OB_FAIL(state->words_.create(bucket_count, ObMemAttr("TokenizeWords")))) {
        state->helper_.reset();
      } else {
        state->configured_ = true;
      }
    } else if (OB_FAIL(parse_fulltext(expr, ctx, param))) {
    } else if (OB_FAIL(param.try_load_dictionary_for_ik())) {
      // Dictionary loading keeps its existing transaction semantics per row.
    }
    int64_t doc_len = 0;
    ObIJsonBase *json = nullptr;
    profile.set_parser(param.parser_name_, param.output_mode_ == TokenizeParam::ALL);
    const uint64_t parse_start = profile.stamp();
    bool unique_output = param.output_mode_ == TokenizeParam::DEFAULT && !profile.force_legacy();
    if (OB_SUCC(ret) && unique_output) {
      ret = state->unique_terms_.start(common::hash::cal_next_prime(bucket_count),
                                      param.fulltext_.ptr(), param.fulltext_.length());
      bool fallback = false;
      if (OB_SUCC(ret) && !param.fulltext_.empty()) {
        ret = state->helper_.segment_unique(param.meta_, param.fulltext_.ptr(),
                                           param.fulltext_.length(), state->unique_terms_, fallback,
                                           param.builtin_ik_dictionary_);
      }
      if (fallback) {
        profile.fallback_ = true;
        state->unique_terms_.reuse();
        unique_output = false;
      }
    }
    if (OB_SUCC(ret) && !unique_output && !state->words_.created()) {
      ret = state->words_.create(bucket_count, ObMemAttr("TokenizeWords"));
    }
    if (OB_SUCC(ret) && !unique_output && !param.fulltext_.empty()) {
      ret = state->helper_.segment(param.meta_, param.fulltext_.ptr(),
                                  param.fulltext_.length(), doc_len, state->words_);
    }
    profile.parse_ns_ += profile.elapsed(parse_start);
    const uint64_t pack_start = profile.stamp();
    if (OB_SUCC(ret)) {
      if (param.output_mode_ == TokenizeParam::DEFAULT) {
        ret = unique_output
            ? pack_token_array_binary(expr, ctx, state->unique_terms_, datum)
            : pack_token_array_binary(expr, ctx, state->words_, datum);
      } else if (OB_FAIL(state->helper_.make_detail_json(state->words_, doc_len, json))) {
      } else {
        ret = ObJsonExprHelper::pack_json_res(expr, ctx, tmp, json, datum);
      }
    }
    profile.pack_ns_ += profile.elapsed(pack_start);
    // Both output paths finish in expr-owned storage, so no token or JSON
    // pointers escape this invocation, including on empty input and errors.
    if (slot_id < 0) {
      const uint64_t release_start = profile.stamp();
      state->clear_document();
      profile.release_ns_ += profile.elapsed(release_start);
    }
  }
  return ret;
}

int ObExprTokenize::tokenize_fulltext(const TokenizeParam &param,
                                      TokenizeParam::OUTPUT_MODE mode,
                                      ObIAllocator &allocator,
                                      ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  storage::ObFTParseHelper tokenize_helper;
  const int64_t ft_word_bkt_cnt = MIN(MAX(param.fulltext_.length() / 2, 2), 997);
  int64_t doc_len = 0;
  storage::ObFTWordMap token_map;

  if (TokenizeParam::OUTPUT_MODE::DEFAULT != mode && TokenizeParam::OUTPUT_MODE::ALL != mode) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tokenize_helper.init(&allocator, param.parser_name_, param.parsed_properties_))) {
  } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr("FTWordMap")))) {
  } else if (
      (0 != param.fulltext_.length())
      && OB_FAIL(tokenize_helper.segment(
                     param.meta_,
                     param.fulltext_.ptr(),
                     param.fulltext_.length(),
                     doc_len,
                     token_map))) {
  } else {
    switch (param.output_mode_) {
    case TokenizeParam::OUTPUT_MODE::DEFAULT: {
      if (OB_FAIL(tokenize_helper.make_token_array_json(token_map, result))) {
      } else {
        // pass
      }
      break;
    }
    case TokenizeParam::OUTPUT_MODE::ALL: {
      if (OB_FAIL(tokenize_helper.make_detail_json(token_map, doc_len, result))) {
      } else {
        // pass
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

ObExprTokenize::TokenizeParam ::TokenizeParam()
  : allocator_(ObMemAttr("TokenizeParam")),
    parser_name_(ObString(OB_DEFAULT_FULLTEXT_PARSER_NAME)),
    meta_(),
    fulltext_(),
    output_mode_(OUTPUT_MODE::DEFAULT)
{
}

int ObExprTokenize::TokenizeParam::parse_json_param(const ObIJsonBase *obj)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObIJsonBase *val;

  if (OB_UNLIKELY(nullptr == obj)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ObJsonNodeType::J_OBJECT != obj->json_type()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (obj->element_count() == 0) {
    // no data
  } else if (OB_FAIL(obj->get_object_value(0, str, val))) {
  } else if (0 == str.case_compare(CASE_INDICATOR_STR)) {
    if (ObJsonNodeType::J_STRING != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("UPPER")) {
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("LOWER")) {
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "case indentifier");
  } else if (0 == str.case_compare(OUTPUT_MODE_STR)) {
    if (ObJsonNodeType::J_STRING != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "output mode should be string default or all");
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("DEFAULT")) {
      output_mode_ = DEFAULT;
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("ALL")) {
      output_mode_ = ALL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "output mode should be string default or all");
    }
  } else if (0 == str.case_compare(STOPWORDS_LIST_STR)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "stopwords");
  } else if (0 == str.case_compare(ADDITIONAL_ARGS_STR)) {
    if (ObJsonNodeType::J_ARRAY != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser arguments");
    } else {
      ObString json_str;
      if (OB_FAIL(storage::ObFTParserJsonProps::tokenize_array_to_props_json(allocator_, val, json_str))) {
        ObSqlString message;
        message.append_fmt("format in %s form", ADDITIONAL_ARGS_STR);
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, message.ptr());
      } else {
        properties_ = json_str;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    ObSqlString message;
    message.append_fmt("config: %s", str.ptr());
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, message.ptr());
  }
  return ret;
}

int ObExprTokenize::parse_param(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                common::ObArenaAllocator &allocator,
                                TokenizeParam &param)
{
  int ret = OB_SUCCESS;

  ObDatum *parser_params_datum;
  ObString raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());

  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_fulltext(expr, ctx, param))) {
  } else if (OB_FAIL(parse_parser_name(expr, ctx, param))) {
  } else if (OB_FAIL(parse_parser_properties(expr, ctx, temp_allocator, param))) {
  } else if (OB_FAIL(param.reform_parser_properties(param.properties_))) {
  } else if (OB_FAIL(param.try_load_dictionary_for_ik())) {
  }
  return ret;
}

int ObExprTokenize::construct_ft_parser_inner_name(const ObString &input_str, TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  storage::ObFTParser parser;

  // The canonical name lives only for this invocation; avoid allocating an
  // arena page for it when there are no user-supplied parser properties.
  if (OB_FAIL(parser.init(input_str))) {
  } else if (OB_FAIL(parser.serialize_to_str(param.parser_name_buf_, sizeof(param.parser_name_buf_)))) {
  } else {
    param.parser_name_ = ObString::make_string(param.parser_name_buf_);
  }
  return ret;
}

int ObExprTokenize::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(param_num < 1 || param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString expr_name(N_TOKENIZE);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, expr_name.length(), expr_name.ptr());
  } else {
    // just okay
  }

  ObLength length = ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length();

  if (OB_SUCC(ret)) {
    // set res type
    type.set_json();
    type.set_length(length); // keep consistent with other json expr, maybe calc it later.

    // param type set, skip charset after first param
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (ob_is_string_type(types[i].get_type())) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

    // handle param
    if (param_num >= 2) {
      types[1].set_varchar();
    }

    if (param_num >= 3) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, 2, N_TOKENIZE))) {
      }
    }
  }

  return ret;
}

int ObExprTokenize::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK((rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 3));
  if (OB_SUCC(ret)) {
    // do register
    rt_expr.eval_func_ = eval_tokenize;
    bool reusable = true;
    for (int64_t i = 1; reusable && i < rt_expr.arg_cnt_; ++i) {
      reusable = raw_expr.get_param_expr(i)->is_const_expr();
    }
    rt_expr.extra_ = reusable ? 1 : 0;
  }
  return ret;
}

int ObExprTokenize::parse_fulltext(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param)
{
  int ret = OB_SUCCESS;

  ObDatum *fulltext_datum;

  if (OB_FAIL(expr.args_[0]->eval(ctx, fulltext_datum))) {
  } else {
    if (fulltext_datum->is_null()) {
      // do nothing, return empty result
      param.fulltext_ = ObString::make_empty_string();
    } else {
      param.fulltext_ = fulltext_datum->get_string();
    }
    param.meta_.set_varchar(); // as we hardcoded in fts_index
    param.meta_.set_collation_type(expr.args_[0]->obj_meta_.get_collation_type());
  }
  return ret;
}

int ObExprTokenize::parse_parser_name(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  ObDatum *parser_datum = nullptr;
  ObString raw_parser_name;

  if (expr.arg_cnt_ < 2) {
    raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, parser_datum))) {
  } else {
    if (parser_datum->is_null()) {
      raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
    } else {
      ObString name = parser_datum->get_string();
      raw_parser_name = name.trim();
    }
  }

  if (OB_FAIL(ret)) {
    // already logged
  } else if (OB_FAIL(construct_ft_parser_inner_name(raw_parser_name, param))) {
  }

  return ret;
}

int ObExprTokenize::parse_parser_properties(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            MultimodeAlloctor &mm_alloc,
                                            TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *base = nullptr;

  if (expr.arg_cnt_ < 3) {
    // do nothing
  } else {
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, mm_alloc, 2, base, is_null))) {
    } else {
      if (ObJsonNodeType::J_ARRAY != base->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser args should be in an array.");
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < base->element_count(); ++i) {
          ObIJsonBase *node = nullptr;
          if (OB_FAIL(base->get_array_element(i, node))) {
          } else if (ObJsonNodeType::J_OBJECT != (node->json_type())) {
            ret = OB_INVALID_ARGUMENT;
          } else if (OB_FAIL(param.parse_json_param(node))) {
          }
        } // for
      }
    }
  }

  return ret;
}

int ObExprTokenize::TokenizeParam::reform_parser_properties(const ObString &properties)
{
  int ret = OB_SUCCESS;
  storage::ObFTParserJsonProps parser_properties;
  storage::ObFTParser parser;

  if (OB_FAIL(parser.parse_from_str(parser_name_.ptr(), parser_name_.length()))) {
  } else if (properties.empty()) {
    // No user parser options: materialize the same built-in defaults directly.
    // Output-mode options are independent of parser properties.
    ret = parsed_properties_.read_from_props(parser, parser_properties, true);
  } else if (OB_FAIL(parser_properties.init())) {
  } else if (OB_FAIL(parser_properties.parse_from_valid_str(properties))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser properties invalid.");
  } else if (OB_FAIL(parser_properties.rebuild_props_for_ddl(parser_name_,
                                                             ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                                             true))) {
  } else if (OB_FAIL(parsed_properties_.read_from_props(parser, parser_properties))) {
  }
  builtin_ik_dictionary_ = true;
  if (OB_SUCC(ret) && parser.is_ik() && !properties.empty()) {
    // Do not infer a custom/table dictionary's immutability from generation.
    // Missing values mean the same defaults inserted by property rebuilding.
    ObString value;
    int check = parser_properties.config_get_dict_table(value);
    builtin_ik_dictionary_ = check == OB_SEARCH_NOT_FOUND || (check == OB_SUCCESS
        && value == ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE));
    value.reset();
    check = parser_properties.config_get_quantifier_table(value);
    builtin_ik_dictionary_ = builtin_ik_dictionary_ && (check == OB_SEARCH_NOT_FOUND || (check == OB_SUCCESS
        && value == ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE)));
    value.reset();
    check = parser_properties.config_get_stopword_table(value);
    builtin_ik_dictionary_ = builtin_ik_dictionary_ && (check == OB_SEARCH_NOT_FOUND || (check == OB_SUCCESS
        && value == ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE)));
  }

  return ret;
}

int ObExprTokenize::TokenizeParam::try_load_dictionary_for_ik()
{
  int ret = OB_SUCCESS;
  bool need_to_load_dic = false;
  if (OB_FAIL(share::ObFtsIndexBuilderUtil::check_need_to_load_dic(
          parser_name_, need_to_load_dic))) {
  } else if (!need_to_load_dic) {
    dictionary_loader_.reset();
    dictionary_charset_ = CHARSET_INVALID;
    dictionary_generation_ = 0;
  } else {
    const ObCharsetType charset = ObCharset::charset_type_by_coll(meta_.get_collation_type());
    const uint64_t generation = storage::ObFTParseData::instance().generation();
    if (dictionary_charset_ != charset || dictionary_generation_ != generation) {
      dictionary_loader_.reset();
    }
    if (!dictionary_loader_.is_valid()) {
      if (OB_FAIL(storage::ObGenDicLoader::get_instance().get_dic_loader(
                      ObString::make_string(storage::ObFTSLiteral::PARSER_NAME_IK),
                      charset, dictionary_loader_))) {
      } else {
        dictionary_charset_ = charset;
        dictionary_generation_ = generation;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!dictionary_loader_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(dictionary_loader_.get_loader()->try_load_dictionary_in_trans())) {
    }
  }
  // A failed parser check, lookup or load must not retain a ready identity.
  // Retry the normal registry and transaction checks on the next invocation.
  if (OB_FAIL(ret)) {
    dictionary_loader_.reset();
    dictionary_charset_ = CHARSET_INVALID;
    dictionary_generation_ = 0;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
