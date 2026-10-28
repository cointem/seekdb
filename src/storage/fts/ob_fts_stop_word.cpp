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

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_server_runtime.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_parser_helper.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace storage
{

////////////////////////////////////////////////////////////////////////////////
// class ObStopWordChecker
ObStopWordChecker::~ObStopWordChecker()
{
  destroy();
}

int ObStopWordChecker::init()
{
  int ret = OB_SUCCESS;
  
  

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet"))) {
  } else {
    ObObjMeta stop_meta;
    stop_meta.set_varchar();
    stop_meta.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);

    stopword_type_.set_meta(stop_meta);
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
      }
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

void ObStopWordChecker::destroy()
{
  if (inited_) {
    stopword_set_.destroy();
    inited_ = false;
  }
}

int ObStopWordChecker::check_stopword(const ObFTWord &word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  
  
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    common::ObString cmp_str;
    if (ObCharset::charset_type_by_coll(word.get_collation_type()) == CHARSET_UTF8MB4) {
      cmp_str = word.get_word().get_string();
      ObFTWord converted(cmp_str.length(), cmp_str.ptr(), stopword_type_);
      ret = stopword_set_.exist_refactored(converted);
    } else {
      common::ObArenaAllocator allocator(lib::ObMemAttr("ChkStopWord"));
      if (OB_FAIL(common::ObCharset::charset_convert(
                                       allocator,
                                       word.get_word().get_string(),
                                       word.get_collation_type(),
                                       stopword_type_.get_collation_type(),
                                       cmp_str))) {
      } else {
        ObFTWord converted(cmp_str.length(), cmp_str.ptr(), stopword_type_);
        ret = stopword_set_.exist_refactored(converted);
      }
    }
    if (OB_HASH_NOT_EXIST == ret) {
      is_stopword = false;
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      is_stopword = true;
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ob_casedn_utf8mb4 decodes, applies caseinfo->page[wc].tolower and
// reencodes. For valid CJK characters whose actual table entry is identity,
// both conversion and its temporary allocation can be skipped. Keep all other
// non-ASCII scripts/charsets on ObCharset::tolower (including invalid UTF-8).
bool ft_word_lower_identity(const ObCharsetInfo *cs, bool utf8,
                            const char *word, int64_t length)
{
  if (nullptr == cs || cs->mbminlen != 1) { return false; }
  const unsigned char *cursor = reinterpret_cast<const unsigned char *>(word);
  const unsigned char *end = cursor + length;
  while (cursor < end) {
    if (*cursor < 128) {
      if (*cursor >= 'A' && *cursor <= 'Z') { return false; }
      ++cursor;
    } else {
      if (!utf8 || nullptr == cs->caseinfo) { return false; }
      ob_wc_t wc = 0;
      const int bytes = cs->cset->mb_wc(cs, &wc, cursor, end);
      if (bytes <= 0 || !((wc >= 0x3400 && wc <= 0x4DBF)
          || (wc >= 0x4E00 && wc <= 0x9FFF) || (wc >= 0x20000 && wc <= 0x2FA1F))) {
        return false;
      }
      if (wc <= cs->caseinfo->maxchar) {
        const ObUnicaseInfoChar *page = cs->caseinfo->page[wc >> 8];
        if (nullptr != page && page[wc & 0xFF].tolower != wc) { return false; }
      }
      cursor += bytes;
    }
  }
  return true;
}

// class ObAddWord
ObAddWord::ObAddWord(
    const ObFTParserProperty &property,
    const ObObjMeta &meta,
    const ObAddWordFlag &flag,
    common::ObIAllocator &allocator,
    ObFTWordMap &word_map,
    common::ObArenaAllocator *normalization)
  : word_meta_(meta),
    allocator_(allocator),
    word_map_(word_map),
    normalization_(normalization),
    min_max_word_cnt_(0),
    non_stopword_cnt_(0),
    stopword_cnt_(0),
    min_token_size_(property.min_token_size_),
    max_token_size_(property.max_token_size_),
    flag_(flag)
{
}

int ObAddWord::process_word(
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt,
    const int64_t word_freq)
{
  return process_word_impl(word, word_len, char_cnt, word_freq, false);
}

int ObAddWord::process_word_view(const char *word, int64_t word_len,
                                 int64_t char_cnt, int64_t word_freq)
{
  if (flag_.casedown() && nullptr != normalization_) { normalization_->reuse(); }
  return process_word_impl(word, word_len, char_cnt, word_freq, true);
}

int ObAddWord::process_word_impl(const char *word, int64_t word_len,
                                 int64_t char_cnt, int64_t word_freq, bool borrowed)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord src_word(word_len, word, word_meta_);
  ObFTWord dst_word;
  if (OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len || 0 >= char_cnt || 0 >= word_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_min_max_word(char_cnt)) {
    ++min_max_word_cnt_;
  } else if (OB_FAIL(casedown_word(src_word, dst_word, borrowed))) {
  } else if (OB_FAIL(check_stopword(dst_word, is_stopword))) {
  } else if (OB_UNLIKELY(is_stopword)) {
    ++stopword_cnt_;
  } else if (OB_FAIL(groupby_word(dst_word, word_freq, borrowed))) {
  } else {
    non_stopword_cnt_ += word_freq;
  }
  return ret;
}

bool ObAddWord::is_min_max_word(const int64_t c_len) const
{
  return flag_.min_max_word() && (c_len < min_token_size_ || c_len > max_token_size_);
}

int ObAddWord::casedown_word(const ObFTWord &src, ObFTWord &dst, bool borrowed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (borrowed && flag_.casedown()
      && ObCharset::charset_type_by_coll(word_meta_.get_collation_type()) == CHARSET_UTF8MB4
      && ft_word_lower_identity(ObCharset::get_charset(word_meta_.get_collation_type()),
          ObCharset::charset_type_by_coll(word_meta_.get_collation_type()) == CHARSET_UTF8MB4,
          src.get_word().ptr_, src.get_word().len_)) {
    dst = src;
  } else if (flag_.casedown()) {
    ObString dst_str;
    if (OB_FAIL(ObCharset::tolower(
                    word_meta_.get_collation_type(),
                    src.get_word().get_string(),
                    dst_str,
                    borrowed && nullptr != normalization_ ? *normalization_ : allocator_))) {
    } else {
      ObFTWord tmp(dst_str.length(), dst_str.ptr(), word_meta_);
      dst = tmp;
    }
  } else {
    dst = src;
  }
  return ret;
}

int ObAddWord::check_stopword(const ObFTWord &ft_word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ft_word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flag_.stopword()) {
    ObStopWordChecker *stop_word_checker = ObFTParseData::instance().stop_word_checker();
    if (OB_ISNULL(stop_word_checker)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(stop_word_checker->check_stopword(ft_word, is_stopword))) {
    }
  }
  return ret;
}

int ObAddWord::retain_word(const ObFTWord &word, ObFTWord &retained)
{
  ObString copy;
  const int ret = ob_write_string(allocator_, word.get_word().get_string(), copy);
  if (OB_SUCCESS == ret) { retained = ObFTWord(copy.length(), copy.ptr(), word_meta_); }
  return ret;
}

int ObAddWord::groupby_word(const ObFTWord &word, int64_t word_freq, bool borrowed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(word.empty() || word_freq <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (borrowed) {
    // Retain only a missing key or changed last spelling, within one lookup.
    // Return callback errors explicitly so an OOM cannot be overwritten by a
    // successful lookup status, and never publish transient parser/scratch data.
    auto update = [&](auto &entry) -> int {
      int result = OB_SUCCESS;
      if (!flag_.groupby_word()) { result = OB_HASH_EXIST; }
      else {
        if (entry.first.get_word().get_string() != word.get_word().get_string()) {
          ObFTWord retained;
          if (OB_SUCCESS != (result = retain_word(word, retained))) {
          } else { entry.first = retained; }
        }
        if (OB_SUCCESS == result) { entry.second += word_freq; }
      }
      return result;
    };
    auto prepare = [&](auto &entry) -> int {
      ObFTWord retained;
      const int result = retain_word(word, retained);
      if (OB_SUCCESS == result) { entry.first = retained; }
      return result;
    };
    ret = word_map_.set_or_update(word, 1/*legacy first occurrence*/, update, prepare);
  } else if (!flag_.groupby_word()) {
    if (OB_FAIL(word_map_.set_refactored(word, 1/*word count*/))) {
    }
  } else {
    // A single lookup updates an existing term without reallocating its node.
    // Keep the last spelling, as set_refactored(overwrite) did: collations may
    // consider different byte sequences equal.
    auto update = [&](auto &entry) {
      entry.first = word;
      entry.second += word_freq;
    };
    if (OB_FAIL(word_map_.set_or_update(word, 1/*first occurrence*/, update))) {
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
