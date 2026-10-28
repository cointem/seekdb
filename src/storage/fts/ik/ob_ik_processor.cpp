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

#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_ac_matcher.h"
#include "lib/container/ob_se_array.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
class IKDecodedChars
{
public:
  struct Char {
    ob_wc_t unicode_;
    uint32_t offset_;
    uint8_t length_;
    ObFTCharUtil::CharType type_;
  };
  static constexpr int64_t MAX_DOCUMENT_BYTES = 64 * 1024;
  common::ObSEArray<Char, 128> chars_;
  IKACMatcher::Lease matcher_;
  uint64_t generation_ = 0;
  bool attempted_ = false;
  int64_t current_ = 0;
  int64_t lookup_ = 0;
  uint32_t state_ = 0;
  void reuse()
  {
    chars_.reuse();
    current_ = lookup_ = 0;
    state_ = 0;
  }
};

bool TokenizeContext::enable_decoded_pipeline(uint64_t generation)
{
  decoded_enabled_ = false;
  if (cursor_ != 0 || fulltext_len_ <= 0 || fulltext_len_ > IKDecodedChars::MAX_DOCUMENT_BYTES
      || ObCharset::charset_type_by_coll(coll_type_) != CHARSET_UTF8MB4) { return false; }
  if (nullptr == decoded_) { decoded_ = OB_NEW(IKDecodedChars, "IKDecoded"); }
  if (nullptr == decoded_) { return false; }
  decoded_->reuse();
  if (!decoded_->attempted_ || decoded_->generation_ != generation) {
    decoded_->matcher_ = IKACMatcher::acquire(generation);
    decoded_->generation_ = generation;
    decoded_->attempted_ = true;
  }
  if (!decoded_->matcher_ || OB_SUCCESS != decoded_->chars_.reserve(fulltext_len_)) { return false; }
  const auto *begin = reinterpret_cast<const unsigned char *>(fulltext_);
  const auto *cursor = begin;
  const auto *end = begin + fulltext_len_;
  while (cursor < end) {
    ob_wc_t unicode = 0;
    // Match classify_first_char's strict UTF8 decoder (including surrogate
    // rejection). Any failure replays the untouched document through the DAT.
    const int bytes = common::ob_charset_decode_unicode<CHARSET_UTF8MB4>(cursor, end, unicode);
    if (bytes <= 0) { decoded_->reuse(); return false; }
    ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
    if (ObUnicodeBlockUtils::is_alpha(unicode)) { type = ObFTCharUtil::CharType::ENGLISH_LETTER; }
    else if (ObUnicodeBlockUtils::is_arabic(unicode)) { type = ObFTCharUtil::CharType::ARABIC_LETTER; }
    else if (ObUnicodeBlockUtils::is_chinese(unicode)) { type = ObFTCharUtil::CharType::CHINESE; }
    else if (ObUnicodeBlockUtils::is_other_cjk(unicode)) { type = ObFTCharUtil::CharType::OTHER_CJK; }
    IKDecodedChars::Char item{unicode, static_cast<uint32_t>(cursor - begin),
                             static_cast<uint8_t>(bytes), type};
    if (OB_SUCCESS != decoded_->chars_.push_back(item)) { decoded_->reuse(); return false; }
    cursor += bytes;
  }
  decoded_enabled_ = true;
  next_char_len_ = decoded_->chars_.at(0).length_;
  next_char_type_ = decoded_->chars_.at(0).type_;
  return true;
}

void TokenizeContext::disable_decoded_pipeline()
{
  decoded_enabled_ = false;
  if (nullptr != decoded_) { decoded_->reuse(); }
}

bool TokenizeContext::decoded_pipeline_enabled() const { return decoded_enabled_; }
ob_wc_t TokenizeContext::current_unicode() const { return decoded_->chars_.at(decoded_->current_).unicode_; }
int64_t TokenizeContext::current_char_index() const { return decoded_->current_; }
int64_t TokenizeContext::char_offset(int64_t index) const { return decoded_->chars_.at(index).offset_; }

int TokenizeContext::match_main_dictionary()
{
  if (next_char_type_ == ObFTCharUtil::CharType::USELESS) {
    decoded_->state_ = 0;
    return OB_SUCCESS;
  }
  decoded_->state_ = decoded_->matcher_->advance(decoded_->state_, current_unicode());
  return decoded_->matcher_->output(decoded_->state_, *this);
}

int TokenizeContext::read_char(int64_t offset, int64_t &length, ObFTCharUtil::CharType &type)
{
  int ret = OB_SUCCESS;
  if (!decoded_enabled_) {
    if (OB_FAIL(ObCharset::first_valid_char(coll_type_, fulltext_ + offset,
                                           fulltext_len_ - offset, length))) {
    } else { ret = ObFTCharUtil::classify_first_char(coll_type_, fulltext_ + offset, length, type); }
  } else {
    if (decoded_->lookup_ >= decoded_->chars_.count()
        || decoded_->chars_.at(decoded_->lookup_).offset_ > offset) { decoded_->lookup_ = 0; }
    while (decoded_->lookup_ < decoded_->chars_.count()
        && decoded_->chars_.at(decoded_->lookup_).offset_ < offset) { ++decoded_->lookup_; }
    if (decoded_->lookup_ >= decoded_->chars_.count()
        || decoded_->chars_.at(decoded_->lookup_).offset_ != offset) { ret = OB_ERR_UNEXPECTED; }
    else {
      const IKDecodedChars::Char &item = decoded_->chars_.at(decoded_->lookup_);
      length = item.length_;
      type = item.type_;
    }
  }
  return ret;
}

int TokenizeContext::ignore_single_cjk(int64_t offset, int64_t length, bool &ignore)
{
  if (!decoded_enabled_) {
    return ObFTCharUtil::is_ignore_single_cjk(coll_type_, fulltext_ + offset, length, ignore);
  }
  ObFTCharUtil::CharType type;
  int ret = read_char(offset, length, type);
  if (OB_SUCCESS == ret) {
    ignore = ObUnicodeBlockUtils::check_ignore_as_single(decoded_->chars_.at(decoded_->lookup_).unicode_);
  }
  return ret;
}

int ObIIKProcessor::process(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  ObFTCharUtil::CharType type;
  const char *ch = nullptr;
  uint8_t char_len = 0;

  if (OB_FAIL(ctx.current_char_type(type))) {
  } else if (OB_FAIL(ctx.current_char(ch, char_len))) {
  } else if (OB_FAIL(do_process(ctx, ch, char_len, type))) {
  }
  return ret;
}

TokenizeContext::TokenizeContext(ObCollationType coll_type,
                                 ObIAllocator &allocator,
                                 const char *fulltext,
                                 int64_t fulltext_len,
                                 bool is_smart)
    : coll_type_(coll_type), fulltext_(fulltext), fulltext_len_(fulltext_len), cursor_(0),
      next_char_len_(0), handle_size_(0), is_smart_(is_smart), token_list_(allocator),
      result_list_(allocator)
{
}

int TokenizeContext::init()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(fulltext_) || fulltext_len_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(prepare_next_char())) {
  }
  return ret;
}

int TokenizeContext::restart(const char *fulltext, int64_t fulltext_len, bool is_smart)
{
  reset_resource();
  disable_decoded_pipeline();
  fulltext_ = fulltext;
  fulltext_len_ = fulltext_len;
  is_smart_ = is_smart;
  cursor_ = 0;
  next_char_len_ = 0;
  next_char_type_ = ObFTCharUtil::CharType::USELESS;
  return init();
}

int TokenizeContext::reset_resource()
{
  handle_size_ = 0;
  result_list_.reset();
  token_list_.reset();
  return OB_SUCCESS;
}

int TokenizeContext::current_char(const char *&ch, uint8_t &char_len)
{
  int ret = OB_SUCCESS;
  if (cursor_ >= fulltext_len_) {
    ret = OB_ITER_END;
  } else {
    ch = fulltext_ + cursor_;
    char_len = next_char_len_;
  }
  return ret;
}

int TokenizeContext::current_char_type(ObFTCharUtil::CharType &type)
{
  int ret = OB_SUCCESS;
  if (cursor_ >= fulltext_len_) {
    ret = OB_ITER_END;
  } else {
    type = next_char_type_;
  }
  return ret;
}

int TokenizeContext::prepare_next_char()
{
  int ret = OB_SUCCESS;
  if (decoded_enabled_) {
    while (decoded_->current_ < decoded_->chars_.count()
        && decoded_->chars_.at(decoded_->current_).offset_ < cursor_) { ++decoded_->current_; }
    if (decoded_->current_ >= decoded_->chars_.count()
        || decoded_->chars_.at(decoded_->current_).offset_ != cursor_) { return OB_ERR_UNEXPECTED; }
    const IKDecodedChars::Char &item = decoded_->chars_.at(decoded_->current_);
    next_char_len_ = item.length_;
    next_char_type_ = item.type_;
    return ret;
  }
  if (OB_FAIL(ObCharset::first_valid_char(coll_type_,
                                          fulltext_ + cursor_,
                                          fulltext_len_ - cursor_,
                                          next_char_len_))) {
  } else if (OB_FAIL(ObFTCharUtil::classify_first_char(coll_type_,
                                                       fulltext_ + cursor_,
                                                       next_char_len_,
                                                       next_char_type_))) {
  }
  return ret;
}

int TokenizeContext::step_next()
{
  int ret = OB_SUCCESS;
  if (cursor_ >= fulltext_len_) {
    ret = OB_ITER_END;
  } else if (cursor_ + next_char_len_ >= fulltext_len_) {
    cursor_ = fulltext_len_;
    next_char_len_ = 0;
    ret = OB_ITER_END;
  } else if (cursor_ < fulltext_len_ && 0 == next_char_len_) {
    // should not happen
    ret = OB_UNEXPECT_INTERNAL_ERROR;
  } else {
    cursor_ += next_char_len_;
    handle_size_++;
    if (OB_FAIL(prepare_next_char())) {
    } else {
    }
  }
  return ret;
}

ObCollationType TokenizeContext::collation() const { return coll_type_; }

int64_t TokenizeContext::get_end_cursor() const { return cursor_ + next_char_len_; }

const char *TokenizeContext::fulltext() const { return fulltext_; }

int64_t TokenizeContext::fulltext_len() const { return fulltext_len_; }

int64_t TokenizeContext::get_cursor() const { return cursor_; }

bool TokenizeContext::is_last() const { return cursor_ + next_char_len_ >= fulltext_len_; }

bool TokenizeContext::iter_end() const { return cursor_ >= fulltext_len_; }

bool TokenizeContext::is_smart() const { return is_smart_; }

int TokenizeContext::add_token(const char *fulltext,
                               int64_t offset,
                               int64_t length,
                               int64_t char_cnt,
                               ObIKTokenType type)
{
  int ret = OB_SUCCESS;
  ObIKToken token;
  token.ptr_ = fulltext;
  token.length_ = length;
  token.offset_ = offset;
  token.char_cnt_ = char_cnt;
  token.type_ = type;
  if (OB_FAIL(token_list_.add_token(token))) {
  }
  return ret;
}

TokenizeContext::~TokenizeContext()
{
  if (nullptr != decoded_) { OB_DELETE(IKDecodedChars, "IKDecoded", decoded_); }
  token_list_.reset();
  result_list_.reset();
}

int TokenizeContext::get_next_token(const char *&word,
                                    int64_t &word_len,
                                    int64_t &offset,
                                    int64_t &char_cnt)
{
  int ret = OB_SUCCESS;
  if (!result_list_.empty()) {
    ObIKToken token = result_list_.get_first();
    result_list_.pop_front();
    if (!result_list_.empty()) {
      if (OB_FAIL(compound(token))) {
      } else {
        // pass
      }
    }
    if (OB_SUCC(ret)) {
      word = token.ptr_;
      word_len = token.length_;
      offset = token.offset_;
      char_cnt = token.char_cnt_;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int TokenizeContext::compound(ObIKToken &token)
{
  int ret = OB_SUCCESS;
  ObList<ObIKToken, ObIAllocator> &list = result_list_;
  if (is_smart_) {
    if (!list.empty()) {
      if (ObIKTokenType::IK_ARABIC_TOKEN == token.type_) {
        const ObIKToken next = list.get_first();
        bool append = false;

        if (ObIKTokenType::IK_CNNUM_TOKEN == next.type_) {
          // handle eng num + chn num
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.char_cnt_ += next.char_cnt_;
            token.type_ = ObIKTokenType::IK_CNNUM_TOKEN;
          }
        } else if (ObIKTokenType::IK_COUNT_TOKEN == next.type_) {
          // handle eng num + chn count
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.char_cnt_ += next.char_cnt_;
            token.type_ = ObIKTokenType::IK_CNQUAN_TOKEN;
          }
        } else {
          // pass
        }
        if (append) {
          list.pop_front();
        }
      }
      // There may be another round of append
      if (OB_SUCC(ret) && !list.empty()) {
        const ObIKToken next = list.get_first();
        bool append = false;
        if (ObIKTokenType::IK_COUNT_TOKEN == next.type_) {
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.type_ = ObIKTokenType::IK_CNQUAN_TOKEN;
          }
        }
        if (append) {
          list.pop_front();
        }
      }
    }
  } else {
    // nothing todo, just return
  }
  return ret;
}

} // namespace storage

} // namespace oceanbase
