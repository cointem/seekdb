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

#include "storage/fts/ob_beng_ft_parser.h"
#include "data_plane/fts/ob_fts_struct.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "data_plane/fts/ob_fts_parser_property.h"
#include "data_plane/fts/ob_fts_unique_terms.h"
#include "storage/fts/ob_fts_stop_word.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

namespace {
// Validate the whole input before publishing a token: an optimization miss can
// replay the original pipeline without rolling frequency counters or maps back.
bool beng_ascii_supported(const ObCharsetInfo *cs, const char *text, int64_t length)
{
  const ObCharsetInfo *utf8 = ObCharset::get_charset(CS_TYPE_UTF8MB4_BIN);
  if (nullptr == cs || nullptr == utf8 || nullptr == text || length <= 0 || length > UINT32_MAX
      || cs->cset != utf8->cset || cs->mbminlen != 1 || nullptr == cs->ctype
      || nullptr == cs->caseinfo || cs->caseinfo->maxchar < 127
      || nullptr == cs->caseinfo->page || nullptr == cs->caseinfo->page[0]) {
    return false;
  }
  const unsigned char *ctype = cs->ctype + 1;
  const ObUnicaseInfoChar *case_page = cs->caseinfo->page[0];
  const unsigned delimiter_flags = _MY_SPC | _MY_CTR | _MY_PNT;
  int64_t run_length = 0;
  for (int64_t i = 0; i < length; ++i) {
    const unsigned char c = static_cast<unsigned char>(text[i]);
    if (c >= 0x80) { return false; }
    const uint32_t expected_lower = c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c;
    // UTF8 uses this Unicode page, including Turkish I -> non-ASCII mappings.
    if (case_page[c].tolower != expected_lower) { return false; }
    run_length = (ctype[c] & delimiter_flags) != 0 ? 0 : run_length + 1;
    if (run_length > 512) { return false; }
  }
  return true;
}

template <typename Sink>
int beng_ascii_scan(const ObCharsetInfo *cs, const ObFTParserProperty &property,
                    const char *text, int64_t length, Sink &sink)
{
  int ret = OB_SUCCESS;
  const unsigned char *ctype = cs->ctype + 1;
  const ObUnicaseInfoChar *case_page = cs->caseinfo->page[0];
  const unsigned delimiter_flags = _MY_SPC | _MY_CTR | _MY_PNT;
  const unsigned alnum_flags = _MY_U | _MY_L | _MY_NMR;
  char lowercase[ObBEngFTParser::FT_MAX_WORD_LEN];
  int64_t cursor = 0;
  while (OB_SUCC(ret) && cursor < length) {
    while (cursor < length
        && (ctype[static_cast<unsigned char>(text[cursor])] & delimiter_flags) != 0) { ++cursor; }
    int64_t first_alnum = -1, last_alnum = -1;
    bool uppercase = false;
    while (cursor < length
        && (ctype[static_cast<unsigned char>(text[cursor])] & delimiter_flags) == 0) {
      const unsigned char c = static_cast<unsigned char>(text[cursor]);
      if ((ctype[c] & alnum_flags) != 0) {
        if (first_alnum < 0) { first_alnum = cursor; }
        last_alnum = cursor;
      }
      uppercase |= c >= 'A' && c <= 'Z';
      ++cursor;
    }
    if (first_alnum >= 0) {
      const int64_t token_length = last_alnum - first_alnum + 1;
      // BENG applies min/max to trimmed, normalized bytes (ASCII chars here).
      if (token_length >= property.min_token_size_ && token_length <= property.max_token_size_) {
        const char *word = text + first_alnum;
        if (uppercase) {
          for (int64_t i = 0; i < token_length; ++i) {
            lowercase[i] = static_cast<char>(case_page[static_cast<unsigned char>(word[i])].tolower);
          }
          word = lowercase;
        }
        ret = sink(word, token_length);
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
      }
    }
  }
  return ret;
}
}

int try_beng_ascii_segment(const ObCharsetInfo *cs, const ObFTParserProperty &property,
                           const char *text, int64_t length, ObAddWord &add_word,
                           bool &handled)
{
  handled = property.min_token_size_ >= 0 && property.max_token_size_ <= ObBEngFTParser::FT_MAX_WORD_LEN
      && beng_ascii_supported(cs, text, length);
  if (!handled) { return OB_SUCCESS; }
  auto sink = [&](const char *word, int64_t size) {
    // Synchronous retention keeps stack/lowercase and input views out of the
    // persistent word map; every occurrence reaches the frequency/length sink.
    return add_word.process_word_view(word, size, size, 1);
  };
  return beng_ascii_scan(cs, property, text, length, sink);
}

int try_beng_ascii_unique(const ObCharsetInfo *cs, const ObObjMeta &meta,
                          const ObFTParserProperty &property,
                          const char *text, int64_t length,
                          FTUniqueTerms &terms, bool &handled)
{
  handled = terms.size() == 0 && !terms.overflow()
      && ObCharset::charset_type_by_coll(meta.get_collation_type()) == CHARSET_UTF8MB4
      && property.min_token_size_ >= 0 && property.max_token_size_ <= ObBEngFTParser::FT_MAX_WORD_LEN
      && beng_ascii_supported(cs, text, length);
  if (!handled) { return OB_SUCCESS; }
  ObStopWordChecker *checker = ObFTParseData::instance().stop_word_checker();
  auto sink = [&](const char *word, int64_t size) {
    int ret = OB_SUCCESS;
    ObFTWord key(size, word, meta);
    bool stop = false;
    if (nullptr == checker) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(checker->check_stopword(key, stop))) {
    } else if (!stop) { ret = terms.add(key, meta); }
    return OB_SUCC(ret) && terms.overflow() ? OB_ITER_END : ret;
  };
  return beng_ascii_scan(cs, property, text, length, sink);
}

int ObBEngFTParser::get_next_token(const char *&word, int64_t &word_len,
                                   int64_t &char_len, int64_t &word_freq)
{
  word = nullptr;
  word_len = char_len = word_freq = 0;
  const char *view = nullptr;
  int64_t length = 0, chars = 0, freq = 0;
  int ret = get_next_token_view(view, length, chars, freq);
  if (OB_SUCC(ret)) {
    char *buf = static_cast<char *>(output_allocator_->alloc(length));
    if (nullptr == buf) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else {
      MEMCPY(buf, view, length);
      word = buf;
      word_len = length;
      char_len = chars;
      word_freq = freq;
    }
  }
  return ret;
}

int ObBEngFTParser::get_next_token_view(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  ObDatum token;
  int64_t token_freq = 0;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(token_stream_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(token_stream_->get_next(token, token_freq))) {
    if (OB_ITER_END != ret) {
    }
  } else if (OB_ISNULL(token.ptr_) || OB_UNLIKELY(0 >= token.len_ || 0 >= token_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    word = token.ptr_;
    word_len = token.len_;
    char_len = token.len_;
    word_freq = token_freq;
    LOG_DEBUG("succeed to add word", K(ObString(word_len, word)), K(word_freq));
  }
  return ret;
}

int ObBEngFTParser::restart(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (nullptr == param || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (UINT32_MAX < param->ft_length_) {
    ret = OB_NOT_SUPPORTED;
  } else {
    output_allocator_ = param->allocator_;
    if (is_inited_ && analysis_ctx_.cs_ != param->cs_) {
      reset();
    }
    if (!is_inited_) {
      // The pipeline must outlive the caller's document allocator.
      ObFTParserParam persistent_param = *param;
      persistent_param.allocator_ = &allocator_;
      ret = init(&persistent_param);
    } else {
      doc_.set_string(param->fulltext_, param->ft_length_);
      ret = segment(doc_, token_stream_);
    }
  }
  return ret;
}

int ObBEngFTParser::init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < param->ft_length_)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    doc_.set_string(param->fulltext_, param->ft_length_);
    analysis_ctx_.cs_ = param->cs_;
    analysis_ctx_.filter_stopword_ = false;
    analysis_ctx_.need_grouping_ = false;
    if (OB_FAIL(english_analyzer_.init(analysis_ctx_, *param->allocator_))) {
    } else if (OB_FAIL(segment(doc_, token_stream_))) {
    } else if (OB_ISNULL(token_stream_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObBEngFTParser::segment(
    const common::ObDatum &doc,
    share::ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(doc.ptr_) || OB_UNLIKELY(0 >= doc.len_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < doc.len_)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(english_analyzer_.analyze(doc, token_stream))) {
  }
  return ret;
}

void ObBEngFTParser::reset()
{
  analysis_ctx_.reset();
  english_analyzer_.reset();
  doc_.reset();
  token_stream_ = nullptr;
  is_inited_ = false;
}

ObBasicEnglishFTParserDesc::ObBasicEnglishFTParserDesc() {}

int ObBasicEnglishFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObBEngFTParser *parser = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(parser = OB_NEWx(ObBEngFTParser, param->allocator_, *(param->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(parser->init(param))) {
  } else {
    iter = parser;
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObBEngFTParser, param->allocator_, parser);
  }

  return ret;
}

void ObBasicEnglishFTParserDesc::free_token_iter(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->allocator_);
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
  }
}

int ObBasicEnglishFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_min_max_word();
  flag.set_stop_word();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
