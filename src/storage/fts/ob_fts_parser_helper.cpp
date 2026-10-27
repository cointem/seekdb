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
#include <cstdlib>
#include <cstring>
#include "storage/fts/ob_fts_struct.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_parser_helper.h"
#include "data_plane/fts/ob_fts_unique_terms.h"

#include "common/json_type/ob_json_tree.h"
#include "share/ob_force_print_log.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/ob_beng_ft_parser.h"
#include "storage/fts/ob_ik_ft_parser.h"
#include "storage/fts/ob_ngram2_ft_parser.h"
#include "storage/fts/ob_ngram_ft_parser.h"
#include "data_plane/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_whitespace_ft_parser.h"

namespace oceanbase
{
namespace storage
{

static bool ft_tokenize_profile_no_ac()
{
  static const bool disabled = []() {
    const char *profile = std::getenv("FTS_TOKENIZE_PROFILE");
    const char *flag = std::getenv("FTS_TOKENIZE_PROFILE_NO_AC");
    return nullptr != profile && 0 == std::strcmp(profile, "1")
        && nullptr != flag && 0 == std::strcmp(flag, "1");
  }();
  return disabled;
}

// ob_casedn_utf8mb4 decodes, applies caseinfo->page[wc].tolower and
// reencodes. For valid CJK characters whose actual table entry is identity,
// both conversion and its temporary allocation can be skipped. Keep all other
// non-ASCII scripts/charsets on ObCharset::tolower (including invalid UTF-8).
static bool ft_unique_lower_identity(const ObCharsetInfo *cs, bool utf8,
                                     const char *word, int64_t length)
{
  if (cs->mbminlen != 1) { return false; }
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

void FTUniqueTerms::reuse()
{
  entries_.reuse();
  heads_.reuse();
  input_ = nullptr;
  input_length_ = 0;
  slab_ = nullptr;
  remaining_ = owned_bytes_ = 0;
  overflow_ = false;
  if (strings_.total() > 256 * 1024) { strings_.reset(); }
  else { strings_.reuse(); }
}

int FTUniqueTerms::start(int64_t legacy_bucket_count, const char *input, int64_t length)
{
  reuse();
  int ret = OB_SUCCESS;
  input_ = input;
  input_length_ = length;
  if (legacy_bucket_count <= 0 || legacy_bucket_count > 2048) {
    overflow_ = true;
  } else if (OB_FAIL(heads_.prepare_allocate(legacy_bucket_count))) {
  } else {
    MEMSET(heads_.get_data(), -1, legacy_bucket_count * sizeof(int32_t));
  }
  return ret;
}

int FTUniqueTerms::add(const ObFTWord &word, const ObObjMeta &meta)
{
  int ret = OB_SUCCESS;
  uint64_t hash = 0;
  if (OB_FAIL(word.hash(hash))) {
  } else {
    const int64_t bucket = hash % heads_.count();
    int32_t index = heads_.at(bucket);
    while (index >= 0 && !(entries_.at(index).first == word)) {
      index = entries_.at(index).next_;
    }
    const ObString spelling = word.get_word().get_string();
    if (index >= 0 && entries_.at(index).first.get_word().get_string() == spelling) {
      // Keep stable storage when the spelling itself has not changed.
    } else if (index < 0 && entries_.count() >= MAX_TERMS) {
      overflow_ = true;
    } else {
      const char *ptr = spelling.ptr();
      const uintptr_t begin = reinterpret_cast<uintptr_t>(input_);
      const uintptr_t value = reinterpret_cast<uintptr_t>(ptr);
      const bool borrowed = value >= begin && value - begin <= static_cast<uint64_t>(input_length_)
          && static_cast<uint64_t>(spelling.length()) <= input_length_ - (value - begin);
      if (!borrowed) {
        if (spelling.length() > MAX_OWNED_BYTES - owned_bytes_) {
          overflow_ = true;
        } else {
          if (remaining_ < spelling.length()) {
            remaining_ = MAX(4096, spelling.length());
            slab_ = static_cast<char *>(strings_.alloc(remaining_));
            if (nullptr == slab_) { ret = OB_ALLOCATE_MEMORY_FAILED; }
          }
          if (OB_SUCC(ret)) {
            MEMCPY(slab_, ptr, spelling.length());
            ptr = slab_;
            slab_ += spelling.length();
            remaining_ -= spelling.length();
            owned_bytes_ += spelling.length();
          }
        }
      }
      if (OB_SUCC(ret) && !overflow_) {
        ObFTWord retained(spelling.length(), ptr, meta);
        if (index >= 0) {
          entries_.at(index).first = retained; // Last spelling; keep chain position.
        } else {
          Entry entry{retained, heads_.at(bucket)};
          ret = entries_.push_back(entry);
          if (OB_SUCC(ret)) { heads_.at(bucket) = entries_.count() - 1; }
        }
      }
    }
  }
  return ret;
}

const char *ObFTParser::NAME_STR[ObFTParser::ParserType::FTP_MAX] = {
#define FT_PARSER_TYPE(ftp_type, parser_name) #parser_name,
  FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE
};

bool ObFTParser::is_builtin() const
{
  bool is_builtin = false;
#define FT_PARSER_TYPE(ftp_type, parser_name) is_builtin = is_builtin || is_##parser_name();
  FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE
  return is_builtin;
}

int ObFTParser::init(const common::ObString &parser_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parser_name_.set_name(parser_name))) {
  } else if (!is_builtin()) {
    ret = OB_FUNCTION_NOT_DEFINED;
    LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, parser_name.length(), parser_name.ptr());
  } else {
    parser_version_ = BUILTIN_VERSION;
  }
  return ret;
}

int ObFTParser::parse_from_str(const char *parser_name, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parser_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(buf_len >= OB_FT_PARSER_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char name[OB_FT_PARSER_NAME_LENGTH];
    char *saveptr = nullptr;
    char *token = nullptr;
    char *end_ptr = nullptr;
    MEMCPY(name, parser_name, buf_len);
    name[buf_len] = '\0';
    if (OB_ISNULL(token = STRTOK_R(name, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(parser_name_.set_name(token))) {
    } else if (OB_ISNULL(token = STRTOK_R(nullptr, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ob_strtoll(token, end_ptr, parser_version_))) {
    } else if (OB_NOT_NULL(token = STRTOK_R(nullptr, ".", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (!is_builtin()) {
      ret = OB_FUNCTION_NOT_DEFINED;
      LOG_USER_ERROR(OB_FUNCTION_NOT_DEFINED, static_cast<int>(buf_len), parser_name);
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

// The fulltext parser name consists of two parts: name and version, e.g. default_parser.1,
// separated by dot. This function is designed to serialize them into cstring.
int ObFTParser::serialize_to_str(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < OB_FT_PARSER_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, "%.*s.%ld", parser_name_.len(), parser_name_.str(),
          parser_version_))) {
  }
  return ret;
}

int ObFTParser::get_desc(const ObIFTParserDesc *&parser_desc) const
{
  int ret = OB_SUCCESS;
  parser_desc = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_space()) {
    static const ObWhiteSpaceFTParserDesc desc;
    parser_desc = &desc;
  } else if (is_ngram()) {
    static const ObNgramFTParserDesc desc;
    parser_desc = &desc;
  } else if (is_beng()) {
    static const ObBasicEnglishFTParserDesc desc;
    parser_desc = &desc;
  } else if (is_ik()) {
    static const ObIKFTParserDesc desc;
    parser_desc = &desc;
  } else if (is_ngram2()) {
    static const ObNgram2FTParserDesc desc;
    parser_desc = &desc;
  } else {
    ret = OB_FUNCTION_NOT_DEFINED;
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObFTParseData

static ObFTParseData *g_ftparse_data = nullptr;
static constexpr const char *FTPARSE_DATA_MEMORY_LABEL = "FtParse";

int ObFTParseData::init_global()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(g_ftparse_data)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(g_ftparse_data = OB_NEW(ObFTParseData, FTPARSE_DATA_MEMORY_LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(g_ftparse_data->init())) {
  }
  return ret;
}

void ObFTParseData::deinit_global()
{
  if (OB_NOT_NULL(g_ftparse_data)) {
    OB_DELETE(ObFTParseData, FTPARSE_DATA_MEMORY_LABEL, g_ftparse_data);
    g_ftparse_data = nullptr;
  }
}

ObFTParseData &ObFTParseData::instance()
{
  return *g_ftparse_data;
}

ObFTParseData::~ObFTParseData()
{
  destroy();
}

int ObFTParseData::init()
{
  generation_.fetch_add(1, std::memory_order_acq_rel);
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr;
  mem_attr.label_ = FTPARSE_DATA_MEMORY_LABEL;

  if (OB_FAIL(handler_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      mem_attr))) {
  } else if (OB_FAIL(init_and_set_stopword_list())) {
  } else if (OB_FAIL(init_dict_hub())) {
  } else {
    is_inited_ = true;
    FLOG_INFO("succeeded to initialize fulltext parser data", KP(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObFTParseData::init_and_set_stopword_list()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stop_word_checker_ = OB_NEWx(ObStopWordChecker, &handler_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(stop_word_checker_->init())) {
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObStopWordChecker, &handler_allocator_, stop_word_checker_);
    stop_word_checker_ = nullptr;
  }

  return ret;
}

int ObFTParseData::init_dict_hub()
{
  // make dict
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_hub_ = OB_NEWx(ObFTDictHub, &handler_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(dict_hub_->init())) {
  }
  return ret;
}

void ObFTParseData::destroy()
{
  generation_.fetch_add(1, std::memory_order_acq_rel);
  if (OB_NOT_NULL(stop_word_checker_)) {
    stop_word_checker_->destroy();
    OB_DELETEx(ObStopWordChecker, &handler_allocator_, stop_word_checker_);
    stop_word_checker_ = nullptr;
  }

  if (!OB_ISNULL(dict_hub_)) {
    dict_hub_->destroy();
    dict_hub_->~ObFTDictHub();
    handler_allocator_.free(dict_hub_);
    dict_hub_ = nullptr;
  }

  handler_allocator_.reset();
  is_inited_ = false;
}

int ObFTParseData::get_dict_hub(ObFTDictHub *&hub)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_hub_)) {
    ret = OB_NOT_INIT;
  } else {
    hub = dict_hub_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObFTParseHelper
// This workspace is owned by one helper/operator. No mutable parser state is
// shared across sessions. Clear all scratch-backed lists before reusing the arena.
class FTParserWorkspace
{
public:
  FTParserWorkspace(bool is_ik, ObFTDictHub *hub)
      : objects_("FTParserObjects"), scratch_("FTParserScratch"),
        ik_(objects_, hub, &scratch_), beng_(objects_), is_ik_(is_ik) {}
  int open(ObFTParserParam &param, ObITokenIterator *&iter)
  {
    int ret = OB_SUCCESS;
    if (is_ik_) {
      ret = ik_.restart(param);
      iter = &ik_;
    } else {
      ret = beng_.restart(&param);
      iter = &beng_;
    }
    return ret;
  }
  bool enable_default_pipeline(uint64_t generation)
  { return !is_ik_ || ik_.enable_default_pipeline(generation); }
  int next_view(ObITokenIterator *iter, const char *&word, int64_t &length,
                int64_t &chars, int64_t &freq)
  {
    return is_ik_ ? iter->get_next_token(word, length, chars, freq)
                  : beng_.get_next_token_view(word, length, chars, freq);
  }
  int64_t retained_bytes() const { return objects_.total() + scratch_.total(); }
  void finish()
  {
    if (is_ik_) {
      ik_.clear_document();
      scratch_.reuse();
    } else {
      beng_.clear_document();
    }
  }
private:
  ObArenaAllocator objects_;
  ObArenaAllocator scratch_;
  ObIKFTParser ik_;
  ObBEngFTParser beng_;
  bool is_ik_;
};

int ObFTParseHelper::segment(
    const ObFTParserProperty &property,
    const int64_t parser_version,
    const ObIFTParserDesc *parser_desc,
    const ObCharsetInfo *cs,
    const char *ft,
    const int64_t ft_len,
    common::ObIAllocator &allocator,
    ObAddWord &add_word,
    FTParserWorkspace *workspace)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parser_version < 0 || nullptr == parser_desc || nullptr == cs || nullptr == ft || 0 >= ft_len)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObFTParserParam param;
    ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    param.cs_ = cs;
    param.fulltext_ = ft;
    param.ft_length_ = ft_len;
    param.parser_version_ = parser_version;
    param.ngram_token_size_ = property.ngram_token_size_;
    param.ik_param_.mode_
        = (property.ik_mode_smart_ ? ObFTIKParam::Mode::SMART : ObFTIKParam::Mode::MAX_WORD);
    param.min_ngram_size_ = property.min_ngram_token_size_;
    param.max_ngram_size_ = property.max_ngram_token_size_;

    if (OB_FAIL(nullptr == workspace ? parser_desc->segment(&param, iter) : workspace->open(param, iter))) {
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const char *word = nullptr;
      int64_t word_len = 0;
      int64_t char_cnt = 0;
      int64_t word_freq = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_token(word, word_len, char_cnt, word_freq))) {
          if (OB_ITER_END != ret) {
          }
        } else if (OB_FAIL(add_word.process_word(word, word_len, char_cnt, word_freq))) {
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (nullptr != workspace) {
      workspace->finish();
    } else if (OB_NOT_NULL(iter)) {
      parser_desc->free_token_iter(&param, iter);
      iter = nullptr;
    }
  }
  return ret;
}

ObFTParseHelper::ObFTParseHelper()
    : allocator_(nullptr),
    parser_desc_(nullptr),
    parser_name_(),
    add_word_flag_(),
    parser_property_(),
    is_inited_(false),
    workspace_(nullptr)
{
}

ObFTParseHelper::~ObFTParseHelper()
{
  reset();
}

int ObFTParseHelper::init(
    common::ObIAllocator *allocator,
    const common::ObString &parser_name,
    const common::ObString &parser_properties,
    bool reuse_parser)
{
  int ret = OB_SUCCESS;
  ObFTParser parser;
  ObFTParserProperty properties;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(parser_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parser.parse_from_str(parser_name.ptr(), parser_name.length()))) {
  } else if (OB_FAIL(properties.parse_for_parser_helper(parser, parser_properties))) {
  } else {
    ret = init(allocator, parser_name, properties, reuse_parser);
  }
  return ret;
}

int ObFTParseHelper::init(common::ObIAllocator *allocator,
                          const common::ObString &parser_name,
                          const ObFTParserProperty &properties,
                          bool reuse_parser)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(parser_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parser_name_.parse_from_str(parser_name.ptr(), parser_name.length()))) {
  } else if (OB_FAIL(parser_name_.get_desc(parser_desc_))) {
  } else if (OB_ISNULL(parser_desc_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(set_add_word_flag(*parser_desc_))) {
  } else {
    allocator_ = allocator;
    parser_property_ = properties;
    if (reuse_parser && (parser_name_.is_ik() || parser_name_.is_beng())) {
      ObFTDictHub *hub = nullptr;
      if (parser_name_.is_ik() && OB_FAIL(ObFTParseData::instance().get_dict_hub(hub))) {
      } else if (OB_ISNULL(workspace_ = OB_NEW(FTParserWorkspace, "FTParserWork", parser_name_.is_ik(), hub))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    is_inited_ = OB_SUCC(ret);
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int64_t ObFTParseHelper::retained_workspace_bytes() const
{
  return nullptr == workspace_ ? 0 : workspace_->retained_bytes();
}

void ObFTParseHelper::reset()
{
  if (nullptr != workspace_) {
    OB_DELETE(FTParserWorkspace, "FTParserWork", workspace_);
    workspace_ = nullptr;
  }
  parser_desc_ = nullptr;
  allocator_ = nullptr;
  add_word_flag_.clear();
  is_inited_ = false;
}

int ObFTParseHelper::segment(
    const ObObjMeta &meta,
    const char *fulltext,
    const int64_t fulltext_len,
    int64_t &doc_length,
    ObFTWordMap &words) const
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;
  ObCollationType type = meta.get_collation_type();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == type || type >= CS_TYPE_PINYIN_BEGIN_MARK)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(type))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (words.size() > 0) {
      words.reuse();
    }
    ObAddWord add_word(parser_property_, meta, add_word_flag_, *allocator_, words);
    if (OB_FAIL(segment(
                    parser_property_,
                    parser_name_.get_parser_version(),
                    parser_desc_,
                    cs,
                    fulltext,
                    fulltext_len,
                    *allocator_,
                    add_word, workspace_))) {
    } else {
      doc_length = add_word.get_add_word_count();
    }
  }
  LOG_DEBUG("ft parse segment", K(ret), K(type), K(add_word_flag_), K(parser_name_),
      K(ObString(fulltext_len, fulltext)), K(words.size()));
  return ret;
}

int ObFTParseHelper::segment_unique(const ObObjMeta &meta, const char *fulltext,
                                    int64_t fulltext_len, FTUniqueTerms &terms,
                                    bool &fallback, bool builtin_ik_dictionary) const
{
  int ret = OB_SUCCESS;
  fallback = false;
  const ObCharsetInfo *cs = ObCharset::get_charset(meta.get_collation_type());
  if (!is_inited_) { return OB_NOT_INIT; }
  if (meta.get_collation_type() == CS_TYPE_INVALID
      || meta.get_collation_type() >= CS_TYPE_PINYIN_BEGIN_MARK
      || nullptr == cs || fulltext == nullptr || fulltext_len <= 0) { return OB_INVALID_ARGUMENT; }
  if (parser_name_.is_ik() && (!builtin_ik_dictionary
      || parser_name_.get_parser_version() != ObFTParser::BUILTIN_VERSION)) {
    fallback = true;
    return ret;
  }
  // Other parsers retain the ordinary path until they offer explicit view lifetimes.
  if (nullptr == workspace_ || terms.overflow() || fulltext_len > 1024 * 1024) {
    fallback = true;
    return ret;
  }
  if (parser_name_.is_beng()) {
    bool handled = false;
    ret = try_beng_ascii_unique(cs, meta, parser_property_, fulltext, fulltext_len, terms, handled);
    if (OB_FAIL(ret) || handled) {
      fallback = terms.overflow();
      return ret;
    }
  }
  ObFTParserParam param;
  param.allocator_ = allocator_;
  param.cs_ = cs;
  param.fulltext_ = fulltext;
  param.ft_length_ = fulltext_len;
  param.parser_version_ = parser_name_.get_parser_version();
  param.ngram_token_size_ = parser_property_.ngram_token_size_;
  param.min_ngram_size_ = parser_property_.min_ngram_token_size_;
  param.max_ngram_size_ = parser_property_.max_ngram_token_size_;
  param.ik_param_.mode_ = parser_property_.ik_mode_smart_
      ? ObFTIKParam::Mode::SMART : ObFTIKParam::Mode::MAX_WORD;
  ObITokenIterator *iter = nullptr;
  ObArenaAllocator normalization("FTUniqueNorm");
  const bool utf8 = ObCharset::charset_type_by_coll(meta.get_collation_type()) == CHARSET_UTF8MB4;
  if (OB_FAIL(workspace_->open(param, iter))) {
  } else if (parser_name_.is_ik() && !ft_tokenize_profile_no_ac()
      && !workspace_->enable_default_pipeline(ObFTParseData::instance().generation())) {
    // Unsupported input, cold-build/OOM/budget miss: no terms have escaped.
    fallback = true;
  } else {
    while (OB_SUCC(ret) && !terms.overflow()) {
      const char *word = nullptr;
      int64_t length = 0, chars = 0, freq = 0;
      if (OB_FAIL(workspace_->next_view(iter, word, length, chars, freq))) {
      } else if (nullptr == word || length <= 0 || chars <= 0 || freq <= 0) {
        ret = OB_INVALID_ARGUMENT;
      } else if (add_word_flag_.min_max_word()
          && (chars < parser_property_.min_token_size_ || chars > parser_property_.max_token_size_)) {
      } else {
        ObString normalized(length, word);
        if (add_word_flag_.casedown()) {
          if (!ft_unique_lower_identity(cs, utf8, word, length)) {
            normalization.reuse();
            ret = ObCharset::tolower(meta.get_collation_type(), ObString(length, word),
                                     normalized, normalization);
          }
        }
        // Preserve document views when case conversion leaves bytes unchanged
        // (notably Chinese). The temporary conversion buffer can then be reused.
        if (normalized.length() == length && 0 == MEMCMP(normalized.ptr(), word, length)) {
          normalized.assign_ptr(word, length);
        }
        ObFTWord key(normalized.length(), normalized.ptr(), meta);
        bool stop = false;
        if (OB_SUCC(ret) && add_word_flag_.stopword()) {
          ObStopWordChecker *checker = ObFTParseData::instance().stop_word_checker();
          if (nullptr == checker) { ret = OB_ERR_UNEXPECTED; }
          else { ret = checker->check_stopword(key, stop); }
        }
        if (OB_SUCC(ret) && !stop) { ret = terms.add(key, meta); }
      }
    }
    if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
  }
  if (terms.overflow()) { fallback = true; }
  workspace_->finish();
  return ret;
}

int ObFTParseHelper::check_is_the_same(
    const common::ObString &parser_name_str,
    const common::ObString &parser_properties,
    bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (is_inited_) {
    storage::ObFTParser parser_name;
    ObFTParserProperty parser_property;
    if (OB_UNLIKELY(parser_name_str.empty())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(parser_name.parse_from_str(parser_name_str.ptr(), parser_name_str.length()))) {
    } else if (OB_FAIL(parser_property.parse_for_parser_helper(parser_name, parser_properties))) {
    } else if (parser_name == parser_name_ && parser_property.is_equal(parser_property_)) {
      is_same = true;
    }
  }
  return ret;
}

int ObFTParseHelper::make_detail_json(
    const ObFTWordMap &words,
    const int64_t doc_length,
    common::ObIJsonBase *&json_root)
{
 int ret = OB_SUCCESS;

 ObJsonObject *root_obj = nullptr;

 ObJsonInt *cnt = nullptr;

 ObJsonArray *token_array = nullptr;

 if (OB_ISNULL(root_obj = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
 } else if (OB_ISNULL(cnt = OB_NEWx(ObJsonInt, allocator_, doc_length))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
 } else if (OB_ISNULL(token_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
   ret = OB_ALLOCATE_MEMORY_FAILED;
 } else {
   for (ObFTWordMap::const_iterator it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it) {
     ObString key = it->first.get_word().get_string();
     ObJsonObject *node = nullptr;
     ObJsonInt *token_cnt_node = nullptr;
     if (OB_ISNULL(node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
     } else if (OB_ISNULL(token_cnt_node = OB_NEWx(ObJsonInt, allocator_, it->second))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
     } else if (OB_FAIL(node->add(key, token_cnt_node))) {
     } else if (OB_FAIL(token_array->append(node))) {
     } else {
       // pass
     }

     if (OB_FAIL(ret)) {
       OB_DELETEx(ObJsonObject, allocator_, node);
       OB_DELETEx(ObJsonInt, allocator_, token_cnt_node);
     }
   } // for

   if (OB_SUCC(ret)) {
     if (OB_FAIL(root_obj->add(ENTRY_NAME_TOKENS, token_array))) {
     } else if (OB_FAIL(root_obj->add(ENTRY_NAME_DOC_LEN, cnt))) {
     }
   }
 }
  if (OB_SUCC(ret)) {
    json_root = root_obj;
  } else {
    OB_DELETEx(ObJsonObject, allocator_, root_obj);
    OB_DELETEx(ObJsonInt, allocator_, cnt);
    OB_DELETEx(ObJsonArray, allocator_, token_array);
  }

  return ret;
}

int ObFTParseHelper::make_token_array_json(
    const ObFTWordMap &words,
    common::ObIJsonBase *&json_root)
{
  int ret = OB_SUCCESS;
  ObJsonArray *token_array = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(token_array = OB_NEWx(ObJsonArray, allocator_, allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (ObFTWordMap::const_iterator it = words.begin(); OB_SUCC(ret) && it != words.end(); ++it) {
      ObString key = it->first.get_word().get_string();
      ObJsonString *token = nullptr;
      if (OB_UNLIKELY(OB_ISNULL(token = OB_NEWx(ObJsonString, allocator_, key)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (OB_FAIL(token_array->append(token))) {
          OB_DELETEx(ObJsonString, allocator_, token);
        } else {
        }
      }
    } // for
  }
  if (OB_SUCC(ret)) {
    json_root = token_array;
  } else {
    OB_DELETEx(ObJsonArray, allocator_, token_array);
  }
  return ret;
}

int ObFTParseHelper::set_add_word_flag(const ObIFTParserDesc &ftparser_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ftparser_desc.get_add_word_flag(add_word_flag_))) {
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
