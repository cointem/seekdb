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

#ifndef _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_
#define _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "data_plane/fts/ob_fts_parser_property.h"
#include "data_plane/fts/ob_fts_parser_name.h"
#include "data_plane/fts/dict/ob_dic_loader.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "data_plane/fts/ob_fts_unique_terms.h"
#include "common/json_type/ob_json_tree.h"

namespace oceanbase
{
namespace sql
{
class MultimodeAlloctor;
class ObExprTokenize : public ObStringExprOperator
{
public:
  explicit ObExprTokenize(common::ObIAllocator &alloc);
  ~ObExprTokenize() override;
  /**
   * @brief evaluate function
   * @param expr expression
   * @param ctx expression evaluation context
   * @param expr_datum expression result
   * @note see cg_expr REG_OP and g_expr_eval_functions
   */
  static int eval_tokenize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  struct TokenizeParam
  {
  public:
    static constexpr const char *CASE_INDICATOR_STR = "case";
    static constexpr const char *OUTPUT_MODE_STR = "output";
    static constexpr const char *STOPWORDS_LIST_STR = "stopwords";
    static constexpr const char *ADDITIONAL_ARGS_STR = "additional_args";

  public:
    TokenizeParam();

    int parse_json_param(const ObIJsonBase *obj);

    // check and reform parser properties to standard format
    int reform_parser_properties(const ObString &properties);
    int try_load_dictionary_for_ik();

  public:
    // for property and tmp json string
    mutable ObArenaAllocator allocator_;
    char parser_name_buf_[storage::OB_FT_PARSER_NAME_LENGTH];
    ObString parser_name_;
    ObString properties_;
    storage::ObFTParserProperty parsed_properties_;
    ObObjMeta meta_;
    ObString fulltext_;
    // A context lease pins its immutable loader identity, never document results.
    storage::ObDicLoaderHandle dictionary_loader_;
    ObCharsetType dictionary_charset_ = CHARSET_INVALID;
    uint64_t dictionary_generation_ = 0;
    bool builtin_ik_dictionary_ = true;
    enum OUTPUT_MODE
    {
      DEFAULT,
      ALL,
    } output_mode_;
  };

private:
  // A private lease owns this state; it contains configuration, never text results.
  class TokenizeContext
  {
  public:
    TokenizeContext() : document_allocator_("TokenizeDoc"), configured_(false),
        json_allocator_("TokenizeJson"), token_array_(&json_allocator_) {}
    ~TokenizeContext() = default;
    int make_token_array_json(common::ObIJsonBase *&result);
    void clear_document()
    {
      token_array_.clear();
      for (int64_t i = 0; i < token_count_; ++i) {
        token_nodes_[i].set_value(nullptr, 0);
        token_nodes_[i].set_parent(nullptr);
      }
      token_count_ = 0;
      unique_terms_.reuse();
      if (words_.created() && words_.size() > 0) { words_.reuse(); } // Drop keys before releasing their backing strings.
      if (document_allocator_.total() > 1024 * 1024) {
        document_allocator_.reset();
      } else {
        document_allocator_.reuse();
      }
      param_.fulltext_.reset();
    }
    TokenizeParam param_;
    common::ObArenaAllocator document_allocator_;
    storage::ObFTParseHelper helper_;
    storage::ObFTWordMap words_;
    storage::FTUniqueTerms unique_terms_;
    bool configured_;
    ObString parser_key_;
    ObString options_key_;
    int64_t null_mask_ = 0;
    uint64_t generation_ = 0;
    ObCollationType collation_ = CS_TYPE_INVALID;
    int64_t word_bucket_count_ = 0;
    common::ObArenaAllocator json_allocator_;
    common::ObJsonArray token_array_;
    common::ObJsonString *token_nodes_ = nullptr;
    int64_t token_capacity_ = 0;
    int64_t token_count_ = 0;
  };
  class TokenizePool;
  static int eval_reusable(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum);

private:
  static int parse_param(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         common::ObArenaAllocator &allocator,
                         TokenizeParam &param);

  static int parse_fulltext(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param);
  static int parse_parser_name(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param);
  static int parse_parser_properties(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     MultimodeAlloctor &mm_alloc,
                                     TokenizeParam &param);

  static int tokenize_fulltext(const TokenizeParam &param,
                               TokenizeParam::OUTPUT_MODE mode,
                               common::ObIAllocator &allocator,
                               ObIJsonBase *&result);

  static int construct_ft_parser_inner_name(const ObString &input_str, TokenizeParam &param);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTokenize);
};

} // namespace sql
} // namespace oceanbase

#endif // _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_
