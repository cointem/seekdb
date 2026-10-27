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

#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"

namespace oceanbase
{
namespace sql
{
// The outer query observes only min(limit, max(visible_hits - offset, 0)).
// Keep LIMIT/OFFSET and all storage visibility checks in their normal operators;
// only implicit relevance ordering and scoring become unnecessary. Locking,
// SQL_CALC_FOUND_ROWS and recursive queries have additional observations.
inline bool is_fts_capped_count(const ObDMLStmt *parent, const ObDMLStmt *child)
{
  bool eligible = false;
  // The optimizer context retains the EXPLAIN wrapper, whereas rewrite's
  // parent stack contains the underlying SELECT. Both must prove the same view.
  if (nullptr != parent && parent->is_explain_stmt()) {
    parent = static_cast<const ObExplainStmt *>(parent)->get_explain_query_stmt();
  }
  if (nullptr != parent && nullptr != child && parent->is_select_stmt() && child->is_select_stmt()) {
    const ObSelectStmt *outer = static_cast<const ObSelectStmt *>(parent);
    const ObSelectStmt *inner = static_cast<const ObSelectStmt *>(child);
    if (outer->is_single_table_stmt() && outer->get_table_item(0)->is_generated_table()
        && outer->get_table_item(0)->ref_query_ == inner
        && outer->is_scala_group_by() && outer->get_aggr_item_size() == 1
        && outer->get_select_item_size() == 1
        && outer->get_select_item(0).expr_ == outer->get_aggr_item(0)
        && outer->get_aggr_item(0)->get_expr_type() == T_FUN_COUNT
        && outer->get_aggr_item(0)->get_param_count() == 0
        && !outer->get_aggr_item(0)->is_param_distinct()
        && !outer->is_set_stmt() && !outer->has_distinct() && !outer->has_having()
        && !outer->has_order_by() && !outer->has_limit()
        && !outer->is_calc_found_rows() && !outer->has_recursive_cte()
        && !outer->has_window_function() && !outer->has_select_into() && !outer->has_for_update()
        && outer->get_condition_size() == 0
        && outer->get_subquery_expr_size() == 0 && !outer->is_contains_assignment()
        && inner->is_single_table_stmt() && inner->get_table_item(0)->is_basic_table()
        && !inner->is_set_stmt() && !inner->has_group_by() && !inner->has_distinct()
        && !inner->has_having() && !inner->has_window_function() && !inner->has_order_by()
        && !inner->has_for_update() && !inner->has_select_into()
        && !inner->is_calc_found_rows() && !inner->has_recursive_cte()
        && !inner->is_contains_assignment() && inner->get_subquery_expr_size() == 0
        && nullptr != inner->get_limit_expr() && inner->get_limit_expr()->is_const_expr()
        && (nullptr == inner->get_offset_expr() || inner->get_offset_expr()->is_const_expr())
        && nullptr == inner->get_limit_percent_expr() && !inner->is_fetch_with_ties()
        && inner->get_match_exprs().count() == 1 && inner->get_condition_size() == 1) {
      const ObRawExpr *match = inner->get_match_exprs().at(0);
      const ObRawExpr *filter = inner->get_condition_exprs().at(0);
      eligible = nullptr != match && nullptr != filter
          && static_cast<const ObMatchFunRawExpr *>(match)->get_mode_flag() == NATURAL_LANGUAGE_MODE
          && nullptr != static_cast<const ObMatchFunRawExpr *>(match)->get_search_key()
          && static_cast<const ObMatchFunRawExpr *>(match)->get_search_key()->is_const_expr()
          && (filter == match || (filter->get_expr_type() == T_OP_BOOL
              && filter->get_param_count() == 1 && filter->get_param_expr(0) == match));
      // Do not suppress observable side effects or errors in computed projections.
      for (int64_t i = 0; eligible && i < inner->get_select_item_size(); ++i) {
        const ObRawExpr *expr = inner->get_select_item(i).expr_;
        eligible = nullptr != expr && (expr->is_column_ref_expr() || expr->is_const_raw_expr());
      }
    }
  }
  return eligible;
}
}
}
