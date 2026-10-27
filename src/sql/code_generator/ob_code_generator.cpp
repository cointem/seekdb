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

#define USING_LOG_PREFIX SQL_CG

#include "config_bridge.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/ob_physical_plan.h"
#include "share/schema/ai_column_schema.h"
#include <functional>
#include <set>

namespace oceanbase
{
namespace sql
{

int ObCodeGenerator::generate(const ObLogPlan &log_plan,
                              ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = 0;
  OZ(detect_batch_size(log_plan, batch_size));
  if (OB_SUCC(ret) && batch_size > 0) {
    log_plan.get_optimizer_context().set_batch_size(batch_size);
    phy_plan.set_batch_size(batch_size);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_exprs(log_plan, phy_plan))) {
  } else if (OB_FAIL(generate_operators(log_plan, phy_plan))) {
  }

  // Keep column-level checks outside row filters and before the first write.
  // In particular, filtering to READY rows must not bypass strict-read mode.
  if (OB_SUCC(ret)) {
    std::set<std::string> checks;
    auto &guard = *log_plan.get_optimizer_context().get_exec_ctx()->get_sql_ctx()->schema_guard_;
    std::function<void(const ObDMLStmt *)> collect = [&](const ObDMLStmt *stmt) {
      using namespace share::schema;
      if (stmt == nullptr || OB_FAIL(ret)) { return; }
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_items().count(); ++i) {
        const auto &item = stmt->get_column_items().at(i);
        if (item.expr_ == nullptr || !item.expr_->is_referred_by_normal()
            || (item.expr_->get_column_flags() & AI_COLUMN_FLAG) == 0) { continue; }
        const auto *table_item = stmt->get_table_item_by_id(item.table_id_);
        if (table_item == nullptr || !table_item->is_basic_table()) { continue; }
        const ObTableSchema *table = nullptr;
        const ObDatabaseSchema *database = nullptr;
        if (OB_FAIL(guard.get_table_schema(table_item->ref_id_, table))) {
        } else if (table == nullptr) { ret = OB_SCHEMA_ERROR; }
        else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
        } else if (database == nullptr) { ret = OB_SCHEMA_ERROR; }
        else {
          AiColumnDefinition def;
          def.name = AiColumnDefinition::string(item.expr_->get_column_name());
          std::string query = "SELECT " + std::to_string(table->get_table_id()) + ","
              + std::to_string(item.column_id_) + " FROM "
              + AiColumnDefinition::identifier(AiColumnDefinition::string(database->get_database_name_str()))
              + "." + AiColumnDefinition::identifier(AiColumnDefinition::string(table->get_table_name_str()))
              + " WHERE " + AiColumnDefinition::identifier(def.hidden("state")) + " <> 1 LIMIT 1";
          checks.insert(query);
        }
      }
      ObSEArray<ObSelectStmt *, 4> children;
      if (OB_SUCC(ret) && OB_FAIL(stmt->get_child_stmts(children))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < children.count(); ++i) { collect(children.at(i)); }
      }
    };
    collect(log_plan.get_stmt());
    for (const auto &check : checks) {
      if (OB_FAIL(ret)) { break; }
      ObString saved;
      if (OB_FAIL(ob_write_string(phy_plan.get_allocator(), ObString(check.size(), check.data()), saved))) {
      } else if (OB_FAIL(phy_plan.ai_column_checks_.push_back(saved))) {
      }
    }
  }

  return ret;
}
//1. Generate the old execution plan, used for initializing all expression operators
//   and initialize to ObExpr's op_, for mixed running of new and old expressions, will be removed later
//2. Get all expressions that will be used during execution
//3. Generate all physical expressions
int ObCodeGenerator::generate_exprs(const ObLogPlan &log_plan,
                                    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  CK(NULL != exec_ctx && NULL != exec_ctx->get_physical_plan_ctx());
  CK(exec_ctx->get_my_session() != NULL);
  if (OB_SUCC(ret)) {
    ObStaticEngineExprCG expr_cg(
        phy_plan.get_allocator(),
        log_plan.get_optimizer_context().get_session_info(),
        exec_ctx->get_sql_ctx()->schema_guard_,
        exec_ctx->get_physical_plan_ctx()->get_original_param_cnt(),
        param_store_->count());
    // init ctx for operator cg
    expr_cg.set_batch_size(phy_plan.get_batch_size());
    if (OB_FAIL(expr_cg.generate(log_plan.get_optimizer_context().get_all_exprs(),
                                 phy_plan.get_expr_frame_info()))) {
    } else {
      phy_plan.get_next_expr_id() = phy_plan.get_expr_frame_info().need_ctx_cnt_;
    }
  }

  return ret;
}

int ObCodeGenerator::generate_operators(const ObLogPlan &log_plan,
                                        ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG static_engin_cg;
  if (OB_FAIL(static_engin_cg.generate(log_plan, phy_plan))) {
  }
  return ret;
}

static int check_fts_posting_batch(const ObLogPlan &log_plan, bool &posting_batch)
{
  int ret = OB_SUCCESS;
  posting_batch = false;
  ObOptimizerContext &opt_ctx = log_plan.get_optimizer_context();
  const TableItem *target = nullptr;
  const share::schema::ObTableSchema *schema = nullptr;
  if (!opt_ctx.is_insert_stmt_in_online_ddl()) {
  } else if (OB_ISNULL(opt_ctx.get_root_stmt()) || OB_ISNULL(opt_ctx.get_schema_guard())
      || OB_ISNULL(target = opt_ctx.get_root_stmt()->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(opt_ctx.get_schema_guard()->get_table_schema(target->ddl_table_id_, schema))) {
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Match the table-scan posting batch path. Partitioned FTS DDL retains its
    // scalar tablet/PDML routing protocol.
    posting_batch = !schema->is_partitioned_table()
        && (schema->is_fts_index_aux() || schema->is_fts_doc_word_aux());
  }
  return ret;
}

int ObCodeGenerator::detect_batch_size(
    const ObLogPlan &log_plan, int64_t &batch_size)
{
  int ret = OB_SUCCESS;
  bool vectorize = false;
  bool stop_checking = false;
  batch_size = 0;
  ObBasicSessionInfo *session =
      log_plan.get_optimizer_context().get_session_info();
  ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  CK(NULL != exec_ctx && NULL != exec_ctx->get_physical_plan_ctx());
  bool has_registered_vec_op = false;
  if (OB_ISNULL(log_plan.get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObStaticEngineCG::exist_registered_vec_op(*log_plan.get_plan_root(), true,
                                                               has_registered_vec_op))) {
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(session)) {
    // empty session disable batch processing
  } else {
    
    double scan_cardinality = 0;
    // TODO bin.lb: move to optimizer and more sophisticated rules
    int64_t lob_rowsets_max_rows = config::_lob_rowsets_max_rows();
    const ObOptParamHint *opt_params = &log_plan.get_stmt()->get_query_ctx()->get_global_hint().opt_params_;
    if (OB_FAIL(opt_params->get_integer_opt_param(ObOptParamHint::LOB_ROWSETS_MAX_ROWS, lob_rowsets_max_rows))) {
    } else {
      // TODO bin.lb; check all sub plans
      OZ(ObStaticEngineCG::check_vectorize_supported(vectorize,
                                                     stop_checking,
                                                     scan_cardinality,
                                                     log_plan.get_plan_root()));
    }
    if (OB_FAIL(ret)) {
    } else if (log_plan.get_optimizer_context().has_var_assign()
               && !log_plan.get_optimizer_context().is_var_assign_only_in_root_stmt()) {
      // Assignments in a child statement may feed a parent statement without being
      // projected by the child. Keep the whole plan scalar so their row-by-row side
      // effects cannot be reused by batch evaluation.
      batch_size = 0;
    } else if (log_plan.get_optimizer_context().has_var_assign() || !vectorize) {
      // Retain the historical single-row batch path for root-statement assignments
      // when a registered vector operator exists. This preserves evaluation order
      // without forcing that operator onto an unsupported scalar path.
      // set max_batch_size = 1
      // if all physical operator is not registered as vec op, disable vectorization
      if (has_registered_vec_op) {
        batch_size = 1;
      } else {
        batch_size = 0;
      }
    } else if (vectorize) {
      ObArenaAllocator alloc;
      ObRawExprUniqueSet flattened_exprs(true);
      OZ(flattened_exprs.flatten_and_add_raw_exprs(log_plan.get_optimizer_context()
                                                           .get_all_exprs()));
      ObStaticEngineExprCG expr_cg(
          alloc,
          log_plan.get_optimizer_context().get_session_info(),
          exec_ctx->get_sql_ctx()->schema_guard_,
          exec_ctx->get_physical_plan_ctx()->get_original_param_cnt(),
          0);
      int64_t rowsets_max_rows = config::_rowsets_max_rows();
      bool posting_batch = false;
      bool has_rowsets_hint = false;
      bool has_lob_rowsets_hint = false;
      OZ(check_fts_posting_batch(log_plan, posting_batch));
      OZ(opt_params->has_opt_param(ObOptParamHint::ROWSETS_MAX_ROWS, has_rowsets_hint));
      OZ(opt_params->has_opt_param(ObOptParamHint::LOB_ROWSETS_MAX_ROWS, has_lob_rowsets_hint));
      // A default source estimate of one document does not describe the
      // expanded postings. Avoid only the tiny-scan heuristic, before frame
      // allocation; retain all expression, LOB and side-effect limits. Explicit
      // rowset hints keep the established sizing and precedence unchanged.
      const bool use_posting_default = posting_batch && !has_rowsets_hint && !has_lob_rowsets_hint;
      const double batch_cardinality = use_posting_default
          ? MAX(scan_cardinality, static_cast<double>(ObStaticEngineExprCG::SMALL_SCAN_CARDINALITY))
          : scan_cardinality;
      OZ(expr_cg.detect_batch_size(flattened_exprs, batch_size,
                                   rowsets_max_rows,
                                   config::_rowsets_target_maxsize(),
                                   batch_cardinality,
                                   lob_rowsets_max_rows));
      if (OB_SUCC(ret) && use_posting_default) {
        // Bound default per-expression frames and the batch-owned posting
        // staging area even when the configured rowset limit is much larger.
        batch_size = MIN(batch_size, 256);
      }
      // overwrite batch size if hint is specified
      OZ(opt_params->get_integer_opt_param(ObOptParamHint::ROWSETS_MAX_ROWS, batch_size));

      int tmp_ret = OB_SUCCESS;
      tmp_ret = OB_E(EventTable::EN_ENABLE_RANDOM_BATCH_SIZE) OB_SUCCESS;
      if (OB_SUCCESS != tmp_ret) {
        static const int64_t min = 1;
        static const int64_t max = rowsets_max_rows * 2;
        batch_size = common::ObRandom::rand(min, max);
      }
    }
    // TODO qubin.qb: remove the tracelog when rowsets/batch_size is displayed
    // in plan
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
