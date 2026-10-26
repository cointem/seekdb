#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ai_column_runtime.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "share/schema/ai_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase { namespace sql {
using namespace common;
using namespace share::schema;

bool AiColumnRuntime::needs_after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &definition)
{
  auto &ctx = op.get_exec_ctx();
  if (!definition.is_primary_index_ || ctx.get_my_session()->is_ai_column_internal()) { return false; }
  const ObTableSchema *table = nullptr;
  if (ctx.get_sql_ctx()->schema_guard_->get_table_schema(definition.das_base_ctdef_.index_tid_, table) != OB_SUCCESS
      || table == nullptr) { return false; }
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    if ((table->get_column_schema_by_idx(i)->get_column_flags() & AI_COLUMN_FLAG) != 0) { return true; }
  }
  return false;
}

int AiColumnRuntime::after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &definition, ObDmlEventType event)
{
  int ret = OB_SUCCESS;
  if (!needs_after_row(op, definition)) { return ret; }
  auto &ctx = op.get_exec_ctx();
  auto &guard = *ctx.get_sql_ctx()->schema_guard_;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  const bool deleting = event == ObDmlEventType::DE_DELETING;
  const bool inserting = event == ObDmlEventType::DE_INSERTING;
  if (OB_FAIL(guard.get_table_schema(definition.das_base_ctdef_.index_tid_, table))) {
  } else if (table == nullptr || table->is_table_without_pk()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI column DML without a primary key");
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
  } else if (database == nullptr) { ret = OB_SCHEMA_ERROR; }
  if (OB_FAIL(ret)) { return ret; }

  auto expression_index = [&](uint64_t id) -> int64_t {
    for (int64_t i = 0; i < definition.column_ids_.count(); ++i) {
      if (definition.column_ids_.at(i) == id) { return i; }
    }
    return -1;
  };
  auto changed = [&](uint64_t id) -> bool {
    if (inserting || deleting) { return true; }
    int64_t idx = expression_index(id);
    ObDatum *old_value = nullptr, *new_value = nullptr;
    if (idx < 0 || idx >= definition.old_row_.count() || idx >= definition.new_row_.count()) {
      // An update projection can omit unchanged columns.
      return false;
    }
    if (OB_FAIL(definition.old_row_.at(idx)->eval(op.get_eval_ctx(), old_value))) { return false; }
    if (OB_FAIL(definition.new_row_.at(idx)->eval(op.get_eval_ctx(), new_value))) { return false; }
    return old_value->is_null() != new_value->is_null()
        || (!old_value->is_null() && (old_value->len_ != new_value->len_
            || MEMCMP(old_value->ptr_, new_value->ptr_, old_value->len_) != 0));
  };
  auto predicate = [&](const ExprFixedArray &row, std::string &out) -> int {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    for (int64_t k = 0; OB_SUCC(ret) && k < table->get_rowkey_info().get_size(); ++k) {
      const auto *key = table->get_rowkey_info().get_column(k);
      int64_t idx = expression_index(key->column_id_);
      const ObColumnSchemaV2 *column = table->get_column_schema(key->column_id_);
      ObDatum *datum = nullptr;
      ObObj value;
      char buf[32768]; int64_t pos = 0;
      if (idx < 0 || idx >= row.count() || column == nullptr) { ret = OB_ERR_UNEXPECTED; }
      else if (OB_FAIL(row.at(idx)->eval(op.get_eval_ctx(), datum))) {
      } else if (OB_FAIL(datum->to_obj(value, row.at(idx)->obj_meta_))) {
      } else if (OB_FAIL(value.print_sql_literal(buf, sizeof(buf), pos, op.get_obj_print_params()))) {
      } else {
        if (k != 0) { out += " AND "; }
        out += AiColumnDefinition::identifier(AiColumnDefinition::string(column->get_column_name_str()))
            + " = " + std::string(buf, pos);
      }
    }
    return ret;
  };
  bool pk_changed = false;
  for (int64_t k = 0; OB_SUCC(ret) && k < table->get_rowkey_info().get_size(); ++k) {
    pk_changed |= changed(table->get_rowkey_info().get_column(k)->column_id_);
  }
  std::vector<AiColumnDefinition> affected;
  for (int64_t i = 0; OB_SUCC(ret) && i < table->get_column_count(); ++i) {
    const auto *spec = table->get_column_schema_by_idx(i);
    if ((spec->get_column_flags() & AI_RESULT_SPEC_COLUMN_FLAG) == 0) { continue; }
    AiColumnDefinition ai;
    bool invalidate = pk_changed;
    if (OB_FAIL(ai.load(spec->get_cur_default_value().get_string()))) { break; }
    const auto *state = table->get_column_schema(ai.hidden("state").c_str());
    int64_t state_index = state == nullptr ? -1 : expression_index(state->get_column_id());
    if (!inserting && !deleting && state_index >= 0 && state_index < definition.new_row_.count()) {
      ObDatum *value = nullptr;
      if (OB_FAIL(definition.new_row_.at(state_index)->eval(op.get_eval_ctx(), value))) { break; }
      invalidate |= !value->is_null() && value->get_int() == -1;
    }
    for (const auto &name : ai.dependencies) {
      const auto *dep = table->get_column_schema(name.c_str());
      if (dep == nullptr) { ret = OB_SCHEMA_ERROR; break; }
      invalidate |= changed(dep->get_column_id());
    }
    if (invalidate) { affected.push_back(ai); }
  }
  if (OB_FAIL(ret) || affected.empty()) { return ret; }
  std::string rowkey, old_rowkey;
  if (OB_FAIL(predicate(deleting ? definition.old_row_ : definition.new_row_, rowkey))) {
  } else if (!inserting && OB_FAIL(predicate(definition.old_row_, old_rowkey))) {
  } else if (OB_FAIL(op.open_inner_conn())) {
  } else if (OB_FAIL(op.begin_nested_session(true))) {
  } else {
    // This flag is never exposed as a SQL variable. Nested invalidation must not
    // enqueue itself, and only internal maintenance may name the hidden columns.
    auto *session = ctx.get_my_session();
    bool saved_internal = session->is_ai_column_internal();
    session->set_ai_column_internal(true);
    std::string request;
    if (!deleting) {
      ObMySQLProxy::MySQLResult result;
      ObString value;
      if (OB_FAIL(op.execute_read("SELECT UUID()", result))) {
      } else if (OB_FAIL(result.get_result()->next())) {
      } else if (OB_FAIL(result.get_result()->get_varchar(int64_t(0), value))) {
      } else { request = AiColumnDefinition::string(value); }
    }
    const std::string tid = std::to_string(table->get_table_id());
    const std::string target = AiColumnDefinition::identifier(AiColumnDefinition::string(database->get_database_name_str()))
        + "." + AiColumnDefinition::identifier(AiColumnDefinition::string(table->get_table_name_str()));
    std::string assignments;
    for (const auto &ai : affected) {
      if (OB_FAIL(ret)) { break; }
      const auto *column = table->get_column_schema(ai.name.c_str());
      std::string cleanup = "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=" + tid
          + " AND column_id=" + std::to_string(column->get_column_id())
          + " AND rowkey_predicate IN (" + AiColumnDefinition::literal(rowkey) + ","
          + AiColumnDefinition::literal(old_rowkey) + ")";
      if (OB_FAIL(op.execute_write(cleanup.c_str()))) { break; }
      if (!deleting) {
        if (!assignments.empty()) { assignments += ','; }
        assignments += AiColumnDefinition::identifier(ai.name) + "=NULL,"
            + AiColumnDefinition::identifier(ai.hidden("state")) + "=0,"
            + AiColumnDefinition::identifier(ai.hidden("request")) + "=" + AiColumnDefinition::literal(request);
        std::string enqueue = "INSERT INTO oceanbase.__all_ai_column_task(table_id,column_id,request_id,rowkey_predicate,"
            "definition_version,definition,prompt,state,retry_count,eligible_ts) SELECT " + tid
            + "," + std::to_string(column->get_column_id()) + "," + AiColumnDefinition::literal(request)
            + "," + AiColumnDefinition::literal(rowkey) + "," + std::to_string(column->get_schema_version())
            + "," + AiColumnDefinition::literal(ai.json()) + ",(" + ai.prompt
            + "),0,0,NOW(6) FROM " + target + " WHERE " + rowkey;
        ret = op.execute_write(enqueue.c_str());
      }
    }
    if (OB_SUCC(ret) && !deleting) {
      std::string invalidate = "UPDATE " + target + " SET " + assignments + " WHERE " + rowkey;
      ret = op.execute_write(invalidate.c_str());
    }
    session->set_ai_column_internal(saved_internal);
    int end_ret = op.end_nested_session();
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  if (ret != OB_SUCCESS) { LOG_WARN("AI column task maintenance failed", K(ret), K(table->get_table_id())); }
  return ret;
}

int AiColumnRuntime::check_read(ObExecContext &ctx, const ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  bool strict = false;
  auto *session = ctx.get_my_session();
  if (session->is_ai_column_internal() || !session->is_user_session() || plan.ai_column_checks_.empty()) { return ret; }
  if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_AI_COLUMN_STRICT_READ, strict)) || !strict) { return ret; }
  sqlclient::ObISQLConnectionGuard connection;
  ObIInnerSQLConnection *inner = as_inner_sql_connection(session->get_inner_conn());
  ObSQLSessionInfo::StmtSavedValue saved;
  ObIInnerSQLConnection::SavedValue saved_connection;
  if (inner == nullptr) {
    if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection))) {
    } else { inner = as_inner_sql_connection(connection.get_ptr()); }
  }
  if (OB_FAIL(ret)) {
  } else if (inner == nullptr) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(inner->begin_nested_session(saved, saved_connection, true))) {
  } else {
    session->set_ai_column_internal(true);
    for (int64_t i = 0; OB_SUCC(ret) && i < plan.ai_column_checks_.count(); ++i) {
      ObMySQLProxy::MySQLResult result;
      std::string query = AiColumnDefinition::string(plan.ai_column_checks_.at(i));
      if (OB_FAIL(inner->execute_read(query.c_str(), result))) {
      } else if ((ret = result.get_result()->next()) == OB_ITER_END) { ret = OB_SUCCESS; }
      else if (OB_SUCC(ret)) {
        int64_t table_id = 0, column_id = 0;
        if (OB_FAIL(result.get_result()->get_int(int64_t(0), table_id))) {
        } else if (OB_FAIL(result.get_result()->get_int(int64_t(1), column_id))) {
        } else {
          ret = OB_AI_COLUMN_NOT_READY;
          FORWARD_USER_ERROR_MSG(OB_AI_COLUMN_NOT_READY,
              "AI column has unfinished tasks for table_id=%ld, column_id=%ld", table_id, column_id);
        }
      }
    }
    session->set_ai_column_internal(false);
    int end_ret = inner->end_nested_session(saved, saved_connection);
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}
}} // namespace oceanbase::sql
