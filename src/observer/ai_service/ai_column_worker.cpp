#define USING_LOG_PREFIX SERVER
#include "observer/ai_service/ai_column_worker.h"
#include "share/schema/ai_column_schema.h"
#include "share/ob_server_struct.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_timeout_ctx.h"
#include <optional>

namespace oceanbase { namespace observer {
using namespace common;
using share::schema::AiColumnDefinition;
namespace {
using Cells = std::vector<std::optional<std::string>>;
int read_rows(ObISQLClient &client, const std::string &sql, int columns, std::vector<Cells> &rows)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy::MySQLResult result;
  if (OB_FAIL(client.read(result, sql.c_str()))) { return ret; }
  auto *rs = result.get_result();
  while (OB_SUCC(ret) && OB_SUCC(ret = rs->next())) {
    Cells row;
    for (int i = 0; OB_SUCC(ret) && i < columns; ++i) {
      ObString value;
      int read_ret = rs->get_varchar(i, value);
      if (read_ret == OB_ERR_NULL_VALUE) { row.emplace_back(std::nullopt); }
      else if (read_ret != OB_SUCCESS) { ret = read_ret; }
      else { row.emplace_back(AiColumnDefinition::string(value)); }
    }
    if (OB_SUCC(ret)) { rows.push_back(std::move(row)); }
  }
  return ret == OB_ITER_END ? OB_SUCCESS : ret;
}
const std::string &cell(const Cells &row, size_t i)
{
  static const std::string empty;
  return row.at(i).has_value() ? row.at(i).value() : empty;
}
std::string literal(const std::string &value) { return AiColumnDefinition::literal(value); }
std::string identifier(const std::string &value) { return AiColumnDefinition::identifier(value); }
int write(ObISQLClient &client, const std::string &sql)
{
  int64_t affected = 0;
  return client.write(sql.c_str(), affected);
}
int validate_result(const AiColumnDefinition &definition, const std::string &value)
{
  int ret = OB_SUCCESS;
  ObObjType type = static_cast<ObObjType>(definition.result_type);
  if (!definition.embedding && (type == ObJsonType || ob_is_numeric_type(type))) {
    ObArenaAllocator allocator;
    ObIJsonBase *json = nullptr;
    ObString text(value.size(), value.data());
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, text,
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json))) {
    } else if (ob_is_numeric_type(type)) {
      auto kind = json->json_type();
      if (kind != ObJsonNodeType::J_INT && kind != ObJsonNodeType::J_UINT
          && kind != ObJsonNodeType::J_DOUBLE && kind != ObJsonNodeType::J_DECIMAL) { ret = OB_INVALID_ARGUMENT; }
      if (ob_is_integer_type(type) && kind != ObJsonNodeType::J_INT && kind != ObJsonNodeType::J_UINT) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}
}

AiColumnWorker &AiColumnWorker::instance()
{
  static AiColumnWorker worker;
  return worker;
}

void AiColumnWorker::run1()
{
  lib::set_thread_name("AiColumnWorker");
  while (!has_set_stop()) {
    if (GCTX.sql_proxy_ != nullptr) {
      ObTimeoutCtx timeout;
      timeout.set_timeout(60L * 1000 * 1000);
      int ret = process_tasks();
      if (ret != OB_SUCCESS && ret != OB_TABLE_NOT_EXIST) { LOG_WARN("AI task poll failed", K(ret)); }
      ret = publish_groups();
      if (ret != OB_SUCCESS && ret != OB_TABLE_NOT_EXIST && REACH_TIME_INTERVAL(1000000)) { LOG_WARN("AI result publication failed", K(ret)); }
    }
    ob_usleep(100000);
  }
}

int AiColumnWorker::process_tasks()
{
  int ret = OB_SUCCESS;
  auto &proxy = *GCTX.sql_proxy_;
  std::vector<Cells> tasks;
  if (OB_FAIL(read_rows(proxy,
      "SELECT CAST(table_id AS CHAR),CAST(column_id AS CHAR),request_id,definition,prompt,"
      "CAST(retry_count AS CHAR),UUID() FROM oceanbase.__all_ai_column_task "
      "WHERE ((state IN (0,3) AND eligible_ts<=NOW(6)) OR (state=2 AND lease_expiry<NOW(6))) "
      "ORDER BY eligible_ts LIMIT 32", 7, tasks))) { return ret; }
  for (const auto &task : tasks) {
    if (has_set_stop()) { break; }
    AiColumnDefinition definition;
    const auto &json = cell(task, 3);
    if (OB_FAIL(definition.load(ObString(json.size(), json.data())))) { return ret; }
    std::string key = "table_id=" + cell(task, 0) + " AND column_id=" + cell(task, 1)
        + " AND request_id=" + literal(cell(task, 2));
    std::string owner = cell(task, 6);
    std::string claim = "UPDATE oceanbase.__all_ai_column_task SET state=2,lease_owner=" + literal(owner)
        + ",lease_expiry=TIMESTAMPADD(SECOND,90,NOW(6)) WHERE " + key
        + " AND ((state IN (0,3) AND eligible_ts<=NOW(6)) OR (state=2 AND lease_expiry<NOW(6)))";
    int64_t affected = 0;
    if (OB_FAIL(proxy.write(claim.c_str(), affected))) { return ret; }
    if (affected != 1) { continue; }

    // The claim is committed before calling a model. No user transaction or
    // row lock is held across the network request.
    std::vector<Cells> result;
    std::string invocation = std::string("SELECT CAST(") + (definition.embedding ? "AI_EMBED(" : "AI_COMPLETE(")
        + literal(definition.model) + "," + (task[4].has_value() ? literal(cell(task, 4)) : "NULL");
    if (definition.embedding) { invocation += "," + std::to_string(definition.dimension); }
    else { invocation += "," + definition.completion_config(); }
    invocation += ") AS CHAR)";
    int execution_ret = read_rows(proxy, invocation, 1, result);
    if (execution_ret == OB_SUCCESS && (result.size() != 1 || !result[0][0].has_value())) {
      execution_ret = OB_INVALID_ARGUMENT;
    }
    if (execution_ret == OB_SUCCESS) { execution_ret = validate_result(definition, cell(result[0], 0)); }
    std::string update = "UPDATE oceanbase.__all_ai_column_task SET ";
    if (execution_ret == OB_SUCCESS) {
      update += "state=4,result=" + literal(cell(result[0], 0)) + ",last_error=0";
    } else {
      int64_t retries = atoll(cell(task, 5).c_str());
      bool retry = retries < definition.retries;
      update += std::string("state=") + (retry ? "3" : "5")
          + ",last_error=" + std::to_string(execution_ret);
      if (retry) {
        update += ",retry_count=retry_count+1,eligible_ts=TIMESTAMPADD(SECOND,"
            + std::to_string(1LL << std::min<int64_t>(retries, 6)) + ",NOW(6))";
      }
    }
    update += ",lease_expiry=NULL WHERE " + key + " AND state=2 AND lease_expiry>=NOW(6) AND lease_owner=" + literal(owner);
    if (OB_FAIL(write(proxy, update))) { return ret; }
  }
  return ret;
}

int AiColumnWorker::publish_groups()
{
  int ret = OB_SUCCESS;
  std::vector<Cells> groups;
  auto &proxy = *GCTX.sql_proxy_;
  if (OB_FAIL(read_rows(proxy,
      "SELECT CAST(table_id AS CHAR),request_id FROM oceanbase.__all_ai_column_task "
      "GROUP BY table_id,request_id HAVING SUM(state NOT IN (4,5))=0 LIMIT 32", 2, groups))) { return ret; }
  for (const auto &group : groups) {
    if (has_set_stop()) { break; }
    ObMySQLTransaction transaction;
    std::string group_key = "table_id=" + cell(group, 0) + " AND request_id=" + literal(cell(group, 1));
    if (OB_FAIL(transaction.start(&proxy))) { return ret; }
    auto *session = query::ObInnerSQLConnectionAccess::get_session(transaction.get_connection());
    bool saved = session != nullptr && session->is_ai_column_internal();
    if (session != nullptr) { session->set_ai_column_internal(true); }
    std::vector<Cells> tasks, tables;
    if (OB_FAIL(read_rows(transaction,
        "SELECT definition,result,CAST(state AS CHAR),rowkey_predicate,CAST(column_id AS CHAR),CAST(definition_version AS CHAR) "
        "FROM oceanbase.__all_ai_column_task WHERE " + group_key + " FOR UPDATE", 6, tasks))) {
    } else if (OB_FAIL(read_rows(transaction,
        "SELECT d.database_name,t.table_name FROM oceanbase.__all_table t JOIN oceanbase.__all_database d "
        "ON d.database_id=t.database_id WHERE t.table_id=" + cell(group, 0), 2, tables))) {
    } else if (!tasks.empty()) {
      bool terminal = true;
      std::vector<std::pair<std::string, std::string>> assignments;
      std::string generations;
      for (const auto &task : tasks) {
        if (cell(task, 2) != "4" && cell(task, 2) != "5") { terminal = false; break; }
        const auto &json = cell(task, 0);
        AiColumnDefinition definition;
        if (OB_FAIL(definition.load(ObString(json.size(), json.data())))) { break; }
        std::vector<Cells> metadata;
        if (OB_FAIL(read_rows(transaction,
            "SELECT a.definition FROM oceanbase.__all_ai_column a JOIN oceanbase.__all_column c "
            "ON a.table_id=c.table_id AND a.column_id=c.column_id WHERE a.table_id=" + cell(group, 0)
            + " AND a.column_id=" + cell(task, 4)
            + " AND a.definition_version=" + cell(task, 5)
            + " AND c.schema_version=" + cell(task, 5), 1, metadata))) { break; }
        if (metadata.empty() || cell(metadata[0], 0) != json) { continue; }
        const std::string state = identifier(definition.hidden("state"));
        const std::string column = identifier(definition.name);
        const std::string matches = identifier(definition.hidden("request")) + "=" + literal(cell(group, 1));
        std::string value = cell(task, 2) == "4" ? literal(cell(task, 1)) : "NULL";
        assignments.emplace_back(column + "=" + value + "," + state + "=" + (cell(task, 2) == "4" ? "1" : "2"),
            column + "=NULL," + state + "=2");
        generations += " AND " + matches;
      }
      if (OB_SUCC(ret) && terminal) {
        if (!tables.empty() && !assignments.empty()) {
          const std::string update = "UPDATE " + identifier(cell(tables[0], 0)) + "." + identifier(cell(tables[0], 1)) + " SET ";
          const std::string predicate = " WHERE " + cell(tasks[0], 3) + generations;
          // All columns remain in this one transaction. A conversion failure
          // affects that column only; successful siblings still become READY.
          for (const auto &assignment : assignments) {
            if (OB_FAIL(ret)) { break; }
            ret = write(transaction, update + assignment.first + predicate);
            if (ret == OB_DATA_OUT_OF_RANGE || ret == OB_ERR_DATA_TOO_LONG
                || ret == OB_ERR_DATA_TRUNCATED || ret == OB_ERR_WARN_DATA_OUT_OF_RANGE
                || ret == OB_ERR_TRUNCATED_WRONG_VALUE || ret == OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD
                || ret == OB_ERR_INCORRECT_STRING_VALUE || ret == OB_INVALID_ARGUMENT) {
              // Conversion errors roll back the statement, not this transaction.
              transaction.reset_last_error();
              ret = write(transaction, update + assignment.second + predicate);
            }
          }
        }
        if (OB_SUCC(ret)) { ret = write(transaction, "DELETE FROM oceanbase.__all_ai_column_task WHERE " + group_key); }
      }
    }
    if (session != nullptr) { session->set_ai_column_internal(saved); }
    int end_ret = transaction.end(ret == OB_SUCCESS);
    if (OB_SUCC(ret)) { ret = end_ret; }
    if (OB_FAIL(ret)) { break; }
  }
  return ret;
}
}} // namespace oceanbase::observer
