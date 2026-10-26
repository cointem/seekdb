#pragma once

#include <string>
#include <vector>
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "common/json_type/ob_json_tree.h"
#include "common/json_type/ob_json_base.h"
#include "common/mysqlclient/ob_isql_client.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase { namespace share { namespace schema {

// A definition is stored in the default of the hidden result-spec column as
// well as in __all_ai_column. Consequently schema snapshots and restore retain
// the definition even while the table has no rows.
struct AiColumnDefinition
{
  std::string name, model, prompt, argument, normalized_expr;
  std::vector<std::string> dependencies;
  bool embedding = false;
  int64_t retries = 2;
  int64_t dimension = 0;
  int64_t result_type = 0;

  std::string completion_config() const
  {
    using namespace common;
    ObObjType type = static_cast<ObObjType>(result_type);
    const char *instruction = type == ObJsonType ? "Return exactly one valid JSON value"
        : ob_is_integer_type(type) ? "Return exactly one base-10 integer"
        : ob_is_numeric_type(type) ? "Return exactly one valid JSON number" : nullptr;
    std::string config = argument.empty() ? "NULL" : argument;
    if (instruction != nullptr) {
      config = "JSON_SET(COALESCE(CAST((" + config + ") AS JSON),JSON_OBJECT()),'$.system_prompt','"
          + instruction + "')";
    }
    return config;
  }

  static std::string string(common::ObString s) { return std::string(s.ptr(), s.length()); }
  static std::string identifier(const std::string &s)
  {
    std::string out("`");
    for (char c : s) { out += c; if (c == '`') { out += c; } }
    return out + "`";
  }
  static std::string literal(const std::string &s)
  {
    static const char hex[] = "0123456789abcdef";
    std::string out("CONVERT(X'");
    for (unsigned char c : s) { out += hex[c >> 4]; out += hex[c & 15]; }
    return out + "' USING utf8mb4)";
  }
  static std::string json_string(const std::string &s)
  {
    static const char hex[] = "0123456789abcdef";
    std::string out("\"");
    for (unsigned char c : s) {
      if (c == '"' || c == '\\') { out += '\\'; out += c; }
      else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
      else { out += c; }
    }
    return out + "\"";
  }
  std::string hidden(const char *suffix) const
  {
    char buf[64];
    snprintf(buf, sizeof(buf), "__ai_%016llx_%s",
        static_cast<unsigned long long>(common::ob_crc64(name.data(), name.size())), suffix);
    return buf;
  }
  std::string json() const
  {
    std::string out = "{\"name\":" + json_string(name) + ",\"model\":" + json_string(model)
        + ",\"prompt\":" + json_string(prompt) + ",\"argument\":" + json_string(argument)
        + ",\"normalized_expr\":" + json_string(normalized_expr)
        + ",\"embedding\":" + (embedding ? "1" : "0")
        + ",\"retries\":" + std::to_string(retries)
        + ",\"dimension\":" + std::to_string(dimension)
        + ",\"result_type\":" + std::to_string(result_type) + ",\"dependencies\":[";
    for (size_t i = 0; i < dependencies.size(); ++i) {
      if (i != 0) { out += ','; }
      out += json_string(dependencies[i]);
    }
    return out + "]}";
  }
  int load(common::ObString text)
  {
    using namespace common;
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    ObIJsonBase *base = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, text,
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, base))) {
    } else if (base->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      auto *obj = static_cast<ObJsonObject *>(base);
      auto read_string = [&](const char *key, std::string &out) {
        ObJsonNode *v = obj->get_value(ObString(key));
        if (v == nullptr || v->json_type() != ObJsonNodeType::J_STRING) { ret = OB_INVALID_ARGUMENT; }
        else { out.assign(v->get_data(), v->get_data_length()); }
      };
      auto read_int = [&](const char *key, int64_t &out) {
        ObJsonNode *v = obj->get_value(ObString(key));
        if (v == nullptr || v->json_type() != ObJsonNodeType::J_INT) { ret = OB_INVALID_ARGUMENT; }
        else { out = v->get_int(); }
      };
      read_string("name", name); read_string("model", model); read_string("prompt", prompt);
      read_string("argument", argument); read_string("normalized_expr", normalized_expr);
      read_int("retries", retries); read_int("dimension", dimension);
      read_int("result_type", result_type);
      int64_t kind = 0; read_int("embedding", kind); embedding = kind != 0;
      ObJsonNode *deps = obj->get_value(ObString("dependencies"));
      dependencies.clear();
      if (deps == nullptr || deps->json_type() != ObJsonNodeType::J_ARRAY) { ret = OB_INVALID_ARGUMENT; }
      else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < deps->element_count(); ++i) {
          ObJsonNode *v = deps->get_value(i);
          if (v->json_type() != ObJsonNodeType::J_STRING) { ret = OB_INVALID_ARGUMENT; }
          else { dependencies.emplace_back(v->get_data(), v->get_data_length()); }
        }
      }
    }
    return ret;
  }

  int make_hidden(const ObColumnSchemaV2 &result, int which, ObColumnSchemaV2 &column) const
  {
    using namespace common;
    int ret = OB_SUCCESS;
    const char *suffixes[] = {"state", "request", "spec"};
    column.set_table_id(result.get_table_id());
    column.set_is_hidden(true);
    column.set_nullable(false);
    ObObj value;
    if (which == 0) { column.set_data_type(ObIntType); value.set_int(0); }
    else {
      column.set_data_type(ObVarcharType);
      column.set_data_length(which == 1 ? 64 : 16384);
      column.set_charset_type(CHARSET_UTF8MB4);
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    std::string spec = json();
    if (which == 1) { value.set_varchar(ObString("")); }
    if (which == 2) {
      column.add_column_flag(AI_RESULT_SPEC_COLUMN_FLAG);
      value.set_varchar(ObString(spec.size(), spec.data()));
    }
    if (which != 0) { value.set_collation_type(CS_TYPE_UTF8MB4_BIN); }
    std::string column_name = hidden(suffixes[which]);
    if (OB_FAIL(column.set_column_name(column_name.c_str()))) {
    } else if (OB_FAIL(column.set_cur_default_value(value, false))) {
    } else if (OB_FAIL(column.set_orig_default_value(value))) {
    }
    return ret;
  }
};

inline int persist_ai_column(common::ObISQLClient &client, uint64_t table_id,
                             const ObColumnSchemaV2 &spec)
{
  using namespace common;
  int ret = OB_SUCCESS;
  if ((spec.get_column_flags() & AI_RESULT_SPEC_COLUMN_FLAG) != 0) {
    AiColumnDefinition def;
    if (OB_FAIL(def.load(spec.get_cur_default_value().get_string()))) {
    } else {
      const std::string tid = std::to_string(table_id);
      const std::string table = "oceanbase.__all_column";
      auto col_id = [&](std::string name) {
        return "(SELECT column_id FROM " + table + " WHERE table_id=" + tid
            + " AND column_name=" + AiColumnDefinition::literal(name) + ")";
      };
      std::string sql = "REPLACE INTO oceanbase.__all_ai_column(table_id,column_id,column_name,"
          "state_column_id,request_column_id,result_spec_column_id,normalized_expr,definition,max_retries,definition_version) VALUES("
          + tid + "," + col_id(def.name) + "," + AiColumnDefinition::literal(def.name)
          + "," + col_id(def.hidden("state")) + "," + col_id(def.hidden("request"))
          + "," + std::to_string(spec.get_column_id()) + "," + AiColumnDefinition::literal(def.normalized_expr)
          + "," + AiColumnDefinition::literal(def.json()) + "," + std::to_string(def.retries)
          + ",(SELECT schema_version FROM " + table + " WHERE table_id=" + tid
          + " AND column_name=" + AiColumnDefinition::literal(def.name) + "))";
      int64_t affected = 0;
      ret = client.write(sql.c_str(), affected);
    }
  }
  return ret;
}

}}} // namespace oceanbase::share::schema
