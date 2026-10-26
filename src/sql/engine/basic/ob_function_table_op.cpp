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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include "common/json_type/ob_json_tree.h"
#include "common/json_type/ob_json_base.h"
#include "file_sql_bridge.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_, column_ids_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObVarcharType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_file;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    if (file_scan_ != nullptr) {
      try {
        (*static_cast<rust::Box<file_sql::FileScan> *>(file_scan_))->rescan();
      } catch (const rust::Error &error) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
      }
    }
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  close_file();
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  close_file();
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

void ObFunctionTableOp::close_file()
{
  using FileBox = rust::Box<file_sql::FileScan>;
  if (file_scan_ != nullptr) {
    static_cast<FileBox *>(file_scan_)->~FileBox();
    ctx_.get_allocator().free(file_scan_);
    file_scan_ = nullptr;
  }
  file_row_allocator_.reset();
}

int ObFunctionTableOp::inner_get_next_row_file()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  file_row_allocator_.reuse();
  using FileBox = rust::Box<file_sql::FileScan>;
  bool enabled = false;
  ObString secure_root;
  if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (!enabled) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_SECURE_FILE_PRIV, secure_root))) {
  } else {
    try {
      if (file_scan_ == nullptr) {
        ObDatum *argument = nullptr;
        if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, argument))) {
        } else {
          const ObString binding = argument->get_string();
          ObSqlString projection;
          ret = projection.append("[");
          for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_ids_.count(); ++i) {
            ret = projection.append_fmt("%s%lu", i == 0 ? "" : ",", MY_SPEC.column_ids_.at(i) - OB_APP_MIN_COLUMN_ID);
          }
          if (OB_SUCC(ret)) { ret = projection.append("]"); }
          if (OB_SUCC(ret)) {
            auto scan = file_sql::open_projected_file(rust::Str(binding.ptr(), binding.length()),
                rust::Str(secure_root.empty() ? "" : secure_root.ptr(), secure_root.length()),
                rust::Str(projection.ptr(), projection.length()));
            file_scan_ = OB_NEWx(FileBox, &ctx_.get_allocator(), std::move(scan));
            if (file_scan_ == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
          }
        }
      }
      if (OB_SUCC(ret)) {
        rust::String row = (*static_cast<FileBox *>(file_scan_))->next_row();
        ObString data;
        ObIJsonBase *json = nullptr;
        if (row.empty()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(ob_write_string(file_row_allocator_, ObString(row.size(), row.data()), data))) {
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&file_row_allocator_, data,
                      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json))) {
        } else if (json == nullptr || json->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          auto *values = static_cast<ObJsonArray *>(json);
          for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
            const uint64_t index = MY_SPEC.column_ids_.at(i) - OB_APP_MIN_COLUMN_ID;
            ObExpr *expr = MY_SPEC.column_exprs_.at(i);
            ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
            ObObj source;
            if (index >= values->element_count()) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              ObJsonNode *value = values->get_value(index);
              switch (value->json_type()) {
                case ObJsonNodeType::J_NULL: source.set_null(); break;
                case ObJsonNodeType::J_BOOLEAN: source.set_tinyint(value->get_boolean()); break;
                case ObJsonNodeType::J_INT: source.set_int(value->get_int()); break;
                case ObJsonNodeType::J_UINT: source.set_uint64(value->get_uint()); break;
                case ObJsonNodeType::J_DOUBLE: source.set_double(value->get_double()); break;
                case ObJsonNodeType::J_DECIMAL: source.set_number(value->get_decimal_data()); break;
                case ObJsonNodeType::J_STRING:
                  source.set_varchar(static_cast<ObJsonString *>(value)->get_str());
                  source.set_collation_type(CS_TYPE_UTF8MB4_BIN);
                  break;
                default: ret = OB_ERR_UNEXPECTED; break;
              }
            }
            if (OB_SUCC(ret)) {
              ObObj converted;
              const ObDataTypeCastParams params = ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session());
              ObAccuracy accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[expr->datum_meta_.type_];
              ObCastCtx cast(&file_row_allocator_, &params, CM_NONE, expr->obj_meta_.get_collation_type(), &accuracy);
              ObSqlObjCastRuntime runtime(&ctx_);
              cast.runtime_ = &runtime;
              if (source.is_null()) {
                datum.set_null();
              } else if (OB_FAIL(ObObjCaster::to_type(expr->datum_meta_.type_,
                        expr->obj_meta_.get_collation_type(), cast, source, converted))) {
              } else if (OB_FAIL(datum.from_obj(converted, expr->obj_datum_map_))) {
              }
              if (OB_SUCC(ret)) {
                expr->set_evaluated_projected(eval_ctx_);
              }
            }
          }
        }
      }
    } catch (const rust::Error &error) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
