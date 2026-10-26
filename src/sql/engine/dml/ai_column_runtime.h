#pragma once

#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase { namespace sql {
class ObTableModifyOp;
class ObExecContext;
class ObPhysicalPlan;
class AiColumnRuntime
{
public:
  static bool needs_after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &definition);
  static int after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &definition, ObDmlEventType event);
  static int check_read(ObExecContext &ctx, const ObPhysicalPlan &plan);
};
}}
