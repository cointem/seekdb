#pragma once

#include "share/ob_thread_pool.h"

namespace oceanbase { namespace observer {
class AiColumnWorker : public share::ObThreadPool
{
public:
  static AiColumnWorker &instance();
  void run1() override;
private:
  int process_tasks();
  int publish_groups();
};
}}
