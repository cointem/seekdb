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

#define USING_LOG_PREFIX STORAGE

#include "ob_text_daat_iter.h"

namespace oceanbase
{
namespace storage
{
int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
  } else if (nullptr != param.base_param_->relevance_expr_
      && OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  } else {
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
    const ObExpr *filter = iter_param_->filter_expr_;
    const ObExpr *hit = iter_param_->relevance_proj_expr_;
    // A small unscored union can merge document IDs directly. Only eliminate
    // the filter when it is exactly the truth test of the positive hit marker.
    const bool hit_filter = nullptr == filter || filter == hit
        || (T_OP_BOOL == filter->type_ && filter->arg_cnt_ == 1 && filter->args_[0] == hit);
    hit_union_enabled_ = nullptr == iter_param_->relevance_expr_
        && sql::NATURAL_LANGUAGE_MODE == mode_flag_ && !function_lookup_mode_
        && dim_iters_->count() <= 8 && hit_filter;
    if (hit_union_enabled_) {
      auto *funcs = ObDatumFuncs::get_basic_func(iter_param_->id_proj_expr_->datum_meta_.type_,
          iter_param_->id_proj_expr_->datum_meta_.cs_type_);
      hit_id_cmp_ = funcs->null_first_cmp_;
      if (OB_ISNULL(hit_id_cmp_)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObTextDaaTIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (!hit_union_enabled_) {
    ret = ObSRDaaTIterImpl::get_next_rows(capacity, count);
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (capacity == 0) {
    // Same empty-batch contract as the generic merger.
  } else if (capacity < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (iter_param_->limit_param_->is_valid()
      && output_row_cnt_ >= iter_param_->limit_param_->limit_) {
    ret = OB_ITER_END;
  } else {
    auto advance = [&](int64_t i) -> int {
      int rc = static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->get_next_row();
      if (OB_ITER_END == rc) {
        iter_domain_ids_[i] = nullptr;
        rc = OB_SUCCESS;
      } else if (OB_SUCCESS == rc) {
        rc = dim_iters_->at(i)->get_curr_id(iter_domain_ids_[i]);
      }
      return rc;
    };
    if (!hit_union_inited_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
        ret = advance(i);
      }
      hit_union_inited_ = OB_SUCC(ret);
    }
    const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
    while (OB_SUCC(ret) && count < real_capacity) {
      const ObDatum *smallest = nullptr;
      uint64_t winners = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
        const ObDatum *id = iter_domain_ids_[i];
        int cmp = 0;
        if (nullptr == id) {
        } else if (nullptr == smallest) {
          smallest = id;
          winners = 1ULL << i;
        } else if (OB_FAIL(hit_id_cmp_(*id, *smallest, cmp, nullptr))) {
        } else if (cmp < 0) {
          smallest = id;
          winners = 1ULL << i;
        } else if (cmp == 0) {
          winners |= 1ULL << i;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (nullptr == smallest) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(ObSRDaaTIterImpl::cache_result(count, *smallest, 1.0))) {
      } else {
        // Cache before advancing: a refill may overwrite the winning input.
        while (OB_SUCC(ret) && winners != 0) {
          const int64_t i = __builtin_ctzll(winners);
          winners &= winners - 1;
          ret = advance(i);
        }
      }
    }
    if ((OB_SUCC(ret) || OB_ITER_END == ret) && count > 0) {
      int project_ret = ObSRDaaTIterImpl::project_results(count);
      if (OB_SUCCESS != project_ret) {
        ret = project_ret;
      }
    }
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  hit_union_enabled_ = false;
  hit_union_inited_ = false;
  hit_id_cmp_ = nullptr;
  bm25_param_estimator_.reset();
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse(const bool switch_tablet)
{
  hit_union_inited_ = false;
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count())) {
    // do nothing
  } else {
    bm25_param_estimator_.reuse(switch_tablet);
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (nullptr != iter_param_->relevance_expr_) {
    if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
    }
  }
  return ret;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    bm25_param_estimator_() {}

void ObTextBMWIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse(switch_tablet);
}

void ObTextBMWIter::reset()
{
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bm25_param_estimator_.is_estimated())) {
    ret = OB_ERR_UNEXPECTED;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter(
        bm25_param_estimator_.get_total_doc_cnt(), bm25_param_estimator_.get_avg_doc_token_cnt()))) {
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
