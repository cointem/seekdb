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
#include <cmath>
#include <limits>

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
    count_only_ = param.count_only_;
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
      const ObDatumMeta &id_meta = iter_param_->id_proj_expr_->datum_meta_;
      count_binary_ids_ = common::ObVarcharType == id_meta.type_ && CS_TYPE_BINARY == id_meta.cs_type_;
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
  if (count_only_ && hit_union_enabled_) {
    ret = get_next_count_rows(capacity, count);
  } else if (!hit_union_enabled_) {
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
      int rc = static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->next_doc_id(iter_domain_ids_[i], MIN(capacity, iter_param_->max_batch_size_));
      if (OB_ITER_END == rc) {
        iter_domain_ids_[i] = nullptr;
        rc = OB_SUCCESS;
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

int ObTextDaaTIter::compare_count_ids(const ObDatum &left, const ObDatum &right, int &cmp) const
{
  int ret = OB_SUCCESS;
  if (count_binary_ids_ && !left.is_null() && !right.is_null()
      && left.get_string().length() == 16 && right.get_string().length() == 16) {
    // TABLET_SEQUENCE IDs are stored as native bytes, but ordered by binary
    // collation. Comparing decoded uint64 values would change that ordering.
    cmp = MEMCMP(left.ptr_, right.ptr_, 16);
  } else {
    ret = hit_id_cmp_(left, right, cmp, nullptr);
  }
  return ret;
}

int ObTextDaaTIter::get_next_count_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  const common::ObLimitParam &limit = *iter_param_->limit_param_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (capacity < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (capacity == 0) {
  } else if (limit.is_valid() && output_row_cnt_ >= limit.limit_) {
    ret = OB_ITER_END;
  } else {
    const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
    while (OB_SUCC(ret) && count < real_capacity) {
      const ObDocIdExt *runs[8] = {};
      int64_t sizes[8] = {};
      int64_t first = -1;
      uint64_t winners = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
        auto *iter = static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i));
        if ((count_exhausted_ & (1ULL << i)) != 0) {
        } else if (OB_FAIL(iter->peek_doc_id_run(runs[i], sizes[i], real_capacity))) {
          if (OB_ITER_END == ret) {
            count_exhausted_ |= 1ULL << i;
            ret = OB_SUCCESS;
          }
        } else {
          int cmp = -1;
          if (first >= 0 && OB_FAIL(compare_count_ids(runs[i][0].get_datum(), runs[first][0].get_datum(), cmp))) {
          } else if (first < 0 || cmp < 0) {
            first = i;
            winners = 1ULL << i;
          } else if (cmp == 0) {
            winners |= 1ULL << i;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (first < 0) {
        ret = OB_ITER_END;
      } else {
        // With one winning stream, every ID strictly below the next stream's
        // head is unique to it. Consume that whole sorted run without per-hit
        // winner searches, result ID copies, score markers, or output projections.
        int64_t run_length = 1;
        if (0 == (winners & (winners - 1))) {
          const ObDatum *boundary = nullptr;
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
            if (i != first && sizes[i] > 0) {
              const ObDatum &head = runs[i][0].get_datum();
              int cmp = -1;
              if (nullptr != boundary && OB_FAIL(compare_count_ids(head, *boundary, cmp))) {
              } else if (nullptr == boundary || cmp < 0) {
                boundary = &head;
              }
            }
          }
          run_length = sizes[first];
          if (OB_SUCC(ret) && nullptr != boundary) {
            // Gallop from the head before binary search: interleaved streams
            // usually have a run of one, and must not pay log(batch_size)
            // comparisons on every hit.
            int64_t low = 1;
            int64_t high = MIN(2L, run_length);
            while (OB_SUCC(ret) && low < run_length) {
              const int64_t probe = high - 1;
              int cmp = 0;
              if (OB_FAIL(compare_count_ids(runs[first][probe].get_datum(), *boundary, cmp))) {
              } else if (cmp >= 0) {
                high = probe;
                break;
              } else {
                low = probe + 1;
                high = MIN(run_length, high * 2);
              }
            }
            while (OB_SUCC(ret) && low < high) {
              const int64_t mid = low + (high - low) / 2;
              int cmp = 0;
              if (OB_FAIL(compare_count_ids(runs[first][mid].get_datum(), *boundary, cmp))) {
              } else if (cmp < 0) {
                low = mid + 1;
              } else {
                high = mid;
              }
            }
            run_length = low;
          }
        } else {
          // All live streams may share an entire prefix. Count it once and
          // move all cursors together, instead of repeating the winner search
          // for each duplicate hit. A nonwinning head forbids this shortcut.
          bool all_equal = true;
          int64_t common_size = sizes[first];
          for (int64_t i = 0; i < dim_iters_->count(); ++i) {
            if (sizes[i] > 0) {
              all_equal = all_equal && (winners & (1ULL << i)) != 0;
              common_size = MIN(common_size, sizes[i]);
            }
          }
          int64_t demand = real_capacity - count;
          if (limit.is_valid() && input_row_cnt_ < limit.offset_) {
            demand += MIN(common_size, limit.offset_ - input_row_cnt_);
          }
          common_size = MIN(common_size, demand);
          for (int64_t j = 1; OB_SUCC(ret) && all_equal && j < common_size; ++j) {
            for (int64_t i = 0; OB_SUCC(ret) && all_equal && i < dim_iters_->count(); ++i) {
              if (i != first && sizes[i] > 0) {
                int cmp = 0;
                if (OB_FAIL(compare_count_ids(runs[first][j].get_datum(), runs[i][j].get_datum(), cmp))) {
                } else {
                  all_equal = cmp == 0;
                }
              }
            }
            if (OB_SUCC(ret) && all_equal) {
              ++run_length;
            }
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t skip = limit.is_valid() && input_row_cnt_ < limit.offset_
              ? MIN(run_length, limit.offset_ - input_row_cnt_) : 0;
          int64_t emit = MIN(run_length - skip, real_capacity - count);
          if (limit.is_valid()) {
            emit = MIN(emit, limit.limit_ - output_row_cnt_);
          }
          const int64_t consumed = skip + emit;
          // Each (token, docID) key is unique after the normal storage MVCC
          // scan. Equal heads across different tokens contribute only once.
          while (winners != 0) {
            const int64_t i = __builtin_ctzll(winners);
            winners &= winners - 1;
            static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->consume_doc_id_run(consumed);
          }
          input_row_cnt_ += consumed;
          output_row_cnt_ += emit;
          count += emit;
          if (limit.is_valid() && output_row_cnt_ >= limit.limit_) {
            ret = OB_ITER_END;
          }
        }
      }
    }
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  count_only_ = false;
  count_binary_ids_ = false;
  count_exhausted_ = 0;
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
  count_exhausted_ = 0;
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
    bm25_param_estimator_(), dense_top_k_enabled_(false) {}

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
  dense_top_k_enabled_ = false;
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
  } else {
    const ObExpr *filter = iter_param_->filter_expr_;
    const ObExpr *hit = iter_param_->relevance_proj_expr_;
    const bool hit_filter = nullptr == filter || filter == hit
        || (T_OP_BOOL == filter->type_ && filter->arg_cnt_ == 1 && filter->args_[0] == hit);
    dense_top_k_enabled_ = sql::NATURAL_LANGUAGE_MODE == param.mode_flag_
        && !param.function_lookup_mode_ && nullptr == iter_param_->dim_weights_
        && iter_param_->field_boost_ == 1.0 && dim_iters_->count() > 1
        && dim_iters_->count() <= 8 && hit_filter;
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

int ObTextBMWIter::try_dense_top_k_search(bool &handled)
{
  int ret = OB_SUCCESS;
  handled = false;
  if (!dense_top_k_enabled_) {
    return ret;
  }
  // MAXSCORE partitions terms by their cumulative upper bounds. Only essential
  // terms generate candidates; the others are sought and scored on demand.
  // Small term sets can do this directly, without rebuilding a merge heap for
  // every candidate. Posting decoding and BM25 evaluation remain batched below.
  const int64_t dim_count = dim_iters_->count();
  int64_t order[8];
  double bounds[8];
  double prefix[8];
  const ObDatum *ids[8] = {};
  ObTextRetrievalBlockMaxIter *iters[8];
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_count; ++i) {
    order[i] = i;
    iters[i] = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(iters[i]->get_dim_max_score(bounds[i]))) {
    } else if (!std::isfinite(bounds[i]) || bounds[i] < 0.0) {
      // MAXSCORE requires nonnegative finite upper bounds. Keep BMW otherwise.
      return OB_SUCCESS;
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < dim_count; ++i) {
    const int64_t idx = order[i];
    int64_t j = i;
    while (j > 0 && bounds[order[j - 1]] > bounds[idx]) {
      order[j] = order[j - 1];
      --j;
    }
    order[j] = idx;
  }
  double sum = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_count; ++i) {
    sum = std::nextafter(sum + bounds[order[i]], std::numeric_limits<double>::infinity());
    prefix[i] = sum;
  }
  // Transfer only live iterators. Exhausted dimensions have no current row.
  while (OB_SUCC(ret) && !merge_heap_->empty()) {
    const ObSRMergeItem *item = nullptr;
    if (OB_FAIL(merge_heap_->top(item))) {
    } else if (OB_FAIL(iters[item->iter_idx_]->get_curr_id(ids[item->iter_idx_]))) {
    } else if (OB_FAIL(merge_heap_->pop())) {
    }
  }
  handled = OB_SUCC(ret);
  LOG_DEBUG("switch text retrieval from BMW to MAXSCORE", K(ret), K(dim_count), K(top_k_count_));
  int64_t essential_begin = 0;
  // Retain a conservative margin for differences in floating point sum order.
  const double margin = 16 * std::numeric_limits<double>::epsilon() * std::max(1.0, sum);
  while (OB_SUCC(ret)) {
    const double threshold = get_top_k_threshold();
    while (essential_begin < dim_count && prefix[essential_begin] + margin < threshold) {
      ++essential_begin;
    }
    const ObDatum *candidate = nullptr;
    uint64_t winners = 0;
    for (int64_t j = essential_begin; OB_SUCC(ret) && j < dim_count; ++j) {
      const int64_t i = order[j];
      int cmp = 0;
      if (nullptr == ids[i]) {
      } else if (nullptr == candidate) {
        candidate = ids[i];
        winners = 1ULL << i;
      } else if (OB_FAIL(domain_id_cmp_.compare(*ids[i], *candidate, cmp))) {
      } else if (cmp < 0) {
        candidate = ids[i];
        winners = 1ULL << i;
      } else if (cmp == 0) {
        winners |= 1ULL << i;
      }
    }
    if (OB_FAIL(ret) || nullptr == candidate) {
      break;
    }
    double score = 0.0;
    for (uint64_t pending = winners; OB_SUCC(ret) && pending != 0; pending &= pending - 1) {
      double term_score = 0.0;
      if (OB_FAIL(iters[__builtin_ctzll(pending)]->get_curr_score(term_score))) {
      } else {
        score += term_score;
      }
    }
    bool competitive = true;
    for (int64_t j = essential_begin - 1; OB_SUCC(ret) && competitive && j >= 0; --j) {
      const int64_t i = order[j];
      int cmp = 0;
      if (score + prefix[j] + margin < threshold) {
        competitive = false;
      } else if (nullptr == ids[i]) {
      } else if (OB_FAIL(iters[i]->advance_to(*candidate))) {
        if (OB_ITER_END == ret) {
          ids[i] = nullptr;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iters[i]->get_curr_id(ids[i]))) {
      } else if (OB_FAIL(domain_id_cmp_.compare(*ids[i], *candidate, cmp))) {
      } else if (cmp == 0) {
        double term_score = 0.0;
        if (OB_FAIL(iters[i]->get_curr_score(term_score))) {
        } else {
          score += term_score;
        }
      }
    }
    if (OB_SUCC(ret) && competitive) {
      ret = process_collected_row(*candidate, score);
    }
    // The candidate ID belongs to an input batch; cache it before refilling.
    while (OB_SUCC(ret) && winners != 0) {
      const int64_t i = __builtin_ctzll(winners);
      winners &= winners - 1;
      if (OB_FAIL(iters[i]->get_next_row())) {
        if (OB_ITER_END == ret) {
          ids[i] = nullptr;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iters[i]->get_curr_id(ids[i]))) {
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
