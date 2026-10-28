// Standalone differential test of the production pointer radix sorter.
// clang++ -std=c++17 -I src this_file.cpp -o /tmp/test_fts_radix
#include "sql/engine/sort/fts_byte_radix_sort.h"
#include <cassert>
#include <cstdio>
#include <numeric>
#include <random>
#include <string>
#include <vector>

struct Row { std::string value; int id; };
struct Key { const char *ptr_; int64_t len_; };
static const auto key_of = [](const Row *row) {
  return Key{row->value.data(), static_cast<int64_t>(row->value.size())};
};

static void check(int size, int mode, unsigned seed, int cancel_after = -1)
{
  std::mt19937 rng(seed);
  std::vector<Row> storage;
  storage.reserve(size);
  for (int i = 0; i < size; ++i) {
    std::string text;
    if (mode == 1) { text = std::string(i % 4, static_cast<char>(i % 3)); }
    else if (mode == 2) { text = std::string(i % 70, 'a'); }
    else if (mode == 3) { text = std::string(300, '\0') + std::to_string(rng() % 1000); }
    else if (mode == 4) { text = std::string(80, static_cast<char>(255)); }
    else if (mode == 7) { text = std::string(i % 40, 'a') + 'b' + std::to_string(i); }
    else {
      text.resize(rng() % 96);
      for (char &c : text) { c = static_cast<char>(rng() % 256); }
    }
    storage.push_back({text, i});
  }
  std::vector<Row *> rows;
  for (Row &row : storage) { rows.push_back(&row); }
  const auto less = [](const Row *a, const Row *b) { return a->value < b->value; };
  if (mode == 5 || mode == 6) {
    std::sort(rows.begin(), rows.end(), less);
    if (mode == 6) { std::reverse(rows.begin(), rows.end()); }
  }
  auto expected = rows;
  std::sort(expected.begin(), expected.end(), less);
  int checks = 0;
  const int ret = oceanbase::sql::fts_byte_radix_sort(rows.data(), rows.size(), key_of, [&] {
    return cancel_after >= 0 && ++checks >= cancel_after ? -17 : 0;
  });
  if (cancel_after < 0) {
    assert(ret == 0);
    for (int i = 0; i < size; ++i) { assert(rows[i]->value == expected[i]->value); }
  } else { assert(ret == -17); }
  std::vector<int> ids;
  for (const Row *row : rows) { ids.push_back(row->id); }
  std::sort(ids.begin(), ids.end());
  for (int i = 0; i < size; ++i) { assert(ids[i] == i); }
}

int main()
{
  int cases = 0;
  for (int size : {0, 1, 2, 63, 64, 65, 4095, 4096, 4097, 16384}) {
    for (int mode = 0; mode < 8; ++mode) { check(size, mode, size * 131 + mode); ++cases; }
  }
  for (unsigned seed = 0; seed < 200; ++seed) { check(64 + seed * 19, 0, seed); ++cases; }
  for (int limit = 1; limit <= 12; ++limit) { check(16384, 0, 73, limit); ++cases; }
  std::printf("PASS %d radix differential/permutation/cancellation cases\n", cases);
}
