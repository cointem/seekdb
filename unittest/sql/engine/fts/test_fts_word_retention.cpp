// Focused regression for deferred-retention hashmap insertion. Uses the real
// ObHashMap overload, a counting hash and deterministic allocation failures.
#include "lib/hash/ob_hashmap.h"
#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <string>

using namespace oceanbase;
using namespace oceanbase::common;

namespace {
struct Key {
  const char *text_ = nullptr;
  int64_t size_ = 0;
};
struct CountingHash {
  static int calls_;
  int operator()(const Key &key, uint64_t &value) const
  {
    ++calls_;
    value = 0;
    for (int64_t i = 0; i < key.size_; ++i) {
      value = value * 131 + std::tolower(static_cast<unsigned char>(key.text_[i]));
    }
    value = 0; // Force collisions to exercise non-head hits and insertion order.
    return OB_SUCCESS;
  }
};
int CountingHash::calls_ = 0;
struct Equal {
  bool operator()(const Key &a, const Key &b) const
  {
    if (a.size_ != b.size_) { return false; }
    for (int64_t i = 0; i < a.size_; ++i) {
      if (std::tolower(static_cast<unsigned char>(a.text_[i]))
          != std::tolower(static_cast<unsigned char>(b.text_[i]))) { return false; }
    }
    return true;
  }
};
using Node = hash::HashMapTypes<Key, int64_t>::AllocType;
struct Nodes {
  bool fail_ = false;
  void set_attr(const ObMemAttr &) {}
  Node *alloc()
  {
    void *memory = fail_ ? nullptr : std::malloc(sizeof(Node));
    return nullptr == memory ? nullptr : new (memory) Node;
  }
  void free(Node *node) { node->~Node(); std::free(node); }
};
struct Buckets {
  explicit Buckets(const lib::ObLabel & = ObModIds::OB_HASH_BUCKET) {}
  void set_attr(const ObMemAttr &) {}
  void *alloc(int64_t size) { return std::malloc(size); }
  void free(void *pointer) { std::free(pointer); }
};
using Map = hash::ObHashMap<Key, int64_t, hash::NoPthreadDefendMode,
    CountingHash, Equal, Nodes, hash::NormalPointer, Buckets>;

void check()
{
  Map map;
  assert(map.create(3, "RetainTest") == OB_SUCCESS);
  std::deque<std::string> strings;
  bool fail_retention = false;
  int retention_calls = 0;
  int prepare_calls = 0;
  auto retain = [&](const Key &source, Key &owned) -> int {
    ++retention_calls;
    if (fail_retention) { return OB_ALLOCATE_MEMORY_FAILED; }
    strings.emplace_back(source.text_, source.size_);
    owned = Key{strings.back().data(), static_cast<int64_t>(strings.back().size())};
    return OB_SUCCESS;
  };
  auto add = [&](char *text) {
    Key key{text, static_cast<int64_t>(std::strlen(text))};
    auto update = [&](auto &entry) -> int {
      if (entry.first.size_ != key.size_
          || std::memcmp(entry.first.text_, key.text_, key.size_) != 0) {
        Key owned;
        const int ret = retain(key, owned);
        if (ret != OB_SUCCESS) { return ret; }
        entry.first = owned;
      }
      ++entry.second;
      return OB_SUCCESS;
    };
    auto prepare = [&](auto &entry) -> int {
      ++prepare_calls;
      Key owned;
      const int ret = retain(key, owned);
      if (ret == OB_SUCCESS) { entry.first = owned; }
      return ret;
    };
    const int hashes_before = CountingHash::calls_;
    const int ret = map.set_or_update(key, 1, update, prepare);
    assert(CountingHash::calls_ == hashes_before + 1);
    return ret;
  };

  char first[] = "alpha";
  assert(add(first) == OB_SUCCESS && map.size() == 1);
  const auto *first_node = &*map.begin();
  const char *first_storage = first_node->first.text_;
  first[0] = 'X'; // The published key must already own its bytes.
  assert(std::string(first_storage, 5) == "alpha");
  char same[] = "alpha";
  assert(add(same) == OB_SUCCESS && map.size() == 1);
  assert(&*map.begin() == first_node && map.begin()->second == 2);
  assert(map.begin()->first.text_ == first_storage && retention_calls == 1 && prepare_calls == 1);

  char spelling[] = "ALPHA";
  fail_retention = true;
  assert(add(spelling) == OB_ALLOCATE_MEMORY_FAILED);
  assert(map.size() == 1 && &*map.begin() == first_node && map.begin()->second == 2);
  assert(map.begin()->first.text_ == first_storage); // Existing failure is not swallowed.
  fail_retention = false;
  assert(add(spelling) == OB_SUCCESS && map.begin()->second == 3);
  assert(&*map.begin() == first_node && std::string(map.begin()->first.text_, 5) == "ALPHA");
  spelling[0] = 'X';
  assert(std::string(map.begin()->first.text_, 5) == "ALPHA");

  char missing[] = "beta";
  const int prepared_before = prepare_calls;
  fail_retention = true;
  assert(add(missing) == OB_ALLOCATE_MEMORY_FAILED && map.size() == 1);
  assert(prepare_calls == prepared_before + 1);
  fail_retention = false;
  map.get_local_allocer().fail_ = true;
  assert(add(missing) == OB_ALLOCATE_MEMORY_FAILED && map.size() == 1);
  map.get_local_allocer().fail_ = false;
  assert(add(missing) == OB_SUCCESS && map.size() == 2);
  char tail_spelling[] = "alpha";
  assert(add(tail_spelling) == OB_SUCCESS && map.size() == 2);
  auto ordered = map.begin();
  assert(std::string(ordered->first.text_, ordered->first.size_) == "beta");
  ++ordered;
  assert(&*ordered == first_node && ordered->second == 4);
  assert(std::string(ordered->first.text_, ordered->first.size_) == "alpha");
  // Every surviving key points to retained strings, including after failures.
  for (auto entry = map.begin(); entry != map.end(); ++entry) {
    assert(entry->first.text_ != same && entry->first.text_ != missing && entry->first.text_ != spelling);
  }
  map.destroy();
}
} // namespace

int main()
{
  check();
  std::puts("PASS: single hash, retained insertion, repeat hit, last spelling, prepare/existing/node OOM");
}
