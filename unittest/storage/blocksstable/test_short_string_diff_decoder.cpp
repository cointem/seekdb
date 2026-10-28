// Standalone regression for the actual production decode plan.
// c++ -std=c++17 -O2 -I src this_file.cpp -o /tmp/test_short_string_diff
#include "storage/blocksstable/encoding/ob_short_string_diff_decoder.h"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <random>
#include <vector>
#include <sys/mman.h>
#include <unistd.h>

using oceanbase::blocksstable::ShortStringDiffDecoder;

static void check(const int length, const uint32_t mask, const bool page_boundary)
{
  constexpr int rows = 7;
  std::mt19937 random(mask * 131 + length);
  std::array<char, 16> common{};
  std::array<uint8_t, 16> positions{};
  std::array<std::array<char, 16>, rows> original{};
  int diff_count = 0;
  for (int byte = 0; byte < length; ++byte) {
    if ((mask & (1u << byte)) != 0) {
      positions[diff_count++] = byte;
    } else {
      common[byte] = static_cast<char>(random());
    }
    for (int row = 0; row < rows; ++row) {
      original[row][byte] = (mask & (1u << byte)) != 0
          ? static_cast<char>(random()) : common[byte];
    }
  }
  std::vector<char> storage(std::max(1, rows * diff_count));
  char *source = storage.data();
  const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  char *mapping = nullptr;
  if (page_boundary) {
    mapping = static_cast<char *>(mmap(nullptr, page_size * 3, PROT_NONE,
                                     MAP_PRIVATE | MAP_ANON, -1, 0));
    assert(mapping != MAP_FAILED);
    assert(mprotect(mapping + page_size, page_size, PROT_READ | PROT_WRITE) == 0);
    source = mapping + 2 * page_size - rows * diff_count;
  }
  for (int row = 0; row < rows; ++row) {
    for (int i = 0; i < diff_count; ++i) {
      source[row * diff_count + i] = original[row][positions[i]];
    }
  }
  bool use_x86_simd = false;
#if defined(__x86_64__)
  use_x86_simd = __builtin_cpu_supports("ssse3");
#endif
  ShortStringDiffDecoder decoder(common.data(), positions.data(), diff_count, use_x86_simd);
  const int selection[] = {6, 0, 3, 6, 1, 5, 2, 4}; // sparse, repeated and descending row IDs
  for (const int row : selection) {
    std::array<char, 48> output;
    std::array<char, 48> scalar;
    output.fill(0x5a);
    scalar.fill(0x5a);
    const char *cell = source + row * diff_count;
    decoder.decode(cell, (rows - row) * diff_count, output.data() + 16);
    decoder.decode_scalar(cell, scalar.data() + 16);
    assert(output == scalar);
    assert(std::equal(original[row].begin(), original[row].end(), output.begin() + 16));
    for (int i = 0; i < 16; ++i) {
      assert(output[i] == 0x5a && output[i + 32] == 0x5a);
    }
  }
  if (mapping != nullptr) {
    assert(munmap(mapping, page_size * 3) == 0);
  }
}

int main()
{
  int cases = 0;
  for (int length = 1; length <= 16; ++length) {
    for (uint32_t mask = 0; mask < (1u << length); ++mask) {
      check(length, mask, false);
      ++cases;
    }
    const uint32_t all = (1u << length) - 1;
    const uint32_t masks[] = {0, 1, 1u << (length - 1), all, all & 0x5555u, all & 0xaaaau};
    for (uint32_t mask : masks) {
      check(length, mask, true);
      ++cases;
    }
  }
  std::printf("PASS: %d exhaustive mask/width and guarded-page cases, scalar/native equality\n", cases);
}
