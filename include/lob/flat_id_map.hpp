#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

namespace lob {

// Open-addressing map from a non-zero order id to a value. All memory is allocated
// up front, so inserts on the matching path never call the allocator. Linear probing
// with backward-shift deletion keeps probe chains short without tombstones.
template <typename V>
class FlatIdMap {
public:
    explicit FlatIdMap(std::size_t maxEntries) {
        std::size_t size = 16;
        while (size < maxEntries * 2) size <<= 1;
        slots_.resize(size);
        mask_ = size - 1;
    }

    V* find(uint64_t key) {
        for (std::size_t i = indexOf(key);; i = (i + 1) & mask_) {
            if (slots_[i].key == key) return &slots_[i].value;
            if (slots_[i].key == 0) return nullptr;
        }
    }
    bool contains(uint64_t key) { return find(key) != nullptr; }

    // The caller guarantees at most maxEntries live keys and that key is absent.
    void insert(uint64_t key, V value) {
        std::size_t i = indexOf(key);
        while (slots_[i].key != 0) i = (i + 1) & mask_;
        slots_[i] = Slot{key, value};
        ++size_;
    }

    bool erase(uint64_t key) {
        std::size_t i = indexOf(key);
        for (;; i = (i + 1) & mask_) {
            if (slots_[i].key == key) break;
            if (slots_[i].key == 0) return false;
        }
        std::size_t hole = i;
        for (std::size_t j = (hole + 1) & mask_; slots_[j].key != 0; j = (j + 1) & mask_) {
            const std::size_t home = indexOf(slots_[j].key);
            // Move j back only if its home does not lie cyclically in (hole, j].
            if (((j - home) & mask_) >= ((j - hole) & mask_)) {
                slots_[hole] = slots_[j];
                hole = j;
            }
        }
        slots_[hole] = Slot{};
        --size_;
        return true;
    }

    std::size_t size() const { return size_; }

private:
    struct Slot {
        uint64_t key = 0;
        V value{};
    };

    std::size_t indexOf(uint64_t key) const {
        return static_cast<std::size_t>((key * 0x9E3779B97F4A7C15ULL) >> 20) & mask_;
    }

    std::vector<Slot> slots_;
    std::size_t mask_ = 0;
    std::size_t size_ = 0;
};

}  // namespace lob
