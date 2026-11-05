#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>
#include <stdexcept>

namespace lob {
template <typename T>
class MemoryPool {
public:
    explicit MemoryPool(std::size_t blocks) : storage_(blocks) {
        free_.reserve(blocks);
        for (std::size_t i = blocks; i-- > 0;) free_.push_back(i);
    }
    T* allocate() {
        if (free_.empty()) return nullptr;
        std::size_t i = free_.back();
        free_.pop_back();
        return &storage_[i];
    }
    void release(T* ptr) {
        if (!ptr) return;
        auto index = static_cast<std::size_t>(ptr - storage_.data());
        if (index >= storage_.size()) throw std::runtime_error("pointer is not from this pool");
        free_.push_back(index);
    }
    std::size_t available() const { return free_.size(); }
    std::size_t capacity() const { return storage_.size(); }
private:
    std::vector<T> storage_;
    std::vector<std::size_t> free_;
};
}
