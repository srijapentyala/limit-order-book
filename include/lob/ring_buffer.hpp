#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <thread>

#if defined(__APPLE__)
#include <pthread/qos.h>
#endif

namespace lob {

#if defined(__APPLE__) && defined(__aarch64__)
inline constexpr std::size_t kCacheLine = 128;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

inline void cpuRelax() {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#else
    std::this_thread::yield();
#endif
}

// macOS has no core pinning, but this QoS class steers a thread to the performance cores.
// Only the in-process benchmark uses it. On the socket path it made tail latency worse,
// because spinning threads at this priority compete with the threads the kernel wakes.
inline void preferFastCore() {
#if defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

// Single-producer single-consumer ring. Exactly one thread may push and exactly
// one thread may pop. Each side keeps a cached copy of the other side's index, so
// it only touches the shared cache line when the cached value says full or empty.
template <typename T, std::size_t Capacity>
class SpscRing {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0, "capacity must be a power of two");

public:
    SpscRing() : slots_(std::make_unique<T[]>(Capacity)) {}
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    bool push(const T& item) {
        const auto tail = tail_.load(std::memory_order_relaxed);
        const auto next = (tail + 1) & kMask;
        if (next == cachedHead_) {
            cachedHead_ = head_.load(std::memory_order_acquire);
            if (next == cachedHead_) return false;
        }
        slots_[tail] = item;
        tail_.store(next, std::memory_order_release);
        return true;
    }

    std::optional<T> pop() {
        const auto head = head_.load(std::memory_order_relaxed);
        if (head == cachedTail_) {
            cachedTail_ = tail_.load(std::memory_order_acquire);
            if (head == cachedTail_) return std::nullopt;
        }
        T item = slots_[head];
        head_.store((head + 1) & kMask, std::memory_order_release);
        return item;
    }

    // Hands up to `limit` items to `fn` and publishes the new head once, so a burst
    // costs one store to the shared line rather than one per item.
    template <typename Fn>
    std::size_t consume(std::size_t limit, Fn&& fn) {
        auto head = head_.load(std::memory_order_relaxed);
        if (head == cachedTail_) {
            cachedTail_ = tail_.load(std::memory_order_acquire);
            if (head == cachedTail_) return 0;
        }
        std::size_t done = 0;
        while (done < limit && head != cachedTail_) {
            fn(slots_[head]);
            head = (head + 1) & kMask;
            ++done;
        }
        head_.store(head, std::memory_order_release);
        return done;
    }

    std::size_t capacity() const { return Capacity - 1; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    std::size_t cachedTail_ = 0;  // consumer only
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    std::size_t cachedHead_ = 0;  // producer only
    alignas(kCacheLine) std::unique_ptr<T[]> slots_;
};

}  // namespace lob
