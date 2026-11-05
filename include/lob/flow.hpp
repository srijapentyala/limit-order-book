#pragma once
#include "lob/types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

namespace lob {

struct FlowEvent {
    bool cancel = false;
    Order order;
};

struct FlowConfig {
    std::size_t events = 1000000;
    uint64_t seed = 42;
    uint64_t idBase = 0;
    uint32_t midPrice = 100000;
    int passivePct = 50;     // limit orders that usually rest
    int cancelPct = 40;      // cancels of an earlier order, which may already be gone
    int aggressivePct = 10;  // limit orders priced through the touch
    uint32_t bandTicks = 32;
    uint32_t maxQty = 100;
};

// Deterministic synthetic order flow. It is shaped like a busy book (mostly passive
// adds and cancels near a wandering mid) but is not derived from real market data.
inline std::vector<FlowEvent> makeFlow(const FlowConfig& cfg) {
    std::mt19937_64 rng(cfg.seed);
    std::vector<FlowEvent> events;
    events.reserve(cfg.events);
    std::vector<uint64_t> live;
    live.reserve(cfg.events / 2);
    int64_t mid = cfg.midPrice;
    uint64_t nextId = cfg.idBase + 1;

    for (std::size_t i = 0; i < cfg.events; ++i) {
        if (i % 64 == 0) mid += static_cast<int64_t>(rng() % 3) - 1;
        const int roll = static_cast<int>(rng() % 100);
        FlowEvent event;
        if (roll < cfg.cancelPct && !live.empty()) {
            const std::size_t pick = rng() % live.size();
            event.cancel = true;
            event.order.id = live[pick];
            live[pick] = live.back();
            live.pop_back();
        } else {
            const bool buy = (rng() & 1) != 0;
            const bool aggressive = roll >= cfg.cancelPct + cfg.passivePct;
            const auto offset = static_cast<int64_t>(1 + rng() % cfg.bandTicks);
            const int64_t price = (buy != aggressive) ? mid - offset : mid + offset;
            event.order.id = nextId++;
            event.order.price = static_cast<uint32_t>(std::max<int64_t>(price, 1));
            event.order.qty = static_cast<uint32_t>(1 + rng() % cfg.maxQty);
            event.order.buy = buy;
            live.push_back(event.order.id);
        }
        events.push_back(event);
    }
    return events;
}

// Nearest-rank percentile of an ascending vector.
template <typename T>
T percentile(const std::vector<T>& sorted, double q) {
    if (sorted.empty()) return T{};
    auto rank = static_cast<std::size_t>(q / 100.0 * static_cast<double>(sorted.size()));
    return sorted[std::min(rank, sorted.size() - 1)];
}

}  // namespace lob
