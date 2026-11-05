#pragma once
#include "lob/codec.hpp"
#include "lob/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

namespace lob {

// Binary market-data feed. Every book change is one fixed 48-byte little-endian frame:
//
//   offset size field
//        0    8 seq        increases by exactly one per frame
//        8    8 tsNs       steady_clock time at the matcher; same-host comparison only
//       16    8 id         order id (the taker, for a trade)
//       24    8 makerId    trades only
//       32    4 price
//       36    4 qty        add: resting qty, delete: removed qty, trade: fill qty
//       40    4 remaining  trades only: what the maker has left
//       44    1 type       FeedType
//       45    1 side       1 = buy. For a trade, the taker's side.
//       46    2 reserved
enum class FeedType : uint8_t { Add = 1, Delete = 2, Trade = 3 };

constexpr std::size_t kFeedFrameSize = 48;

struct FeedFrame {
    uint64_t seq = 0;
    uint64_t tsNs = 0;
    uint64_t id = 0;
    uint64_t makerId = 0;
    uint32_t price = 0;
    uint32_t qty = 0;
    uint32_t remaining = 0;
    FeedType type = FeedType::Add;
    bool buy = false;
};

inline std::array<uint8_t, kFeedFrameSize> encodeFeed(const FeedFrame& f) {
    std::array<uint8_t, kFeedFrameSize> out{};
    writeU64(out.data(), f.seq);
    writeU64(out.data() + 8, f.tsNs);
    writeU64(out.data() + 16, f.id);
    writeU64(out.data() + 24, f.makerId);
    writeU32(out.data() + 32, f.price);
    writeU32(out.data() + 36, f.qty);
    writeU32(out.data() + 40, f.remaining);
    out[44] = static_cast<uint8_t>(f.type);
    out[45] = f.buy ? 1 : 0;
    return out;
}

inline FeedFrame decodeFeed(const uint8_t* p) {
    FeedFrame f;
    f.seq = readU64(p);
    f.tsNs = readU64(p + 8);
    f.id = readU64(p + 16);
    f.makerId = readU64(p + 24);
    f.price = readU32(p + 32);
    f.qty = readU32(p + 36);
    f.remaining = readU32(p + 40);
    f.type = static_cast<FeedType>(p[44]);
    f.buy = p[45] != 0;
    return f;
}

// Feed consumer: reassembles frames from arbitrary TCP chunks, detects sequence gaps,
// and rebuilds a replica of the book from nothing but the frames. There is no snapshot
// message, so a subscriber must connect before the flow it wants to replicate.
class FeedHandler {
public:
    using TradeCallback = std::function<void(const FeedFrame&)>;

    void onTrade(TradeCallback callback) { onTrade_ = std::move(callback); }

    void onBytes(const uint8_t* data, std::size_t n) {
        std::size_t used = 0;
        if (!partial_.empty()) {
            const std::size_t take = std::min(kFeedFrameSize - partial_.size(), n);
            partial_.insert(partial_.end(), data, data + take);
            used = take;
            if (partial_.size() < kFeedFrameSize) return;
            apply(decodeFeed(partial_.data()));
            partial_.clear();
        }
        while (n - used >= kFeedFrameSize) {
            apply(decodeFeed(data + used));
            used += kFeedFrameSize;
        }
        if (used < n) partial_.assign(data + used, data + n);
    }

    uint64_t frames() const { return frames_; }
    uint64_t gaps() const { return gaps_; }
    uint64_t lastSeq() const { return lastSeq_; }
    uint64_t inconsistencies() const { return inconsistencies_; }
    uint64_t trades() const { return trades_; }
    std::size_t liveOrders() const { return orders_.size(); }

    std::optional<uint32_t> bestBid() const {
        return bids_.empty() ? std::nullopt : std::optional<uint32_t>{bids_.rbegin()->first};
    }
    std::optional<uint32_t> bestAsk() const {
        return asks_.empty() ? std::nullopt : std::optional<uint32_t>{asks_.begin()->first};
    }
    uint64_t depthAt(bool buy, uint32_t price) const {
        const auto& side = buy ? bids_ : asks_;
        auto it = side.find(price);
        return it == side.end() ? 0 : it->second;
    }

private:
    struct Replica {
        bool buy;
        uint32_t price;
        uint32_t qty;
    };

    void adjust(bool buy, uint32_t price, int64_t delta) {
        auto& side = buy ? bids_ : asks_;
        auto it = side.emplace(price, 0).first;
        it->second = static_cast<uint64_t>(static_cast<int64_t>(it->second) + delta);
        if (it->second == 0) side.erase(it);
    }

    void apply(const FeedFrame& f) {
        ++frames_;
        if (lastSeq_ != 0 && f.seq != lastSeq_ + 1) ++gaps_;
        lastSeq_ = f.seq;
        switch (f.type) {
            case FeedType::Add:
                orders_[f.id] = Replica{f.buy, f.price, f.qty};
                adjust(f.buy, f.price, f.qty);
                break;
            case FeedType::Delete: {
                auto it = orders_.find(f.id);
                if (it == orders_.end()) {
                    ++inconsistencies_;
                    break;
                }
                adjust(it->second.buy, it->second.price, -static_cast<int64_t>(it->second.qty));
                orders_.erase(it);
                break;
            }
            case FeedType::Trade: {
                ++trades_;
                auto it = orders_.find(f.makerId);
                if (it == orders_.end() || it->second.qty < f.qty) {
                    ++inconsistencies_;
                } else {
                    it->second.qty -= f.qty;
                    adjust(it->second.buy, it->second.price, -static_cast<int64_t>(f.qty));
                    if (it->second.qty != f.remaining) ++inconsistencies_;
                    if (it->second.qty == 0) orders_.erase(it);
                }
                if (onTrade_) onTrade_(f);
                break;
            }
            default:
                ++inconsistencies_;
        }
    }

    std::vector<uint8_t> partial_;
    std::unordered_map<uint64_t, Replica> orders_;
    std::map<uint32_t, uint64_t> bids_;
    std::map<uint32_t, uint64_t> asks_;
    TradeCallback onTrade_;
    uint64_t frames_ = 0;
    uint64_t gaps_ = 0;
    uint64_t lastSeq_ = 0;
    uint64_t inconsistencies_ = 0;
    uint64_t trades_ = 0;
};

}  // namespace lob
