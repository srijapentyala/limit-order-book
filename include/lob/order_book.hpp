#pragma once
#include "lob/flat_id_map.hpp"
#include "lob/memory_pool.hpp"
#include "lob/types.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

namespace lob {

// Price-time priority book for one instrument.
//
// Each side is a ladder indexed directly by price in ticks, so finding a level is an
// array access. A bitmap over the ladder finds the next occupied price after a level
// empties, one 64-tick word at a time. Orders at a level form an intrusive doubly
// linked FIFO whose nodes come from a fixed pool, and an id map points at the node,
// so a cancel is O(1) wherever the order sits in its queue. After construction the
// callback overload of submit() does not allocate.
class OrderBook {
public:
    struct Config {
        uint32_t maxPrice = (1u << 18) - 1;
        std::size_t maxOrders = 1u << 18;
    };

    enum class Status : uint8_t {
        Accepted = 0,
        InvalidQty = 1,
        InvalidId = 2,
        DuplicateId = 3,
        InvalidPrice = 4,
        BookFull = 5,  // matched what it could, but the remainder had no node to rest in
    };

    struct SubmitResult {
        Status status = Status::Accepted;
        uint32_t filled = 0;
        uint32_t resting = 0;
    };

    struct Resting {
        bool buy = false;
        uint32_t price = 0;
        uint32_t qty = 0;
    };

    OrderBook() : OrderBook(Config{}) {}
    explicit OrderBook(const Config& config)
        : maxPrice_(config.maxPrice),
          pool_(config.maxOrders),
          index_(config.maxOrders),
          bids_(config.maxPrice + 2),
          asks_(config.maxPrice + 2),
          bidBits_(config.maxPrice / 64 + 1),
          askBits_(config.maxPrice / 64 + 1),
          noAsk_(config.maxPrice + 1),
          bestBid_(kNoBid),
          bestAsk_(config.maxPrice + 1) {}

    template <typename OnTrade>
    SubmitResult submit(const Order& order, OnTrade&& onTrade) {
        SubmitResult result;
        if (order.qty == 0) return reject(Status::InvalidQty);
        if (order.id == 0) return reject(Status::InvalidId);
        if (!order.market && (order.price == 0 || order.price > maxPrice_)) return reject(Status::InvalidPrice);
        if (index_.contains(order.id)) return reject(Status::DuplicateId);

        uint32_t remaining = order.qty;
        if (order.buy) matchBuy(order, remaining, onTrade);
        else matchSell(order, remaining, onTrade);
        result.filled = order.qty - remaining;

        if (remaining > 0 && !order.market) {
            if (rest(order, remaining)) result.resting = remaining;
            else result.status = Status::BookFull;
        }
        return result;
    }

    std::vector<Trade> submit(const Order& order) {
        std::vector<Trade> trades;
        submit(order, [&](const Trade& trade) { trades.push_back(trade); });
        return trades;
    }

    std::optional<Resting> cancelOrder(uint64_t id) {
        Node** found = index_.find(id);
        if (found == nullptr) return std::nullopt;
        Node* node = *found;
        Resting removed{node->buy, node->price, node->qty};
        if (node->buy) {
            Level& level = bids_[node->price];
            unlink(level, node);
            if (level.head == nullptr) {
                clearBit(bidBits_, node->price);
                if (node->price == bestBid_) bestBid_ = nextBidBelow(node->price);
            }
        } else {
            Level& level = asks_[node->price];
            unlink(level, node);
            if (level.head == nullptr) {
                clearBit(askBits_, node->price);
                if (node->price == bestAsk_) bestAsk_ = nextAskAbove(node->price);
            }
        }
        index_.erase(id);
        pool_.release(node);
        return removed;
    }

    bool cancel(uint64_t id) { return cancelOrder(id).has_value(); }

    std::optional<uint32_t> bestBid() const {
        return bestBid_ == kNoBid ? std::nullopt : std::optional<uint32_t>{bestBid_};
    }
    std::optional<uint32_t> bestAsk() const {
        return bestAsk_ == noAsk_ ? std::nullopt : std::optional<uint32_t>{bestAsk_};
    }
    uint64_t depthAt(bool buy, uint32_t price) const {
        if (price == 0 || price > maxPrice_) return 0;
        return buy ? bids_[price].qty : asks_[price].qty;
    }
    std::size_t liveOrders() const { return index_.size(); }

private:
    static constexpr uint32_t kNoBid = 0;

    struct Node {
        uint64_t id = 0;
        Node* next = nullptr;
        Node* prev = nullptr;
        uint32_t price = 0;
        uint32_t qty = 0;
        uint32_t owner = 0;
        bool buy = false;
    };

    struct Level {
        Node* head = nullptr;
        Node* tail = nullptr;
        uint64_t qty = 0;
    };

    static SubmitResult reject(Status status) {
        SubmitResult result;
        result.status = status;
        return result;
    }

    static void setBit(std::vector<uint64_t>& bits, uint32_t price) { bits[price >> 6] |= 1ULL << (price & 63); }
    static void clearBit(std::vector<uint64_t>& bits, uint32_t price) { bits[price >> 6] &= ~(1ULL << (price & 63)); }

    uint32_t nextAskAbove(uint32_t price) const {
        uint32_t start = price + 1;
        if (start > maxPrice_) return noAsk_;
        std::size_t word = start >> 6;
        uint64_t bits = askBits_[word] & (~0ULL << (start & 63));
        while (true) {
            if (bits != 0) {
                return static_cast<uint32_t>(word << 6) + static_cast<uint32_t>(__builtin_ctzll(bits));
            }
            if (++word >= askBits_.size()) return noAsk_;
            bits = askBits_[word];
        }
    }

    uint32_t nextBidBelow(uint32_t price) const {
        if (price <= 1) return kNoBid;
        uint32_t start = price - 1;
        std::size_t word = start >> 6;
        uint32_t offset = start & 63;
        uint64_t bits = bidBits_[word] & (offset == 63 ? ~0ULL : ((1ULL << (offset + 1)) - 1));
        while (true) {
            if (bits != 0) {
                return static_cast<uint32_t>(word << 6) + 63u - static_cast<uint32_t>(__builtin_clzll(bits));
            }
            if (word == 0) return kNoBid;
            bits = bidBits_[--word];
        }
    }

    static void unlink(Level& level, Node* node) {
        if (node->prev != nullptr) node->prev->next = node->next;
        else level.head = node->next;
        if (node->next != nullptr) node->next->prev = node->prev;
        else level.tail = node->prev;
        level.qty -= node->qty;
    }

    // Fills `remaining` against one level in FIFO order. The trade prints at the
    // maker's price, so a taker that crosses the spread gets the price improvement.
    template <typename OnTrade>
    void fillLevel(const Order& taker, uint32_t& remaining, Level& level, OnTrade& onTrade) {
        while (remaining > 0 && level.head != nullptr) {
            Node* maker = level.head;
            const uint32_t fill = std::min(remaining, maker->qty);
            maker->qty -= fill;
            level.qty -= fill;
            remaining -= fill;
            onTrade(Trade{taker.id, maker->id, maker->price, fill, maker->qty, maker->owner, taker.buy});
            if (maker->qty == 0) {
                unlink(level, maker);
                index_.erase(maker->id);
                pool_.release(maker);
            }
        }
    }

    template <typename OnTrade>
    void matchBuy(const Order& taker, uint32_t& remaining, OnTrade& onTrade) {
        while (remaining > 0 && bestAsk_ != noAsk_ && (taker.market || bestAsk_ <= taker.price)) {
            Level& level = asks_[bestAsk_];
            fillLevel(taker, remaining, level, onTrade);
            if (level.head == nullptr) {
                clearBit(askBits_, bestAsk_);
                bestAsk_ = nextAskAbove(bestAsk_);
            }
        }
    }

    template <typename OnTrade>
    void matchSell(const Order& taker, uint32_t& remaining, OnTrade& onTrade) {
        while (remaining > 0 && bestBid_ != kNoBid && (taker.market || bestBid_ >= taker.price)) {
            Level& level = bids_[bestBid_];
            fillLevel(taker, remaining, level, onTrade);
            if (level.head == nullptr) {
                clearBit(bidBits_, bestBid_);
                bestBid_ = nextBidBelow(bestBid_);
            }
        }
    }

    bool rest(const Order& order, uint32_t remaining) {
        Node* node = pool_.allocate();
        if (node == nullptr) return false;
        node->id = order.id;
        node->price = order.price;
        node->qty = remaining;
        node->owner = order.owner;
        node->buy = order.buy;
        node->next = nullptr;

        Level& level = order.buy ? bids_[order.price] : asks_[order.price];
        node->prev = level.tail;
        if (level.tail != nullptr) level.tail->next = node;
        else level.head = node;
        level.tail = node;
        level.qty += remaining;

        if (order.buy) {
            setBit(bidBits_, order.price);
            if (order.price > bestBid_) bestBid_ = order.price;
        } else {
            setBit(askBits_, order.price);
            if (order.price < bestAsk_) bestAsk_ = order.price;
        }
        index_.insert(order.id, node);
        return true;
    }

    uint32_t maxPrice_;
    MemoryPool<Node> pool_;
    FlatIdMap<Node*> index_;
    std::vector<Level> bids_;
    std::vector<Level> asks_;
    std::vector<uint64_t> bidBits_;
    std::vector<uint64_t> askBits_;
    uint32_t noAsk_;
    uint32_t bestBid_;
    uint32_t bestAsk_;
};

}  // namespace lob
