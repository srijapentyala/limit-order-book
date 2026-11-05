#pragma once
#include "lob/types.hpp"

#include <algorithm>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

// A deliberately plain book (ordered maps of deques, linear cancel) used only as a test
// oracle. It is slow and obvious, which is the point: the fast book must agree with it
// on every trade.
class ReferenceBook {
public:
    std::vector<lob::Trade> submit(lob::Order order) {
        std::vector<lob::Trade> trades;
        if (order.qty == 0 || order.id == 0 || index_.count(order.id) != 0) return trades;
        if (order.buy) match(order, asks_, trades, true);
        else match(order, bids_, trades, false);
        if (order.qty > 0 && !order.market) rest(order);
        return trades;
    }

    bool cancel(uint64_t id) {
        auto found = index_.find(id);
        if (found == index_.end()) return false;
        if (found->second.first) eraseFrom(bids_, found->second.second, id);
        else eraseFrom(asks_, found->second.second, id);
        index_.erase(found);
        return true;
    }

    std::optional<uint32_t> bestBid() const {
        return bids_.empty() ? std::nullopt : std::optional<uint32_t>{bids_.begin()->first};
    }
    std::optional<uint32_t> bestAsk() const {
        return asks_.empty() ? std::nullopt : std::optional<uint32_t>{asks_.begin()->first};
    }
    std::size_t liveOrders() const { return index_.size(); }

    uint64_t depthAt(bool buy, uint32_t price) const {
        uint64_t total = 0;
        if (buy) {
            auto it = bids_.find(price);
            if (it != bids_.end()) for (const auto& o : it->second) total += o.qty;
        } else {
            auto it = asks_.find(price);
            if (it != asks_.end()) for (const auto& o : it->second) total += o.qty;
        }
        return total;
    }

    template <typename Fn>
    void forEachLevel(Fn&& fn) const {
        for (const auto& [price, queue] : bids_) {
            uint64_t total = 0;
            for (const auto& o : queue) total += o.qty;
            fn(true, price, total);
        }
        for (const auto& [price, queue] : asks_) {
            uint64_t total = 0;
            for (const auto& o : queue) total += o.qty;
            fn(false, price, total);
        }
    }

private:
    using Bids = std::map<uint32_t, std::deque<lob::Order>, std::greater<uint32_t>>;
    using Asks = std::map<uint32_t, std::deque<lob::Order>>;

    template <typename Book>
    void match(lob::Order& taker, Book& opposite, std::vector<lob::Trade>& trades, bool buying) {
        while (taker.qty > 0 && !opposite.empty()) {
            auto level = opposite.begin();
            if (!taker.market) {
                if (buying && level->first > taker.price) break;
                if (!buying && level->first < taker.price) break;
            }
            auto& queue = level->second;
            while (taker.qty > 0 && !queue.empty()) {
                lob::Order& maker = queue.front();
                const uint32_t fill = std::min(taker.qty, maker.qty);
                maker.qty -= fill;
                taker.qty -= fill;
                trades.push_back(lob::Trade{taker.id, maker.id, level->first, fill, maker.qty, maker.owner, buying});
                if (maker.qty == 0) {
                    index_.erase(maker.id);
                    queue.pop_front();
                }
            }
            if (queue.empty()) opposite.erase(level);
        }
    }

    template <typename Book>
    void eraseFrom(Book& book, uint32_t price, uint64_t id) {
        auto level = book.find(price);
        if (level == book.end()) return;
        auto& queue = level->second;
        for (auto it = queue.begin(); it != queue.end(); ++it) {
            if (it->id == id) {
                queue.erase(it);
                break;
            }
        }
        if (queue.empty()) book.erase(level);
    }

    void rest(const lob::Order& order) {
        if (order.buy) bids_[order.price].push_back(order);
        else asks_[order.price].push_back(order);
        index_[order.id] = {order.buy, order.price};
    }

    Bids bids_;
    Asks asks_;
    std::unordered_map<uint64_t, std::pair<bool, uint32_t>> index_;
};
