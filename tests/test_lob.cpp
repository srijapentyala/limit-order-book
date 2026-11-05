#include "lob/codec.hpp"
#include "lob/feed.hpp"
#include "lob/flat_id_map.hpp"
#include "lob/flow.hpp"
#include "lob/memory_pool.hpp"
#include "lob/order_book.hpp"
#include "lob/redis_resp.hpp"
#include "lob/ring_buffer.hpp"
#include "reference_book.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <thread>
#include <unordered_map>

using lob::Order;
using Status = lob::OrderBook::Status;

TEST(Book, PriceTimePriorityAndPartialFill) {
    lob::OrderBook book;
    EXPECT_TRUE(book.submit(Order{1, 100, 5, true, false}).empty());
    EXPECT_TRUE(book.submit(Order{2, 100, 5, true, false}).empty());
    auto trades = book.submit(Order{3, 100, 7, false, false});
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_EQ(trades[0].makerId, 1u);
    EXPECT_EQ(trades[0].qty, 5u);
    EXPECT_EQ(trades[1].makerId, 2u);
    EXPECT_EQ(trades[1].qty, 2u);
    EXPECT_EQ(trades[1].makerRemaining, 3u);
    EXPECT_EQ(book.liveOrders(), 1u);
    EXPECT_EQ(*book.bestBid(), 100u);
    EXPECT_EQ(book.depthAt(true, 100), 3u);
}

TEST(Book, BetterPriceBeatsEarlierTime) {
    lob::OrderBook book;
    book.submit(Order{1, 100, 5, false, false});
    book.submit(Order{2, 99, 5, false, false});
    auto trades = book.submit(Order{3, 100, 1, true, false});
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].makerId, 2u);
}

TEST(Book, TakerGetsMakersPrice) {
    lob::OrderBook book;
    book.submit(Order{1, 100, 5, false, false});
    auto trades = book.submit(Order{2, 105, 5, true, false});
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].price, 100u);
    EXPECT_FALSE(book.bestBid().has_value());
}

TEST(Book, MarketSweepsAsksAndDoesNotRest) {
    lob::OrderBook book;
    book.submit(Order{1, 101, 2, false, false});
    book.submit(Order{2, 103, 2, false, false});
    auto trades = book.submit(Order{3, 0, 5, true, true});
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_EQ(trades[0].price, 101u);
    EXPECT_EQ(trades[1].price, 103u);
    EXPECT_EQ(book.liveOrders(), 0u);
    EXPECT_FALSE(book.bestAsk().has_value());
}

TEST(Book, MarketWithNoLiquidityLeavesNothing) {
    lob::OrderBook book;
    EXPECT_TRUE(book.submit(Order{1, 0, 5, false, true}).empty());
    EXPECT_EQ(book.liveOrders(), 0u);
}

TEST(Book, CancelRemovesOrder) {
    lob::OrderBook book;
    book.submit(Order{7, 50, 3, false, false});
    EXPECT_TRUE(book.cancel(7));
    EXPECT_FALSE(book.cancel(7));
    EXPECT_EQ(book.liveOrders(), 0u);
    auto trades = book.submit(Order{8, 50, 1, true, false});
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.liveOrders(), 1u);
}

TEST(Book, CancelFromMiddleOfQueueKeepsFifo) {
    lob::OrderBook book;
    book.submit(Order{1, 100, 1, true, false});
    book.submit(Order{2, 100, 1, true, false});
    book.submit(Order{3, 100, 1, true, false});
    EXPECT_TRUE(book.cancel(2));
    auto trades = book.submit(Order{4, 100, 2, false, false});
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_EQ(trades[0].makerId, 1u);
    EXPECT_EQ(trades[1].makerId, 3u);
}

TEST(Book, CancellingTheTouchMovesTheBestPrice) {
    lob::OrderBook book;
    book.submit(Order{1, 100, 1, true, false});
    book.submit(Order{2, 90, 1, true, false});
    book.submit(Order{3, 110, 1, false, false});
    book.submit(Order{4, 120, 1, false, false});
    book.cancel(1);
    EXPECT_EQ(*book.bestBid(), 90u);
    book.cancel(3);
    EXPECT_EQ(*book.bestAsk(), 120u);
    book.cancel(2);
    book.cancel(4);
    EXPECT_FALSE(book.bestBid().has_value());
    EXPECT_FALSE(book.bestAsk().has_value());
}

TEST(Book, BestPriceScanCrossesBitmapWords) {
    lob::OrderBook book;
    book.submit(Order{1, 5000, 1, true, false});
    book.submit(Order{2, 63, 1, true, false});
    book.submit(Order{3, 64, 1, true, false});
    book.cancel(1);
    EXPECT_EQ(*book.bestBid(), 64u);
    book.cancel(3);
    EXPECT_EQ(*book.bestBid(), 63u);

    book.submit(Order{4, 6000, 1, false, false});
    book.submit(Order{5, 127, 1, false, false});
    book.submit(Order{6, 128, 1, false, false});
    book.cancel(5);
    EXPECT_EQ(*book.bestAsk(), 128u);
    book.cancel(6);
    EXPECT_EQ(*book.bestAsk(), 6000u);
}

TEST(Book, RejectsBadOrdersWithAReason) {
    lob::OrderBook::Config cfg;
    cfg.maxPrice = 1000;
    cfg.maxOrders = 8;
    lob::OrderBook book(cfg);
    auto noop = [](const lob::Trade&) {};
    EXPECT_EQ(book.submit(Order{1, 10, 0, true, false}, noop).status, Status::InvalidQty);
    EXPECT_EQ(book.submit(Order{0, 10, 1, true, false}, noop).status, Status::InvalidId);
    EXPECT_EQ(book.submit(Order{1, 0, 1, true, false}, noop).status, Status::InvalidPrice);
    EXPECT_EQ(book.submit(Order{1, 1001, 1, true, false}, noop).status, Status::InvalidPrice);
    EXPECT_EQ(book.submit(Order{1, 1000, 1, true, false}, noop).status, Status::Accepted);
    EXPECT_EQ(book.submit(Order{1, 900, 1, true, false}, noop).status, Status::DuplicateId);
    EXPECT_EQ(book.liveOrders(), 1u);
}

TEST(Book, FullBookStillMatchesAndReportsTheRemainder) {
    lob::OrderBook::Config cfg;
    cfg.maxPrice = 1000;
    cfg.maxOrders = 2;
    lob::OrderBook book(cfg);
    uint64_t fills = 0;
    auto count = [&](const lob::Trade& t) { fills += t.qty; };
    EXPECT_EQ(book.submit(Order{1, 100, 1, true, false}, count).status, Status::Accepted);
    EXPECT_EQ(book.submit(Order{2, 101, 1, true, false}, count).status, Status::Accepted);
    EXPECT_EQ(book.submit(Order{3, 102, 1, true, false}, count).status, Status::BookFull);
    EXPECT_EQ(book.liveOrders(), 2u);
    // A crossing order needs no node for the quantity it fills.
    auto result = book.submit(Order{4, 101, 1, false, false}, count);
    EXPECT_EQ(result.status, Status::Accepted);
    EXPECT_EQ(result.filled, 1u);
    // That fill freed a node, so the book can take a new resting order again.
    EXPECT_EQ(book.submit(Order{5, 50, 1, true, false}, count).status, Status::Accepted);
    EXPECT_EQ(fills, 1u);
}

// The core correctness check: replay random flow through the fast book and the plain
// oracle, and require the same trades in the same order and the same book afterwards.
TEST(Book, MatchesReferenceBookOnRandomFlow) {
    for (uint64_t seed : {1u, 2u, 3u}) {
        lob::OrderBook::Config cfg;
        cfg.maxPrice = 2000;
        cfg.maxOrders = 200000;
        lob::OrderBook fast(cfg);
        ReferenceBook slow;
        std::mt19937_64 rng(seed);
        std::vector<uint64_t> ids;
        uint64_t next = 1;
        for (int step = 0; step < 200000; ++step) {
            const int roll = static_cast<int>(rng() % 100);
            if (roll < 35 && !ids.empty()) {
                const std::size_t pick = rng() % ids.size();
                ASSERT_EQ(fast.cancel(ids[pick]), slow.cancel(ids[pick]));
                ids[pick] = ids.back();
                ids.pop_back();
            } else {
                Order order;
                order.id = next++;
                order.buy = (rng() & 1) != 0;
                order.market = roll >= 97;
                order.price = order.market ? 0 : static_cast<uint32_t>(900 + rng() % 200);
                order.qty = static_cast<uint32_t>(1 + rng() % 50);
                ids.push_back(order.id);
                const auto a = fast.submit(order);
                const auto b = slow.submit(order);
                ASSERT_EQ(a.size(), b.size()) << "step " << step;
                for (std::size_t i = 0; i < a.size(); ++i) {
                    ASSERT_EQ(a[i].makerId, b[i].makerId) << "step " << step;
                    ASSERT_EQ(a[i].price, b[i].price) << "step " << step;
                    ASSERT_EQ(a[i].qty, b[i].qty) << "step " << step;
                    ASSERT_EQ(a[i].makerRemaining, b[i].makerRemaining) << "step " << step;
                }
            }
            ASSERT_EQ(fast.bestBid(), slow.bestBid()) << "step " << step;
            ASSERT_EQ(fast.bestAsk(), slow.bestAsk()) << "step " << step;
            ASSERT_EQ(fast.liveOrders(), slow.liveOrders()) << "step " << step;
        }
        slow.forEachLevel([&](bool buy, uint32_t price, uint64_t qty) { EXPECT_EQ(fast.depthAt(buy, price), qty); });
    }
}

TEST(Book, PoolIsReusedSoFlowLargerThanTheBookKeepsWorking) {
    lob::OrderBook::Config cfg;
    cfg.maxPrice = 1000;
    cfg.maxOrders = 4;
    lob::OrderBook book(cfg);
    auto noop = [](const lob::Trade&) {};
    for (uint64_t id = 1; id <= 100000; ++id) {
        ASSERT_EQ(book.submit(Order{id, 500, 1, true, false}, noop).status, Status::Accepted);
        ASSERT_TRUE(book.cancel(id));
    }
    EXPECT_EQ(book.liveOrders(), 0u);
}

TEST(FlatIdMap, AgreesWithUnorderedMapUnderChurn) {
    lob::FlatIdMap<uint64_t> flat(4096);
    std::unordered_map<uint64_t, uint64_t> model;
    std::mt19937_64 rng(5);
    for (int i = 0; i < 400000; ++i) {
        // A small key space forces collisions and long probe chains through deletes.
        const uint64_t key = 1 + rng() % 5000;
        if (model.count(key) != 0) {
            ASSERT_TRUE(flat.erase(key));
            model.erase(key);
        } else if (model.size() < 4096) {
            flat.insert(key, key * 3);
            model[key] = key * 3;
        }
        const uint64_t probe = 1 + rng() % 5000;
        auto* found = flat.find(probe);
        ASSERT_EQ(found != nullptr, model.count(probe) != 0);
        if (found != nullptr) ASSERT_EQ(*found, model[probe]);
    }
    EXPECT_EQ(flat.size(), model.size());
}

TEST(Ring, WrapsAndRejectsWhenFull) {
    lob::SpscRing<int, 4> ring;
    EXPECT_TRUE(ring.push(1));
    EXPECT_TRUE(ring.push(2));
    EXPECT_TRUE(ring.push(3));
    EXPECT_FALSE(ring.push(4));
    EXPECT_EQ(*ring.pop(), 1);
    EXPECT_TRUE(ring.push(5));
    EXPECT_EQ(*ring.pop(), 2);
    EXPECT_EQ(*ring.pop(), 3);
    EXPECT_EQ(*ring.pop(), 5);
    EXPECT_FALSE(ring.pop().has_value());
}

TEST(Ring, ConsumeStopsAtLimitAndKeepsOrder) {
    lob::SpscRing<int, 8> ring;
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(ring.push(i));
    std::vector<int> seen;
    EXPECT_EQ(ring.consume(3, [&](int v) { seen.push_back(v); }), 3u);
    EXPECT_EQ(ring.consume(10, [&](int v) { seen.push_back(v); }), 2u);
    EXPECT_EQ(ring.consume(10, [&](int v) { seen.push_back(v); }), 0u);
    EXPECT_EQ(seen, (std::vector<int>{0, 1, 2, 3, 4}));
}

// Under ThreadSanitizer this is the check that the memory ordering is actually correct.
TEST(Ring, TwoThreadsTransferEveryItemInOrder) {
    auto ring = std::make_unique<lob::SpscRing<uint64_t, 1024>>();
    constexpr uint64_t kItems = 2000000;
    std::thread producer([&] {
        for (uint64_t i = 0; i < kItems; ++i) {
            while (!ring->push(i)) std::this_thread::yield();
        }
    });
    uint64_t expected = 0;
    bool ordered = true;
    while (expected < kItems) {
        const auto n = ring->consume(64, [&](uint64_t v) {
            if (v != expected) ordered = false;
            ++expected;
        });
        if (n == 0) std::this_thread::yield();
    }
    producer.join();
    EXPECT_TRUE(ordered);
}

TEST(Pool, ReusesReleasedBlocks) {
    lob::MemoryPool<Order> pool(2);
    auto* a = pool.allocate();
    auto* b = pool.allocate();
    EXPECT_EQ(pool.allocate(), nullptr);
    pool.release(a);
    auto* c = pool.allocate();
    EXPECT_EQ(c, a);
    pool.release(b);
    pool.release(c);
    EXPECT_EQ(pool.available(), 2u);
}

TEST(Codec, RoundTrip) {
    auto bytes = lob::encode(lob::MsgType::New, 42, 10050, 8, 1);
    auto msg = lob::decode(bytes.data(), bytes.size());
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->type, lob::MsgType::New);
    EXPECT_EQ(msg->id, 42u);
    EXPECT_EQ(msg->price, 10050u);
    EXPECT_EQ(msg->qty, 8u);
    EXPECT_EQ(msg->flags, 1);
}

TEST(Codec, ShortFrameIsRejected) {
    auto bytes = lob::encode(lob::MsgType::New, 42, 10050, 8, 1);
    EXPECT_FALSE(lob::decode(bytes.data(), bytes.size() - 1).has_value());
}

TEST(Resp, EncodesSet) {
    auto cmd = lob::respSet("lob:bid", "100");
    EXPECT_EQ(cmd, "*3\r\n$3\r\nSET\r\n$7\r\nlob:bid\r\n$3\r\n100\r\n");
}

TEST(Resp, EncodesOrderState) {
    EXPECT_EQ(lob::respOrderQty(7, 3), "*4\r\n$4\r\nHSET\r\n$7\r\norder:7\r\n$3\r\nqty\r\n$1\r\n3\r\n");
    EXPECT_EQ(lob::respOrderDelete(7), "*2\r\n$3\r\nDEL\r\n$7\r\norder:7\r\n");
}

namespace {
lob::FeedFrame sampleFrame(uint64_t seq, lob::FeedType type, uint64_t id, uint32_t price, uint32_t qty, bool buy) {
    lob::FeedFrame f;
    f.seq = seq;
    f.type = type;
    f.id = id;
    f.price = price;
    f.qty = qty;
    f.buy = buy;
    return f;
}
}  // namespace

TEST(Feed, FrameRoundTrip) {
    lob::FeedFrame f = sampleFrame(9, lob::FeedType::Trade, 11, 100, 3, true);
    f.tsNs = 123456789;
    f.makerId = 12;
    f.remaining = 4;
    const auto bytes = lob::encodeFeed(f);
    const auto back = lob::decodeFeed(bytes.data());
    EXPECT_EQ(back.seq, 9u);
    EXPECT_EQ(back.tsNs, 123456789u);
    EXPECT_EQ(back.id, 11u);
    EXPECT_EQ(back.makerId, 12u);
    EXPECT_EQ(back.price, 100u);
    EXPECT_EQ(back.qty, 3u);
    EXPECT_EQ(back.remaining, 4u);
    EXPECT_EQ(back.type, lob::FeedType::Trade);
    EXPECT_TRUE(back.buy);
}

TEST(Feed, HandlerReassemblesFramesFromOneByteChunks) {
    lob::FeedHandler handler;
    const auto a = lob::encodeFeed(sampleFrame(1, lob::FeedType::Add, 1, 100, 5, true));
    const auto b = lob::encodeFeed(sampleFrame(2, lob::FeedType::Add, 2, 110, 5, false));
    for (uint8_t byte : a) handler.onBytes(&byte, 1);
    for (uint8_t byte : b) handler.onBytes(&byte, 1);
    EXPECT_EQ(handler.frames(), 2u);
    EXPECT_EQ(*handler.bestBid(), 100u);
    EXPECT_EQ(*handler.bestAsk(), 110u);
    EXPECT_EQ(handler.gaps(), 0u);
}

TEST(Feed, HandlerCountsASequenceGap) {
    lob::FeedHandler handler;
    const auto a = lob::encodeFeed(sampleFrame(1, lob::FeedType::Add, 1, 100, 5, true));
    const auto b = lob::encodeFeed(sampleFrame(5, lob::FeedType::Add, 2, 100, 5, true));
    handler.onBytes(a.data(), a.size());
    handler.onBytes(b.data(), b.size());
    EXPECT_EQ(handler.gaps(), 1u);
    EXPECT_EQ(handler.depthAt(true, 100), 10u);
}

TEST(Feed, HandlerFlagsATradeAgainstAnUnknownOrder) {
    lob::FeedHandler handler;
    auto trade = sampleFrame(1, lob::FeedType::Trade, 2, 100, 5, true);
    trade.makerId = 999;
    const auto bytes = lob::encodeFeed(trade);
    handler.onBytes(bytes.data(), bytes.size());
    EXPECT_EQ(handler.inconsistencies(), 1u);
}

TEST(Flow, IsDeterministicForASeed) {
    lob::FlowConfig cfg;
    cfg.events = 1000;
    const auto a = lob::makeFlow(cfg);
    const auto b = lob::makeFlow(cfg);
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].order.id, b[i].order.id);
        EXPECT_EQ(a[i].order.price, b[i].order.price);
        EXPECT_EQ(a[i].cancel, b[i].cancel);
    }
}
