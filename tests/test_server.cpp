#include "lob/codec.hpp"
#include "lob/feed.hpp"
#include "lob/flow.hpp"
#include "lob/redis_resp.hpp"
#include "lob/server.hpp"
#include "reference_book.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

int connectLocal(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    timeval tv{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}

void sendFrame(int fd, lob::MsgType type, uint64_t id, uint32_t price, uint32_t qty, uint8_t flags) {
    const auto frame = lob::encode(type, id, price, qty, flags);
    ASSERT_EQ(::send(fd, frame.data(), frame.size(), 0), static_cast<ssize_t>(frame.size()));
}

std::optional<lob::Decoded> readFrame(int fd) {
    uint8_t buf[lob::kWireSize];
    std::size_t got = 0;
    while (got < sizeof(buf)) {
        const ssize_t r = ::recv(fd, buf + got, sizeof(buf) - got, 0);
        if (r <= 0) return std::nullopt;
        got += static_cast<std::size_t>(r);
    }
    return lob::decode(buf, got);
}

lob::ServerConfig testConfig() {
    lob::ServerConfig config;
    config.orderPort = 0;
    config.feedPort = 0;
    config.loopbackOnly = true;
    config.book.maxPrice = 1u << 18;
    config.book.maxOrders = 1u << 18;
    return config;
}

// Speaks just enough RESP to accept commands and reply +OK to each, like a Redis would.
class FakeRedis {
public:
    FakeRedis() {
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(listener_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listener_, 4);
        socklen_t len = sizeof(addr);
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve(); });
    }
    ~FakeRedis() {
        ::shutdown(listener_, SHUT_RDWR);
        ::close(listener_);
        for (int fd : conns_) ::shutdown(fd, SHUT_RDWR);
        thread_.join();
        for (int fd : conns_) ::close(fd);
    }
    uint16_t port() const { return port_; }
    std::string received() {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_;
    }

private:
    void serve() {
        int fd = ::accept(listener_, nullptr, nullptr);
        if (fd < 0) {
            // The sync-mode startup probe connects and leaves; there may be a second one.
            return;
        }
        std::vector<std::thread> workers;
        while (fd >= 0) {
            conns_.push_back(fd);
            workers.emplace_back([this, fd] {
                char buf[4096];
                while (true) {
                    const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                    if (n <= 0) return;
                    std::string chunk(buf, static_cast<std::size_t>(n));
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        data_ += chunk;
                    }
                    // One array header per command; reply so a blocking client can proceed.
                    std::size_t commands = 0;
                    for (std::size_t i = 0; i + 1 < chunk.size(); ++i) {
                        if (chunk[i] == '*' && (i == 0 || chunk[i - 1] == '\n')) ++commands;
                    }
                    const std::string ok = "+OK\r\n";
                    for (std::size_t i = 0; i < commands; ++i) ::send(fd, ok.data(), ok.size(), 0);
                }
            });
            fd = ::accept(listener_, nullptr, nullptr);
        }
        for (auto& w : workers) w.join();
    }

    int listener_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
    std::vector<int> conns_;
    std::mutex mutex_;
    std::string data_;
};

template <typename Pred>
bool waitFor(Pred pred) {
    for (int i = 0; i < 500; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

}  // namespace

TEST(Server, CrossingOrderGetsTradeThenAckAndMakerIsTold) {
    lob::Server server(testConfig());
    server.start();
    int maker = connectLocal(server.orderPort());
    int taker = connectLocal(server.orderPort());

    sendFrame(maker, lob::MsgType::New, 1, 250, 4, 0);
    auto ack = readFrame(maker);
    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(ack->type, lob::MsgType::Ack);
    EXPECT_EQ(ack->qty, 4u);

    sendFrame(taker, lob::MsgType::New, 9, 250, 3, lob::kFlagBuy);
    auto trade = readFrame(taker);
    ASSERT_TRUE(trade.has_value());
    EXPECT_EQ(trade->type, lob::MsgType::Trade);
    EXPECT_EQ(trade->id, 9u);
    EXPECT_EQ(trade->qty, 3u);
    auto takerAck = readFrame(taker);
    ASSERT_TRUE(takerAck.has_value());
    EXPECT_EQ(takerAck->type, lob::MsgType::Ack);
    EXPECT_EQ(takerAck->qty, 0u);

    auto makerFill = readFrame(maker);
    ASSERT_TRUE(makerFill.has_value());
    EXPECT_EQ(makerFill->type, lob::MsgType::Trade);
    EXPECT_EQ(makerFill->id, 1u);
    EXPECT_NE(makerFill->flags & lob::kFlagMaker, 0);

    ::close(maker);
    ::close(taker);
    server.stop();
    EXPECT_EQ(server.stats().trades.load(), 1u);
}

TEST(Server, CancelIsAcknowledgedAndUnknownCancelIsRejected) {
    lob::Server server(testConfig());
    server.start();
    int fd = connectLocal(server.orderPort());
    sendFrame(fd, lob::MsgType::New, 1, 100, 7, lob::kFlagBuy);
    ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Ack);
    sendFrame(fd, lob::MsgType::Cancel, 1, 0, 0, 0);
    auto ack = readFrame(fd);
    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(ack->type, lob::MsgType::CancelAck);
    EXPECT_EQ(ack->qty, 7u);
    sendFrame(fd, lob::MsgType::Cancel, 1, 0, 0, 0);
    EXPECT_EQ(readFrame(fd)->type, lob::MsgType::Reject);
    ::close(fd);
}

TEST(Server, InvalidOrderIsRejectedWithItsReason) {
    lob::Server server(testConfig());
    server.start();
    int fd = connectLocal(server.orderPort());
    sendFrame(fd, lob::MsgType::New, 1, 0, 5, lob::kFlagBuy);
    auto reply = readFrame(fd);
    ASSERT_TRUE(reply.has_value());
    EXPECT_EQ(reply->type, lob::MsgType::Reject);
    EXPECT_EQ(reply->flags, static_cast<uint8_t>(lob::OrderBook::Status::InvalidPrice));
    ::close(fd);
}

TEST(Server, FrameSplitAcrossWritesIsReassembled) {
    lob::Server server(testConfig());
    server.start();
    int fd = connectLocal(server.orderPort());
    const auto frame = lob::encode(lob::MsgType::New, 1, 100, 7, lob::kFlagBuy);
    ASSERT_EQ(::send(fd, frame.data(), 5, 0), 5);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_EQ(::send(fd, frame.data() + 5, frame.size() - 5, 0), static_cast<ssize_t>(frame.size() - 5));
    auto ack = readFrame(fd);
    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(ack->type, lob::MsgType::Ack);
    EXPECT_EQ(ack->id, 1u);
    ::close(fd);
}

TEST(Server, ClientSendingAGarbageTypeIsDisconnectedAndOthersKeepWorking) {
    lob::Server server(testConfig());
    server.start();
    int bad = connectLocal(server.orderPort());
    int good = connectLocal(server.orderPort());
    const auto garbage = lob::encode(static_cast<lob::MsgType>(99), 1, 1, 1, 0);
    ASSERT_EQ(::send(bad, garbage.data(), garbage.size(), 0), static_cast<ssize_t>(garbage.size()));
    uint8_t byte;
    EXPECT_EQ(::recv(bad, &byte, 1, 0), 0);  // orderly close from the engine
    sendFrame(good, lob::MsgType::New, 2, 100, 1, lob::kFlagBuy);
    EXPECT_EQ(readFrame(good)->type, lob::MsgType::Ack);
    ::close(bad);
    ::close(good);
    server.stop();
    EXPECT_EQ(server.stats().malformedClients.load(), 1u);
}

// End to end through TCP and all three threads. A subscriber rebuilds the book from the
// binary feed alone, and the result must equal a reference book fed the same flow.
TEST(Server, FeedReplicaMatchesReferenceBookAfterRandomFlow) {
    lob::Server server(testConfig());
    server.start();
    int feed = connectLocal(server.feedPort());
    // The feed socket is registered via the gateway; give the accept a moment to land.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    lob::FlowConfig flow;
    flow.events = 50000;
    flow.seed = 11;
    const auto events = lob::makeFlow(flow);

    // Read until the engine closes the feed on shutdown, which it does only after
    // flushing every frame, so nothing can still be in flight when the checks run.
    lob::FeedHandler handler;
    std::thread feedReader([&] {
        std::vector<uint8_t> buf(1 << 16);
        while (true) {
            const ssize_t got = ::recv(feed, buf.data(), buf.size(), 0);
            if (got <= 0) break;
            handler.onBytes(buf.data(), static_cast<std::size_t>(got));
        }
    });

    int fd = connectLocal(server.orderPort());
    ReferenceBook reference;
    uint64_t referenceTrades = 0;
    uint64_t referenceAddsAndDeletes = 0;
    std::thread sender([&] {
        std::vector<uint8_t> out;
        for (const auto& ev : events) {
            const auto frame = ev.cancel ? lob::encode(lob::MsgType::Cancel, ev.order.id, 0, 0, 0)
                                         : lob::encode(lob::MsgType::New, ev.order.id, ev.order.price, ev.order.qty,
                                                       ev.order.buy ? lob::kFlagBuy : 0);
            out.insert(out.end(), frame.begin(), frame.end());
            if (out.size() > 1800) {
                ASSERT_EQ(::send(fd, out.data(), out.size(), 0), static_cast<ssize_t>(out.size()));
                out.clear();
            }
        }
        if (!out.empty()) ASSERT_EQ(::send(fd, out.data(), out.size(), 0), static_cast<ssize_t>(out.size()));
    });

    std::size_t replies = 0;
    while (replies < events.size()) {
        auto frame = readFrame(fd);
        ASSERT_TRUE(frame.has_value()) << "after " << replies << " replies";
        if (frame->type != lob::MsgType::Trade) ++replies;
    }
    sender.join();

    for (const auto& ev : events) {
        if (ev.cancel) {
            if (reference.cancel(ev.order.id)) ++referenceAddsAndDeletes;
        } else {
            const auto before = reference.liveOrders();
            const auto trades = reference.submit(ev.order);
            referenceTrades += trades.size();
            uint64_t emptied = 0;
            for (const auto& t : trades) emptied += t.makerRemaining == 0 ? 1 : 0;
            if (reference.liveOrders() + emptied == before + 1) ++referenceAddsAndDeletes;
        }
    }

    ::close(fd);
    server.stop();
    feedReader.join();

    EXPECT_EQ(handler.frames(), referenceTrades + referenceAddsAndDeletes);
    EXPECT_EQ(handler.gaps(), 0u);
    EXPECT_EQ(handler.inconsistencies(), 0u);
    EXPECT_EQ(handler.trades(), referenceTrades);
    EXPECT_EQ(handler.liveOrders(), reference.liveOrders());
    EXPECT_EQ(handler.bestBid(), reference.bestBid());
    EXPECT_EQ(handler.bestAsk(), reference.bestAsk());
    reference.forEachLevel([&](bool buy, uint32_t price, uint64_t qty) { EXPECT_EQ(handler.depthAt(buy, price), qty); });

    ::close(feed);
}

TEST(Server, TwoClientsNeverLoseReplies) {
    lob::Server server(testConfig());
    server.start();
    constexpr std::size_t kEach = 20000;
    const auto drive = [&](uint64_t base) {
        int fd = connectLocal(server.orderPort());
        lob::FlowConfig flow;
        flow.events = kEach;
        flow.seed = base;
        flow.idBase = base << 40;
        const auto events = lob::makeFlow(flow);
        std::thread sender([&] {
            for (const auto& ev : events) {
                const auto frame = ev.cancel ? lob::encode(lob::MsgType::Cancel, ev.order.id, 0, 0, 0)
                                             : lob::encode(lob::MsgType::New, ev.order.id, ev.order.price, ev.order.qty,
                                                           ev.order.buy ? lob::kFlagBuy : 0);
                ::send(fd, frame.data(), frame.size(), 0);
            }
        });
        std::size_t replies = 0;
        while (replies < kEach) {
            auto frame = readFrame(fd);
            ASSERT_TRUE(frame.has_value());
            if (frame->type != lob::MsgType::Trade) ++replies;
        }
        sender.join();
        ::close(fd);
    };
    std::thread a(drive, 1);
    std::thread b(drive, 2);
    a.join();
    b.join();
    server.stop();
    EXPECT_EQ(server.stats().newOrders.load() + server.stats().cancels.load(), 2 * kEach);
}

// A client that sends but never reads fills its reply buffer. It must be cut off without
// stalling the matcher or anyone else, and its descriptor must stay valid until the
// gateway has let go of it.
TEST(Server, ClientThatNeverReadsIsDroppedAndOthersKeepWorking) {
    auto config = testConfig();
    config.maxClientBuffer = 4096;
    lob::Server server(config);
    server.start();
    int stuck = connectLocal(server.orderPort());
    int shrink = 1024;
    ::setsockopt(stuck, SOL_SOCKET, SO_RCVBUF, &shrink, sizeof(shrink));
    timeval sendLimit{2, 0};
    ::setsockopt(stuck, SOL_SOCKET, SO_SNDTIMEO, &sendLimit, sizeof(sendLimit));

    std::thread flood([&] {
        for (uint64_t id = 1; id <= 400000; ++id) {
            const auto frame = lob::encode(lob::MsgType::New, id, 0, 1, lob::kFlagBuy);  // rejected, still replied to
            if (::send(stuck, frame.data(), frame.size(), 0) <= 0) return;
            if ((id & 1023) == 0 && server.stats().slowClientDrops.load() > 0) return;
        }
    });
    EXPECT_TRUE(waitFor([&] { return server.stats().slowClientDrops.load() >= 1; }));
    flood.join();

    // New connections work, and each hears only its own replies.
    for (uint64_t id = 500000; id < 500020; ++id) {
        int fd = connectLocal(server.orderPort());
        sendFrame(fd, lob::MsgType::New, id, 100, 1, lob::kFlagBuy);
        auto reply = readFrame(fd);
        ASSERT_TRUE(reply.has_value());
        EXPECT_EQ(reply->type, lob::MsgType::Ack);
        EXPECT_EQ(reply->id, id);
        ::close(fd);
    }
    ::close(stuck);
    server.stop();
}

TEST(Server, TradeLogGetsOneLinePerTrade) {
    const std::string path = ::testing::TempDir() + "lob_trades_test.jsonl";
    ::remove(path.c_str());
    auto config = testConfig();
    config.tradeLog = path;
    {
        lob::Server server(config);
        server.start();
        int fd = connectLocal(server.orderPort());
        sendFrame(fd, lob::MsgType::New, 1, 100, 5, 0);
        ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Ack);
        sendFrame(fd, lob::MsgType::New, 2, 100, 5, lob::kFlagBuy);
        ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Trade);
        ::close(fd);
        server.stop();
    }
    FILE* file = std::fopen(path.c_str(), "r");
    ASSERT_NE(file, nullptr);
    char line[256];
    ASSERT_NE(std::fgets(line, sizeof(line), file), nullptr);
    EXPECT_EQ(std::string(line), "{\"taker\":2,\"maker\":1,\"price\":100,\"qty\":5}\n");
    EXPECT_EQ(std::fgets(line, sizeof(line), file), nullptr);
    std::fclose(file);
    ::remove(path.c_str());
}

TEST(Server, CachesOrderStateInRedisAndRemovesItWhenFilled) {
    FakeRedis redis;
    auto config = testConfig();
    config.redisHost = "127.0.0.1";
    config.redisPort = redis.port();
    lob::Server server(config);
    server.start();
    int fd = connectLocal(server.orderPort());
    sendFrame(fd, lob::MsgType::New, 1, 100, 5, lob::kFlagBuy);
    ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Ack);
    sendFrame(fd, lob::MsgType::New, 2, 100, 2, 0);
    ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Trade);
    sendFrame(fd, lob::MsgType::Cancel, 1, 0, 0, 0);
    ASSERT_TRUE(waitFor([&] { return redis.received().find("DEL") != std::string::npos; }));
    const auto seen = redis.received();
    EXPECT_NE(seen.find(lob::respOrderState(1, true, 100, 5)), std::string::npos);
    EXPECT_NE(seen.find(lob::respOrderQty(1, 3)), std::string::npos);
    EXPECT_NE(seen.find(lob::respOrderDelete(1)), std::string::npos);
    ::close(fd);
    server.stop();
    EXPECT_EQ(server.stats().redisDropped.load(), 0u);
}

TEST(Server, SyncRedisBaselineWritesThroughBeforeAcking) {
    FakeRedis redis;
    auto config = testConfig();
    config.redisHost = "127.0.0.1";
    config.redisPort = redis.port();
    config.syncRedis = true;
    lob::Server server(config);
    server.start();
    int fd = connectLocal(server.orderPort());
    sendFrame(fd, lob::MsgType::New, 1, 100, 5, lob::kFlagBuy);
    ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Ack);
    // In sync mode the write finished before the ack was emitted, so no wait is needed.
    EXPECT_NE(redis.received().find(lob::respOrderState(1, true, 100, 5)), std::string::npos);
    ::close(fd);
}

TEST(Server, EngineKeepsTradingWhenRedisIsDown) {
    auto config = testConfig();
    config.redisHost = "127.0.0.1";
    config.redisPort = 1;  // nothing listens here
    lob::Server server(config);
    server.start();
    int fd = connectLocal(server.orderPort());
    sendFrame(fd, lob::MsgType::New, 1, 100, 5, lob::kFlagBuy);
    ASSERT_EQ(readFrame(fd)->type, lob::MsgType::Ack);
    ::close(fd);
    server.stop();
    EXPECT_EQ(server.stats().redisCommands.load(), 0u);
    EXPECT_GE(server.stats().redisDropped.load(), 1u);
}

TEST(Server, SyncRedisRefusesToStartWithoutRedis) {
    auto config = testConfig();
    config.redisHost = "127.0.0.1";
    config.redisPort = 1;
    config.syncRedis = true;
    lob::Server server(config);
    EXPECT_THROW(server.start(), std::runtime_error);
}

TEST(Server, PortInUseIsReportedNotSwallowed) {
    lob::Server first(testConfig());
    first.start();
    auto config = testConfig();
    config.orderPort = first.orderPort();
    lob::Server second(config);
    EXPECT_THROW(second.start(), std::runtime_error);
}
