#pragma once
#include "lob/order_book.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace lob {

struct ServerConfig {
    uint16_t orderPort = 9000;  // 0 picks an ephemeral port
    uint16_t feedPort = 9001;
    bool loopbackOnly = false;
    OrderBook::Config book;
    std::string tradeLog;  // JSON lines; empty disables
    std::string redisHost;  // empty disables the order-state cache
    uint16_t redisPort = 6379;
    bool syncRedis = false;  // baseline: block the matcher on a Redis round trip per event
    std::size_t maxClientBuffer = 16u * 1024 * 1024;
    std::size_t maxRedisBuffer = 8u * 1024 * 1024;
};

struct ServerStats {
    std::atomic<uint64_t> newOrders{0};
    std::atomic<uint64_t> cancels{0};
    std::atomic<uint64_t> rejects{0};
    std::atomic<uint64_t> trades{0};
    std::atomic<uint64_t> feedFrames{0};
    std::atomic<uint64_t> malformedClients{0};
    std::atomic<uint64_t> slowClientDrops{0};
    std::atomic<uint64_t> redisCommands{0};
    std::atomic<uint64_t> redisDropped{0};
};

// Three threads joined by two lock-free SPSC rings:
//
//   gateway --(inbound)--> matcher --(outbound)--> publisher
//
// The gateway owns socket reads, the matcher alone touches the book, and the publisher
// owns every socket write plus Redis and the trade log. Nothing on the matcher can block
// on a slow peer: the publisher never waits on a socket, it buffers up to a limit and
// then drops that peer. A full ring back-pressures the stage behind it, and no order is
// ever dropped between stages.
class Server {
public:
    explicit Server(ServerConfig config);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();  // throws std::runtime_error if a port or a required peer is unavailable
    void stop();

    uint16_t orderPort() const { return orderPort_; }
    uint16_t feedPort() const { return feedPort_; }
    const ServerStats& stats() const { return stats_; }

    struct Impl;

private:
    ServerConfig config_;
    ServerStats stats_;
    std::unique_ptr<Impl> impl_;
    uint16_t orderPort_ = 0;
    uint16_t feedPort_ = 0;
    bool started_ = false;
};

}  // namespace lob
