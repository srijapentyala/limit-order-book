#include "lob/server.hpp"

#include "lob/codec.hpp"
#include "lob/feed.hpp"
#include "lob/redis_resp.hpp"
#include "lob/ring_buffer.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace lob {
namespace {

constexpr std::size_t kInboundSlots = 1u << 16;
constexpr std::size_t kOutboundSlots = 1u << 18;
constexpr std::size_t kPublisherBatch = 4096;

uint64_t nowNs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

void setNonBlocking(int fd) { ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }

void setNoDelay(int fd) {
    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
}

int openListener(uint16_t port, bool loopbackOnly, uint16_t& boundPort) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd, 64) != 0) {
        const std::string why = std::strerror(errno);
        ::close(fd);
        throw std::runtime_error("cannot listen on port " + std::to_string(port) + ": " + why);
    }
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    boundPort = ntohs(addr.sin_port);
    setNonBlocking(fd);
    return fd;
}

int connectTo(const std::string& host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0) return -1;
    int fd = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (fd >= 0 && ::connect(fd, result->ai_addr, result->ai_addrlen) != 0) {
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(result);
    if (fd >= 0) setNoDelay(fd);
    return fd;
}

// A socket that is written to without ever waiting. Unsent bytes stay queued up to a
// limit, and past that limit append() fails so the caller can drop the peer.
class TxConn {
public:
    explicit TxConn(int fd = -1) : fd_(fd) {}

    int fd() const { return fd_; }
    std::size_t pending() const { return dead_ ? 0 : buf_.size() - off_; }
    bool dead() const { return dead_; }

    // Stops all traffic but keeps the descriptor open. shutdown() makes the gateway's
    // poll report the socket, and the gateway then sends ClientDown, which is what
    // finally closes it. Closing here would free the fd number while the gateway still
    // polls it, and a new connection could inherit it and receive the wrong replies.
    void kill() {
        dead_ = true;
        buf_.clear();
        off_ = 0;
        ::shutdown(fd_, SHUT_RDWR);
    }

    bool append(const void* data, std::size_t n, std::size_t limit) {
        if (pending() + n > limit) return false;
        const auto* p = static_cast<const uint8_t*>(data);
        buf_.insert(buf_.end(), p, p + n);
        return true;
    }

    bool flush() {
        while (off_ < buf_.size()) {
            const ssize_t sent = ::send(fd_, buf_.data() + off_, buf_.size() - off_, 0);
            if (sent > 0) {
                off_ += static_cast<std::size_t>(sent);
            } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return true;
            } else if (sent < 0 && errno == EINTR) {
                continue;
            } else {
                return false;
            }
        }
        buf_.clear();
        off_ = 0;
        return true;
    }

private:
    int fd_;
    std::vector<uint8_t> buf_;
    std::size_t off_ = 0;
    bool dead_ = false;
};

enum class InKind : uint8_t { New, Cancel, ClientUp, ClientDown, FeedUp, Stop };

struct Inbound {
    InKind kind = InKind::Stop;
    int fd = -1;
    uint32_t client = 0;
    Order order;
};

enum class OutKind : uint8_t { Ack, Reject, CancelAck, CancelReject, Trade, Add, Delete, ClientUp, ClientDown, FeedUp, Stop };

struct Outbound {
    OutKind kind = OutKind::Stop;
    bool buy = false;
    uint8_t status = 0;
    int fd = -1;
    uint32_t client = 0;
    uint32_t makerClient = 0;
    uint64_t id = 0;
    uint64_t makerId = 0;
    uint64_t tsNs = 0;
    uint32_t price = 0;
    uint32_t qty = 0;
    uint32_t remaining = 0;
};

// Writes order state to Redis over one pipelined connection. The cache is best effort,
// so when Redis falls behind or goes away, commands are counted and dropped; it can
// never slow the publisher.
class RedisSink {
public:
    RedisSink(std::string host, uint16_t port, std::size_t limit, ServerStats& stats)
        : host_(std::move(host)), port_(port), limit_(limit), stats_(stats) {
        reconnect();
    }
    ~RedisSink() {
        if (conn_.fd() >= 0) ::close(conn_.fd());
    }

    bool connected() const { return conn_.fd() >= 0; }

    void command(const std::string& cmd) {
        if (!connected() || !conn_.append(cmd.data(), cmd.size(), limit_)) {
            stats_.redisDropped.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        stats_.redisCommands.fetch_add(1, std::memory_order_relaxed);
    }

    void flush() {
        if (!connected()) {
            const auto now = std::chrono::steady_clock::now();
            if (now - lastAttempt_ > std::chrono::seconds(5)) reconnect();
            return;
        }
        bool alive = conn_.flush();
        char scratch[16384];
        while (alive) {
            const ssize_t got = ::recv(conn_.fd(), scratch, sizeof(scratch), MSG_DONTWAIT);
            if (got > 0) continue;
            if (got == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) alive = false;
            break;
        }
        if (!alive) {
            ::close(conn_.fd());
            conn_ = TxConn();
            lastAttempt_ = std::chrono::steady_clock::now();
        }
    }

private:
    void reconnect() {
        lastAttempt_ = std::chrono::steady_clock::now();
        const int fd = connectTo(host_, port_);
        if (fd < 0) return;
        setNonBlocking(fd);
        conn_ = TxConn(fd);
    }

    std::string host_;
    uint16_t port_;
    std::size_t limit_;
    ServerStats& stats_;
    TxConn conn_;
    std::chrono::steady_clock::time_point lastAttempt_{};
};

// The naive design kept as a measurable baseline: the matcher waits for Redis to
// acknowledge each book change before it can process the next order.
class BlockingRedis {
public:
    BlockingRedis(const std::string& host, uint16_t port) : fd_(connectTo(host, port)) {
        if (fd_ < 0) throw std::runtime_error("sync redis: cannot connect to " + host);
    }
    ~BlockingRedis() { ::close(fd_); }

    void command(const std::string& cmd) {
        std::size_t put = 0;
        while (put < cmd.size()) {
            const ssize_t sent = ::send(fd_, cmd.data() + put, cmd.size() - put, 0);
            if (sent <= 0) return;
            put += static_cast<std::size_t>(sent);
        }
        char reply[256];
        while (true) {
            const ssize_t got = ::recv(fd_, reply, sizeof(reply), 0);
            if (got <= 0) return;
            if (got >= 2 && reply[got - 2] == '\r' && reply[got - 1] == '\n') return;
        }
    }

private:
    int fd_;
};

}  // namespace

struct Server::Impl {
    SpscRing<Inbound, kInboundSlots> inbound;
    SpscRing<Outbound, kOutboundSlots> outbound;
    std::atomic<bool> stopping{false};
    std::thread gateway;
    std::thread matcher;
    std::thread publisher;
    int orderListener = -1;
    int feedListener = -1;
};

namespace {

template <typename Ring, typename T>
void pushBlocking(Ring& ring, const T& item) {
    int spins = 0;
    while (!ring.push(item)) {
        if (++spins < 256) cpuRelax();
        else std::this_thread::yield();
    }
}

// Spin for low wake-up latency, then back off so an idle engine does not hold a core.
class IdleWait {
public:
    void reset() { idle_ = 0; }
    void wait() {
        ++idle_;
        if (idle_ < 20000) cpuRelax();
        else if (idle_ < 40000) std::this_thread::yield();
        else std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

private:
    uint64_t idle_ = 0;
};

void runGateway(Server::Impl& impl, ServerStats& stats) {
    struct Client {
        uint32_t id;
        uint8_t partial[kWireSize];
        std::size_t have = 0;
    };

    std::vector<pollfd> fds;
    fds.push_back(pollfd{impl.orderListener, POLLIN, 0});
    fds.push_back(pollfd{impl.feedListener, POLLIN, 0});
    std::unordered_map<int, Client> clients;
    uint32_t nextClient = 1;
    std::vector<uint8_t> scratch(1 << 16);

    const auto handleFrame = [&](const uint8_t* frame, uint32_t clientId) -> bool {
        const auto msg = decode(frame, kWireSize);
        if (!msg) return false;
        Inbound in;
        in.client = clientId;
        in.order.id = msg->id;
        in.order.owner = clientId;
        if (msg->type == MsgType::New) {
            in.kind = InKind::New;
            in.order.price = msg->price;
            in.order.qty = msg->qty;
            in.order.buy = (msg->flags & kFlagBuy) != 0;
            in.order.market = (msg->flags & kFlagMarket) != 0;
        } else if (msg->type == MsgType::Cancel) {
            in.kind = InKind::Cancel;
        } else {
            return false;
        }
        pushBlocking(impl.inbound, in);
        return true;
    };

    while (!impl.stopping.load(std::memory_order_acquire)) {
        const int ready = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), 10);
        if (ready <= 0) continue;

        std::vector<int> dropped;
        for (std::size_t i = 2; i < fds.size(); ++i) {
            if ((fds[i].revents & (POLLIN | POLLHUP | POLLERR)) == 0) continue;
            const int fd = fds[i].fd;
            Client& client = clients.at(fd);
            bool drop = false;
            const ssize_t got = ::recv(fd, scratch.data(), scratch.size(), 0);
            if (got == 0) {
                drop = true;
            } else if (got < 0) {
                drop = errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR;
            } else {
                std::size_t used = 0;
                const auto n = static_cast<std::size_t>(got);
                if (client.have > 0) {
                    const std::size_t take = std::min(kWireSize - client.have, n);
                    std::memcpy(client.partial + client.have, scratch.data(), take);
                    client.have += take;
                    used = take;
                    if (client.have == kWireSize) {
                        client.have = 0;
                        if (!handleFrame(client.partial, client.id)) drop = true;
                    }
                }
                while (!drop && n - used >= kWireSize) {
                    if (!handleFrame(scratch.data() + used, client.id)) drop = true;
                    used += kWireSize;
                }
                if (!drop && used < n) {
                    client.have = n - used;
                    std::memcpy(client.partial, scratch.data() + used, client.have);
                }
                if (drop) stats.malformedClients.fetch_add(1, std::memory_order_relaxed);
            }
            if (drop) dropped.push_back(fd);
        }

        // The publisher closes the descriptor once it has seen ClientDown, which comes
        // after every reply to this client. The number therefore cannot be reused while
        // a stale reply is still on its way to it.
        for (int fd : dropped) {
            Inbound in;
            in.kind = InKind::ClientDown;
            in.client = clients.at(fd).id;
            ::shutdown(fd, SHUT_RD);
            clients.erase(fd);
            fds.erase(std::find_if(fds.begin() + 2, fds.end(), [fd](const pollfd& p) { return p.fd == fd; }));
            pushBlocking(impl.inbound, in);
        }

        if ((fds[0].revents & POLLIN) != 0) {
            while (true) {
                const int fd = ::accept(impl.orderListener, nullptr, nullptr);
                if (fd < 0) break;
                setNonBlocking(fd);
                setNoDelay(fd);
                Client client;
                client.id = nextClient++;
                Inbound in;
                in.kind = InKind::ClientUp;
                in.client = client.id;
                in.fd = fd;
                clients.emplace(fd, client);
                fds.push_back(pollfd{fd, POLLIN, 0});
                pushBlocking(impl.inbound, in);
            }
        }
        if ((fds[1].revents & POLLIN) != 0) {
            while (true) {
                const int fd = ::accept(impl.feedListener, nullptr, nullptr);
                if (fd < 0) break;
                setNonBlocking(fd);
                setNoDelay(fd);
                Inbound in;
                in.kind = InKind::FeedUp;
                in.fd = fd;
                pushBlocking(impl.inbound, in);
            }
        }
    }

    Inbound stop;
    stop.kind = InKind::Stop;
    pushBlocking(impl.inbound, stop);
}

void runMatcher(Server::Impl& impl, const ServerConfig& config, ServerStats& stats) {
    OrderBook book(config.book);
    std::unique_ptr<BlockingRedis> blocking;
    if (config.syncRedis) blocking = std::make_unique<BlockingRedis>(config.redisHost, config.redisPort);
    IdleWait idle;
    bool running = true;

    const auto emit = [&](const Outbound& out) { pushBlocking(impl.outbound, out); };

    const auto handle = [&](const Inbound& in) {
        Outbound out;
        out.client = in.client;
        out.fd = in.fd;
        switch (in.kind) {
            case InKind::New: {
                stats.newOrders.fetch_add(1, std::memory_order_relaxed);
                const auto result = book.submit(in.order, [&](const Trade& trade) {
                    Outbound fill;
                    fill.kind = OutKind::Trade;
                    fill.client = in.client;
                    fill.makerClient = trade.makerOwner;
                    fill.id = trade.takerId;
                    fill.makerId = trade.makerId;
                    fill.price = trade.price;
                    fill.qty = trade.qty;
                    fill.remaining = trade.makerRemaining;
                    fill.buy = trade.takerBuy;
                    fill.tsNs = nowNs();
                    stats.trades.fetch_add(1, std::memory_order_relaxed);
                    if (blocking) {
                        blocking->command(trade.makerRemaining == 0 ? respOrderDelete(trade.makerId)
                                                                    : respOrderQty(trade.makerId, trade.makerRemaining));
                    }
                    emit(fill);
                });
                if (result.resting > 0) {
                    Outbound add;
                    add.kind = OutKind::Add;
                    add.id = in.order.id;
                    add.price = in.order.price;
                    add.qty = result.resting;
                    add.buy = in.order.buy;
                    add.tsNs = nowNs();
                    if (blocking) blocking->command(respOrderState(add.id, add.buy, add.price, add.qty));
                    emit(add);
                }
                out.id = in.order.id;
                out.price = in.order.price;
                if (result.status == OrderBook::Status::Accepted) {
                    out.kind = OutKind::Ack;
                    out.qty = result.resting;
                } else {
                    out.kind = OutKind::Reject;
                    out.status = static_cast<uint8_t>(result.status);
                    out.qty = result.filled;
                    stats.rejects.fetch_add(1, std::memory_order_relaxed);
                }
                emit(out);
                break;
            }
            case InKind::Cancel: {
                stats.cancels.fetch_add(1, std::memory_order_relaxed);
                out.id = in.order.id;
                const auto removed = book.cancelOrder(in.order.id);
                if (removed) {
                    Outbound del;
                    del.kind = OutKind::Delete;
                    del.id = in.order.id;
                    del.price = removed->price;
                    del.qty = removed->qty;
                    del.buy = removed->buy;
                    del.tsNs = nowNs();
                    if (blocking) blocking->command(respOrderDelete(del.id));
                    emit(del);
                    out.kind = OutKind::CancelAck;
                    out.price = removed->price;
                    out.qty = removed->qty;
                } else {
                    out.kind = OutKind::CancelReject;
                    stats.rejects.fetch_add(1, std::memory_order_relaxed);
                }
                emit(out);
                break;
            }
            case InKind::ClientUp: out.kind = OutKind::ClientUp; emit(out); break;
            case InKind::ClientDown: out.kind = OutKind::ClientDown; emit(out); break;
            case InKind::FeedUp: out.kind = OutKind::FeedUp; emit(out); break;
            case InKind::Stop: out.kind = OutKind::Stop; emit(out); running = false; break;
        }
    };

    while (running) {
        if (impl.inbound.consume(256, handle) > 0) idle.reset();
        else idle.wait();
    }
}

void runPublisher(Server::Impl& impl, const ServerConfig& config, ServerStats& stats) {
    std::unordered_map<uint32_t, TxConn> clients;
    std::vector<TxConn> feeds;
    std::unique_ptr<RedisSink> redis;
    if (!config.redisHost.empty() && !config.syncRedis) {
        redis = std::make_unique<RedisSink>(config.redisHost, config.redisPort, config.maxRedisBuffer, stats);
    }
    std::ofstream tradeLog;
    if (!config.tradeLog.empty()) tradeLog.open(config.tradeLog, std::ios::app);
    uint64_t seq = 0;
    IdleWait idle;
    bool running = true;
    bool wroteTrades = false;
    std::vector<int> deadFeeds;

    const auto sendToClient = [&](uint32_t id, MsgType type, uint64_t orderId, uint32_t price, uint32_t qty, uint8_t flags) {
        auto it = clients.find(id);
        if (it == clients.end() || it->second.dead()) return;
        const auto frame = encode(type, orderId, price, qty, flags);
        if (!it->second.append(frame.data(), frame.size(), config.maxClientBuffer)) {
            stats.slowClientDrops.fetch_add(1, std::memory_order_relaxed);
            it->second.kill();
        }
    };

    const auto publish = [&](FeedType type, const Outbound& ev) {
        FeedFrame frame;
        frame.seq = ++seq;
        frame.tsNs = ev.tsNs;
        frame.id = ev.id;
        frame.makerId = ev.makerId;
        frame.price = ev.price;
        frame.qty = ev.qty;
        frame.remaining = ev.remaining;
        frame.type = type;
        frame.buy = ev.buy;
        const auto bytes = encodeFeed(frame);
        stats.feedFrames.fetch_add(1, std::memory_order_relaxed);
        for (auto& feed : feeds) {
            if (!feed.append(bytes.data(), bytes.size(), config.maxClientBuffer)) deadFeeds.push_back(feed.fd());
        }
    };

    const auto handle = [&](const Outbound& ev) {
        switch (ev.kind) {
            case OutKind::Ack:
                sendToClient(ev.client, MsgType::Ack, ev.id, ev.price, ev.qty, 0);
                break;
            case OutKind::Reject:
                sendToClient(ev.client, MsgType::Reject, ev.id, ev.price, ev.qty, ev.status);
                break;
            case OutKind::CancelAck:
                sendToClient(ev.client, MsgType::CancelAck, ev.id, ev.price, ev.qty, 0);
                break;
            case OutKind::CancelReject:
                sendToClient(ev.client, MsgType::Reject, ev.id, 0, 0, 0);
                break;
            case OutKind::Trade:
                sendToClient(ev.client, MsgType::Trade, ev.id, ev.price, ev.qty, ev.buy ? kFlagBuy : 0);
                sendToClient(ev.makerClient, MsgType::Trade, ev.makerId, ev.price, ev.qty,
                             static_cast<uint8_t>(kFlagMaker | (ev.buy ? 0 : kFlagBuy)));
                publish(FeedType::Trade, ev);
                if (redis) {
                    redis->command(ev.remaining == 0 ? respOrderDelete(ev.makerId) : respOrderQty(ev.makerId, ev.remaining));
                }
                if (tradeLog) {
                    tradeLog << "{\"taker\":" << ev.id << ",\"maker\":" << ev.makerId << ",\"price\":" << ev.price
                             << ",\"qty\":" << ev.qty << "}\n";
                    wroteTrades = true;
                }
                break;
            case OutKind::Add:
                publish(FeedType::Add, ev);
                if (redis) redis->command(respOrderState(ev.id, ev.buy, ev.price, ev.qty));
                break;
            case OutKind::Delete:
                publish(FeedType::Delete, ev);
                if (redis) redis->command(respOrderDelete(ev.id));
                break;
            case OutKind::ClientUp:
                clients.emplace(ev.client, TxConn(ev.fd));
                break;
            case OutKind::ClientDown: {
                auto it = clients.find(ev.client);
                if (it != clients.end()) {
                    if (!it->second.dead()) it->second.flush();
                    ::close(it->second.fd());
                    clients.erase(it);
                }
                break;
            }
            case OutKind::FeedUp:
                feeds.emplace_back(ev.fd);
                break;
            case OutKind::Stop:
                running = false;
                break;
        }
    };

    const auto dropFeed = [&](int fd) {
        auto it = std::find_if(feeds.begin(), feeds.end(), [fd](const TxConn& c) { return c.fd() == fd; });
        if (it != feeds.end()) {
            stats.slowClientDrops.fetch_add(1, std::memory_order_relaxed);
            ::close(fd);
            feeds.erase(it);
        }
    };

    const auto flushAll = [&] {
        for (auto& [id, conn] : clients) {
            if (!conn.dead() && !conn.flush()) conn.kill();
        }
        for (auto& feed : feeds) {
            if (!feed.flush()) deadFeeds.push_back(feed.fd());
        }
        for (int fd : deadFeeds) dropFeed(fd);
        deadFeeds.clear();
        if (redis) redis->flush();
        if (wroteTrades) {
            tradeLog.flush();
            wroteTrades = false;
        }
    };

    auto lastFlush = std::chrono::steady_clock::now();
    while (running) {
        if (impl.outbound.consume(kPublisherBatch, handle) > 0) {
            idle.reset();
            flushAll();
            lastFlush = std::chrono::steady_clock::now();
        } else {
            // Retries bytes the kernel refused earlier even when no new event arrives.
            const auto now = std::chrono::steady_clock::now();
            if (now - lastFlush > std::chrono::milliseconds(1)) {
                flushAll();
                lastFlush = now;
            }
            idle.wait();
        }
    }
    // Shutdown: give subscribers a bounded chance to drain before their sockets close.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (true) {
        flushAll();
        bool pending = false;
        for (const auto& [id, conn] : clients) pending = pending || conn.pending() > 0;
        for (const auto& feed : feeds) pending = pending || feed.pending() > 0;
        if (!pending || std::chrono::steady_clock::now() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (auto& [id, conn] : clients) ::close(conn.fd());
    for (auto& feed : feeds) ::close(feed.fd());
}

}  // namespace

Server::Server(ServerConfig config) : config_(std::move(config)), impl_(std::make_unique<Impl>()) {}

Server::~Server() { stop(); }

void Server::start() {
    if (started_) return;
    if (config_.syncRedis && config_.redisHost.empty()) throw std::runtime_error("sync redis needs a redis host");
    if (config_.syncRedis) {
        // Fails here, on the caller's thread, rather than later inside the matcher.
        BlockingRedis probe(config_.redisHost, config_.redisPort);
    }
    ::signal(SIGPIPE, SIG_IGN);
    impl_->orderListener = openListener(config_.orderPort, config_.loopbackOnly, orderPort_);
    try {
        impl_->feedListener = openListener(config_.feedPort, config_.loopbackOnly, feedPort_);
    } catch (...) {
        ::close(impl_->orderListener);
        throw;
    }
    started_ = true;
    impl_->publisher = std::thread(runPublisher, std::ref(*impl_), std::cref(config_), std::ref(stats_));
    impl_->matcher = std::thread(runMatcher, std::ref(*impl_), std::cref(config_), std::ref(stats_));
    impl_->gateway = std::thread(runGateway, std::ref(*impl_), std::ref(stats_));
}

void Server::stop() {
    if (!started_) return;
    started_ = false;
    impl_->stopping.store(true, std::memory_order_release);
    impl_->gateway.join();
    impl_->matcher.join();
    impl_->publisher.join();
    ::close(impl_->orderListener);
    ::close(impl_->feedListener);
}

}  // namespace lob
