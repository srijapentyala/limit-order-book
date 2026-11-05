// TCP load generator. Each connection gets a sender and a receiver thread.
//
// With --rate R it is open loop: messages are due at a fixed schedule, and latency is
// measured from when a message was due, not when it was sent, so an engine stall counts
// against every message queued behind it. With --rate 0 it runs flat out, bounded by
// --window messages in flight, which measures capacity rather than latency under load.
//
// The engine answers every message with exactly one Ack, CancelAck, or Reject, in order
// per connection, so the i-th reply belongs to the i-th message sent.
#include "lob/codec.hpp"
#include "lob/flow.hpp"
#include "lob/ring_buffer.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

struct Options {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;
    std::size_t orders = 1000000;
    double rate = 0;
    std::size_t window = 4096;
    std::size_t batch = 32;
    int conns = 1;
    uint64_t seed = 7;
    bool timeline = false;
    int64_t coalesceUs = 0;
};

int connectTcp(const Options& opt) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(opt.port);
    ::inet_pton(AF_INET, opt.host.c_str(), &addr.sin_addr);
    if (fd < 0 || ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "cannot connect to " << opt.host << ":" << opt.port << "\n";
        std::exit(1);
    }
    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    return fd;
}

bool writeAll(int fd, const uint8_t* data, std::size_t n) {
    std::size_t put = 0;
    while (put < n) {
        const ssize_t sent = ::send(fd, data + put, n - put, 0);
        if (sent <= 0) return false;
        put += static_cast<std::size_t>(sent);
    }
    return true;
}

struct Conn {
    std::vector<lob::FlowEvent> flow;
    std::vector<std::atomic<int64_t>> due;
    std::vector<uint32_t> latencyNs;
    std::atomic<std::size_t> replied{0};
    uint64_t trades = 0;
    uint64_t rejects = 0;
    bool failed = false;
    explicit Conn(std::size_t n) : due(n) {}
};

void sender(int fd, Conn& conn, const Options& opt, int64_t startNs, double perConnRate) {
    std::vector<uint8_t> out;
    out.reserve(opt.batch * lob::kWireSize);
    const std::size_t total = conn.flow.size();
    std::size_t i = 0;
    // Nothing is sent before the schedule starts, or a reply could beat its own due time.
    while (nowNs() < startNs) lob::cpuRelax();
    while (i < total) {
        std::size_t end;
        if (perConnRate > 0) {
            const auto dueNow = static_cast<std::size_t>(static_cast<double>(nowNs() - startNs) * perConnRate / 1e9) + 1;
            end = std::min({dueNow, i + opt.batch, total});
            if (end <= i) {
                lob::cpuRelax();
                continue;
            }
            // Hold a short batch back briefly so that one syscall can carry several
            // messages. The wait still counts as latency, since it is measured from the
            // time the message was due.
            if (opt.coalesceUs > 0 && end - i < opt.batch) {
                const auto oldestDue = startNs + static_cast<int64_t>(static_cast<double>(i) * 1e9 / perConnRate);
                if (nowNs() - oldestDue < opt.coalesceUs * 1000) {
                    lob::cpuRelax();
                    continue;
                }
            }
        } else {
            const std::size_t inFlight = i - conn.replied.load(std::memory_order_acquire);
            if (inFlight >= opt.window) {
                lob::cpuRelax();
                continue;
            }
            end = std::min({i + opt.batch, i + opt.window - inFlight, total});
        }
        const int64_t sendNs = nowNs();
        out.clear();
        for (std::size_t k = i; k < end; ++k) {
            const auto& ev = conn.flow[k];
            const int64_t due = perConnRate > 0
                                    ? startNs + static_cast<int64_t>(static_cast<double>(k) * 1e9 / perConnRate)
                                    : sendNs;
            conn.due[k].store(due, std::memory_order_relaxed);
            const uint8_t flags = ev.order.buy ? lob::kFlagBuy : 0;
            const auto frame = ev.cancel ? lob::encode(lob::MsgType::Cancel, ev.order.id, 0, 0, 0)
                                         : lob::encode(lob::MsgType::New, ev.order.id, ev.order.price, ev.order.qty, flags);
            out.insert(out.end(), frame.begin(), frame.end());
        }
        if (!writeAll(fd, out.data(), out.size())) {
            conn.failed = true;
            return;
        }
        i = end;
    }
}

void receiver(int fd, Conn& conn) {
    std::vector<uint8_t> buf(1 << 16);
    std::size_t have = 0;
    std::size_t replied = 0;
    const std::size_t total = conn.flow.size();
    while (replied < total) {
        const ssize_t got = ::recv(fd, buf.data() + have, buf.size() - have, 0);
        if (got <= 0) {
            conn.failed = true;
            return;
        }
        have += static_cast<std::size_t>(got);
        const int64_t t = nowNs();
        std::size_t used = 0;
        while (have - used >= lob::kWireSize) {
            const auto type = static_cast<lob::MsgType>(buf[used]);
            used += lob::kWireSize;
            if (type == lob::MsgType::Trade) {
                ++conn.trades;
                continue;
            }
            if (type == lob::MsgType::Reject) ++conn.rejects;
            const int64_t due = conn.due[replied].load(std::memory_order_relaxed);
            conn.latencyNs.push_back(static_cast<uint32_t>(std::clamp<int64_t>(t - due, 0, UINT32_MAX)));
            ++replied;
            conn.replied.store(replied, std::memory_order_release);
        }
        if (used < have) std::memmove(buf.data(), buf.data() + used, have - used);
        have -= used;
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string a = argv[i];
        const std::string v = argv[i + 1];
        if (a == "--host") opt.host = v;
        else if (a == "--port") opt.port = static_cast<uint16_t>(std::stoi(v));
        else if (a == "--orders") opt.orders = std::stoul(v);
        else if (a == "--rate") opt.rate = std::stod(v);
        else if (a == "--window") opt.window = std::stoul(v);
        else if (a == "--batch") opt.batch = std::stoul(v);
        else if (a == "--conns") opt.conns = std::stoi(v);
        else if (a == "--seed") opt.seed = std::stoull(v);
        else if (a == "--timeline") opt.timeline = v == "1";
        else if (a == "--coalesce-us") opt.coalesceUs = std::stoll(v);
        else {
            std::cerr << "usage: loadgen [--host H] [--port P] [--orders N] [--rate EVENTS_PER_SEC]\n"
                         "               [--window N] [--batch N] [--conns N] [--seed S] [--timeline 1] [--coalesce-us N]\n";
            return 2;
        }
    }

    std::vector<std::unique_ptr<Conn>> conns;
    const std::size_t perConn = opt.orders / static_cast<std::size_t>(opt.conns);
    for (int c = 0; c < opt.conns; ++c) {
        auto conn = std::make_unique<Conn>(perConn);
        lob::FlowConfig flow;
        flow.events = perConn;
        flow.seed = opt.seed + static_cast<uint64_t>(c);
        // Each connection needs its own id range, or orders would collide as duplicates.
        flow.idBase = (static_cast<uint64_t>(c) + 1) << 40;
        conn->flow = lob::makeFlow(flow);
        conn->latencyNs.reserve(perConn);
        conns.push_back(std::move(conn));
    }

    const double perConnRate = opt.rate / opt.conns;
    std::vector<int> fds;
    for (int c = 0; c < opt.conns; ++c) fds.push_back(connectTcp(opt));

    const int64_t startNs = nowNs() + 50'000'000;
    const auto wallStart = Clock::now();
    std::vector<std::thread> threads;
    for (int c = 0; c < opt.conns; ++c) {
        threads.emplace_back(receiver, fds[c], std::ref(*conns[c]));
        threads.emplace_back(sender, fds[c], std::ref(*conns[c]), std::cref(opt), startNs, perConnRate);
    }
    for (auto& t : threads) t.join();
    const double elapsed = std::chrono::duration<double>(Clock::now() - wallStart).count() - 0.05;
    for (int fd : fds) ::close(fd);

    if (opt.timeline && opt.rate > 0) {
        // Per-second view of connection 0, on stderr: shows whether delay is a brief stall
        // that drains or a queue that never clears.
        const auto& lat = conns[0]->latencyNs;
        const auto perSecond = static_cast<std::size_t>(perConnRate);
        for (std::size_t from = 0; from < lat.size(); from += perSecond) {
            std::vector<uint32_t> slice(lat.begin() + static_cast<std::ptrdiff_t>(from),
                                        lat.begin() + static_cast<std::ptrdiff_t>(std::min(from + perSecond, lat.size())));
            std::sort(slice.begin(), slice.end());
            std::cerr << "t=" << from / perSecond << "s p50_us=" << lob::percentile(slice, 50) / 1000
                      << " p99_us=" << lob::percentile(slice, 99) / 1000 << " max_us=" << slice.back() / 1000 << "\n";
        }
    }

    std::vector<uint32_t> all;
    uint64_t trades = 0;
    uint64_t rejects = 0;
    bool failed = false;
    for (auto& conn : conns) {
        all.insert(all.end(), conn->latencyNs.begin(), conn->latencyNs.end());
        trades += conn->trades;
        rejects += conn->rejects;
        failed = failed || conn->failed;
    }
    std::sort(all.begin(), all.end());

    const auto us = [&](double q) { return static_cast<double>(lob::percentile(all, q)) / 1000.0; };
    std::cout << "{\"mode\":\"" << (opt.rate > 0 ? "open_loop" : "saturated") << "\",\"target_rate\":" << opt.rate
              << ",\"conns\":" << opt.conns << ",\"replies\":" << all.size() << ",\"trade_frames\":" << trades
              << ",\"rejects\":" << rejects << ",\"seconds\":" << elapsed
              << ",\"msgs_per_sec\":" << static_cast<uint64_t>(static_cast<double>(all.size()) / elapsed)
              << ",\"rtt_us\":{\"p50\":" << us(50) << ",\"p95\":" << us(95) << ",\"p99\":" << us(99)
              << ",\"p999\":" << us(99.9) << ",\"max\":" << us(100) << "}"
              << ",\"failed\":" << (failed ? "true" : "false") << "}" << std::endl;
    return failed ? 1 : 0;
}
