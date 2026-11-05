// In-process benchmarks. No sockets are involved, so these show the cost of the book
// and the rings alone. tools/loadgen.cpp measures the engine through TCP.
#include "lob/flow.hpp"
#include "lob/order_book.hpp"
#include "lob/ring_buffer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

lob::OrderBook::Config bookConfig(std::size_t events) {
    lob::OrderBook::Config cfg;
    cfg.maxPrice = (1u << 20) - 1;
    cfg.maxOrders = std::max<std::size_t>(events, 1u << 16);
    return cfg;
}

uint64_t gTrades = 0;

// Replays the flow once and returns events per second.
double runBook(const std::vector<lob::FlowEvent>& flow) {
    lob::OrderBook book(bookConfig(flow.size()));
    uint64_t trades = 0;
    const auto start = Clock::now();
    for (const auto& ev : flow) {
        if (ev.cancel) book.cancel(ev.order.id);
        else book.submit(ev.order, [&](const lob::Trade&) { ++trades; });
    }
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
    gTrades = trades;
    return static_cast<double>(flow.size()) / seconds;
}

// Times every 8th operation. Reading the clock is itself not free, and on Apple Silicon
// steady_clock only ticks every ~41.7 ns, so values this small are coarse.
std::vector<uint32_t> runBookLatency(const std::vector<lob::FlowEvent>& flow) {
    lob::OrderBook book(bookConfig(flow.size()));
    std::vector<uint32_t> samples;
    samples.reserve(flow.size() / 8 + 1);
    uint64_t trades = 0;
    for (std::size_t i = 0; i < flow.size(); ++i) {
        const auto& ev = flow[i];
        if ((i & 7) == 0) {
            const auto t0 = Clock::now();
            if (ev.cancel) book.cancel(ev.order.id);
            else book.submit(ev.order, [&](const lob::Trade&) { ++trades; });
            samples.push_back(static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count()));
        } else {
            if (ev.cancel) book.cancel(ev.order.id);
            else book.submit(ev.order, [&](const lob::Trade&) { ++trades; });
        }
    }
    std::sort(samples.begin(), samples.end());
    return samples;
}

struct InMsg {
    lob::FlowEvent event;
    int64_t tsNs = 0;
};
struct OutMsg {
    int64_t tsNs = 0;
    uint32_t last = 0;
};

int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

struct PipelineResult {
    double eventsPerSec = 0;
    double elapsed = 0;
    std::vector<uint32_t> latency;
};

// producer --ring--> matcher --ring--> consumer, the same shape as the server without
// the sockets. ratePerSec == 0 drives it flat out. Latency runs from the time a message
// was due to enter the first ring, so a stall counts against every message behind it.
PipelineResult runPipeline(const std::vector<lob::FlowEvent>& flow, double ratePerSec) {
    auto in = std::make_unique<lob::SpscRing<InMsg, 1u << 16>>();
    auto out = std::make_unique<lob::SpscRing<OutMsg, 1u << 16>>();
    PipelineResult result;

    std::thread matcher([&] {
        lob::preferFastCore();
        lob::OrderBook book(bookConfig(flow.size()));
        uint64_t trades = 0;
        std::size_t seen = 0;
        while (seen < flow.size()) {
            auto n = in->consume(256, [&](const InMsg& msg) {
                if (msg.event.cancel) book.cancel(msg.event.order.id);
                else book.submit(msg.event.order, [&](const lob::Trade&) { ++trades; });
                OutMsg reply{msg.tsNs, 0};
                while (!out->push(reply)) lob::cpuRelax();
            });
            if (n == 0) lob::cpuRelax();
            seen += n;
        }
    });

    std::thread consumer([&] {
        lob::preferFastCore();
        result.latency.reserve(flow.size() / 8 + 1);
        std::size_t seen = 0;
        while (seen < flow.size()) {
            auto n = out->consume(256, [&](const OutMsg& msg) {
                if ((seen & 7) == 0) result.latency.push_back(static_cast<uint32_t>(nowNs() - msg.tsNs));
                ++seen;
            });
            if (n == 0) lob::cpuRelax();
        }
    });

    // The matcher allocates its book when the thread starts, so let that finish first
    // rather than counting page faults against the first messages.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const auto start = Clock::now();
    const int64_t startNs = nowNs();
    for (std::size_t i = 0; i < flow.size(); ++i) {
        InMsg msg;
        msg.event = flow[i];
        if (ratePerSec > 0) {
            msg.tsNs = startNs + static_cast<int64_t>(static_cast<double>(i) * 1e9 / ratePerSec);
            while (nowNs() < msg.tsNs) lob::cpuRelax();
        } else {
            msg.tsNs = nowNs();
        }
        while (!in->push(msg)) lob::cpuRelax();
    }
    matcher.join();
    consumer.join();
    result.elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    result.eventsPerSec = static_cast<double>(flow.size()) / result.elapsed;
    std::sort(result.latency.begin(), result.latency.end());
    return result;
}

void printLatency(const char* name, const std::vector<uint32_t>& s) {
    std::cout << "\"" << name << "\":{\"p50\":" << lob::percentile(s, 50) << ",\"p95\":" << lob::percentile(s, 95)
              << ",\"p99\":" << lob::percentile(s, 99) << ",\"p999\":" << lob::percentile(s, 99.9)
              << ",\"max\":" << (s.empty() ? 0 : s.back()) << "}";
}

}  // namespace

int main(int argc, char** argv) {
    lob::preferFastCore();
    lob::FlowConfig cfg;
    cfg.events = 5000000;
    double paced = 500000;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string a = argv[i];
        if (a == "--events") cfg.events = std::stoul(argv[i + 1]);
        else if (a == "--seed") cfg.seed = std::stoull(argv[i + 1]);
        else if (a == "--rate") paced = std::stod(argv[i + 1]);
        else {
            std::cerr << "usage: bench [--events N] [--seed S] [--rate EVENTS_PER_SEC]\n";
            return 2;
        }
    }

    const auto flow = lob::makeFlow(cfg);
    std::vector<double> rates;
    for (int run = 0; run < 5; ++run) rates.push_back(runBook(flow));
    std::sort(rates.begin(), rates.end());
    const auto latency = runBookLatency(flow);
    const auto saturated = runPipeline(flow, 0);
    const auto steady = runPipeline(flow, paced);

    std::cout << "{\"events\":" << flow.size() << ",\"seed\":" << cfg.seed << ",\"trades\":" << gTrades
              << ",\"book_events_per_sec\":{\"median\":" << static_cast<uint64_t>(rates[2])
              << ",\"min\":" << static_cast<uint64_t>(rates[0]) << ",\"max\":" << static_cast<uint64_t>(rates[4]) << "},";
    printLatency("book_op_ns", latency);
    std::cout << ",\"pipeline_saturated\":{\"events_per_sec\":" << static_cast<uint64_t>(saturated.eventsPerSec) << ",";
    printLatency("latency_ns", saturated.latency);
    std::cout << "},\"pipeline_paced\":{\"target_rate\":" << static_cast<uint64_t>(paced)
              << ",\"events_per_sec\":" << static_cast<uint64_t>(steady.eventsPerSec) << ",";
    printLatency("latency_ns", steady.latency);
    std::cout << "}}" << std::endl;
    return 0;
}
