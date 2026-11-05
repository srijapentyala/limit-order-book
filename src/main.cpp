#include "lob/server.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {
std::atomic<bool> gStop{false};
void onSignal(int) { gStop.store(true); }

void usage() {
    std::cerr << "usage: engine [--port N] [--feed-port N] [--loopback] [--trade-log FILE]\n"
                 "              [--redis HOST[:PORT]] [--sync-redis]\n"
                 "              [--max-price TICKS] [--max-orders N]\n";
}
}  // namespace

int main(int argc, char** argv) {
    lob::ServerConfig config;
    config.book.maxPrice = (1u << 20) - 1;
    config.book.maxOrders = 1u << 21;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string a = argv[i];
            const auto value = [&]() -> std::string {
                if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
                return argv[++i];
            };
            if (a == "--port") config.orderPort = static_cast<uint16_t>(std::stoi(value()));
            else if (a == "--feed-port") config.feedPort = static_cast<uint16_t>(std::stoi(value()));
            else if (a == "--loopback") config.loopbackOnly = true;
            else if (a == "--trade-log") config.tradeLog = value();
            else if (a == "--sync-redis") config.syncRedis = true;
            else if (a == "--max-price") config.book.maxPrice = static_cast<uint32_t>(std::stoul(value()));
            else if (a == "--max-orders") config.book.maxOrders = std::stoul(value());
            else if (a == "--redis") {
                const std::string target = value();
                const auto colon = target.find(':');
                config.redisHost = target.substr(0, colon);
                if (colon != std::string::npos) config.redisPort = static_cast<uint16_t>(std::stoi(target.substr(colon + 1)));
            } else {
                usage();
                return 2;
            }
        }

        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);

        lob::Server server(config);
        server.start();
        std::cout << "engine listening order=" << server.orderPort() << " feed=" << server.feedPort() << std::endl;
        while (!gStop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        server.stop();

        const auto& s = server.stats();
        std::cout << "{\"new\":" << s.newOrders << ",\"cancels\":" << s.cancels << ",\"rejects\":" << s.rejects
                  << ",\"trades\":" << s.trades << ",\"feed_frames\":" << s.feedFrames
                  << ",\"redis_commands\":" << s.redisCommands << ",\"redis_dropped\":" << s.redisDropped
                  << ",\"slow_client_drops\":" << s.slowClientDrops << ",\"malformed_clients\":" << s.malformedClients
                  << "}" << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "engine: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
