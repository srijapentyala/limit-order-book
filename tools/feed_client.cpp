// Subscribes to the binary feed, keeps a replica book, and prints the top of book once a
// second plus a summary on exit. With --jsonl it prints each trade as a JSON line.
#include "lob/feed.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    uint16_t port = 9001;
    bool jsonl = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--host" && i + 1 < argc) host = argv[++i];
        else if (a == "--port" && i + 1 < argc) port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (a == "--jsonl") jsonl = true;
        else {
            std::cerr << "usage: feed_client [--host H] [--port P] [--jsonl]\n";
            return 2;
        }
    }

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (fd < 0 || ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "cannot connect to " << host << ":" << port << "\n";
        return 1;
    }

    lob::FeedHandler handler;
    if (jsonl) {
        handler.onTrade([](const lob::FeedFrame& f) {
            std::cout << "{\"seq\":" << f.seq << ",\"taker\":" << f.id << ",\"maker\":" << f.makerId
                      << ",\"price\":" << f.price << ",\"qty\":" << f.qty << "}\n";
        });
    }

    std::vector<uint8_t> buf(1 << 16);
    auto last = std::chrono::steady_clock::now();
    while (true) {
        const ssize_t got = ::recv(fd, buf.data(), buf.size(), 0);
        if (got <= 0) break;
        handler.onBytes(buf.data(), static_cast<std::size_t>(got));
        const auto now = std::chrono::steady_clock::now();
        if (!jsonl && now - last >= std::chrono::seconds(1)) {
            last = now;
            std::cout << "seq=" << handler.lastSeq() << " bid=" << handler.bestBid().value_or(0)
                      << " ask=" << handler.bestAsk().value_or(0) << " live=" << handler.liveOrders()
                      << " gaps=" << handler.gaps() << std::endl;
        }
    }
    ::close(fd);
    std::cerr << "frames=" << handler.frames() << " trades=" << handler.trades() << " gaps=" << handler.gaps()
              << " inconsistencies=" << handler.inconsistencies() << "\n";
    return handler.gaps() == 0 && handler.inconsistencies() == 0 ? 0 : 1;
}
