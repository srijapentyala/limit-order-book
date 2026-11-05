#pragma once
#include <cstddef>
#include <cstdint>

namespace lob {

struct Order {
    uint64_t id = 0;
    uint32_t price = 0;
    uint32_t qty = 0;
    bool buy = true;
    bool market = false;
    uint32_t owner = 0;  // gateway client that submitted the order
};

struct Trade {
    uint64_t takerId = 0;
    uint64_t makerId = 0;
    uint32_t price = 0;
    uint32_t qty = 0;
    uint32_t makerRemaining = 0;
    uint32_t makerOwner = 0;
    bool takerBuy = true;
};

enum class MsgType : uint8_t {
    New = 1,        // client -> engine
    Cancel = 2,     // client -> engine
    Trade = 3,      // engine -> client: a fill on one of your orders
    Ack = 4,        // engine -> client: order processed, qty = resting remainder
    Reject = 5,     // engine -> client: flags = OrderBook::Status
    CancelAck = 6,  // engine -> client: qty = quantity removed from the book
};

constexpr std::size_t kWireSize = 18;  // type u8, id u64, price u32, qty u32, flags u8

constexpr uint8_t kFlagBuy = 1;
constexpr uint8_t kFlagMarket = 2;
constexpr uint8_t kFlagMaker = 4;  // on a Trade frame: the id is your resting order

}  // namespace lob
