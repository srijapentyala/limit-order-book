#pragma once
#include "lob/types.hpp"
#include <array>
#include <optional>
#include <utility>

namespace lob {
inline void writeU32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}
inline void writeU64(uint8_t* p, uint64_t v) {
    for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
}
inline uint32_t readU32(const uint8_t* p) {
    return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}
inline uint64_t readU64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= uint64_t(p[i]) << (8 * i);
    return v;
}

// Wire layout, little endian, 18 bytes:
// type u8 | id u64 | price u32 | qty u32 | flags u8
// flags bit0 = buy, bit1 = market. For a trade, id is the taker id.
inline std::array<uint8_t, kWireSize> encode(MsgType type, uint64_t id, uint32_t price, uint32_t qty, uint8_t flags) {
    std::array<uint8_t, kWireSize> out{};
    out[0] = static_cast<uint8_t>(type);
    writeU64(out.data() + 1, id);
    writeU32(out.data() + 9, price);
    writeU32(out.data() + 13, qty);
    out[17] = flags;
    return out;
}
struct Decoded {
    MsgType type;
    uint64_t id;
    uint32_t price;
    uint32_t qty;
    uint8_t flags;
};
inline std::optional<Decoded> decode(const uint8_t* bytes, std::size_t n) {
    if (n < kWireSize) return std::nullopt;
    Decoded msg;
    msg.type = static_cast<MsgType>(bytes[0]);
    msg.id = readU64(bytes + 1);
    msg.price = readU32(bytes + 9);
    msg.qty = readU32(bytes + 13);
    msg.flags = bytes[17];
    return msg;
}
}
