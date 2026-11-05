#pragma once
#include <cstdint>
#include <string>

namespace lob {

inline std::string respBulk(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
inline std::string respSet(const std::string& key, const std::string& value) {
    return "*3\r\n" + respBulk("SET") + respBulk(key) + respBulk(value);
}
inline std::string respDel(const std::string& key) {
    return "*2\r\n" + respBulk("DEL") + respBulk(key);
}
// The order-state cache: one hash per resting order, deleted when it leaves the book.
inline std::string respOrderState(uint64_t id, bool buy, uint32_t price, uint32_t qty) {
    return "*8\r\n" + respBulk("HSET") + respBulk("order:" + std::to_string(id)) +
           respBulk("side") + respBulk(buy ? "buy" : "sell") +
           respBulk("price") + respBulk(std::to_string(price)) +
           respBulk("qty") + respBulk(std::to_string(qty));
}
inline std::string respOrderQty(uint64_t id, uint32_t qty) {
    return "*4\r\n" + respBulk("HSET") + respBulk("order:" + std::to_string(id)) +
           respBulk("qty") + respBulk(std::to_string(qty));
}
inline std::string respOrderDelete(uint64_t id) {
    return respDel("order:" + std::to_string(id));
}

}  // namespace lob
