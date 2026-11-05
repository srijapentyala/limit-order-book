#!/usr/bin/env python3
"""Stream trades from the engine's binary feed into a Kafka topic.

The bridge is a normal feed subscriber, so Kafka is never on the matching path. A slow
or missing broker can only make this process fall behind. The engine then drops this
subscriber once its buffer limit is reached and keeps trading.

    python kafka_bridge.py --feed 127.0.0.1:9001 --bootstrap localhost:19092 --topic trades

Without --bootstrap it prints each trade as a JSON line, which is handy for checking the
feed with no broker running. A sequence gap is counted and reported, never hidden.
"""
from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
from dataclasses import dataclass
from typing import Callable, Iterator, Optional

FRAME_SIZE = 48
_FRAME = struct.Struct("<QQQQIIIBBH")
assert _FRAME.size == FRAME_SIZE

ADD, DELETE, TRADE = 1, 2, 3
_TYPE_NAMES = {ADD: "add", DELETE: "delete", TRADE: "trade"}


@dataclass(frozen=True)
class Frame:
    seq: int
    ts_ns: int
    order_id: int
    maker_id: int
    price: int
    qty: int
    remaining: int
    type: int
    buy: bool


def parse_frames(buffer: bytearray) -> Iterator[Frame]:
    """Yield each complete frame and remove it from the buffer; a partial tail stays."""
    whole = len(buffer) - len(buffer) % FRAME_SIZE
    for offset in range(0, whole, FRAME_SIZE):
        seq, ts, oid, maker, price, qty, remaining, kind, side, _ = _FRAME.unpack_from(buffer, offset)
        yield Frame(seq, ts, oid, maker, price, qty, remaining, kind, side != 0)
    del buffer[:whole]


def trade_message(frame: Frame) -> dict:
    return {
        "seq": frame.seq,
        "taker": frame.order_id,
        "maker": frame.maker_id,
        "price": frame.price,
        "qty": frame.qty,
        "taker_side": "buy" if frame.buy else "sell",
        "engine_ts_ns": frame.ts_ns,
    }


class Bridge:
    def __init__(self, publish: Callable[[dict], None]) -> None:
        self.publish = publish
        self.frames = 0
        self.trades = 0
        self.gaps = 0
        self.last_seq = 0
        self._buffer = bytearray()

    def feed(self, data: bytes) -> None:
        self._buffer.extend(data)
        for frame in parse_frames(self._buffer):
            self.frames += 1
            if self.last_seq and frame.seq != self.last_seq + 1:
                self.gaps += 1
            self.last_seq = frame.seq
            if frame.type == TRADE:
                self.trades += 1
                self.publish(trade_message(frame))


def make_publisher(bootstrap: Optional[str], topic: str):
    if not bootstrap:
        return lambda message: print(json.dumps(message), flush=True), lambda: None
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=5,
        value_serializer=lambda m: json.dumps(m).encode(),
        # Trades of one taker stay in order within a partition.
        key_serializer=lambda k: str(k).encode(),
    )
    return lambda m: producer.send(topic, key=m["taker"], value=m), producer.flush


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--feed", default="127.0.0.1:9001", help="engine feed host:port")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers; omit to print")
    parser.add_argument("--topic", default="trades")
    args = parser.parse_args(argv)

    host, _, port = args.feed.partition(":")
    publish, flush = make_publisher(args.bootstrap, args.topic)
    bridge = Bridge(publish)
    with socket.create_connection((host, int(port))) as sock:
        print(f"subscribed to {args.feed}", file=sys.stderr, flush=True)
        try:
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                bridge.feed(data)
        except KeyboardInterrupt:
            pass
    flush()
    print(json.dumps({"frames": bridge.frames, "trades": bridge.trades, "gaps": bridge.gaps}), file=sys.stderr)
    return 0 if bridge.gaps == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
