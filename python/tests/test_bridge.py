"""Runs the real engine binary and checks the bridge decodes what it actually publishes."""
import os
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

import kafka_bridge as kb  # noqa: E402

ENGINE = Path(os.environ.get("LOB_ENGINE", ROOT / "build" / "engine"))
WIRE = struct.Struct("<BQIIB")
NEW, CANCEL = 1, 2
BUY = 1


def frame(seq, kind=kb.TRADE, order_id=2, maker=1, price=100, qty=5, remaining=0, buy=True):
    return kb._FRAME.pack(seq, 123, order_id, maker, price, qty, remaining, kind, 1 if buy else 0, 0)


def test_the_pinned_kafka_client_imports_on_this_python():
    # The client is imported lazily, so without this a broken pin only shows up at run time.
    from kafka import KafkaProducer  # noqa: F401


def test_parse_keeps_a_partial_tail_for_the_next_read():
    data = bytearray(frame(1) + frame(2)[:10])
    frames = list(kb.parse_frames(data))
    assert [f.seq for f in frames] == [1]
    assert len(data) == 10


def test_bridge_publishes_only_trades():
    published = []
    bridge = kb.Bridge(published.append)
    bridge.feed(frame(1, kind=kb.ADD) + frame(2, kind=kb.DELETE) + frame(3, kind=kb.TRADE))
    assert bridge.frames == 3
    assert bridge.trades == 1
    assert published == [
        {"seq": 3, "taker": 2, "maker": 1, "price": 100, "qty": 5, "taker_side": "buy", "engine_ts_ns": 123}
    ]


def test_bridge_handles_a_frame_split_across_reads():
    published = []
    bridge = kb.Bridge(published.append)
    whole = frame(1)
    bridge.feed(whole[:7])
    assert published == []
    bridge.feed(whole[7:])
    assert len(published) == 1


def test_bridge_counts_a_sequence_gap():
    bridge = kb.Bridge(lambda m: None)
    bridge.feed(frame(1) + frame(5))
    assert bridge.gaps == 1


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def engine():
    if not ENGINE.exists():
        pytest.skip(f"engine binary not built: {ENGINE}")
    order_port, feed_port = free_port(), free_port()
    proc = subprocess.Popen(
        [str(ENGINE), "--port", str(order_port), "--feed-port", str(feed_port), "--loopback",
         "--max-price", "100000", "--max-orders", "10000"],
        stdout=subprocess.PIPE, text=True,
    )
    assert "engine listening" in proc.stdout.readline()
    yield order_port, feed_port
    proc.terminate()
    proc.wait(timeout=5)


def read_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        assert chunk, "connection closed early"
        data += chunk
    return data


def test_bridge_decodes_a_live_engine_feed(engine):
    order_port, feed_port = engine
    published = []
    bridge = kb.Bridge(published.append)
    with socket.create_connection(("127.0.0.1", feed_port)) as feed, \
            socket.create_connection(("127.0.0.1", order_port)) as orders:
        time.sleep(0.05)  # the subscriber must be registered before the flow starts
        orders.sendall(WIRE.pack(NEW, 1, 1000, 10, 0))        # resting sell
        read_exact(orders, 18)
        orders.sendall(WIRE.pack(NEW, 2, 1000, 4, BUY))       # crosses for 4
        read_exact(orders, 18)  # trade
        feed.settimeout(5)
        while bridge.frames < 2:
            bridge.feed(feed.recv(4096))

    assert bridge.gaps == 0
    assert len(published) == 1
    trade = published[0]
    assert (trade["taker"], trade["maker"], trade["price"], trade["qty"]) == (2, 1, 1000, 4)
    assert trade["taker_side"] == "buy"
