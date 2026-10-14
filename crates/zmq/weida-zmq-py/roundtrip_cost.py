"""What a REQ/REP round trip costs from Python, against the Rust one.

The same shape as `crates/zmq/weida-zmq/examples/roundtrip_cost.rs` — one
request out, one reply back, over `inproc://` and over loopback `tcp://`, at an
empty payload and at 1 KiB — so that the two medians are comparable and the
difference is the language boundary rather than the measurement. The numbers
are in `docs/IMPLEMENTATION.md` §4.15.

    .venv/bin/python crates/zmq/weida-zmq-py/roundtrip_cost.py

`pyzmq`'s own round trip is measured too where it is installed, because "what
does the boundary cost" is only half the question: the other half is what the
same boundary costs the binding everybody already uses.
"""

import asyncio
import statistics
import time

import weida_zmq
from weida_zmq import sync

ROUNDS = 2000
SIZES = [0, 1024]


def median(samples):
    return statistics.median(samples) * 1e9  # nanoseconds


def endpoint_for(transport):
    return "inproc://cost" if transport == "inproc" else "tcp://127.0.0.1:0"


async def asyncio_round_trips(transport, size):
    context = weida_zmq.Context(worker_threads=2)
    server = weida_zmq.RepSocket(context)
    client = weida_zmq.ReqSocket(context)
    bound = await server.bind(endpoint_for(transport))
    await client.connect(bound)

    payload = bytes(size)
    samples = []
    for _ in range(ROUNDS):
        started = time.perf_counter()
        await client.send(payload)
        request = await server.recv()
        await server.send(request)
        await client.recv()
        samples.append(time.perf_counter() - started)
    return median(samples)


def sync_round_trips(transport, size):
    context = sync.Context(worker_threads=2)
    server = sync.RepSocket(context)
    client = sync.ReqSocket(context)
    bound = server.bind(endpoint_for(transport))
    client.connect(bound)

    payload = bytes(size)
    samples = []
    for _ in range(ROUNDS):
        started = time.perf_counter()
        client.send(payload)
        request = server.recv()
        server.send(request)
        client.recv()
        samples.append(time.perf_counter() - started)
    return median(samples)


def pyzmq_round_trips(transport, size):
    try:
        import zmq
    except ImportError:
        return None
    context = zmq.Context()
    try:
        server = context.socket(zmq.REP)
        client = context.socket(zmq.REQ)
        server.bind("inproc://cost" if transport == "inproc" else "tcp://127.0.0.1:0")
        client.connect(server.getsockopt(zmq.LAST_ENDPOINT).decode())

        payload = bytes(size)
        samples = []
        for _ in range(ROUNDS):
            started = time.perf_counter()
            client.send(payload)
            server.send(server.recv())
            client.recv()
            samples.append(time.perf_counter() - started)
        return median(samples)
    finally:
        context.destroy(linger=0)


def main():
    print(f"{'case':28} {'median (ns)':>14}")
    for transport in ["inproc", "tcp"]:
        for size in SIZES:
            asyncio_ns = asyncio.run(asyncio_round_trips(transport, size))
            print(f"{f'weida asyncio {transport}/{size}':28} {asyncio_ns:14.0f}")
            sync_ns = sync_round_trips(transport, size)
            print(f"{f'weida sync {transport}/{size}':28} {sync_ns:14.0f}")
            foreign_ns = pyzmq_round_trips(transport, size)
            if foreign_ns is not None:
                print(f"{f'pyzmq {transport}/{size}':28} {foreign_ns:14.0f}")


if __name__ == "__main__":
    main()
