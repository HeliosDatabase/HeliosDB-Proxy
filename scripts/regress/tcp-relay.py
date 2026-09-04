#!/usr/bin/env python3
"""Killable TCP relay for failover tests.

    tcp-relay.py <listen_port> <target_host> <target_port>

Accepts connections on 127.0.0.1:<listen_port> and pipes bytes both ways to
<target_host>:<target_port>. On SIGTERM (or SIGINT) it closes EVERY socket —
listener and all relayed connections, both directions — and exits, so a proxy
sitting in front of it sees its backend connections die exactly as if the
backend had crashed, without touching the real backend.
"""
import asyncio
import signal
import sys


class Relay:
    def __init__(self, listen_port: int, target_host: str, target_port: int):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.writers = set()
        self.server = None
        # Created inside run(): on Python 3.9 an Event built before
        # asyncio.run() binds to a different loop and never wakes.
        self.stop = None

    async def pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError, OSError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def handle(self, c_reader: asyncio.StreamReader, c_writer: asyncio.StreamWriter):
        try:
            b_reader, b_writer = await asyncio.open_connection(self.target_host, self.target_port)
        except OSError:
            c_writer.close()
            return
        self.writers.add(c_writer)
        self.writers.add(b_writer)
        try:
            await asyncio.gather(
                self.pipe(c_reader, b_writer),
                self.pipe(b_reader, c_writer),
            )
        finally:
            self.writers.discard(c_writer)
            self.writers.discard(b_writer)

    def shutdown(self):
        # Close everything at once: the far ends see EOF/RST immediately.
        if self.server is not None:
            self.server.close()
        for w in list(self.writers):
            try:
                sock = w.get_extra_info("socket")
                if sock is not None:
                    sock.setsockopt(6, 1, 1)  # TCP_NODELAY
                w.transport.abort()
            except Exception:
                pass
        self.stop.set()

    async def run(self):
        self.stop = asyncio.Event()
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", self.listen_port)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.shutdown)
        print(f"relay 127.0.0.1:{self.listen_port} -> {self.target_host}:{self.target_port} pid={__import__('os').getpid()}", flush=True)
        await self.stop.wait()


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    relay = Relay(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
    try:
        asyncio.run(relay.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
