#!/usr/bin/env python3
"""Killable TCP relay for failover tests.

    tcp-relay.py <listen_port> <target_host> <target_port> [--control FILE]

Accepts connections on 127.0.0.1:<listen_port> and pipes bytes both ways to
<target_host>:<target_port>. On SIGTERM (or SIGINT) it closes EVERY socket —
listener and all relayed connections, both directions — and exits, so a proxy
sitting in front of it sees its backend connections die exactly as if the
backend had crashed, without touching the real backend.

With `--control FILE` the relay re-reads FILE (when its modification time
changes) before forwarding each chunk, so a drill can degrade the path while
the relay runs, without network tooling:

    delay_ms=1500      hold every chunk for 1.5 s before forwarding it
    drop=1             black hole: accept connections, forward nothing until
                       the file says drop=0 again (connections stay open)

A missing or empty file means "pass through".
"""
import asyncio
import os
import signal
import sys


class Control:
    """The relay's runtime knobs, read from a file when it changes."""

    def __init__(self, path):
        self.path = path
        self.mtime = None
        self.delay_ms = 0
        self.drop = False

    def refresh(self):
        if not self.path:
            return
        try:
            mtime = os.stat(self.path).st_mtime_ns
        except OSError:
            mtime = None
        if mtime == self.mtime:
            return
        self.mtime = mtime
        delay_ms, drop = 0, False
        if mtime is not None:
            try:
                with open(self.path, encoding="utf-8") as handle:
                    for line in handle:
                        key, _, value = line.strip().partition("=")
                        if key == "delay_ms":
                            delay_ms = int(value or 0)
                        elif key == "drop":
                            drop = value.strip() in ("1", "true", "yes")
            except (OSError, ValueError):
                pass
        self.delay_ms, self.drop = delay_ms, drop
        print(f"relay control: delay_ms={self.delay_ms} drop={self.drop}", flush=True)


class Relay:
    def __init__(self, listen_port: int, target_host: str, target_port: int, control=None):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.control = Control(control)
        self.writers = set()
        self.server = None
        # Created inside run(): on Python 3.9 an Event built before
        # asyncio.run() binds to a different loop and never wakes.
        self.stop = None

    async def degrade(self):
        """Apply the control file to one chunk: wait out a black hole, then the delay."""
        self.control.refresh()
        while self.control.drop:
            await asyncio.sleep(0.05)
            self.control.refresh()
        if self.control.delay_ms > 0:
            await asyncio.sleep(self.control.delay_ms / 1000.0)

    async def pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await self.degrade()
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
        control = f" control={self.control.path}" if self.control.path else ""
        print(f"relay 127.0.0.1:{self.listen_port} -> {self.target_host}:{self.target_port} pid={os.getpid()}{control}", flush=True)
        await self.stop.wait()


def main():
    args = sys.argv[1:]
    control = None
    if "--control" in args:
        at = args.index("--control")
        if at + 1 >= len(args):
            print(__doc__, file=sys.stderr)
            sys.exit(2)
        control = args[at + 1]
        del args[at:at + 2]
    if len(args) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    relay = Relay(int(args[0]), args[1], int(args[2]), control)
    try:
        asyncio.run(relay.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
