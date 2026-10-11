#!/usr/bin/env python3
"""xlog_connect - connect to a drone over MAVLink and download its ulog files.

Command-line usage:
    xlog_connect.py <port> [outdir]

Exposes reusable async functions so `xlog.py` can drive a connect-and-download
session through a single asyncio event loop:
    drone = await connect_drone(port)
    entries = await download_all(drone, outdir)
"""

import asyncio
import sys
from pathlib import Path

from mavsdk_grpc import System


def explain_windows_blocked_server(exc: OSError) -> None:
    venv_root = Path(__file__).resolve().parent / ".venv"
    binary = venv_root / "Lib" / "site-packages" / "mavsdk_grpc" / "bin" / "mavsdk_server.exe"
    print(
        "\nWindows blocked the MAVSDK server binary before the script could open the serial link.\n"
        "This is a Windows App Control / Defender policy issue, not a Python bug in xlog_connect.py.\n\n"
        f"Blocked file: {binary}\n\n"
        "Fix options:\n"
        "  1. Allow or unblock the executable in Windows Security / App & browser control.\n"
        "  2. Start the server manually, then reconnect using a running MAVSDK server:\n"
        f"     \"{binary}\" -p 50051 --sysid 245 --compid 190 serial:///COM6:115200\n\n"
        "  3. If the server is already running, set:\n"
        "     drone = System(mavsdk_server_address='localhost', port=50051)\n",
        file=sys.stderr,
    )
    if getattr(exc, "winerror", None) is not None:
        print(f"Windows error: {exc.winerror} ({exc})", file=sys.stderr)


def filter_valid_entries(entries):
    valid_entries = []
    for entry in entries:
        entry_id = getattr(entry, "id", None)
        size_bytes = getattr(entry, "size_bytes", None)
        date = getattr(entry, "date", "")

        if (
            entry_id is None
            or entry_id <= 0
            or size_bytes is None
            or size_bytes <= 0
            or not str(date).strip()
        ):
            print(
                f"Skipping invalid log entry: id={entry_id}, date={date}, size_bytes={size_bytes}",
                file=sys.stderr,
            )
            continue
        valid_entries.append(entry)
    return valid_entries


def _system_address(port: str) -> str:
    # Accept either a bare port name ("COM6") or an already-formed MAVSDK
    # connection string (containing "://").
    if "://" in port:
        return port
    return f"serial:///{port}:115200"


async def connect_drone(port="COM6"):
    """Establish a MAVLink connection to a drone on the given serial port (e.g. 'COM6').

    Returns the connected mavsdk_grpc.System instance.
    """
    try:
        drone = System()
        await drone.connect(system_address=_system_address(port))
    except OSError as exc:
        if getattr(exc, "winerror", None) == 4551:
            explain_windows_blocked_server(exc)
            raise SystemExit(1) from exc
        raise

    print("Waiting for drone to connect...")
    async for state in drone.core.connection_state():
        if state.is_connected:
            print("-- Connected to drone!")
            break
    return drone


async def get_entries(drone):
    entries = await drone.log_files.get_entries()
    entries = filter_valid_entries(entries)
    for entry in entries:
        print(f"Log {entry.id} from {entry.date}")
    return entries


async def download_log(drone, entry, outdir="."):
    date_without_colon = entry.date.replace(":", "-")
    filename = str(Path(outdir) / f"log-{date_without_colon}.ulog")
    print(f"Downloading: log {entry.id} from {entry.date} to {filename}")
    previous_progress = -1
    try:
        async for progress in drone.log_files.download_log_file(entry, filename):
            new_progress = round(progress.progress * 100)
            if new_progress != previous_progress:
                sys.stdout.write(f"\r{new_progress} %")
                sys.stdout.flush()
                previous_progress = new_progress
    except Exception as exc:
        message = str(exc)
        if "INVALID_ARGUMENT" in message or "Invalid Argument" in message:
            print(
                f"Skipping invalid log entry {entry.id} from {entry.date}: {message}",
                file=sys.stderr,
            )
            return
        raise
    print()


async def download_all(drone, outdir="."):
    """Fetch the drone's log entry list and download every valid entry into outdir."""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    entries = await get_entries(drone)
    for entry in entries:
        await download_log(drone, entry, outdir)
    return entries


async def run(port, outdir="."):
    drone = await connect_drone(port)
    await download_all(drone, outdir)


if __name__ == "__main__":
    cli_port = sys.argv[1] if len(sys.argv) > 1 else "COM6"
    cli_outdir = sys.argv[2] if len(sys.argv) > 2 else "."
    asyncio.run(run(cli_port, cli_outdir))
