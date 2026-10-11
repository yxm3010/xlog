#!/usr/bin/env python3
"""xlog - connect to a drone, download its logs, and inspect/extract .ulog signals.

Command-line usage:
    xlog.py connect <port>
    xlog.py download <port> [-o OUTDIR]
    xlog.py topics <ulog_file>
    xlog.py fields <ulog_file> <topic_name>
    xlog.py extract <ulog_file> [-c signals.yaml] [-o OUT] [-f csv|json] [-i INSTANCE]

Run `xlog.py -h` or `xlog.py <command> -h` for details on each command.
"""

import argparse
import asyncio
import csv
import json
import os
import sys

import yaml
from pyulog import ULog

import xlog_connect


class XLog:
    def __init__(self, ulog_file=None):
        self.ulog = None
        self.topics = []
        self.drone = None
        if ulog_file is not None:
            self.load_ulog(ulog_file)

    def load_ulog(self, ulog_file):
        """Load a .ulog file so its topics/fields/signals can be inspected and extracted."""
        self.ulog = ULog(ulog_file)
        self.topics = self.discover_topics()

    def discover_topics(self):
        return [ds.name for ds in self.ulog.data_list]

    def discover_dataset(self, topic_name, instance=0):
        matches = [ds for ds in self.ulog.data_list if ds.name == topic_name]
        if not matches:
            raise KeyError(f"Topic not found: {topic_name}")
        return matches[instance].data.keys()

    def get_dataset(self, name):
        return self.ulog.get_dataset(name)

    def get_topics(self):
        return self.topics

    def connect(self, port):
        """Establish a MAVLink connection to the drone via the given USB serial port (e.g. 'COM6').

        Delegates to xlog_connect (MAVSDK) and keeps the connected drone handle on
        self.drone for use by download()/download_log().
        """
        self.drone = asyncio.run(xlog_connect.connect_drone(port))
        return self.drone

    def download(self, outdir="."):
        """Download every ulog file from the connected drone into outdir.

        Requires connect(port) to have been called first in this process.
        """
        if self.drone is None:
            raise RuntimeError("Not connected to a drone. Call connect(port) first.")
        return asyncio.run(xlog_connect.download_all(self.drone, outdir))

    def download_log(self, entry, outdir="."):
        """Download a single log entry (as returned by connecting and listing logs)."""
        if self.drone is None:
            raise RuntimeError("Not connected to a drone. Call connect(port) first.")
        return asyncio.run(xlog_connect.download_log(self.drone, entry, outdir))

    def load_signal_config(self, config_path):
        """Parse a signal-list YAML file (e.g. signals.yaml) into a list of topic entries.

        Expected format:
            signals:
              - topic: <ulog topic name>
                data:
                  - name: <field name as it appears in the dataset, e.g. xyz[0]>
                    alias: <friendly name to expose instead of `name`>

        Returns the raw list of topic entries (list of dicts with "topic" and "data" keys).
        """
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
        return config.get("signals", [])

    def read_signals(self, config_path, instance=0):
        """Read every signal listed in a YAML config file (e.g. signals.yaml) from this ulog.

        For each topic in the config, the matching dataset is looked up and each named
        field is extracted and exposed under its alias, alongside the dataset's timestamps.

        Returns a dict keyed by alias:
            {alias: {"timestamp": ndarray, "value": ndarray}, ...}

        Raises KeyError if a topic or field referenced in the config does not exist in
        the loaded ulog.
        """
        signal_entries = self.load_signal_config(config_path)
        signals = {}
        for entry in signal_entries:
            topic = entry["topic"]
            try:
                dataset = self.ulog.get_dataset(topic, instance)
            except KeyError:
                raise KeyError(f"Topic not found in ulog: {topic}")

            for field in entry.get("data", []):
                name = field["name"]
                alias = field.get("alias", name)
                if name not in dataset.data:
                    raise KeyError(f"Field '{name}' not found in topic '{topic}'")
                signals[alias] = {
                    "timestamp": dataset.data["timestamp"],
                    "value": dataset.data[name],
                }
        return signals

    def convert(self):
        pass


def _cmd_connect(args):
    xlog = XLog()
    try:
        xlog.connect(args.port)
    except SystemExit:
        return 1
    return 0


def _cmd_download(args):
    xlog = XLog()

    # Connect and download within a single asyncio event loop/session, since the
    # MAVSDK drone handle returned by connect_drone() is bound to the loop it was
    # created in.
    async def _connect_and_download():
        xlog.drone = await xlog_connect.connect_drone(args.port)
        return await xlog_connect.download_all(xlog.drone, args.out)

    try:
        entries = asyncio.run(_connect_and_download())
    except SystemExit:
        return 1
    print(f"Downloaded {len(entries)} log(s) to {args.out}/")
    return 0


def _cmd_topics(args):
    xlog = XLog(args.ulog_file)
    for topic in xlog.get_topics():
        print(topic)
    return 0


def _cmd_fields(args):
    xlog = XLog(args.ulog_file)
    try:
        fields = xlog.discover_dataset(args.topic_name, instance=args.instance)
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    for field in fields:
        print(field)
    return 0


def _cmd_extract(args):
    xlog = XLog(args.ulog_file)
    try:
        signals = xlog.read_signals(args.config, instance=args.instance)
    except (KeyError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not args.out and not args.format:
        print(f"{'alias':<30}{'samples':>10}")
        for alias, data in signals.items():
            print(f"{alias:<30}{len(data['value']):>10}")
        return 0

    # Format is explicit via --format, or inferred from --out's extension
    # (a path ending in .json means json, anything else means a csv directory).
    fmt = args.format or ("json" if args.out.lower().endswith(".json") else "csv")

    if fmt == "json":
        out_path = args.out or "signals.json"
        payload = {
            alias: {
                "timestamp": data["timestamp"].tolist(),
                "value": data["value"].tolist(),
            }
            for alias, data in signals.items()
        }
        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote {len(signals)} signal(s) to {out_path}")
        return 0

    # CSV: one file per alias (timestamp, value) inside the output directory.
    out_dir = args.out or "signals_csv"
    os.makedirs(out_dir, exist_ok=True)
    for alias, data in signals.items():
        out_path = os.path.join(out_dir, f"{alias}.csv")
        with open(out_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "value"])
            writer.writerows(zip(data["timestamp"].tolist(), data["value"].tolist()))
    print(f"Wrote {len(signals)} signal(s) to {out_dir}/")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog="xlog.py",
        description="Connect to a drone, download its logs, and inspect/extract .ulog signals.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    connect = subparsers.add_parser(
        "connect", help="Connect to a drone over MAVLink via a USB serial port."
    )
    connect.add_argument("port", help="Serial port, e.g. COM6")
    connect.set_defaults(func=_cmd_connect)

    download = subparsers.add_parser(
        "download", help="Connect to a drone and download all its ulog files."
    )
    download.add_argument("port", help="Serial port, e.g. COM6")
    download.add_argument(
        "-o", "--out", default=".",
        help="Directory to download ulog files into (default: current directory)",
    )
    download.set_defaults(func=_cmd_download)

    topics = subparsers.add_parser("topics", help="List all topics in a ulog file.")
    topics.add_argument("ulog_file")
    topics.set_defaults(func=_cmd_topics)

    fields = subparsers.add_parser("fields", help="List fields available for a topic.")
    fields.add_argument("ulog_file")
    fields.add_argument("topic_name")
    fields.add_argument("-i", "--instance", type=int, default=0, help="Multi-instance index (default: 0)")
    fields.set_defaults(func=_cmd_fields)

    extract = subparsers.add_parser(
        "extract", help="Extract signals from a ulog using a YAML signal config."
    )
    extract.add_argument("ulog_file")
    extract.add_argument(
        "-c", "--config", default="signals.yaml",
        help="Path to the signal-list YAML file (default: signals.yaml)",
    )
    extract.add_argument(
        "-o", "--out",
        help="Output path: a directory for one CSV per signal, or a .json file for a "
             "single combined file. Defaults to signals_csv/ or signals.json depending "
             "on --format. If neither --out nor --format is given, prints a summary table.",
    )
    extract.add_argument(
        "-f", "--format", choices=["csv", "json"], default=None,
        help="Output format. If omitted, inferred from --out's extension "
             "(.json -> json, otherwise csv).",
    )
    extract.add_argument("-i", "--instance", type=int, default=0, help="Multi-instance index (default: 0)")
    extract.set_defaults(func=_cmd_extract)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
