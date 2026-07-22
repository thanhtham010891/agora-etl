"""Recover a producer manifest after Kafka acknowledgement completed first."""

from __future__ import annotations

import argparse
import asyncio

from order_command_center.producer import reconcile_producer_run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("producer_run_id")
    args = parser.parse_args()
    asyncio.run(reconcile_producer_run(producer_run_id=args.producer_run_id))


if __name__ == "__main__":
    main()
