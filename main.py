#!/usr/bin/env python3
"""Compatibility wrapper for the ETL entrypoint."""

from etl.run_etl import main


if __name__ == "__main__":
    raise SystemExit(main())
