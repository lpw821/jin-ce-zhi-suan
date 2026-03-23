from datetime import datetime
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.data_provider import DataProvider
from src.utils.tushare_provider import TushareProvider
from src.utils.akshare_provider import AkshareProvider
from src.utils.config_loader import ConfigLoader


def mask_text(value):
    text = str(value or "").strip()
    if not text:
        return "(empty)"
    if len(text) <= 6:
        return "*" * len(text)
    return f"{text[:3]}{'*' * (len(text) - 5)}{text[-2:]}"


def parse_dt(text):
    return datetime.fromisoformat(str(text).strip())


def build_provider(source, config):
    source_text = str(source or "").strip().lower()
    if source_text == "tushare":
        token = config.get("data_provider.tushare_token")
        print(f"Provider=tushare, token={mask_text(token)}")
        return TushareProvider(token), source_text
    if source_text == "akshare":
        print("Provider=akshare")
        return AkshareProvider(), source_text
    print("Provider=default")
    return DataProvider(), "default"


def run():
    parser = argparse.ArgumentParser(description="Smoke test data provider connectivity")
    parser.add_argument("--source", default="", help="default|akshare|tushare, empty to use config")
    parser.add_argument("--code", default="000001.SZ")
    parser.add_argument("--start", default="2023-06-01T00:00:00")
    parser.add_argument("--end", default="2023-06-02T00:00:00")
    parser.add_argument("--skip-latest", action="store_true")
    args = parser.parse_args()

    config = ConfigLoader.reload()
    source = args.source or config.get("data_provider.source", "default")
    print(f"Smoke test source={source} code={args.code}")

    provider, source = build_provider(source, config)
    start = parse_dt(args.start)
    end = parse_dt(args.end)
    failures = 0

    if not hasattr(provider, "fetch_minute_data"):
        print("FAIL: provider missing fetch_minute_data")
        failures += 1
    else:
        try:
            df = provider.fetch_minute_data(args.code, start, end)
            if df is None:
                print("FAIL: minute data returned None")
                failures += 1
            elif getattr(df, "empty", True):
                print("FAIL: minute data empty")
                failures += 1
            else:
                print(f"OK: minute data rows={len(df)}")
                if "dt" in df.columns:
                    print(f"range={df['dt'].min()} -> {df['dt'].max()}")
        except Exception as e:
            print(f"FAIL: fetch_minute_data exception={e}")
            failures += 1

    if not args.skip_latest:
        if not hasattr(provider, "get_latest_bar"):
            print("FAIL: provider missing get_latest_bar")
            failures += 1
        else:
            try:
                bar = provider.get_latest_bar(args.code)
                if bar:
                    print("OK: latest bar acquired")
                else:
                    print("FAIL: latest bar empty")
                    failures += 1
            except Exception as e:
                print(f"FAIL: get_latest_bar exception={e}")
                failures += 1

    if failures:
        print(f"RESULT: FAILED ({failures})")
        return 1
    print("RESULT: PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(run())
