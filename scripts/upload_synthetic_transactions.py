#!/usr/bin/env python3
"""Generate synthetic transactions and upload them with Elasticsearch Bulk API."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import random
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


DEFAULT_INDEX = "synthetic-transactions"
DEFAULT_COUNT = 1_000_000
DEFAULT_BATCH_SIZE = 5_000
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

MERCHANT_CATEGORIES = (
    "groceries",
    "fuel",
    "travel",
    "restaurants",
    "electronics",
    "healthcare",
    "entertainment",
    "utilities",
    "apparel",
    "marketplace",
)
PAYMENT_METHODS = ("card", "ach", "wallet", "wire", "bnpl")
CHANNELS = ("web", "mobile", "point_of_sale", "api")
COUNTRIES = ("US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "IN", "SG")
CURRENCIES = ("USD", "CAD", "GBP", "EUR", "AUD", "JPY", "BRL", "INR", "SGD")
STATUSES = ("authorized", "captured", "declined", "refunded", "chargeback")


class BulkUploadError(RuntimeError):
    """Raised when a bulk upload response contains permanent failures."""


class BulkUploader:
    def __init__(
        self,
        base_url: str,
        *,
        api_key: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        compress: bool = True,
        verify_certs: bool = True,
        opener: Optional[urllib.request.OpenerDirector] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.compress = compress
        self.opener = opener or build_opener(verify_certs=verify_certs)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-ndjson",
        }

        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"
        elif username and password:
            credentials = f"{username}:{password}".encode("utf-8")
            token = base64.b64encode(credentials).decode("ascii")
            self.headers["Authorization"] = f"Basic {token}"

        if compress:
            self.headers["Content-Encoding"] = "gzip"

    def upload_batch(
        self,
        ndjson_body: bytes,
        *,
        pipeline: Optional[str] = None,
        refresh: bool = False,
    ) -> Dict[str, object]:
        request_body = gzip.compress(ndjson_body) if self.compress else ndjson_body
        url = self._bulk_url(pipeline=pipeline, refresh=refresh)

        for attempt in range(self.max_retries + 1):
            try:
                response = self._post(url, request_body)
                if response.get("errors") is True:
                    raise BulkUploadError(describe_bulk_errors(response))
                return response
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code not in RETRYABLE_STATUS_CODES or attempt >= self.max_retries:
                    raise BulkUploadError(
                        f"Bulk request failed with HTTP {exc.code}: {body}"
                    ) from exc
                self._sleep_before_retry(attempt)
            except (TimeoutError, urllib.error.URLError) as exc:
                if attempt >= self.max_retries:
                    raise BulkUploadError(f"Bulk request failed: {exc}") from exc
                self._sleep_before_retry(attempt)

        raise BulkUploadError("Bulk request failed after retries")

    def _bulk_url(self, *, pipeline: Optional[str], refresh: bool) -> str:
        params = {
            "filter_path": "errors,took,items.*.status,items.*.error",
        }
        if pipeline:
            params["pipeline"] = pipeline
        if refresh:
            params["refresh"] = "true"
        return f"{self.base_url}/_bulk?{urllib.parse.urlencode(params)}"

    def _post(self, url: str, body: bytes) -> Dict[str, object]:
        request = urllib.request.Request(
            url,
            data=body,
            headers=self.headers,
            method="POST",
        )
        with self.opener.open(request, timeout=self.timeout) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload) if payload else {}

    @staticmethod
    def _sleep_before_retry(attempt: int) -> None:
        delay = min(30.0, 0.5 * (2**attempt)) + random.random() * 0.25
        time.sleep(delay)


def generate_transactions(
    count: int,
    *,
    seed: int,
    start_time: datetime,
) -> Iterator[Dict[str, object]]:
    rng = random.Random(seed)
    start_time = start_time.astimezone(timezone.utc)

    for sequence in range(count):
        timestamp = start_time + timedelta(milliseconds=sequence * rng.randint(5, 75))
        amount = round(rng.lognormvariate(3.35, 0.85), 2)
        status = weighted_choice(
            rng,
            (
                ("captured", 86),
                ("authorized", 7),
                ("declined", 5),
                ("refunded", 1.8),
                ("chargeback", 0.2),
            ),
        )
        risk_score = round(min(1.0, rng.betavariate(1.4, 8.5)), 4)
        merchant_category = rng.choice(MERCHANT_CATEGORIES)
        country_index = rng.randrange(len(COUNTRIES))

        yield {
            "@timestamp": isoformat_z(timestamp),
            "transaction_id": f"txn-{sequence:012d}-{rng.getrandbits(32):08x}",
            "account_id": f"acct-{rng.randrange(1, 250_001):06d}",
            "customer_id": f"cust-{rng.randrange(1, 750_001):06d}",
            "merchant_id": f"merch-{rng.randrange(1, 50_001):05d}",
            "merchant_category": merchant_category,
            "amount": amount,
            "currency": CURRENCIES[country_index],
            "country": COUNTRIES[country_index],
            "status": status,
            "payment_method": rng.choice(PAYMENT_METHODS),
            "channel": rng.choice(CHANNELS),
            "card_present": merchant_category in {"groceries", "fuel", "restaurants"}
            and rng.random() < 0.72,
            "latency_ms": rng.randint(12, 2_500),
            "risk_score": risk_score,
            "fraud_flag": status == "chargeback" or risk_score >= 0.82,
        }


def iter_bulk_batches(
    documents: Iterable[Dict[str, object]],
    *,
    index: str,
    batch_size: int,
) -> Iterator[Tuple[int, bytes]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    lines: List[bytes] = []
    count = 0
    for document in documents:
        action = {"index": {"_index": index, "_id": document["transaction_id"]}}
        lines.append(to_json_line(action))
        lines.append(to_json_line(document))
        count += 1

        if count == batch_size:
            yield count, b"".join(lines)
            lines = []
            count = 0

    if count:
        yield count, b"".join(lines)


def to_json_line(value: Dict[str, object]) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=False) + "\n").encode(
        "utf-8"
    )


def weighted_choice(rng: random.Random, choices: Tuple[Tuple[str, float], ...]) -> str:
    total = sum(weight for _, weight in choices)
    point = rng.uniform(0, total)
    upto = 0.0
    for value, weight in choices:
        upto += weight
        if upto >= point:
            return value
    return choices[-1][0]


def build_opener(*, verify_certs: bool) -> urllib.request.OpenerDirector:
    if verify_certs:
        return urllib.request.build_opener()

    context = ssl._create_unverified_context()
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def describe_bulk_errors(response: Dict[str, object], *, sample_size: int = 5) -> str:
    failures: List[Dict[str, object]] = []
    for item in response.get("items", []):
        if not isinstance(item, dict):
            continue
        operation = item.get("index") or item.get("create") or item.get("update")
        if isinstance(operation, dict) and "error" in operation:
            failures.append(operation)
            if len(failures) >= sample_size:
                break

    if not failures:
        return "Bulk request completed with item errors, but no error details returned"
    return "Bulk request completed with item errors. Sample failures: " + json.dumps(
        failures,
        separators=(",", ":"),
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_start_time(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic payment transactions and stream them to "
            "Elasticsearch with the Bulk API."
        )
    )
    parser.add_argument(
        "--url",
        default=os.getenv("ELASTICSEARCH_URL"),
        help="Elasticsearch URL. Defaults to ELASTICSEARCH_URL.",
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX", DEFAULT_INDEX),
        help=f"Target index. Defaults to {DEFAULT_INDEX}.",
    )
    parser.add_argument(
        "--count",
        type=positive_int,
        default=DEFAULT_COUNT,
        help=f"Number of synthetic transactions to generate. Defaults to {DEFAULT_COUNT}.",
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Documents per bulk request. Defaults to {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic data generation.",
    )
    parser.add_argument(
        "--start-time",
        type=parse_start_time,
        default=datetime.now(timezone.utc) - timedelta(days=30),
        help="First transaction timestamp, for example 2026-01-01T00:00:00Z.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_API_KEY"),
        help="Elasticsearch API key. Defaults to ELASTICSEARCH_API_KEY.",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("ELASTICSEARCH_USERNAME"),
        help="Basic auth username. Defaults to ELASTICSEARCH_USERNAME.",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("ELASTICSEARCH_PASSWORD"),
        help="Basic auth password. Defaults to ELASTICSEARCH_PASSWORD.",
    )
    parser.add_argument(
        "--pipeline",
        help="Optional ingest pipeline name for the bulk request.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries per batch for transient HTTP or network errors.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh the target shards after each bulk request.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and batch documents without sending them to Elasticsearch.",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable gzip compression for bulk request bodies.",
    )
    parser.add_argument(
        "--no-verify-certs",
        action="store_true",
        help="Disable TLS certificate verification.",
    )
    parser.add_argument(
        "--progress-interval",
        type=positive_int,
        default=100_000,
        help="Print progress every N uploaded/generated documents.",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    if not args.dry_run and not args.url:
        raise SystemExit("--url or ELASTICSEARCH_URL is required unless --dry-run is set")

    uploader = None
    if not args.dry_run:
        uploader = BulkUploader(
            args.url,
            api_key=args.api_key,
            username=args.username,
            password=args.password,
            timeout=args.timeout,
            max_retries=args.max_retries,
            compress=not args.no_compress,
            verify_certs=not args.no_verify_certs,
        )

    generated = generate_transactions(
        args.count,
        seed=args.seed,
        start_time=args.start_time,
    )
    started_at = time.monotonic()
    processed = 0
    next_progress_at = min(args.progress_interval, args.count)

    for batch_count, body in iter_bulk_batches(
        generated,
        index=args.index,
        batch_size=args.batch_size,
    ):
        if uploader:
            uploader.upload_batch(body, pipeline=args.pipeline, refresh=args.refresh)
        processed += batch_count

        if processed >= next_progress_at or processed == args.count:
            elapsed = max(time.monotonic() - started_at, 0.001)
            rate = processed / elapsed
            action = "generated" if args.dry_run else "uploaded"
            print(
                f"{action} {processed:,}/{args.count:,} documents "
                f"({rate:,.0f} docs/sec)",
                file=sys.stderr,
            )
            while next_progress_at <= processed and next_progress_at < args.count:
                next_progress_at += args.progress_interval

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
