import gzip
import importlib.util
import json
import pathlib
import urllib.error
import urllib.request
import unittest
from datetime import datetime, timezone
from unittest import mock


SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "scripts"
    / "upload_synthetic_transactions.py"
)
SPEC = importlib.util.spec_from_file_location("upload_synthetic_transactions", SCRIPT_PATH)
upload = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upload)


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class CapturingOpener:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    def open(self, request, **kwargs):
        self.requests.append((request, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return Response(outcome)


class SyntheticTransactionTests(unittest.TestCase):
    def test_generate_transactions_is_deterministic_and_shaped(self):
        start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

        first = list(upload.generate_transactions(2, seed=7, start_time=start_time))
        second = list(upload.generate_transactions(2, seed=7, start_time=start_time))

        self.assertEqual(first, second)
        self.assertEqual(first[0]["@timestamp"], "2026-01-01T00:00:00.000Z")
        self.assertRegex(first[0]["transaction_id"], r"^txn-000000000000-[0-9a-f]{8}$")
        self.assertIn(first[0]["status"], upload.STATUSES)
        self.assertIsInstance(first[0]["amount"], float)
        self.assertIsInstance(first[0]["fraud_flag"], bool)

    def test_iter_bulk_batches_outputs_compact_ndjson(self):
        docs = [
            {"transaction_id": "txn-1", "amount": 12.5},
            {"transaction_id": "txn-2", "amount": 13.5},
            {"transaction_id": "txn-3", "amount": 14.5},
        ]

        batches = list(upload.iter_bulk_batches(docs, index="transactions", batch_size=2))

        self.assertEqual([batch_count for batch_count, _ in batches], [2, 1])
        first_payload = batches[0][1].decode("utf-8").splitlines()
        self.assertEqual(len(first_payload), 4)
        self.assertEqual(
            json.loads(first_payload[0]),
            {"index": {"_index": "transactions", "_id": "txn-1"}},
        )
        self.assertEqual(json.loads(first_payload[1]), docs[0])
        self.assertTrue(batches[0][1].endswith(b"\n"))


class BulkUploaderTests(unittest.TestCase):
    def test_api_key_auth_and_gzip_request_body(self):
        opener = CapturingOpener([{"errors": False, "took": 3}])
        uploader = upload.BulkUploader(
            "https://example.es",
            api_key="abc123",
            opener=opener,
        )

        response = uploader.upload_batch(b'{"index":{}}\n{"a":1}\n')

        self.assertEqual(response["errors"], False)
        request, kwargs = opener.requests[0]
        self.assertIn("filter_path=", request.full_url)
        self.assertEqual(request.get_header("Authorization"), "ApiKey abc123")
        self.assertEqual(request.get_header("Content-encoding"), "gzip")
        self.assertEqual(gzip.decompress(request.data), b'{"index":{}}\n{"a":1}\n')
        self.assertEqual(kwargs["timeout"], 30.0)

    def test_basic_auth_header_when_username_and_password_are_set(self):
        opener = CapturingOpener([{"errors": False}])
        uploader = upload.BulkUploader(
            "https://example.es",
            username="elastic",
            password="secret",
            opener=opener,
            compress=False,
        )

        uploader.upload_batch(b'{"index":{}}\n{"a":1}\n')

        request, _ = opener.requests[0]
        self.assertEqual(request.get_header("Authorization"), "Basic ZWxhc3RpYzpzZWNyZXQ=")
        self.assertIsNone(request.get_header("Content-encoding"))
        self.assertEqual(request.data, b'{"index":{}}\n{"a":1}\n')

    def test_retries_transient_http_errors(self):
        http_error = urllib.error.HTTPError(
            "https://example.es/_bulk",
            429,
            "Too Many Requests",
            hdrs={},
            fp=mock.Mock(read=lambda: b"busy"),
        )
        opener = CapturingOpener([http_error, {"errors": False}])
        uploader = upload.BulkUploader(
            "https://example.es",
            opener=opener,
            max_retries=1,
            compress=False,
        )

        with mock.patch.object(upload.BulkUploader, "_sleep_before_retry"):
            uploader.upload_batch(b'{"index":{}}\n{"a":1}\n')

        self.assertEqual(len(opener.requests), 2)

    def test_retries_retryable_bulk_item_errors(self):
        opener = CapturingOpener(
            [
                {
                    "errors": True,
                    "items": [
                        {
                            "index": {
                                "status": 429,
                                "error": {"type": "es_rejected_execution_exception"},
                            }
                        }
                    ],
                },
                {"errors": False},
            ]
        )
        uploader = upload.BulkUploader(
            "https://example.es",
            opener=opener,
            max_retries=1,
            compress=False,
        )

        with mock.patch.object(upload.BulkUploader, "_sleep_before_retry"):
            uploader.upload_batch(b'{"index":{}}\n{"a":1}\n')

        self.assertEqual(len(opener.requests), 2)

    def test_bulk_item_errors_raise_with_sample(self):
        opener = CapturingOpener(
            [
                {
                    "errors": True,
                    "items": [
                        {
                            "index": {
                                "status": 400,
                                "error": {"type": "mapper_parsing_exception"},
                            }
                        }
                    ],
                }
            ]
        )
        uploader = upload.BulkUploader("https://example.es", opener=opener)

        with self.assertRaisesRegex(upload.BulkUploadError, "mapper_parsing_exception"):
            uploader.upload_batch(b'{"index":{}}\n{"a":1}\n')


if __name__ == "__main__":
    unittest.main()
