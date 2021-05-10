#! /usr/bin/sh
node generate_json.js
python3 -m unittest tests/test_backend_concurrency.py tests/test_counters.py tests/test_from_json.py tests/test_sync.py
