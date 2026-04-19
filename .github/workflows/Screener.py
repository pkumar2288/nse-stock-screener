name: NSE Stock Screener

on:
  workflow_dispatch:

jobs:
  run-screener:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Create test file
        run: |
          echo "Testing workflow" > test.txt
          cat test.txt
      - name: List files
        run: ls -la
