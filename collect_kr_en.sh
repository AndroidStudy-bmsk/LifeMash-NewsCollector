#!/bin/bash
python -m news_collector.cli \
  --domains-file kr_domains.json \
  --languages ko,en \
  --since-hours 168 \
  --max-pages 1
