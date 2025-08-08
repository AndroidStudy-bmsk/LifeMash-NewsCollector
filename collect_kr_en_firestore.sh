#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="lifemash-service-account.json"

python -m news_collector.cli \
  --store firestore \
  --domains-file kr_domains.json \
  --languages ko,en \
  --since-hours 168 \
  --max-pages 1 \
  --limit 50
