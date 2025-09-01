#!/usr/bin/env bash
set -euo pipefail
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export JSON_URL="https://solo.braiins.com/users/address.worker"
export POLL_INTERVAL=30
export WINDOW_SIZE=2000
export METRICS_JSON='{"HR 1m (TH/s)":"hashrate1m","HR 5m (TH/s)":"hashrate5m","HR 1h (TH/s)":"hashrate1hr","HR 1d (TH/s)":"hashrate1d","HR 7d (TH/s)":"hashrate7d","Workers":"workers","Shares":"shares","Best Share":"bestshare","Last Share Age (s)":"__computed__.lastshare_age_s","Worker0 HR 1m (TH/s)":"worker[0].hashrate1m"}'
export SCALES_JSON='{"HR 1m (TH/s)":1e-12,"HR 5m (TH/s)":1e-12,"HR 1h (TH/s)":1e-12,"HR 1d (TH/s)":1e-12,"HR 7d (TH/s)":1e-12,"Workers":1,"Shares":1,"Best Share":1,"Last Share Age (s)":1,"Worker0 HR 1m (TH/s)":1e-12}'
python -m uvicorn server:app --reload --port 8000
