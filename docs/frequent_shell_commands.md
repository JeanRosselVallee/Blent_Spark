# HOWTO - Blent Data Project (Ubuntu/Linux)

Operational guide and setup notes for the ETL pipeline on Ubuntu.

## 1. System & Git Setup

### Install Git & Core Tools
```bash
sudo apt update
sudo apt install git software-properties-common
```

### Project Initialization
```bash
git init
git remote add origin https://github.com/JeanRosselVallee/BlentDataProject.git
git pull origin main
```

## 2. Python Environment Management

### Install Specific Python Versions (Deadsnakes PPA)
Airflow requires 3.11, while the ETL script uses 3.13.
```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev
sudo apt install python3.13 python3.13-venv python3.13-dev
```

### Virtual Environments Setup
```bash
# ETL Environment (3.13)
python3.13 -m venv .venv_etl
source .venv_etl/bin/activate
pip install --upgrade pip
pip install -r requirements_etl.txt
deactivate

# Airflow Environment (3.11)
python3.11 -m venv .venv_airflow
source .venv_airflow/bin/activate
pip install --upgrade pip
pip install -r requirements_airflow.txt
deactivate
```

## 3. Execution & Orchestration

### Controlled Execution (Shell Script)
```bash
chmod +x airflow_run_etl.sh

# Boot servers (Daily Mode)
./airflow_run_etl.sh

# Test a specific date
./airflow_run_etl.sh 2026-05-12

# Run backfill
./airflow_run_etl.sh 2026-04-10 2026-04-11
```

### Manual Execution (Python)
```bash
source .venv_etl/bin/activate
python scripts/run_etl.py --scan_date 2026-05-12 --platform Terminal
```

## 4. Process Monitoring & Cleanup

### Check Running Airflow Processes
```bash
ps aux | grep airflow
pgrep -f "airflow scheduler"
pgrep -f "airflow webserver"
```

### Hard Stop Airflow
```bash
pkill -f "airflow" && rm -f ${AIRFLOW_HOME}/*.pid
```

## 5. System Maintenance

### Disk Space Management
Useful if logs or cache fill the partition.
```bash
df -h
sudo apt-get clean
sudo apt-get autoremove --purge
sudo journalctl --vacuum-size=100M
sudo snap set system refresh.retain=2
```

### VS Code SQLTools Fix
Run this if the SQLTools extension cannot find the Node runtime (Snap issue).
```bash
node -e 'require("fs").writeFileSync("/home/jean/snap/code/current/.local/share/vscode-sqltools/.node-runtime", process.execPath)'
```

## 6. Project Snapshots (Documentation)

### Export History & File List
```bash
# Export command history
history 1000 > hist_1000.txt

# List pertinent files (excluding venv/git)
find . -maxdepth 2 -not -path '*/.*' > Project1.txt
```

### Code Quality
```bash
source .venv_etl/bin/activate
black --line-length 79 .
flake8 src/ scripts/
```
