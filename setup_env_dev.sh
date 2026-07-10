#!/bin/bash
set -e  # Stop if any command fails

# Goal:
# - Initialize a Spark environment with 
#   - Python virtual environment
#   - Google Cloud CLI integration.
#
# This script automates the setup of a local Python development
# environment for Google Cloud Dataproc and Spark jobs.
#
# Outputs:
# - Python virtual environment (venv_spark/) with required packages
# - GCloud CLI installed
#
# Usage: 
# source ./setup_env_dev.sh

# __________________________ Debug Functions ______________________

print() {  # Print Log messages in green
    MESSAGE=$1  
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color (Reset)
    echo -e "${GREEN}${MESSAGE}${NC}"    
}

# ___________________________ Main Execution ______________________ 

print "--- Master Setup & GCS Integration ---"

# _____________________________ GCloud CLI  ______________________ 

print "▶️ Step 1. Install GCloud CLI"
if command -v gcloud &> /dev/null; then
    print "GCloud CLI is already installed."
else
    print "GCloud CLI not found. Installing..."

    # Install curl
    if ! command -v curl &> /dev/null; then
        print "curl not found. Installing..."
        sudo apt update && sudo apt install --quiet curl -y
    fi

    # Install SDK
    curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts

    # Update .bashrc to enable GCloud CLI in the future
    if ! grep -q "google-cloud-sdk" "$HOME/.bashrc"; then
        echo 'export PATH=$PATH:$HOME/google-cloud-sdk/bin' >> "$HOME/.bashrc"
    fi
fi

# Update PATH to run "gcloud" within script
export PATH=$PATH:$HOME/google-cloud-sdk/bin

# _________________________ Virtual Environment _____________________

print "▶️ Step 2. Virtual Environment Setup"
VENV_PATH="./venv_spark"
if [ ! -d "$VENV_PATH" ]; then

    # Install venv
    if ! python3 -c "import venv" &> /dev/null; then
        print "venv is not installed. Installing..."
        sudo apt update && sudo apt install --quiet python3-venv -y
    fi

    # Create virtual environment
    print "Creating virtual environment: ${VENV_PATH}..."
    python3 -m venv "$VENV_PATH"
fi

print "Activating Virtual Environment..."
source venv_spark/bin/activate

# _________________________ Python Dependencies ____________________ 

print "▶️ Step 3. Install Python Dependencies"
if [ -f "requirements.txt" ]; then
    print "Installing Python dependencies into ${VENV_PATH}..."

    PIP_BIN="$VENV_PATH/bin/pip"  # Use pip from venv

    "$PIP_BIN" install --upgrade pip
    "$PIP_BIN" install -r "requirements.txt"
else
    print "Warning: requirements.txt not found. Skipping pip install."
fi

# _____________________ End of Main Process ______________________

print "✅ Success: Dev Environment is ready to use."
exit 0