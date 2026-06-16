#!/bin/bash

# setup_env_gcs.sh - Master Environment Setup Script
# Functional goal: Initialize a Spark environment and enable GCS data access
# Technical means: Python Venv (venv_spark), GCloud CLI, Auth Login, Application Default Credentials (ADC)

# Functions Definition

# Colored Print
print() {
    MESSAGE=$1  
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color (Reset)
    echo -e "${GREEN}${MESSAGE}${NC}"    
}

# Main Execution
print "--- Master Setup & GCS Integration ---"


# Step 1. Install system dependencies (curl, venv)
if ! command -v curl &> /dev/null; then
    print "curl not found. Installing..."
    sudo apt update && sudo apt install curl python3-venv -y
fi


# Step 2. Install GCloud CLI
if command -v gcloud &> /dev/null; then
    print "GCloud CLI is already installed."
else
    print "GCloud CLI not found. Installing..."

    # Install curl
    if ! command -v curl &> /dev/null; then
        print "curl not found. Installing..."
        sudo apt update && sudo apt install curl -y
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


# Step 3. Virtual Environment Setup
VENV_PATH="./venv_spark"
if [ ! -d "$VENV_PATH" ]; then

    # Install venv
    if ! python3 -c "import venv" &> /dev/null; then
        print "venv is not installed. Installing..."
        sudo apt update && sudo apt install python3-venv -y
    fi

    # Create virtual environment
    print "Creating virtual environment: ${VENV_PATH}..."
    python3 -m venv "$VENV_PATH"
fi


# Step 4. Install Python Dependencies
if [ -f "requirements.txt" ]; then
    print "Installing Python dependencies into ${VENV_PATH}..."

    PIP_BIN="$VENV_PATH/bin/pip"  # Use pip from venv

    "$PIP_BIN" install --upgrade pip
    "$PIP_BIN" install -r "requirements.txt"
else
    print "Warning: requirements.txt not found. Skipping pip install."
fi


# Step 5. Login to run gcloud commands by this script
echo ""
print "Step 5: Logging in to your Google Account (Browser will open)..."
gcloud auth login


# Step 6. Generate Application Default Credentials (ADC)
# Spark will use ~/.config/gcloud/application_default_credentials.json
echo ""
print "Step 6: Setting up Application Default Credentials for Spark (Browser will open)..."
print "Important: Check the box to allow cloud-platform scope during login."
gcloud auth application-default login


# Step 7. Interactive GCP Project Selection
echo ""
print "Step 7: Configuring Active Project..."

# Get an array of project IDs from GCloud
PROJECTS_LIST=($(gcloud projects list --format="value(projectId)"))
ENUMERATED_PROJECTS_LIST=("${PROJECTS_LIST[@]}") 
NB_PROJECTS=${#ENUMERATED_PROJECTS_LIST[@]}
if [ "$NB_PROJECTS" -eq 0 ]; then
    print "Warning: No GCP projects found."
else
    print "Available projects:"

    # Set prompt PS3 for select menu
    PS3="Please enter the number of the project you want to use: "

    # Select menu : display enumerated list & prompt & get user's choice
    select project in "${ENUMERATED_PROJECTS_LIST}"; do

        # Check selected project name is not empty
        if [ -n "$project" ]; then
            PROJECT_ID=$project
            print "Configuring for project: $PROJECT_ID"
            gcloud config set project "$PROJECT_ID"
            
            # Set GCP_PROJECT_ID in .env file
            touch .env

            # Check variable is in .env
            if grep -q "GCP_PROJECT_ID=" .env; then

                # Update existing variable
                sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$PROJECT_ID/" .env
                print "Updated GCP_PROJECT_ID in .env file."
            else
                # Append new variable
                echo "GCP_PROJECT_ID=$PROJECT_ID" >> .env
                print "Appended GCP_PROJECT_ID to .env file."
            fi

            # Exit the select loop
            break
        else
            print "Invalid selection. Please try again."
        fi
    done
fi

echo ""
print "--- Setup Finished ---"
print "To activate your environment: source venv_spark/bin/activate"
