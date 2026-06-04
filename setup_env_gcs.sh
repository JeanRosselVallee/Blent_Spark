#!/bin/bash

# setup_env_gcs.sh - Master Environment Setup Script
# Functional goal: Initialize a Spark environment and enable GCS data access
# Technical means: Python Venv (venv_spark), GCloud CLI, Auth Login, Application Default Credentials (ADC)

echo "--- Master Setup & GCS Integration ---"

# 0. Install system dependencies (curl, venv)
if ! command -v curl &> /dev/null; then
    echo "curl not found. Installing..."
    sudo apt update && sudo apt install curl python3-venv -y
fi

# 1. Install GCloud CLI
if ! command -v gcloud &> /dev/null; then
    echo "GCloud CLI not found. Installing..."
    curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts
    export PATH=$PATH:$HOME/google-cloud-sdk/bin
    if ! grep -q "google-cloud-sdk" "$HOME/.bashrc"; then
        echo 'export PATH=$PATH:$HOME/google-cloud-sdk/bin' >> "$HOME/.bashrc"
    fi
else
    echo "GCloud CLI is already installed."
fi

# Refresh PATH for the current script execution
export PATH=$PATH:$HOME/google-cloud-sdk/bin

# 2. Virtual Environment Setup (venv_spark)
VENV_PATH="./venv_spark"

if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtual environment: venv_spark..."
    python3 -m venv "$VENV_PATH"
fi

# Use the pip from the venv directly
PIP_BIN="$VENV_PATH/bin/pip"

# 3. Install Python Dependencies
if [ -f "requirements.txt" ]; then
    echo "Installing Python dependencies into venv_spark..."
    "$PIP_BIN" install --upgrade pip
    "$PIP_BIN" install -r "requirements.txt"
else
    echo "Warning: requirements.txt not found. Skipping pip install."
fi

# 4. Login to Google Cloud
echo ""
echo "Step 1: Logging in to your Google Account (Browser will open)..."
gcloud auth login

# 5. Setup Application Default Credentials (ADC) for Spark
echo ""
echo "Step 2: Setting up Application Default Credentials for Spark (Browser will open)..."
echo "Important: Check the box to allow cloud-platform scope during login."
gcloud auth application-default login

# 6. Interactive Project Selection
echo ""
echo "Step 3: Configuring Active Project..."
# Get an array of project IDs from GCloud
PROJECTS_LIST=($(gcloud projects list --format="value(projectId)"))
ENUMERATED_PROJECTS_LIST=("${PROJECTS_LIST[@]}") 
NB_PROJECTS=${#ENUMERATED_PROJECTS_LIST[@]}
if [ "$NB_PROJECTS" -eq 0 ]; then
    echo "Warning: No GCP projects found."
else
    echo "Available projects:"
    # Define prompt PS3
    PS3="Please enter the number of the project you want to use: "
    # Select menu : display enumerated list & prompt & get user's choice
    select project in "${ENUMERATED_PROJECTS_LIST}"; do
        # Check project name is not empty
        if [ -n "$project" ]; then
            PROJECT_ID=$project
            echo "Configuring for project: $PROJECT_ID"
            gcloud config set project "$PROJECT_ID"
            
            # Set GCP_PROJECT_ID in .env file
            if [ -f ".env" ]; then
                # Update existing variable
                if grep -q "GCP_PROJECT_ID=" .env; then
                    sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$PROJECT_ID/" .env
                # Or append it
                else
                    echo "GCP_PROJECT_ID=$PROJECT_ID" >> .env
                fi
                echo "Updated GCP_PROJECT_ID in .env file."
            # Create .env file with variable
            else
                echo "GCP_PROJECT_ID=$PROJECT_ID" > .env
                echo "Created .env file with GCP_PROJECT_ID."
            fi
            # Exit the select loop
            break
        else
            echo "Invalid selection. Please try again."
        fi
    done
fi

echo ""
echo "--- Setup Finished ---"
echo "To activate your environment: source venv_spark/bin/activate"
