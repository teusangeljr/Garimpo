#!/usr/bin/env bash
# exit on error
set -o errexit

# Install dependencies
cd backend
pip install -r requirements.txt

# Download and extract Chrome
CHROME_DIR="/opt/render/project/src/.chrome"

if [ ! -d "$CHROME_DIR/opt/google/chrome" ]; then
  echo "...Downloading Chrome..."
  mkdir -p $CHROME_DIR
  wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
  dpkg -x google-chrome-stable_current_amd64.deb $CHROME_DIR
  rm google-chrome-stable_current_amd64.deb
else
  echo "...Chrome already installed..."
fi

