#!/bin/bash
set -e

# Install Python requirements
pip install -r requirements.txt

# Run DB migration
python -c "from db import create_table; create_table()"

echo "Setup complete." 