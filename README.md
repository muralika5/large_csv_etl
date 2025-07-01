# Large File ETL

## Setup Instructions


1. **Run the setup script** to install dependencies and create the database table:

    ```bash
    ./setup.sh
    ```

2. **Run the ETL pipeline:**

    ```bash
    python main.py --input path/to/input.csv --output path/to/output.csv --db path/to/database.db --batch-size 100000
    ```

    - `--input`: Path to your large input CSV file
    - `--output`: Path for the processed output CSV
    - `--db`: Path to your SQLite database file
    - `--batch-size`: (Optional) Number of rows per chunk (default: 100000)

4. **Run tests:**

    ```bash
    pytest
    ```
