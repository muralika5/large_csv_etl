import argparse

from config import TEMP_DIR
from etl_csv_parser.db_loader import DatabaseLoader
from etl_csv_parser.etl_pipeline import ETLPipeline


def main():
    parser = argparse.ArgumentParser(description='Large CSV ETL Pipeline')
    parser.add_argument('--input_file', required=True, help='Path to input CSV file')
    parser.add_argument('--output_file', default='output/output.csv',
                        help='Path to output (processed) CSV file')
    parser.add_argument('--db', default='etl_database.db',
                        help='Path to SQLite database file')
    parser.add_argument(
        '--batch_size', type=int, default=1000, help='Chunk size for processing (default: 100000)'
    )
    args = parser.parse_args()

    # we assume that tables are already created in the database
    db_loader = DatabaseLoader(args.db)

    pipeline = ETLPipeline(
        csv_path=args.input_file,
        db_loader=db_loader,
        chunk_size=args.batch_size,
    )
    pipeline.run(output_csv_path=args.output_file)


if __name__ == '__main__':
    main()
