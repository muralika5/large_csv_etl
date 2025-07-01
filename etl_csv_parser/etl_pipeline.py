import concurrent.futures
from logging_config import get_logger
from io_handler import CSVProcessor
from config import CHUNK_SIZE, TEMP_DIR, OUTPUT_CSV_PATH
from db_loader import DatabaseLoader

class ETLPipeline:
    def __init__(self, csv_path, db_loader: DatabaseLoader, chunk_size=CHUNK_SIZE, temp_dir=TEMP_DIR):
        self.csv_processor = CSVProcessor(csv_path, chunk_size, temp_dir)
        self.db_loader = db_loader
        self.logger = get_logger(self.__class__.__name__)

    def run(self, output_csv_path=OUTPUT_CSV_PATH):
        self.logger.info('[ETLPipeline] Starting ETL pipeline')
        errors = []
        temp_file_map = dict() # chunk_idx -> temp_file_path
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for chunk_idx, chunk in enumerate(self.csv_processor.read_csv()):
                future = executor.submit(self.csv_processor.process_chunk, chunk, chunk_idx)
                futures.append((chunk_idx, future))
            for chunk_idx, future in futures:
                temp_file = future.result()
                if temp_file:
                    temp_file_map[chunk_idx] = temp_file
                else:
                    errors.append(f'Chunk {chunk_idx} failed or was empty.')
        if not temp_file_map:
            self.logger.error('[ETLPipeline] No chunks processed successfully. Exiting.')
            return
        
        ordered_temp_files = [temp_file_map[idx] for idx in sorted(temp_file_map)]
        
        self.csv_processor.concatenate(output_csv_path, temp_files=ordered_temp_files)
        self.logger.info('[ETLPipeline] Output file concatenation completed.')

        self.db_loader.batch_insert(output_csv_path)
        # Although isolating the above logic would make the batch_insert re parse the entire 
        # file, but it makes the debugging/retry easier and any DB errors won’t affect file concatenation
        self.logger.info('[ETLPipeline] ETL pipeline completed.')
        if errors:
            self.logger.warning(f'[ETLPipeline] Encountered {len(errors)} errors during processing. Check logs for details.')
