import concurrent.futures
from logging_config import logging
from etl_csv_parser.csv import CSVProcessor
from config import CHUNK_SIZE, TEMP_DIR, OUTPUT_CSV_PATH
from etl_csv_parser.db_loader import DatabaseLoader

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, csv_path, db_loader: DatabaseLoader, chunk_size=CHUNK_SIZE, temp_dir=TEMP_DIR):
        self.csv_processor = CSVProcessor(csv_path, chunk_size, temp_dir)
        self.db_loader = db_loader
        

    def _concatenate(self, output_csv_path, ordered_temp_files):
        """
        Combine processed chunks into an output file
        """
        self.csv_processor.concatenate(output_csv_path, temp_files=ordered_temp_files)
        logger.info('[ETLPipeline] Output file concatenation completed.')


    def _transform(self):
        """
        Parse, clean, validate, create chunk files
        """
        errors = []
        temp_file_map = dict() # chunk_idx -> temp_file_path
        temp_files = []
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
            logger.error('[ETLPipeline] No chunks processed successfully. Exiting.')
        else:
            temp_files = [temp_file_map[idx] for idx in sorted(temp_file_map)]
        return temp_files, errors
        
    def run(self, output_csv_path=OUTPUT_CSV_PATH):
        logger.info('[ETLPipeline] Starting ETL pipeline')
        
        ordered_temp_files, errors = self._transform()
        if ordered_temp_files:
            self._concatenate(output_csv_path, ordered_temp_files)
            # todo: can add a logic to delete the temporary chunk files if needed
            try:
                self.db_loader.batch_insert(output_csv_path)
            except Exception as e:
                logger.error(f'[ETLPipeline] Error during batch insert, you may have to retry: {e}')
            # Although isolating the above logic would make the batch_insert re parse the entire 
            # file, but it makes the debugging/retry easier and any DB errors won't affect file concatenation
            logger.info('[ETLPipeline] ETL pipeline completed.')
        if errors:
            logger.warning(f'[ETLPipeline] Encountered {len(errors)} errors during processing. Check logs for details.')
