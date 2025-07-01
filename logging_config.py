import logging.config
import os

logging.config.dictConfig({
   'version': 1,
   'disable_existing_loggers': False,
   'formatters': {
       'standard': {
           'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
           'datefmt': '%Y-%m-%d %H:%M:%S'
       }
   },
   'handlers': {
       'console': {
           'class': 'logging.StreamHandler',
           'level': os.getenv('LOG_LEVEL', 'INFO'),
           'formatter': 'standard',
           'stream': 'ext://sys.stdout'
       },
       # 'file': {
       #     'class': 'logging.FileHandler',
       #     'level': 'INFO',
       #     'formatter': 'standard',
       #     'filename': f"{HOME_DIR}/lala.log",
       #     'mode': 'a'
       # }
   },
   'root': {
       # 'handlers': ['console', 'file'],
       'handlers': ['console'],
       'level': os.getenv('LOG_LEVEL', 'INFO')
   }
})
