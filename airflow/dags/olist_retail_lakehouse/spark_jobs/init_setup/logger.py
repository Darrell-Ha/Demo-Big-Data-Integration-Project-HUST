import logging
from olist_retail_lakehouse.spark_jobs.init_setup.setup_const import LOGGING_FORMAT

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
log = logging.getLogger(__name__)