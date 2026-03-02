# orchestration/__init__.py
import logging
import azure.functions as func
import traceback
from main import main as run_script

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Timer triggered")
    try:
        run_script()
    except Exception as e:
        logging.error("ERROR OCCURRED:")
        logging.error(str(e))
        logging.error(traceback.format_exc())
        raise