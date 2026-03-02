import logging
import azure.functions as func
from main import main as run_script

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Weather job started.")
    run_script()
    logging.info("Weather job finished.")