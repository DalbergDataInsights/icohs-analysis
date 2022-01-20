# from src.pipeline import clean, process, indic
from src.helpers import INDICATORS, make_note, get_unique_indics, get_engine
import os
from datetime import datetime
import json
import pandas as pd
import src.pipeline.indic as indic
from src.db.adpter import (pg_recreate_tables, pg_read)
from src.db import adpter as db  # NOQA: E402
import src.pipeline_main as pipeline
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt

from dotenv import load_dotenv, find_dotenv  # NOQA: E402

load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402

# from src.db import adpter as db  # NOQA: E402
from src.api.ddi_dhis2 import Dhis  # NOQA: E402

START_TIME = datetime.now()

with open(INDICATORS["data_config"], "r", encoding="utf-8") as f:
    VAR_CORR = json.load(f)
import argparse
# import src.api_main as api
# import src.pipeline_main as pipeline

commands = {
    "setupdb": "Set up the postgres SQL database on the very first run",
    "bulk": "Run the API download and the pipeline for all months since Jan 2018",
    "latest": "Run the API download and the pipeline for the latest months",
    "apibulk": "Run the API download for all months since Jan 2018",
    "apilatest": "Run the API download and the pipeline for the latest months",
    "pipeline": "Run the pipeline for all data using already cleaned files",
    "pipelinebulkclean": "Run the pipeline for all data, recleaning all files",
}

def split_data(table_names, path="data/temp", chunk_size=100000):
    files_to_push = []
    for output in table_names:
        make_note(f"Reformatting data for the DHIS2 repo", START_TIME)
        df = db.pg_read(output)
        df = indic.transform_for_dhis2(df=df, map=db.pg_read("indicator"), outtype=output[:3])
        print(df)
        print(output + "=======================================>>>")
        filepath = f"{path}/{output}_dhis.csv"
        chunks_filepath = f"{path}/chunk_{output}"
        df.to_csv(filepath, index=False)
        for index, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
            chunk_filename = chunks_filepath + str(index) + '.csv'
            chunk.to_csv(chunk_filename, index=False)
            files_to_push.append(chunk_filename)
    return files_to_push
    
    


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--action", choices=list(commands.keys()), default="latest")
    parser.add_argument(
        "-m", "--months", choices=[str(i) for i in range(1, 25)], default=3)

    args = parser.parse_args()
    # pipeline.db.pg_recreate_tables()

    #Send off to DHIS2

    api = Dhis(
        os.environ.get("API_USERNAME"),
        os.environ.get("API_PASSWORD"),
        "https://hmis-repo.health.go.ug",

    )
    start_date = datetime.today()
    start_date = '2021-04-01'
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    new_instance_dataset_id = [id_ for name, id_ in get_engine("config/data_elements.json", "new_datasetIDs").items()]
    new_instance_report = [id_ for name, id_ in get_engine("config/data_elements.json", "report_new").items()]
    for i in range(3):
        end_date = start_date - relativedelta(months=1)
        filename_month = format(end_date, '%b %Y').split()[0]
        filename_year = format(end_date, '%b %Y').split()[1]
        data_path = "new_main" + "_" + filename_year + "_" + filename_month + ".csv"
        report_path = "new_report" + "_" + filename_year + "_" + filename_month + ".csv"

        api.get(new_instance_dataset_id, end_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d'), rename=True, filename="data/input/" + data_path, orgUnit=None)
        api.get_report(new_instance_report[0], end_date.strftime('%Y-%m-%d'), filepath="data/input/" + report_path)
        start_date = end_date

    if args.action == "pipelinebulkclean":
        pipeline.clean.move_csv_files_to_input()

    if any(
        args.action in s for s in ["bulk", "latest", "pipeline", "pipelinebulkclean"]
    ):
        pipeline.run()

    files_to_push = split_data(["outlier_output",
    "std_no_outlier_output",
    "iqr_no_outlier_output",
    "report_output"])
  

    # api.post(files_to_push) #-- uncoment to push to dhis2

   