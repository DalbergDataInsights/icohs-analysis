from src.pipeline import clean, process, indic
from src.helpers import INDICATORS, make_note, get_unique_indics, get_engine
import os
from datetime import datetime
import json

from dotenv import load_dotenv, find_dotenv  # NOQA: E402

load_dotenv(find_dotenv(), verbose=True)  # NOQA: E402

from src.db import adpter as db  # NOQA: E402
from src.api.ddi_dhis2 import Dhis  # NOQA: E402

START_TIME = datetime.now()

with open(INDICATORS["data_config"], "r", encoding="utf-8") as f:
    VAR_CORR = json.load(f)
import argparse
import src.api_main as api
import src.pipeline_main as pipeline

commands = {
    "setupdb": "Set up the postgres SQL database on the very first run",
    "bulk": "Run the API download and the pipeline for all months since Jan 2018",
    "latest": "Run the API download and the pipeline for the latest months",
    "apibulk": "Run the API download for all months since Jan 2018",
    "apilatest": "Run the API download and the pipeline for the latest months",
    "pipeline": "Run the pipeline for all data using already cleaned files",
    "pipelinebulkclean": "Run the pipeline for all data, recleaning all files",
}

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--action", choices=list(commands.keys()), default="latest"
    )
    parser.add_argument(
        "-m", "--months", choices=[str(i) for i in range(1, 25)], default=3
    )

    args = parser.parse_args()

    #Send off to DHIS2
    
    api = Dhis(
        os.environ.get("API_USERNAME"),
        os.environ.get("API_PASSWORD"),
        "https://repo.hispuganda.org/repo/api",
    )
    # downloading from dhis2
    new_instance_dataset_id = [id_ for name, id_ in get_engine("config/data_elements.json", "new_datasetIDs").items()]
    api.get(new_instance_dataset_id, "startDate", "endDate", rename=True, filename="data.csv", orgUnit=None)
    api.get_report()
    # Checking for 'special' commands

    if args.action == "setupdb":
        pipeline.db.pg_recreate_tables()

    # Running the API

    if any(args.action in s for s in ["bulk", "apibulk"]):
        api.run("new", "bulk", int(args.months))
        api.run("old", "bulk", int(args.months))

        df = db.pg_read(output) # read from csv
        df = indic.transform_for_dhis2(
            df=df, map=db.pg_read("indicator"), outtype=output[:3]
        )
        filepath = f"data/temp/{output}_dhis.csv"
        df.to_csv(filepath, index=False)
        make_note(f"Publishing {output} to the DHIS2 repo", START_TIME)

        api.post([filepath]) # post to dhis2
    if any(args.action in s for s in ["latest", "apilatest"]):
        api.run("new", "current", int(args.months))

    # Checking if files needs to be moved

    if args.action == "pipelinebulkclean":
        pipeline.clean.move_csv_files_to_input()

    # TODO Also add something that only keeps the files in the three years before

    # Running the pipeline

    if any(
        args.action in s for s in ["bulk", "latest", "pipeline", "pipelinebulkclean"]
    ):
        pipeline.run()
