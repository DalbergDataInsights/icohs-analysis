import argparse
import src.api as api
import src.pipeline as pipeline

commands = {
    "setupdb": "Set up the postgres SQL database on the very first run",
    "checkconfig": "Check coherence of the data_config file before running the pipeline",
    "bulkrun": "Run the API download and the pipeline for all months since Jan 2018",
    "latestrun": "Run the API download and the pipeline for the latest months",
    "apibulkrun": "Run the API download for all months since Jan 2018",
    "apilatestrun": "Run the API download and the pipeline for the latest months",
    "pipelinerun": "Run or rerun the pipeline for all data in input and clean",
}

# TODO add a help

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("action", choice=list(commands.keys()), default="standardrun")
    parser.add_argument("months", choices=range(1, 37), default=3)

    args = parser.parse_args()

    # Checking for 'special' commands

    if args.action == "setupdb":
        pipeline.pg_recreate_tables()

    if args.action == "checkconfig":
        print("TBC")  # TODO Create the function

    # Running the API

    if args.action.isin(["bulkrun", "apibulkrun"]):
        api.run("new", "bulk", args.months)
        api.run("old", "bulk", args.months)

    if args.action.isin(["latestrun", "apilatestrun"]):
        api.run("new", "current", args.months)

    # Checking if files needs to be moved

    if args.action == "pipelinerun":
        print("TBC")
        # TODO Create the function that moves all processed files back to input

    # Running the pipeline

    if args.action.isin(
        ["bulkrun", "apibulkrun", "latestrun", "apilatestrun", "pipelinerun"]
    ):
        pipeline.run()
