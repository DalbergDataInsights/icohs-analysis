import argparse
import src.api_main as api
import src.pipeline_main as pipeline

commands = {
    "setupdb": "Set up the postgres SQL database on the very first run",
    "checkconfig": "Check coherence of the data_config file before running the pipeline",
    "bulk": "Run the API download and the pipeline for all months since Jan 2018",
    "latest": "Run the API download and the pipeline for the latest months",
    "apibulk": "Run the API download for all months since Jan 2018",
    "apilatest": "Run the API download and the pipeline for the latest months",
    "pipeline": "Run the pipeline for all data using already cleaned files",
    "pipelinebulk": "Run the pipeline for all data using, recleaning all files",
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

    # Checking for 'special' commands

    if args.action == "setupdb":
        pipeline.db.pg_recreate_tables()

    if args.action == "checkconfig":
        print("TBC")  # TODO Create the function

    # Running the API

    if any(args.action in s for s in ["bulk", "apibulk"]):
        api.run("new", "bulk", int(args.months))
        api.run("old", "bulk", int(args.months))
        # TODO Chec that ths works for the old instance

    if any(args.action in s for s in ["latest", "apilatest"]):
        api.run("new", "current", args.months)

    # Checking if files needs to be moved

    if args.action == "pipelinebulk":
        print("TBC")
        # TODO Create the function that moves all processed files back to input
        # TODO Also add something that only keeps the files in the three years before

    # Running the pipeline

    if any(args.action in s for s in ["bulk", "latest", "pipeline", "pipelinebulk"]):
        pipeline.run()
