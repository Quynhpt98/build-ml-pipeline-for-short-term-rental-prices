#!/usr/bin/env python
"""
 Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    df = pd.read_csv(artifact_local_path)

    min_price = args.min_price
    max_price = args.max_price

    logger.info("Removing outlier price in dataset...")
    df = df[df.price.between(min_price, max_price)].reset_index(drop=True)
    logger.info(f"Data contains {len(df)} rows with price from {min_price} to {max_price}")
    logger.info("Datatime data type normalizing...")
    dt_cols = 'last_review'
    df[dt_cols] = pd.to_datetime(df[dt_cols])
    logger.info(f"Convert column {dt_cols} to datetime datatype succeed.")
    logger.info("Clean longitude and latitude...")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    logger.info("Clean data completely, save cleaned data...")
 
    # Save result to WanDB
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str, 
        help="Output Artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output type",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="the maximum price to consider",
        required=True
    )
    
    parser.add_argument(
        "--output_description", 
        type=str,
        help="output description",
        required=True
    )

    args = parser.parse_args()

    go(args)
