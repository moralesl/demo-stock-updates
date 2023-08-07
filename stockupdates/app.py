from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit

import awswrangler as wr
import pandas as pd
from urllib import parse

tracer = Tracer()
log = Logger()
metrics = Metrics(namespace="DemoStockUpdates")


# Lambda handler takes s3 ObjectCreated event and for every file record included loads the object in to a pandas dataframe, extract the number of records and adds the metrics and then sorts the data frame by channelid
@tracer.capture_method
def process_stock_updates(event: dict, context: LambdaContext):
    metrics.add_metric(name="S3ObjectInvocation", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name="NumberOfRecords", unit=MetricUnit.Count, value=len(event['Records']))

    for record in event["Records"]:
        # Get the bucket name and unquote the key name
        bucket = record["s3"]["bucket"]["name"]
        key = parse.unquote(record["s3"]["object"]["key"], encoding="utf-8")

        # Load the file into a dataframe
        df = wr.s3.read_parquet(f"s3://{bucket}/{key}")

        # Extract the number of records and add the metrics
        metrics.add_metric(name="NumberOfStockUpdates", unit=MetricUnit.Count, value=len(df))

        # Sort the data frame by accountid/brand id
        df_sorted = df.sort_values(['accountid', 'articleid', 'lastupdated'])

        # Step 2: Use groupby and aggregation to get the latest stock update per article for each brand
        latest_updates = df_sorted.groupby(['accountid', 'articleid'], as_index=False).last()

        # Step 3: Organize the data and create the output dictionary
        brand_articles = latest_updates.groupby('accountid').apply(lambda x: {
            "account_id": x['accountid'].iloc[0],
            "retailer_id": x['retailerid'].iloc[0],
            "amount_articles": len(x),
            "articles": x[["articleid", "articlenumber", "quantity", "productid", "accountid", "channelid"]].to_dict(orient="records")
        }).tolist()

        for brand_article in brand_articles:
            log.debug(f"Stock update for brand {brand_article['account_id']} with {brand_article['amount_articles']} entries: {brand_article['articles']}")
            log.info(brand_article)

# Enrich logging with contextual information from Lambda
@log.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return process_stock_updates(event, context)
