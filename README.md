# Demo Stock Update aggregator
This is a demo application, that takes in stock updates, batches them with Kinesis Firehose and then store it in S3.
Based on the S3 object notifications, Lambda parses the files and group the results based on multiple partitions (retailer, account) and only logs the latest stock update per article.

![Architecture overview](img/Architecture%20Overview.png)

## Getting started

Make sure you have Python 3.11 and the AWS SAM installed. Then you can run

```
sam build
```

After that you can deploy the application. Keep in mind for the first time, you should add the flag `--guided` to configure the stack name and the `StockUpdateBucketName`. See also the [samconfig.example.toml](samconfig.example.toml) example configuration file.

To deploy run
```
sam deploy
```

## Generate load

This project uses [Amazon Kinesis Data Generator (KDG)](https://awslabs.github.io/amazon-kinesis-data-generator/web/producer.html), for generating the example data.

### For real load for 5 minute batching windows with 300 RPS
```js
{
    "accountId" : {{random.number(100)}},
    "retailerId": "retailer-{{random.number(90)}}",
    "channelId" : {{random.number(10)}},
    "productId" : "{{random.number(1000)}}",
    "articleId" : {{random.number(1000)}},
    "articleNumber" : {{random.number(1000)}},
    "lastUpdated" : "{{date.utc('YYYY-MM-DDTHH:mm:ss.SSSZ')}}",
    "quantity" : {{random.number(100)}}
}
```

### For test loads with 1 minute window with 1 RPS
```js
{
    "accountId" : {{random.number(10)}},
    "retailerId": "retailer-{{random.number(1)}}",
    "channelId" : {{random.number(10)}},
    "productId" : "{{random.number(1000)}}",
    "articleId" : {{random.number(10)}},
    "articleNumber" : {{random.number(1000)}},
    "lastUpdated" : "{{date.utc('YYYY-MM-DDTHH:mm:ss.SSSZ')}}",
    "quantity" : {{random.number(100)}}
}
```

## Explore the data
To explore the data, you can use the example [Demo Jupyter Notebook](notebook/demo-stock-update-awswrangler.ipynb) and use the [example.parquet](example.parquet) file.
