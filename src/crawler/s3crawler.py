import asyncio
import datetime

import boto3
import click


@click.command()
@click.option("-d1", "--start", required=True)
@click.option("-d2", "--end", required=True)
@click.option("-p", "--path", required=True)
@click.option("-b", "--bucket", required=True)
@click.option("-o", "--output", required=True)
def crawl(start, end, path, bucket, output):
    """Crawl cloud bucket storage to find files that were created within the specified date range."""
    """ Example usage:  """
    """python s3crawler.py -d1 1/1/2025 -d2 2/1/2025 -p abc/def/ -o output.txt"""
    start_date = datetime.strptime(start, "%m/%d/%Y")
    end_date = datetime.strptime(end, "%m/%d/%Y")
    asyncio.run(crawl_s3_files(bucket, path, start_date, end_date, output))


async def crawl_s3_files(bucket_name, prefix, start_date, end_date, output_file):
    s3 = boto3.client("s3", region_name="us-east-1")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    output = []

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                if start_date <= obj["LastModified"] <= end_date:
                    print(f"Key: {obj['Key']}, Last Modified: {obj['LastModified']}")
                    output.append(
                        f"Key: {obj['Key']}, Last Modified: {obj['LastModified']}"
                    )

    with open(output_file, "w") as file:
        for line in output:
            file.write(line + "\n")


if __name__ == "__main__":
    crawl()
