import asyncio

from data.dataService import get_enabled_root_site_names
import subprocess
import argparse

from py_root_site_name_driver import process_root_site


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scrapeStartDate", help="scrapeStartDate to process")
    args = parser.parse_args()

    scrape_start_date = args.scrapeStartDate

    root_sites = await get_enabled_root_site_names()

    for root_site in root_sites:
        print(f"processing {root_site}")
        if root_site < "workdayhumancapital":
            continue
        await process_root_site("s3", root_site, scrape_start_date)


if __name__ == "__main__":
    asyncio.run(main())
