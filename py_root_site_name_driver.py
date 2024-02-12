import asyncio
import json
import os
import time

import aiohttp
from codetiming import Timer
import argparse

from data.dataService import get_joblist_results, get_child_site_names, get_root_sites
from data.dataService import get_next_job_batch
from data.dataService import detect_remote_job
from model.company_remote_job_report import CompanyRemoteJobReport
from remotejobpolicy.remotejobpolicy import remote_job_policy, write_company_report
from utils.file_utils import list_directory_files

timings = []


async def s3_task(name, work_queue):
    timer = Timer(text=f"Task {name} elapsed time: {{:.1f}}")
    async with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            scrape_config = await work_queue.get()

            # get joblist results
            print(f"Task {name} getting joblist results for: {scrape_config}")
            timer.start()
            joblistresults = await get_joblist_results(scrape_config)
            timer.stop()

            companyremotejobreport = CompanyRemoteJobReport()
            companyremotejobreport.rootsitename = scrape_config["rootSiteName"]
            companyremotejobreport.scrapestartdate = scrape_config["scrapeStartDate"]
            companyremotejobreport.childsitename = scrape_config["childSiteName"]

            # get each job batch  and call remote job detection service on
            # jobPosting
            if joblistresults["totalJobUrlMatchBatches"] > 0:
                for x in range(joblistresults["totalJobUrlMatchBatches"]):
                    jobbatchlist = await get_next_job_batch(scrape_config, x + 1)

                    if jobbatchlist is not None:
                        # loop through all jobs
                        for job in jobbatchlist:
                            remotejobdetectionresponse = await detect_remote_job(job["jobPosting"])
                            await remote_job_policy(scrape_config,
                                                    job["jobPosting"],
                                                    remotejobdetectionresponse,
                                                    companyremotejobreport)

            await write_company_report(scrape_config, companyremotejobreport)
        print(f"task {name} shutting down")


async def load_s3_tasks(work_queue, root_site_name, scrape_start_date):
    # get all the child site names
    scrape_config = {
        "rootSiteName": root_site_name,
        "scrapeStartDate": scrape_start_date
    }
    child_site_names = await get_child_site_names(scrape_config)
    print("child_site_names ", child_site_names)

    # Put some work in the queue
    for child_site_name in child_site_names:
        await work_queue.put({
            "rootSiteName": root_site_name,

            "scrapeStartDate": scrape_start_date,
            "childSiteName": child_site_name,
            "source": "s3"
        })

    # Put some work in the queue
    # for url in [
    #    {
    #        "rootSiteName": rootsitename,
    #        "scrapeStartDate": scrapestartdate,
    #        "childSiteName": "active-opexnow"
    #    }
    # ]:
    #    await work_queue.put(url)

    # Run the tasks
    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(s3_task("One", work_queue)),
            asyncio.create_task(s3_task("Two", work_queue)),
            asyncio.create_task(s3_task("Three", work_queue)),
            asyncio.create_task(s3_task("Four", work_queue)),
            asyncio.create_task(s3_task("Five", work_queue)),
            asyncio.create_task(s3_task("Six", work_queue)),
            asyncio.create_task(s3_task("Seven", work_queue)),
            asyncio.create_task(s3_task("Eight", work_queue)),
            asyncio.create_task(s3_task("Nine", work_queue)),
            asyncio.create_task(s3_task("Ten", work_queue)),
            asyncio.create_task(s3_task("Eleven", work_queue)),
            asyncio.create_task(s3_task("Twelve", work_queue)),
            asyncio.create_task(s3_task("Thirteen", work_queue)),
            asyncio.create_task(s3_task("Fourteen", work_queue)),
            asyncio.create_task(s3_task("Fifteen", work_queue)),
            asyncio.create_task(s3_task("Sixteen", work_queue)),
            asyncio.create_task(s3_task("Seventeen", work_queue)),
            asyncio.create_task(s3_task("Eighteen", work_queue)),
            asyncio.create_task(s3_task("Nineteen", work_queue)),
            asyncio.create_task(s3_task("Twenty", work_queue)),
            asyncio.create_task(s3_task("TwentyOne", work_queue)),
            asyncio.create_task(s3_task("TwentyTwo", work_queue)),
            asyncio.create_task(s3_task("TwentyThree", work_queue)),
            asyncio.create_task(s3_task("TwentyFour", work_queue)),
            asyncio.create_task(s3_task("TwentyFive", work_queue)),
            asyncio.create_task(s3_task("TwentySix", work_queue)),
            asyncio.create_task(s3_task("TwentySeven", work_queue)),
            asyncio.create_task(s3_task("TwentyEight", work_queue)),
            asyncio.create_task(s3_task("TwentyNine", work_queue)),
            asyncio.create_task(s3_task("Thirty", work_queue)),
            asyncio.create_task(s3_task("ThirtyOne", work_queue)),
            asyncio.create_task(s3_task("ThirtyTwo", work_queue)),
            asyncio.create_task(s3_task("ThirtyThree", work_queue)),
            asyncio.create_task(s3_task("ThirtyFour", work_queue)),
            asyncio.create_task(s3_task("ThirtyFive", work_queue)),
            asyncio.create_task(s3_task("ThirtySix", work_queue)),
            asyncio.create_task(s3_task("ThirtySeven", work_queue)),
            asyncio.create_task(s3_task("ThirtyEight", work_queue)),
            asyncio.create_task(s3_task("ThirtyNine", work_queue)),
            asyncio.create_task(s3_task("Forty", work_queue)),
        )


async def filesystem_task(name, work_queue, scrape_config):
    total_elapsed_time = 0
    company_remote_job_report = CompanyRemoteJobReport()
    while not work_queue.empty():
        remote_file_config = await work_queue.get()

        # load the job posting from file
        path = os.path.expanduser(remote_file_config["path"])
        f = open(os.path.join(path, remote_file_config["file"]), )

        # returns JSON object as
        # a dictionary
        job_posting = None
        try:
            job_posting = json.load(f)
        except UnicodeDecodeError as error:
            file = remote_file_config["file"]
            print(f"file {file} tossed exception {error}")
            continue

        start_time = time.time()
        remote_job_detection_response = await detect_remote_job(job_posting)
        end_time = time.time()
        elapsed_time = end_time - start_time
        total_elapsed_time += elapsed_time

        await remote_job_policy(scrape_config,
                                job_posting,
                                remote_job_detection_response,
                                company_remote_job_report)
    await write_company_report(scrape_config, company_remote_job_report)
    print(f"task {name} shutting down")
    timings.append(total_elapsed_time)


async def load_filesystem_tasks(work_queue, root_site_name, scrape_start_date):
    # get all the local file system files to process
    root_path = "~/Documents/remotejobrunner"
    path = root_path + "/" + root_site_name + "/"

    remote_directories = list_directory_files(path)
    for remote_directory in remote_directories:

        if remote_directory == ".DS_Store":
            continue
        if remote_directory == "reports":
            continue

        files_path = path + remote_directory

        remote_files = list_directory_files(files_path)
        for remote_file in remote_files:
            await work_queue.put({
                "path": files_path,
                "file": remote_file
            })

    # Run the tasks
    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(filesystem_task("One", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "One",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("TWO", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "TWO",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("THREE", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "THREE",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("FOUR", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "FOUR",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("FIVE", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "FIVE",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("SIX", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "SIX",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("SEVEN", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "SEVEN",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("EIGHT", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "EIGHT",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("NINE", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "NINE",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("TEN", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "TEN",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("ELEVEN", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "ELEVEN",
                "source": "filesystem"
            })),
            asyncio.create_task(filesystem_task("TWELVE", work_queue, {
                "rootSiteName": root_site_name,
                "scrapeStartDate": scrape_start_date,
                "childSiteName": "allchildern-" + "TWELVE",
                "source": "filesystem"
            })),
        )

        total_time_seconds = 0
        for timing in timings:
            total_time_seconds += timing
        print(total_time_seconds)


async def process_root_site(source, root_site_name, scrape_start_date):
    # Create the queue of work
    work_queue = asyncio.Queue()

    if source == "s3":
        await load_s3_tasks(work_queue, root_site_name, scrape_start_date)

    if source == "filesystem":
        await load_filesystem_tasks(work_queue, root_site_name, scrape_start_date)


async def main():
    """
    This is the main entry point for the program
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="load jobs from source", choices=["s3", "filesystem"])
    parser.add_argument("rootSiteName", help="rootSiteName to process")
    parser.add_argument("scrapeStartDate", help="scrapeStartDate to process")
    args = parser.parse_args()

    source = args.source
    root_site_name = args.rootSiteName
    scrape_start_date = args.scrapeStartDate

    await process_root_site(source, root_site_name, scrape_start_date)


if __name__ == "__main__":
    asyncio.run(main())
