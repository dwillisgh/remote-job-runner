import aiohttp
from codetiming import Timer
from config import settings
from async_retrying import retry


async def get_joblist_results(scrapeconfig):
    async with aiohttp.ClientSession() as session:
        url = '{0}/joblistcrawlresults?rootSiteName={1}&childSiteName={2}&scrapeStartDate={3}'.format(
            settings.SCRAPERESULTSSERVICEROOT,
            scrapeconfig["rootSiteName"],
            scrapeconfig["childSiteName"],
            scrapeconfig["scrapeStartDate"])
        return await call_get_joblist_results(url, session)


@retry(attempts=5)
async def call_get_joblist_results(url, session):
    timer = Timer(text=f"get_joblist_results elapsed time: {{:.1f}}")
    timer.start()
    async with session.get(url) as response:
        timer.stop()
        return await response.json()


async def get_next_job_batch(scrapeconfig, batchnum):
    async with aiohttp.ClientSession() as session:
        url = '{0}/nextjobpostingbatch?rootSiteName={1}&childSiteName={2}&scrapeStartDate={3}&jobPostingBatchId={4}'.format(
            settings.SCRAPERESULTSSERVICEROOT,
            scrapeconfig["rootSiteName"],
            scrapeconfig["childSiteName"],
            scrapeconfig["scrapeStartDate"],
            batchnum)
        return await call_get_next_job_batch(url, session)


@retry(attempts=5)
async def call_get_next_job_batch(url, session):
    timer = Timer(text=f"get_next_job_batch elapsed time: {{:.1f}}")
    timer.start()
    async with session.get(url) as response:
        timer.stop()
        if response.status == 404:
            return None
        return await response.json()


async def detect_remote_job(jobposting):
    async with aiohttp.ClientSession() as session:
        url = '{0}/remotejobdetect'.format(
            settings.REMOTEJOBDETECTIONSERVICEROOT)
        return await call_remote_job_detect(url, session, jobposting)


@retry(attempts=5)
async def call_remote_job_detect(url, session: aiohttp.ClientSession, jobposting):
    timer = Timer(text=f"detect_remote_job elapsed time: {{:.1f}}")
    timer.start()
    async with session.post(url, json=jobposting) as response:
        remotedetectionresponse = await response.text()
        timer.stop()
        return remotedetectionresponse


async def get_child_site_names(scrapeconfig):
    async with aiohttp.ClientSession() as session:
        url = '{0}/atsscraperesultschildkeys?rootSiteName={1}&scrapeStartDate={2}'.format(
            settings.SCRAPERESULTSSERVICEROOT,
            scrapeconfig["rootSiteName"],
            scrapeconfig["scrapeStartDate"])
        return await call_get_child_site_names(url, session)


@retry(attempts=5)
async def call_get_child_site_names(url, session: aiohttp.ClientSession):
    timer = Timer(text=f"get_child_site_names elapsed time: {{:.1f}}")
    timer.start()
    async with session.get(url) as response:
        timer.stop()
        return await response.json()


async def get_root_sites():
    async with aiohttp.ClientSession() as session:
        url = '{0}/parentjobsites'.format(
            settings.JOBSITESERVICEROOT)
        return await call_get_root_sites(url, session)


@retry(attempts=5)
async def call_get_root_sites(url, session: aiohttp.ClientSession):
    timer = Timer(text=f"get_root_sites elapsed time: {{:.1f}}")
    timer.start()
    async with session.get(url) as response:
        timer.stop()
        return await response.json()


async def get_enabled_root_site_names() -> list:
    root_site_names = []
    # get all the enabled root site names
    root_sites = await get_root_sites()
    for root_site in root_sites:
        if root_site["scrapeState"] == "ENABLED":
            root_site_name = root_site["siteName"]
            root_site_names.append(root_site_name)

    root_site_names.sort()
    return root_site_names
