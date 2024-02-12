import json
from json import JSONDecodeError

from model.company_remote_job_report import CompanyRemoteJobReport
from utils.file_utils import write_buffer_to_file, \
    replace_special_chars_to_underscore


async def remote_job_policy(scrape_config, job_posting, remote_job_detection_response_json, companyremotejobreport):
    print(f"remotejobdetectionresponsejson {remote_job_detection_response_json}")
    print("processing ", job_posting["identifier"]["value"])

    companyremotejobreport.rootsitename = scrape_config["rootSiteName"]
    companyremotejobreport.scrapestartdate = scrape_config["scrapeStartDate"]
    companyremotejobreport.childsitename = scrape_config["childSiteName"]

    companyremotejobreport.numjobsprocessed += 1

    remotejobdetectionresponse = None
    try:
        remotejobdetectionresponse = json.loads(remote_job_detection_response_json)
    except JSONDecodeError as error:
        print(f"remotejobdetectionresponse {remotejobdetectionresponse}, error {error}")
        return remotejobdetectionresponse

    clientindicatedjobasremote = False
    clientindicatedjobasremotewithnomatches = False
    remotejobmatchfound = False
    hybridjobmatchfound = False
    jobindicatedasnonremote = False
    remotetermsfoundondescriptionsize = 0
    jobfalsepositiveremotesize = 0
    jobnonremotesize = 0
    remotedescriptionmatchessize = 0
    inspectjobforremote = False
    conflictingremote = False
    remotejobswithunevenremotetermmatches = False
    remotejobswithevenremotetermmatches = False

    remotejobintitleonly = False
    remotejobincityonly = False

    # job is not remote
    if ((remotejobdetectionresponse["descriptionmatches"] is None)
            and (remotejobdetectionresponse["titlematches"] is None)
            and (remotejobdetectionresponse["addresslocalitymatches"] is None)):
        remotejobmatchfound = False
        companyremotejobreport.numnonremotejobs += 1

    # job is remote only in title
    if ((remotejobdetectionresponse["descriptionmatches"] is None)
            and (remotejobdetectionresponse["titlematches"] is not None)
            and (remotejobdetectionresponse["addresslocalitymatches"] is None)):
        remotejobintitleonly = True

    # job is remote only in title
    if ((remotejobdetectionresponse["descriptionmatches"] is None)
            and (remotejobdetectionresponse["titlematches"] is None)
            and (remotejobdetectionresponse["addresslocalitymatches"] is not None)):
        remotejobincityonly = True

    # job is hybrid in description
    if remotejobdetectionresponse["descriptionhybridmatches"] is not None:
        hybridjobmatchfound = True
        companyremotejobreport.numhybridjobs += 1

    # client indicated job as remote
    if remotejobdetectionresponse["jobLocationType"] == "TELECOMMUTE":
        clientindicatedjobasremote = True
        companyremotejobreport.numclientindicatedjobasremote += 1

    if remotejobdetectionresponse["jobLocationType"] == "TELECOMMUTE" and remotejobmatchfound is False:
        clientindicatedjobasremotewithnomatches = True
        companyremotejobreport.numclientindicatedjobasremotewithnomatches += 1

    # description has at least 1 false positive remote match
    if remotejobdetectionresponse["descriptionfalsepositiveremotematches"] is not None:
        jobfalsepositiveremotesize = len(remotejobdetectionresponse["descriptionfalsepositiveremotematches"])
        companyremotejobreport.numfalsepositiveremotejobsmatched += 1

    # description has at least 1 nonremote match
    if remotejobdetectionresponse["descriptionnonremotematches"] is not None:
        jobindicatedasnonremote = True
        jobnonremotesize = len(remotejobdetectionresponse["descriptionnonremotematches"])
        companyremotejobreport.numnonremotejobs += 1

    # remote job with at least 1 description match
    if remotejobdetectionresponse["descriptionmatches"] is not None:
        remotejobmatchfound = True
        companyremotejobreport.numremotematchindescriptionprocessed += 1
        remotedescriptionmatchessize = len(remotejobdetectionresponse["descriptionmatches"])

    # remote job with at least 1 title match
    if remotejobdetectionresponse["titlematches"] is not None:
        remotejobmatchfound = True
        companyremotejobreport.numremotematchintitleprocessed += 1

    # remote job with at least 1 locality match
    if remotejobdetectionresponse["addresslocalitymatches"] is not None:
        remotejobmatchfound = True
        companyremotejobreport.numremotematchincityprocessed += 1

    # found at least 1 remote term in description
    if remotejobdetectionresponse["descriptionremotematches"] is not None:
        remotetermsfoundondescriptionsize = len(remotejobdetectionresponse["descriptionremotematches"])
        companyremotejobreport.numremotematchindescriptionprocessed += 1

    # job indicated as remote yet the client didn't indicate job as remote
    if clientindicatedjobasremote is False and remotejobmatchfound is True:
        companyremotejobreport.numremotejobsnotindicatedbyclient += 1

    # indicate to look at the jobPosting for a potential remote match
    if (remotejobmatchfound is False
            and remotetermsfoundondescriptionsize > 0
            and remotetermsfoundondescriptionsize > (jobfalsepositiveremotesize + jobnonremotesize)):
        companyremotejobreport.numjobtoinvestigateforremote += 1
        inspectjobforremote = True

    # indicate a job with a match as remote job and a job that is not remote
    if (remotejobmatchfound is True
            and jobindicatedasnonremote is True):
        conflictingremote = True
        companyremotejobreport.numjobswithconflictingremote += 1

    if remotejobmatchfound is True:
        companyremotejobreport.numremotejobs += 1

    if (remotejobmatchfound is True
            and remotetermsfoundondescriptionsize !=
            (remotedescriptionmatchessize + jobfalsepositiveremotesize + jobnonremotesize)):
        remotejobswithunevenremotetermmatches = True
        companyremotejobreport.numremotejobswithunevenremoteterms += 1

    if (remotejobmatchfound is True
            and remotetermsfoundondescriptionsize ==
            (remotedescriptionmatchessize + jobfalsepositiveremotesize + jobnonremotesize)):
        remotejobswithevenremotetermmatches = True
        companyremotejobreport.numremotejobswithevenremoteterms += 1

    path = ""
    if scrape_config["source"] == "s3":
        path = "~/Documents/remotejobrunner/" + scrape_config["rootSiteName"]
    else:
        path = ("~/Documents/remotereruns/" + scrape_config["scrapeStartDate"].replace(":", "-")
                + "/" + scrape_config["rootSiteName"] + "/")

    jobpostingpath = path + "/jobpostingnoremote"

    if clientindicatedjobasremotewithnomatches:
        jobpostingpath = path + "/clientindicatedjobasremotewithnomatches"

    if jobindicatedasnonremote:
        jobpostingpath = path + "/negatives"

    if jobfalsepositiveremotesize > 0:
        jobpostingpath = path + "/falsepositives"

    if inspectjobforremote:
        jobpostingpath = path + "/remotewithunevenmatches"

    if conflictingremote:
        jobpostingpath = path + "/conflictingremotejobs"

    if remotejobswithunevenremotetermmatches:
        jobpostingpath = path + "/remotejobswithunevenremotetermmatches"

    if remotejobswithevenremotetermmatches:
        jobpostingpath = path + "/remotejobswithevenremotetermmatches"

    if remotejobintitleonly:
        jobpostingpath = path + "/remotejobintitleonly"

    if remotejobincityonly:
        jobpostingpath = path + "/remotejobincityonly"

    companyname = job_posting["identifier"]["name"]
    companyname = replace_special_chars_to_underscore(companyname, 50)
    jobid = job_posting["identifier"]["value"]
    jobid = replace_special_chars_to_underscore(jobid, 100)
    jobpostingfilename = (scrape_config["rootSiteName"] + "--" + scrape_config["childSiteName"] + "--"
                          + companyname + "--" + jobid)

    # don't write jobpostingnoremote
    if remotetermsfoundondescriptionsize == 0 and remotejobmatchfound is False:
        return

    write_buffer_to_file(jobpostingpath, jobpostingfilename, json.dumps(job_posting))


async def write_company_report(scrape_config, company_remote_job_report):
    # write out company report
    path = ""
    if scrape_config["source"] == "s3":
        path = "~/Documents/remotejobrunner/" + scrape_config["rootSiteName"]
    else:
        path = ("~/Documents/remotereruns/" + scrape_config["scrapeStartDate"].replace(":", "-")
                + "/" + scrape_config["rootSiteName"] + "/")

    report_path = path + "/reports"
    report_filename = (scrape_config["rootSiteName"] + "--" + scrape_config["childSiteName"] + "--"
                       + "companyremotejobreport.json")
    write_buffer_to_file(report_path, report_filename, json.dumps(company_remote_job_report.dict()))
