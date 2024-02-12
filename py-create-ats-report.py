import argparse
import os
import json
from collections import OrderedDict

from model.all_ats_company_remote_report import AllAtsCompanyRemoteJobReport
from model.ats_company_remote_job_report import AtsCompanyRemoteJobReport
from model.company_remote_job_report import CompanyRemoteJobReport
from utils.file_utils import list_directory_files, write_buffer_to_file

parser = argparse.ArgumentParser()
parser.add_argument("--source", help="source to find the company reports to roll up", choices=["s3", "filesystem"])
parser.add_argument("scrapeStartDate", help="scrapeStartDate to process")
args = parser.parse_args()

root_path = ""
if args.source == "s3":
    root_path = "~/Documents/remotejobrunner"
else:
    root_path = "~/Documents/remotereruns/" + args.scrapeStartDate.replace(":", "-")
ats_all_company_remote_job_report = AllAtsCompanyRemoteJobReport()

rootSiteDirectories = list_directory_files(root_path)

for rootSite in rootSiteDirectories:
    if rootSite == ".DS_Store":
        continue
    if rootSite == "atsreports":
        continue
    if args.source == "s3":
        path = root_path + "/" + rootSite + "/reports"
    else:
        path = root_path + "/" + rootSite + "/reports"
    report_files = list_directory_files(path)

    ats_company_remote_job_report = AtsCompanyRemoteJobReport()

    ats_company_remote_job_report.rootsitename = rootSite
    ats_all_company_remote_job_report.numatsprocessed += 1

    for file in report_files:
        print("file ", file)
        if file == ".DS_Store":
            continue

        # Opening JSON file
        path = os.path.expanduser(path)
        f = open(os.path.join(path, file), )

        # returns JSON object as
        # a dictionary
        company_report_dict = json.load(f)

        company_report = CompanyRemoteJobReport(**company_report_dict)

        print("root_site_name ", company_report.rootsitename)
        print("child_site_name ", company_report.childsitename)

        ats_company_remote_job_report.numatscompaniesprocessed += 1

        print("numjobsprocessed ", company_report.numjobsprocessed)
        print("numremotejobs ", company_report.numremotejobs)

        ats_company_remote_job_report.numjobsprocessed += company_report.numjobsprocessed
        ats_company_remote_job_report.numremotejobs += company_report.numremotejobs
        ats_company_remote_job_report.numhybridjobs += company_report.numhybridjobs
        ats_company_remote_job_report.numremotejobswithunevenremoteterms += (
            company_report.numremotejobswithunevenremoteterms)
        ats_company_remote_job_report.numnonremotejobs += (
            company_report.numnonremotejobs)
        ats_company_remote_job_report.numatscompaniesprocessed += (
            company_report.numatscompaniesprocessed)
        ats_company_remote_job_report.numclientindicatedjobasremote += (
            company_report.numclientindicatedjobasremote)
        ats_company_remote_job_report.numclientindicatedjobasremotewithnomatches += (
            company_report.numclientindicatedjobasremotewithnomatches)
        ats_company_remote_job_report.numfalsepositiveremotejobsmatched += (
            company_report.numfalsepositiveremotejobsmatched)
        ats_company_remote_job_report.numjobswithconflictingremote += (
            company_report.numjobswithconflictingremote)
        ats_company_remote_job_report.numjobtoinvestigateforremote += (
            company_report.numjobtoinvestigateforremote)
        ats_company_remote_job_report.numnegativeremotejobsmatched += (
            company_report.numnegativeremotejobsmatched)
        ats_company_remote_job_report.numremotejobsnotindicatedbyclient += (
            company_report.numremotejobsnotindicatedbyclient)
        ats_company_remote_job_report.numremotejobswithevenremoteterms += (
            company_report.numremotejobswithevenremoteterms)
        ats_company_remote_job_report.numremotematchincityprocessed += (
            company_report.numremotematchincityprocessed)
        ats_company_remote_job_report.numremotematchindescriptionprocessed += (
            company_report.numremotematchindescriptionprocessed)
        ats_company_remote_job_report.numremotematchintitleprocessed += (
            company_report.numremotematchintitleprocessed)

        if company_report.numremotejobs > 0:
            if ats_company_remote_job_report.companieswithremotejobs is None:
                ats_company_remote_job_report.companieswithremotejobs = {}
            ats_company_remote_job_report.companieswithremotejobs[company_report.childsitename] \
                = company_report.numremotejobs

        if company_report.numjobtoinvestigateforremote > 0:
            if ats_company_remote_job_report.companieswithjobstoinvestigateforremote is None:
                ats_company_remote_job_report.companieswithjobstoinvestigateforremote = {}
            ats_company_remote_job_report.companieswithjobstoinvestigateforremote[company_report.childsitename] \
                = company_report.numjobtoinvestigateforremote

    # sort companies with remote jobs dict in descending order by value
    if ats_company_remote_job_report.companieswithremotejobs is not None:
        companieswithremotejobs_sorted = OrderedDict(
            sorted(ats_company_remote_job_report.companieswithremotejobs.items(), key=lambda t: t[1], reverse=True))
        ats_company_remote_job_report.companieswithremotejobs = companieswithremotejobs_sorted

    # sort companies with jobs to investigate for remote dict in descending order by value
    if ats_company_remote_job_report.companieswithjobstoinvestigateforremote is not None:
        companieswithjobstoinvestigateforremote_sorted = OrderedDict(
            sorted(ats_company_remote_job_report.companieswithjobstoinvestigateforremote.items(),
                   key=lambda t: t[1],
                   reverse=True))
        ats_company_remote_job_report.companieswithjobstoinvestigateforremote = (
            companieswithjobstoinvestigateforremote_sorted)

    # write ats company report
    write_buffer_to_file(root_path + "/atsreports/",
                         rootSite + "_report.json",
                         json.dumps(ats_company_remote_job_report.dict()))

    ats_all_company_remote_job_report.numjobsprocessed += ats_company_remote_job_report.numjobsprocessed
    ats_all_company_remote_job_report.numremotejobs += ats_company_remote_job_report.numremotejobs
    ats_all_company_remote_job_report.numhybridjobs += ats_company_remote_job_report.numhybridjobs
    ats_all_company_remote_job_report.numremotejobswithunevenremoteterms += (
        ats_company_remote_job_report.numremotejobswithunevenremoteterms)
    ats_all_company_remote_job_report.numnonremotejobs += (
        ats_company_remote_job_report.numnonremotejobs)
    ats_all_company_remote_job_report.numatscompaniesprocessed += (
        ats_company_remote_job_report.numatscompaniesprocessed)
    ats_all_company_remote_job_report.numclientindicatedjobasremote += (
        ats_company_remote_job_report.numclientindicatedjobasremote)
    ats_all_company_remote_job_report.numclientindicatedjobasremotewithnomatches += (
        ats_company_remote_job_report.numclientindicatedjobasremotewithnomatches)
    ats_all_company_remote_job_report.numfalsepositiveremotejobsmatched += (
        ats_company_remote_job_report.numfalsepositiveremotejobsmatched)
    ats_all_company_remote_job_report.numjobswithconflictingremote += (
        ats_company_remote_job_report.numjobswithconflictingremote)
    ats_all_company_remote_job_report.numjobtoinvestigateforremote += (
        ats_company_remote_job_report.numjobtoinvestigateforremote)
    ats_all_company_remote_job_report.numnegativeremotejobsmatched += (
        ats_company_remote_job_report.numnegativeremotejobsmatched)
    ats_all_company_remote_job_report.numremotejobsnotindicatedbyclient += (
        ats_company_remote_job_report.numremotejobsnotindicatedbyclient)
    ats_all_company_remote_job_report.numremotejobswithevenremoteterms += (
        ats_company_remote_job_report.numremotejobswithevenremoteterms)
    ats_all_company_remote_job_report.numremotematchincityprocessed += (
        ats_company_remote_job_report.numremotematchincityprocessed)
    ats_all_company_remote_job_report.numremotematchindescriptionprocessed += (
        ats_company_remote_job_report.numremotematchindescriptionprocessed)
    ats_all_company_remote_job_report.numremotematchintitleprocessed += (
        ats_company_remote_job_report.numremotematchintitleprocessed)

    if ats_company_remote_job_report.numremotejobs > 0:
        if ats_all_company_remote_job_report.atswithremotejobs is None:
            ats_all_company_remote_job_report.atswithremotejobs = {}
        ats_all_company_remote_job_report.atswithremotejobs[ats_company_remote_job_report.rootsitename] = (
            ats_company_remote_job_report.numremotejobs)

    if ats_company_remote_job_report.numjobtoinvestigateforremote > 0:
        if ats_all_company_remote_job_report.atswithjobstoinvestigateforremote is None:
            ats_all_company_remote_job_report.atswithjobstoinvestigateforremote = {}
        ats_all_company_remote_job_report.atswithjobstoinvestigateforremote[ats_company_remote_job_report.rootsitename] \
            = ats_company_remote_job_report.numjobtoinvestigateforremote

# sort ats with remote jobs dict in descending order by value
if ats_all_company_remote_job_report.atswithremotejobs is not None:
    atswithremotejobs_sorted = OrderedDict(
        sorted(ats_all_company_remote_job_report.atswithremotejobs.items(), key=lambda t: t[1], reverse=True))
    ats_all_company_remote_job_report.atswithremotejobs = atswithremotejobs_sorted

# sort companies with jobs to investigate for remote dict in descending order by value
if ats_all_company_remote_job_report.atswithjobstoinvestigateforremote is not None:
    atswithjobstoinvestigateforremote_sorted = OrderedDict(
        sorted(ats_all_company_remote_job_report.atswithjobstoinvestigateforremote.items(),
               key=lambda t: t[1],
               reverse=True))
    ats_all_company_remote_job_report.atswithjobstoinvestigateforremote = (
        atswithjobstoinvestigateforremote_sorted)

if args.source == "s3":
    all_ats_reports_path = root_path + "/atsreports/"
else:
    all_ats_reports_path = root_path + "/atsreports/"

write_buffer_to_file(all_ats_reports_path, "atsallcompanyreport.json",
                     json.dumps(ats_all_company_remote_job_report.dict()))
