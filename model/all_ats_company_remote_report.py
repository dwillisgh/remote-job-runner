from pydantic import BaseModel
from typing import Optional, Dict


class AllAtsCompanyRemoteJobReport(BaseModel):
    # number of job posting processed
    numjobsprocessed: Optional[int] = 0
    # number of non-remote jobs
    numnonremotejobs: Optional[int] = 0
    # number of remote jobs
    numremotejobs: Optional[int] = 0
    # number of hybrid jobs
    numhybridjobs: Optional[int] = 0
    # number of false positive remote matches
    numfalsepositiveremotejobsmatched: Optional[int] = 0
    # number of negative remote matches
    numnegativeremotejobsmatched: Optional[int] = 0
    # number of companies processed
    numatscompaniesprocessed: Optional[int] = 0
    # number of ats processed
    numatsprocessed: Optional[int] = 0
    # number of client indicated jobpostings indicated as remote
    numclientindicatedjobasremote: Optional[int] = 0
    # number of client indicated jobpostings indicated as remote with no remote matches
    numclientindicatedjobasremotewithnomatches: Optional[int] = 0
    # number of remote jobpostings not indicated as remote by client
    numremotejobsnotindicatedbyclient: Optional[int] = 0
    # remote match in title
    numremotematchintitleprocessed: Optional[int] = 0
    # remote match in city
    numremotematchincityprocessed: Optional[int] = 0
    # remote match in description
    numremotematchindescriptionprocessed: Optional[int] = 0
    # number of jobpostings to investigate for remote
    numjobtoinvestigateforremote: Optional[int] = 0
    # number of jobpostings with conflicting remote
    numjobswithconflictingremote: Optional[int] = 0
    # number of jobpostings with at least 1 remote match
    # ,but  it has uneven remote terms found in description
    numremotejobswithunevenremoteterms: Optional[int] = 0
    # number of jobpostings with at least 1 remote match
    # and all remote terms match  a remote pattern (non,falsepositive,remote)
    numremotejobswithevenremoteterms: Optional[int] = 0
    # dict of ats with remote jobs
    atswithremotejobs: Optional[Dict[str, int]]
    # dict of ats with jobs to investigate for remote
    atswithjobstoinvestigateforremote: Optional[Dict[str, int]]
