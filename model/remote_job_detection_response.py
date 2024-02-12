from pydantic import BaseModel
from typing import Optional, List


class RemoteJobDetectionResponse(BaseModel):
    descriptionmatches: Optional[List[str]] = None
    descriptionremotematches: Optional[List[str]] = None
    addresslocalitymatches: Optional[List[str]] = None
    titlematches: Optional[List[str]] = None
    descriptionfalsepositiveremotematches: Optional[List[str]] = None
    descriptionnonremotematches: Optional[List[str]] = None
    jobLocationType: Optional[str] = None