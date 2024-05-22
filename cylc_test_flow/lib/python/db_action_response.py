
from collections import namedtuple
from dataclasses import dataclass, field



@dataclass
class DbActionResponse:
    request: dict
    success: bool
    message: str
    details: dict
    errors: str
