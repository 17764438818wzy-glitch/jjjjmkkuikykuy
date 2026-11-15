"""Immutable observations, separate from scheduler authority and execution state."""
from dataclasses import asdict, dataclass, field
import hashlib
import json

def fingerprint(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

@dataclass(frozen=True)
class PreparedIdentity:
    source_sha256: str
    material_sha256: str
    extraction_contract_sha256: str

    @property
    def key(self):
        return fingerprint(asdict(self))

@dataclass(frozen=True)
class PreparedObservation:
    identity: PreparedIdentity
    checkpoint: dict
    phase: str
    next_step: str
    blocking_reason: str | None
    evidence: dict = field(default_factory=dict)
    required_associations: tuple = ()
    context_sha256: str = ''

    def as_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls,value):
        return cls(**dict(value,identity=PreparedIdentity(**value['identity']),
                          required_associations=tuple(value.get('required_associations',()))))
