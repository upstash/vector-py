from dataclasses import dataclass
from typing import Any, Optional, List, Dict, Union

VectorT = List[float]
IdT = str
CursorT = str
ScoreT = float
MetadataT = Optional[Dict]

ResponseType = Any
ResponseStr = str


class Vector:
    def __init__(self, id: IdT, vector: VectorT, metadata: MetadataT = None):
        self.id = id
        self.vector = vector
        self.metadata = metadata

    def __repr__(self):
        return f"{self.id}, {self.vector}, {self.metadata}"


@dataclass
class DeleteResponse:
    def __init__(self, resp_obj):
        self._json = resp_obj

    def __repr__(self):
        return f"{self._json}"

    @property
    def deleted_count(self) -> int:
        return self._json["deleted"]


@dataclass
class QuerySingularResponse:
    def __init__(self, resp_obj):
        self._json = resp_obj

    def __repr__(self):
        return f"{self._json}"

    @property
    def score(self) -> ScoreT:
        return self._json["score"]

    @property
    def vector(self) -> Union[VectorT, None]:
        if self._json.get("vector") is not None:
            return self._json["vector"]
        return None

    @property
    def id(self) -> IdT:
        return self._json["id"]

    @property
    def metadata(self) -> MetadataT:
        if self._json.get("metadata") is not None:
            return self._json["metadata"]
        return None


QueryResponse = List[QuerySingularResponse]


@dataclass
class SingleVectorResponse:
    def __init__(self, json_obj):
        self._json = json_obj

    def __repr__(self):
        return f"{self._json}"

    @property
    def id(self) -> IdT:
        return self._json["id"]

    @property
    def vector(self) -> Union[VectorT, None]:
        if self._json.get("vector") is not None:
            return self._json["vector"]
        return None

    @property
    def metadata(self) -> MetadataT:
        if self._json.get("metadata") is not None:
            return self._json["metadata"]

        return None


FetchResponse = List[Union[SingleVectorResponse, None]]


@dataclass
class RangeResponse:
    def __init__(self, resp_obj):
        self._json = resp_obj

    def __repr__(self):
        return f"{self._json}"

    @property
    def next_cursor(self) -> CursorT:
        return self._json["nextCursor"]

    @property
    def vectors(self) -> List[SingleVectorResponse]:
        return [
            SingleVectorResponse(vector_obj) for vector_obj in self._json["vectors"]
        ]
