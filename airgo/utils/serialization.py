import base64
import json
from typing import List


def json_serializer(obj: int) -> str:
    return base64.b64encode(json.dumps(obj).encode("utf8")).decode("utf8")


def json_deserializer(obj: str) -> int:
    return json.loads(base64.b64decode(obj).decode("utf8"))


def json_list_serializer(obj_list: List[int]) -> str:
    return json.dumps([json_serializer(obj) for obj in obj_list])
