from typing import Annotated, Any, Callable

from bson import ObjectId
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from crawl_scheduler.utils.loghandler import catch_exception,setup_logger
import sys
sys.excepthook = catch_exception
logger = setup_logger()
class _ObjectIdPydanticAnnotation:

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: Callable[[Any], core_schema.CoreSchema],
    ) -> core_schema.CoreSchema:
        def validate_from_str(input_value: str) -> ObjectId:
            return ObjectId(input_value)

        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(ObjectId),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ],
            serialization=core_schema.to_string_ser_schema(),
        )

PyObjectId = Annotated[
    ObjectId, _ObjectIdPydanticAnnotation
]