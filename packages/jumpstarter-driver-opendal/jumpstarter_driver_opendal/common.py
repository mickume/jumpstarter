# Reference: https://github.com/apache/opendal/blob/main/bindings/python/python/opendal/__init__.pyi
from os import PathLike
from typing import Any, Literal, Optional

import opendal
from pydantic import BaseModel, model_validator

Mode = Literal["rb", "wb"]
HashAlgo = Literal["md5", "sha256"]
PathBuf = str | PathLike


class EntryMode(BaseModel):
    entry_is_file: bool
    entry_is_dir: bool

    @model_validator(mode="before")
    @classmethod
    def __validate(cls, data: Any):
        match data:
            case opendal.EntryMode():
                return {"entry_is_file": data.is_file(), "entry_is_dir": data.is_dir()}
            case _:
                return data

    def is_file(self) -> bool:
        return self._is_file

    def is_dir(self) -> bool:
        return self._is_dir


class Metadata(BaseModel):
    content_disposition: Optional[str]
    content_length: int
    content_md5: Optional[str]
    content_type: Optional[str]
    etag: Optional[str]
    mode: EntryMode


class PresignedRequest(BaseModel):
    """
    Presigned HTTP request

    Allows you to delegate access to a specific file in your storage backend
    without sharing access credentials
    """

    url: str
    """
    HTTP request URL
    """
    method: str
    """
    HTTP method

        GET: download file

        PUT: upload file

        DELETE: delete file
    """
    headers: dict[str, str]
    """
    Additional HTTP headers to send with the request
    """


class Capability(BaseModel):
    stat: bool
    stat_with_if_match: bool
    stat_with_if_none_match: bool

    read: bool
    read_with_if_match: bool
    read_with_if_none_match: bool
    read_with_override_cache_control: bool
    read_with_override_content_disposition: bool
    read_with_override_content_type: bool

    write: bool
    write_can_multi: bool
    write_can_empty: bool
    write_can_append: bool
    write_with_content_type: bool
    write_with_content_disposition: bool
    write_with_cache_control: bool
    write_multi_max_size: Optional[int]
    write_multi_min_size: Optional[int]
    write_total_max_size: Optional[int]

    create_dir: bool
    delete: bool
    copy: bool
    rename: bool

    list: bool
    list_with_limit: bool
    list_with_start_after: bool
    list_with_recursive: bool

    presign: bool
    presign_read: bool
    presign_stat: bool
    presign_write: bool

    shared: bool
    blocking: bool
