import glob
from abc import ABC, abstractmethod
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from logging import getLogger
from tempfile import TemporaryDirectory
from typing import Iterator, List
from urllib.parse import urlparse

import boto3

logger = getLogger(__name__)


class InvalidCommandExcpetion(Exception):
    pass


class ParquetFile(ABC):

    def __post_init__(self):
        self.validation()

    def validation(self) -> None:
        pass

    @abstractmethod
    def is_wildcard(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def resolve_wildcard(self) -> List['ParquetFile']:
        raise NotImplementedError()

    @contextmanager
    @abstractmethod
    def get_local_path(self) -> Iterator[str]:
        raise NotImplementedError()


@dataclass
class LocalParquetFile(ParquetFile):
    path: str

    def is_wildcard(self) -> bool:
        return '*' in self.path

    def resolve_wildcard(self) -> List[ParquetFile]:
        return sorted(
            [LocalParquetFile(f) for f in glob.glob(self.path)],
            key=lambda x: x.path
        )

    @contextmanager
    def get_local_path(self) -> Iterator[str]:
        yield self.path


@dataclass
class S3ParquetFile(ParquetFile):
    aws_session: boto3.Session
    bucket: str
    key: str

    def validation(self):
        if self.is_wildcard() and not self.key.index('*') in (-1, len(self.key) - 1):
            raise InvalidCommandExcpetion('You can use * only end of the path')

    def is_wildcard(self) -> bool:
        return '*' in self.key

    def resolve_wildcard(self) -> List[ParquetFile]:
        list_res = self.aws_session.client('s3')\
            .list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.key[:-1]  # remove *
        )
        if list_res['IsTruncated']:
            raise Exception(f'Too much file match s3://{self.bucket}/{self.key}')

        keys = [e['Key'] for e in list_res['Contents']]
        return sorted(
            [S3ParquetFile(aws_session=self.aws_session, bucket=self.bucket, key=key) for key in keys],
            key=lambda x: x.key
        )

    @contextmanager
    def get_local_path(self) -> Iterator[str]:
        with TemporaryDirectory() as tmp_path:
            localfile = f'{tmp_path}/local.parquet'
            logger.info(f'Download stat parquet file on s3://{self.bucket}/{self.key} -> {localfile}')
            self.aws_session.resource('s3')\
                .meta.client.download_file(self.bucket, self.key, localfile)
            yield localfile


def get_aws_session(profile_name: str='default') -> boto3.Session:
    return boto3.Session(profile_name=profile_name)


def _is_s3_file(filename: str) -> bool:
    return filename[:5] == 's3://'


def to_parquet_file(file_exp: str, awsprofile: str) -> ParquetFile:
    if _is_s3_file(file_exp):
        parsed_url = urlparse(file_exp)
        return S3ParquetFile(
            aws_session=get_aws_session(awsprofile),
            bucket=parsed_url.netloc,
            key=parsed_url.path[1:]
        )
    else:
        return LocalParquetFile(
            path=file_exp
        )


@contextmanager
def get_filepaths_from_objs(objs: List[ParquetFile]):
    with ExitStack() as stack:
        yield sum([
            stack.enter_context(_get_filepaths(obj))
            for obj in objs
        ], [])


def _resolve_wildcard(obj: ParquetFile) -> List[ParquetFile]:
    if not obj.is_wildcard():
        return [obj]
    else:
        return obj.resolve_wildcard()


@contextmanager
def _get_filepaths(obj: ParquetFile) -> Iterator[List[str]]:
    with ExitStack() as stack:
        yield [
            stack.enter_context(pf.get_local_path())
            for pf in _resolve_wildcard(obj)
        ]
