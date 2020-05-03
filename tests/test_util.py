from parquet_tools.commands.utils import (
    LocalParquetFile, S3ParquetFile,
    InvalidCommandExcpetion)
import pytest
from tempfile import NamedTemporaryFile
from contextlib import ExitStack


class TestLocalParquetFile:

    @pytest.mark.parametrize('pf, expected', [
        (LocalParquetFile(path='./test.parquet'), False),
        (LocalParquetFile(path='./*'), True),
    ])
    def test_is_wildcard(self, pf, expected):
        assert pf.is_wildcard() == expected

    @pytest.mark.parametrize('pf, expected', [
        (
            LocalParquetFile(path='./tests/*.parquet'), [
                LocalParquetFile('./tests/test1.parquet'),
                LocalParquetFile('./tests/test2.parquet')
            ]
        ),
    ])
    def test_resolve_wildcard(self, pf, expected):
        assert pf.resolve_wildcard() == expected


class TestS3ParquetFile:

    @pytest.mark.parametrize('bucket, key, expected', [
        ('foo', 'tests/*.parquet', False),
        ('foo', 'tests/*', True),
        ('foo', '*', True)
    ])
    def test_validation(self, aws_session, bucket, key, expected):
        if not expected:
            with pytest.raises(InvalidCommandExcpetion):
                S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        else:
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)

    def test_resolve_wildcard(self, aws_session, parquet_file_s3):
        bucket, _ = parquet_file_s3
        S3ParquetFile(aws_session=aws_session, bucket=bucket, key='*').resolve_wildcard()
