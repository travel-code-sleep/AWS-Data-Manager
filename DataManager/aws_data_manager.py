"""Utils module contains crucial classes and functions that are used in all other modules of meiyume package."""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import gc
import io
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Union
from ast import literal_eval
from colorama import Fore, Style
import boto3
import pandas as pd
import pg8000
# import missingno as msno

os.environ['WDM_LOG_LEVEL'] = '0'


class DataManagerException(Exception):
    """DataManagerException class to define custom exceptions in runtime.
    Args:
        Exception (object): Python exceptions module.
    """

    pass


'''
class Logger(object):
    """Logger creates file handlers to write program execution logs to disk."""

    def __init__(self, task_name: str, path: Path):
        """__init__ initializes the file write stream.
        Args:
            task_name (str): Name of the log file.
            path (Path): Path in which the generated logs will be stored.
        """
        self.filename = path / \
            f'{task_name}_{time.strftime("%Y-%m-%d-%H%M%S")}.log'

    def start_log(self):
        """Start writing logs."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        self.file_handler = logging.FileHandler(self.filename)
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.WARNING)
        self.logger.addHandler(stream_handler)
        return self.logger, self.filename

    def stop_log(self):
        """Stop writing logs and flush the file handlers."""
        # self.logger.removeHandler(self.file_handler)
        del self.logger, self.file_handler
        gc.collect()
'''


class S3FileManager(object):
    """S3FileManager reads from and writes data to aws S3 storage.
    S3FileManager has below major functions:
    1. Find stored files with string search.
    2. Upload files to S3.
    3. Download files from S3.
    4. Read files from S3 into pandas dataframes.
    5. Delete files in S3.
    6. Crete S3 folder path for data upload.
    """

    def __init__(self, profile: str, bucket: Optional[str] = None):
        """__init__ initializes S3FileManager instance with given data bucket.
        Args:
            bucket (str, optional): The S3 bucket from/to which files will be read/downloaded/uploaded.
                                    Defaults to 'meiyume-datawarehouse-prod'.
        """
        if os.environ.get(profile):
            self.s3_client = self.get_s3_client(profile)
        else:
            print(f"    The aws PROFILE:{profile} does not exist in this dev environment.\n\
            Executing {Fore.RED}\033[1m\x1B[3m*set_aws_profile* \033[0m {Style.RESET_ALL} method to create a new profile.\n\
            This profile will store your aws-credentials on your system for future use.\n\
            Credentials can only be accesses from your system. Please input as prompted.")
            self.set_aws_profile()

        if bucket:
            self.bucket = bucket

    def get_s3_client(self, profile: str):
        """get_s3_client

        Args:
            profile (str): [description]

        Returns:
            botocore.client.S3: [description]
        """
        return boto3.client(
            "s3",
            region=literal_eval(os.environ.get(profile)
                                ).get("AWS_REGION"),
            aws_access_key_id=literal_eval(
                os.environ.get(profile)
            ).get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=literal_eval(
                os.environ.get(profile)
            ).get("AWS_SECRET_ACCESS_KEY"),
        )

    def set_aws_profile(self):
        profile = input(
            'enter a descriptive profile name. e.g: web-app-dev-profile: ')
        key = input('enter aws key: ')
        secret_key = input('enter aws secret key: ')
        region = input('enter aws region: ')
        d = {
            'AWS_REGION': region,
            'AWS_ACCESS_KEY_ID': key,
            'AWS_SECRET_ACCESS_KEY': secret_key
        }
        os.environ[profile] = str(d)
        del d
        gc.collect()
        self.s3_client = self.get_s3_client(profile)

    def get_matching_s3_objects(self, bucket: Optional[str] = None, prefix: str = "", suffix: str = ""):
        """get_matching_s3_objects searches S3 with string matching to find relevant keys.

        Args:
            prefix (str, optional): Only fetch objects whose key starts with this prefix. Defaults to "".
            suffix (str, optional): Only fetch objects whose keys end with this suffix. Defaults to "".
        Yields:
            Matching S3 keys.
        """
        if bucket:
            self.bucket = bucket

        paginator = self.s3_client.get_paginator("list_objects_v2")

        kwargs = {'Bucket': self.bucket}

        # We can pass the prefix directly to the S3 API.  If the user has passed
        # a tuple or list of prefixes, we go through them one by one.
        if isinstance(prefix, str):
            prefixes = (prefix, )
        prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix

            for page in paginator.paginate(**kwargs):
                try:
                    contents = page["Contents"]
                except KeyError:
                    break

                for obj in contents:
                    key = obj["Key"]
                    if key.endswith(suffix):
                        yield obj

    def get_matching_s3_keys(self, prefix: str = "", suffix: str = ""):
        """get_matching_s3_keys Generates the matching keys in an S3 bucket.

        Args:
            prefix (str, optional): Only fetch objects whose key starts with this prefix. Defaults to "".
            suffix (str, optional): Only fetch objects whose keys end with this suffix. Defaults to "".
        Yields:
            Any: Matching S3 object key
        """
        yield from (self.get_matching_s3_objects(prefix, suffix))

    def get_last_modified_s3(self, key: str) -> dict:
        """get_last_modified_date_s3 gets the last modified date of a S3 object.

        Args:
            key (str): Object key to find last modified date for.
        Returns:
            dict: Dictionary containing the key and last modified timestamp.
        """
        # s3 = boto3.resource('s3')
        # k = s3.Bucket(self.bucket).Object(key)  # pylint: disable=no-member
        k = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        return {'key_name': k.key, 'key_last_modified': str(k.last_modified)}

    def push_file_s3(self, file_path: Union[str, Path], job_name: str) -> None:
        """push_file_s3 upload file to S3 storage with job name specific prefix.

        Args:
            file_path (Union[str, Path]): File path of the file to be uploaded as a string or Path object.
            job_name (str): Type of file to upload: One of [meta_detail, item, ingredient,
                                                        review, review_summary, image,
                                                        cleaned_pre_algorithm, webappdata]
        """
        # cls.make_manager()
        file_name = str(file_path).split("\\")[-1]

        prefix = self.get_prefix_s3(job_name)
        object_name = prefix+file_name
        # try:
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(str(file_path), self.bucket, object_name)
            print('file pushed successfully.')
        except Exception:
            print('file pushing task failed.')

    def pull_file_s3(self, key: str, file_path: Path = Path.cwd()) -> None:
        """pull_file_s3 dowload file from S3.

        Args:
            key (str): The file object to download.
            file_path (Path, optional): The path in which the downloaded file will be stored.
                                        Defaults to current working directory (Path.cwd()).
        """
        s3 = boto3.resource('s3')
        file_name = str(key).split('/')[-1]
        s3.Bucket(self.bucket).download_file(  # pylint: disable=no-member
            key, f'{file_path}/{file_name}')

    def read_to_dataframe_s3(self, key: str, file_type: str) -> pd.DataFrame:
        """read_to_dataframe_s3 reads S3 object into a pandas dataframe.

        Args:
            key (str): S3 object key.
            file_type (str): File format.
        Raises:
            DataManagerException: Raises exception if incorrect key or file type provied. (Accepted types: csv, feather, pickle, parquet, hdf)
        Returns:
            pd.DataFrame: File data in pandas dataframe.
        """
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.bucket, Key=key)

        try:
            if file_type == 'csv':
                return pd.read_csv(io.BytesIO(obj['Body'].read()), sep='~')
            elif file_type == 'feather':
                return pd.read_feather(io.BytesIO(obj['Body'].read()))
            elif file_type == 'pickle':
                return pd.read_pickle(io.BytesIO(obj['Body'].read()))
            elif file_type == 'parquet':
                return pd.read_parquet(io.BytesIO(obj['Body'].read()))
            elif file_type == 'hdf':
                return pd.read_hdf(io.BytesIO(obj['Body'].read()))
        except Exception as ex:
            raise DataManagerException(
                'Provide correct file key and file type.')

    def delete_file_s3(self, key: str) -> None:
        """delete_file_s3 delete file object from S3.

        Args:
            key (str): The file key to delete.
        """
        # s3 = boto3.resource('s3')
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            # s3.Object(self.bucket, key).delete()  # pylint: disable=no-member
            print('file deleted.')
        except Exception:
            print('delete operation failed')


'''
    def get_prefix_s3(self, job_name: str) -> str:
        """get_prefix_s3 sets the correct S3 file prefix depending on the upload job.

        Args:
            job_name (str): [description]
        Raises:
            DataManagerException: [description]
        Returns:
            str: [description]
        """
        upload_jobs = {
            'source_meta': 'Feeds/BeautyTrendEngine/Source_Meta/Staging/',
            'meta_detail': 'Feeds/BeautyTrendEngine/Meta_Detail/Staging/',
            'item': 'Feeds/BeautyTrendEngine/Item/Staging/',
            'ingredient': 'Feeds/BeautyTrendEngine/Ingredient/Staging/',
            'review': 'Feeds/BeautyTrendEngine/Review/Staging/',
            'review_summary': 'Feeds/BeautyTrendEngine/Review_Summary/Staging/',
            'image': 'Feeds/BeautyTrendEngine/Image/Staging/',
            'cleaned_pre_algorithm': 'Feeds/BeautyTrendEngine/CleanedData/PreAlgorithm/',
            'webapp': 'Feeds/BeautyTrendEngine/WebAppData/',
            'webapp_test': 'Feeds/BeautyTrendEngine/WebAppDevelopmentData/Test/'
        }
        try:
            return upload_jobs[job_name]
        except Exception as ex:
            raise DataManagerException(
                'Unrecognizable job. Please input correct job_name.')
'''

# def read_image_s3(
#     prod_id: str,
#     prefix: str,
#     bucket: str,
# ) -> str:
#     """read_image_s3

#     Args:
#         prod_id (str): [description]
#         prefix (str, optional): [description]. Defaults to f'{S3_PREFIX}/Image/Staging'.
#         bucket (str, optional): [description]. Defaults to S3_BUCKET.
#     Returns:
#         str: [description]
#     """
#     return f"https://{bucket}.s3-{S3_REGION}.amazonaws.com/{prefix}/{prod_id}/{prod_id}_image_1.jpg"


class RedShiftReader(object):
    """RedShiftReader connects to Redshift database and performs table querying for trend engine schema."""

    def __init__(self, db_profile: str):
        """__init__ initializes RedshiftReader instance with all the database connection properties."""
        self.host = 'lifungprod.cctlwakofj4t.ap-southeast-1.redshift.amazonaws.com'
        self.port = 5439
        self.database = 'lifungdb'
        self.user_name = 'btemymuser'
        self.password = 'Lifung123'
        self.conn = pg8000.connect(
            database=self.database, host=self.host, port=self.port,
            user=self.user_name, password=self.password)

    def get_s3_client(self, profile: str):
        """get_s3_client

        Args:
            profile (str): [description]

        """
        return boto3.client(
            "s3",
            region=literal_eval(os.environ.get(profile)
                                ).get("AWS_REGION"),
            aws_access_key_id=literal_eval(
                os.environ.get(profile)
            ).get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=literal_eval(
                os.environ.get(profile)
            ).get("AWS_SECRET_ACCESS_KEY"),
        )

    def set_aws_profile(self):
        profile = input(
            'enter a descriptive profile name. e.g: web-app-dev-profile: ')
        key = input('enter aws key: ')
        secret_key = input('enter aws secret key: ')
        region = input('enter aws region: ')
        d = {
            'AWS_REGION': region,
            'AWS_ACCESS_KEY_ID': key,
            'AWS_SECRET_ACCESS_KEY': secret_key
        }
        os.environ[profile] = str(d)
        del d
        gc.collect()
        self.s3_client = self.get_s3_client(profile)

    def query_database(self, query: str) -> pd.DataFrame:
        """query_database takes a sql query in text format and returns table/view query results as pandas dataframe.

        Args:
            query (str): Sql query as a string in double quotes.
        Returns:
            pd.DataFrame: Dataframe containing query results.
        """
        df = pd.read_sql_query(query, self.conn)
        df.columns = [name.decode('utf-8') for name in df.columns]
        return df


# def log_exception(logger: Logger, additional_information: Optional[str] = None) -> None:
#     """log_exception logs exception when occurred while executing code.

#     Args:
#         logger (Logger): The logger handler with access to log file.
#         additional_information (Optional[str], optional): Any additional text info to add to the exception log. Defaults to None.
#     """
#     exc_type, exc_obj, exc_tb = \
#         sys.exc_info(
#         )
#     file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#     if additional_information:
#         logger.info(str.encode(
#             f'Exception: {exc_type} occurred at line number {exc_tb.tb_lineno}.\
#                 (Filename: {file_name}). {additional_information}', 'utf-8', 'ignore'))
#     else:
#         logger.info(str.encode(
#             f'Exception: {exc_type} occurred at line number {exc_tb.tb_lineno}.\
#             (Filename: {file_name}).', 'utf-8', 'ignore'))
