"""Tiny storage helpers so the job runs on local folders and on s3:// URIs."""

import os
import shutil


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def split_s3(uri: str):
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


def _client():
    import boto3  # imported lazily so local runs and unit tests need no AWS setup

    return boto3.client("s3")


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])


def exists_dir(path: str) -> bool:
    """True if the folder (or S3 prefix) exists and has at least one file in it."""
    if not is_s3(path):
        return os.path.isdir(path)
    bucket, key = split_s3(path)
    resp = _client().list_objects_v2(
        Bucket=bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1
    )
    return resp.get("KeyCount", 0) > 0


def delete_prefix(path: str) -> None:
    """Delete a folder and everything under it (local folder or S3 prefix)."""
    if not is_s3(path):
        if not path.strip("/"):
            raise ValueError("refusing to delete the filesystem root")
        shutil.rmtree(path, ignore_errors=True)
        return
    bucket, key = split_s3(path)
    if not key.strip("/"):
        raise ValueError("refusing to delete a whole bucket")
    prefix = (
        key.rstrip("/") + "/"
    )  # trailing slash: ingest_dt=2017-11-2 must not match ...24
    client, token = _client(), None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        objects = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
        if objects:
            client.delete_objects(
                Bucket=bucket, Delete={"Objects": objects, "Quiet": True}
            )
        if not resp.get("IsTruncated"):
            return
        token = resp["NextContinuationToken"]


def read_text(path: str) -> str:
    if is_s3(path):
        bucket, key = split_s3(path)
        return _client().get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    with open(path) as f:
        return f.read()
