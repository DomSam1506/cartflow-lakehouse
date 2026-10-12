import os
import re
import urllib.parse

# Only the day's marker file starts a run: dt=YYYY-MM-DD/_READY at the bucket root.
KEY = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})/_READY$")


def _glue():
    import boto3

    return boto3.client("glue")


def handler(event, _ctx):
    glue, started = _glue(), []
    for rec in event["Records"]:
        # S3 URL-encodes keys in events: "dt=2017-11-20" arrives as "dt%3D2017-11-20".
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        m = KEY.match(key)
        if not m:
            print(f"ignoring {key}")
            continue
        run = glue.start_job_run(JobName=os.environ["JOB_NAME"],
                                 Arguments={"--dt": m.group(1)})
        print(f"started {os.environ['JOB_NAME']} run {run['JobRunId']} for dt={m.group(1)}")
        started.append(m.group(1))
    return {"started": started}