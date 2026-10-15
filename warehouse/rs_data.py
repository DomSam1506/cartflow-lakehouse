"""Tiny wrapper over the Redshift Data API (no drivers, no VPC access, IAM auth)."""
import time

import boto3

WORKGROUP = "cartflow"
DATABASE = "cartflow"
client = boto3.client("redshift-data")


def _wait(stmt_id):
    while True:
        d = client.describe_statement(Id=stmt_id)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(1)
    if d["Status"] != "FINISHED":
        raise RuntimeError(f"{d['Status']}: {d.get('Error')}")
    return d


def run(sql):
    """One statement. Returns a list of rows (lists of Python values) for SELECTs, else []."""
    sid = client.execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sql=sql)["Id"]
    d = _wait(sid)
    if not d.get("HasResultSet"):
        return []
    rows, token = [], None
    while True:
        kw = {"Id": sid, **({"NextToken": token} if token else {})}
        r = client.get_statement_result(**kw)
        for rec in r["Records"]:
            rows.append([None if "isNull" in f else next(iter(f.values())) for f in rec])
        token = r.get("NextToken")
        if not token:
            return rows


def run_batch(sqls):
    """1-40 statements in ONE transaction: all succeed or all roll back."""
    assert 1 <= len(sqls) <= 40, "Data API batch takes 1-40 statements"
    sid = client.batch_execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sqls=sqls)["Id"]
    return _wait(sid)