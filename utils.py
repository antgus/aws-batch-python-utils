from datetime import datetime
import time
from pprint import pprint
from typing import List, Optional
from enum import Enum
import json

_batch_client = None
_cw_client = None

_LOG_GROUP = '/aws/batch/job'  # default set by aws


class JobFailedException(Exception):
    pass


def set_batch_client(batch_client):
    global _batch_client
    _batch_client = batch_client


def get_batch_client():
    return _batch_client


def set_cloudwatch_client(cw_client):
    global _cw_client
    _cw_client = cw_client


def get_cloudwatch_client():
    return _cw_client

def get_cw_client():
    return _cw_client


def get_job_definition(job_definition_name: str) -> dict:
    rsp = get_batch_client().describe_job_definitions(jobDefinitionName=job_definition_name)
    assert('nextToken' not in rsp)  # no support for pagination yet.
    # pick the job definition with highest revision
    return sorted(rsp['jobDefinitions'], key=lambda x: -x['revision'])[0]


def update_job_definition(new_job_def: dict) -> dict:
    """
    Takes the provided job definition as the correct version.
    Updates the version stored in AWS Batch if it doesn't match the provided job definition
    """
    job_definition_name = new_job_def['jobDefinitionName']
    full_job_def = get_job_definition(job_definition_name=job_definition_name)

    job_def = _remove_version_metadata_from_job_definition(full_job_def)

    if new_job_def != job_def:
        print("")
        print("Job Definition in AWS Batch:")
        pprint(job_def)
        print("local Job Definition:")
        pprint(new_job_def)
        print("Updating the job definition in AWS Batch to match the local version:")
        rsp = _batch_client.register_job_definition(**new_job_def)
        print("Done creating job definition with ARN: %s" % rsp['jobDefinitionArn'])
        time.sleep(20)  # required because job_definition API does not provide read-after-write consistency
        full_job_def = get_job_definition(job_definition_name=job_definition_name)
        job_def = _remove_version_metadata_from_job_definition(full_job_def)
        if new_job_def != job_def:
            print("Local definition:")
            pprint(new_job_def)
            print("Definition in AWS Batch:")
            pprint(job_def)
            raise AssertionError("local job_def differs from the one found in batch, after updating")
    else:
        print("Skipping update of job definition as version in AWS Batch matches local version")
    return full_job_def


def update_job_definition_from_file(json_file_path: str) -> dict:
    job_def = read_job_definition_from_file(json_file_path)
    return update_job_definition(job_def)


def read_job_definition_from_file(json_file_path: str) -> str:
    with open(json_file_path, "r") as f:
        return json.load(f)


def _is_equivalent_job_definition(job_def_a: dict, job_def_b: dict) -> bool:
    empty_values = [[], None, '']
    a = job_def_a.copy()
    b = job_def_b.copy()
    _remove_version_metadata_from_job_definition(a)
    _remove_version_metadata_from_job_definition(b)

    # for a symmetrical comparison, compare both orderings (a,b) and (b,a)
    to_cmp = [(a, b), (b, a)]

    for this, other in to_cmp.items():
        for k, v in this.items():
            if (k not in other and v not in empty_values) \
                    or (k in other and other[k] != v):
                return False


def _remove_version_metadata_from_job_definition(job_def: Optional[dict]):
    """Returns a new dict. Changes are not inplace"""
    job_def = job_def.copy()
    if job_def is not None:
        for key in ['jobDefinitionArn', 'revision', 'status']:
            del job_def[key]
    return job_def


def submit_job(command: List[str], job_definition_arn: str, job_name: str, job_queue: str) -> str:

    container_overrides = {'command': command}

    rsp = get_batch_client().submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition_arn,
        containerOverrides=container_overrides
    )
    job_id = rsp['jobId']
    print(f"Successfully submitted job_id={job_id}")
    return job_id


def track_job(job_id: str, max_log_lines: Optional[int]=None, raise_on_failure=True) -> None:
    """
    Track the status of the given job, outputting the status and logged data as retrieved from cloudwatch
    :param job_id:
    :param max_log_lines the maximum lines of log output to retrieve from cloudwatch per 5 second interval. This limits
    the amount of data retrieved.
    :param limit parameter used in cloudwatch fetch
    :raises JobFailedException
    """
    start_ts = 0
    sleep_duration = 4

    while True:
        job_descriptions = _batch_client.describe_jobs(jobs=[job_id])
        assert(len(job_descriptions['jobs']) == 1)  # there should only be one job with the given id
        job = job_descriptions['jobs'][0]
        status = job['status']
        job_name = job['jobName']

        if status == 'RUNNING':
            log_stream_name = job['container']['logStreamName']
            start_ts += 1
            (start_ts, log_lines) = _get_logs(log_stream_name, start_ts)
            if len(log_lines) > 0:
                print(('\n'.join(log_lines)))

        print(f'JobTracker[{time.asctime()}]: job={job_name} - {job_id} {status}')

        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            pprint(job)
            fail_reason = job['statusReason']
            print(('Fail Reason: %s' % fail_reason))
            time.sleep(10) # Sleep to increase odds that logs reach cloudwatch
            if 'logStreamName' in job['container']:
                log_stream_name = job['container']['logStreamName']
                rows = _get_log_tail(log_stream_name)
                if len(rows) > 0:
                    print(('\n'.join(rows)))

            if raise_on_failure:
                raise JobFailedException(f"job={job_name} - {job_id} {status} - Fail reason: {fail_reason}]")

        time.sleep(sleep_duration)
    pass


def _get_log_tail(log_stream_name: str) -> List[str]:
    result = []
    kwargs = {'logGroupName': _LOG_GROUP,
              'logStreamName': log_stream_name,
              'startFromHead': False}

    log_events = get_cloudwatch_client().get_log_events(**kwargs)

    for event in log_events['events']:
        result.append(_parse_log_event(event))

    return result


def _get_logs(log_stream_name: str, start_time: int):
    result = []
    kwargs = {'logGroupName': _LOG_GROUP,
              'logStreamName': log_stream_name,
              'startTime': start_time,
              'startFromHead': True}

    last_ts = start_time
    while True:
        log_events = get_cloudwatch_client().get_log_events(**kwargs)

        for event in log_events['events']:
            last_ts = event['timestamp']
            result.append(_parse_log_event(event))

        next_token = log_events.get('nextForwardToken', None)
        if next_token and kwargs.get('nextToken') != next_token:
            kwargs['nextToken'] = next_token
        else:
            break
    return last_ts, result


def _parse_log_event(event: dict) -> str:
    timestamp = datetime.utcfromtimestamp(event['timestamp'] / 1000.0).isoformat()
    return '[%s] %s' % ((timestamp + ".000")[:23] + 'Z', event['message'])
