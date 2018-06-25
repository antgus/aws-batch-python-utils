from datetime import datetime
import time
from pprint import pprint
from typing import List, Optional

_batch_client = None
_cw_client = None


def set_batch_client(batch_client):
    global _batch_client
    _batch_client = batch_client


def get_batch_client():
    return _batch_client


def set_cloudwatch_client(cw_client):
    global _cw_client
    _cw_client = cw_client


def get_cw_client():
    return _cw_client


def get_job_definition(job_definition_name: str) -> dict:
    client = get_batch_client()
    client.describe_job_definitions(jobDefinitionName=job_definition_name)


def update_job_definition(job_definition_name: str, job_def: dict) -> None:
    """
    Takes the provided job definition as the correct version.
    Updates the version stored in AWS Batch if it doesn't match the provided job definition
    """
    job_def_in_batch = get_job_definition(job_definition_name=job_definition_name)

    _remove_version_metadata_from_job_definition(job_def_in_batch)

    if job_def != job_def_in_batch:
        print("")
        print("Job Definition in AWS Batch:")
        pprint(job_def_in_batch)
        print("local Job Definition:")
        pprint(job_def)
        print("Updating the job definition in AWS Batch to match the local version:")
        rsp = _batch_client.register_job_definition(**job_def)
        print("Done creating job definition with ARN: %s" % rsp['jobDefinitionArn'])
        time.sleep(20)  # required because job_definition API does not provide read-after-write consistency
        updated_job_def = get_job_definition(job_definition_name=job_definition_name)
        _remove_version_metadata_from_job_definition(updated_job_def)
        if job_def != updated_job_def:
            print("Local definition:")
            pprint(job_def)
            print("Definition in AWS Batch:")
            pprint(updated_job_def)
            raise AssertionError("local job_def differs from the one found in batch")


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
    if job_def is not None:
        for key in ['jobDefinitionArn', 'revision', 'status']:
            del job_def[key]


def update_job_definition_from_file(path_to_local_job_def: str):

    pass


def update_job_definition(local_job_def: dict):
    pass


def submit_job(stuff) -> None:

    submit_job_resp = client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition['jobDefinitionArn'],
        containerOverrides=container_overrides
    )
    pass


JOB_STATUS = []


def track_job(job_id: int, cloudwatch_client, max_log_lines: Optional[int]=None) -> None:
    """
    Track the status of the given job, outputting the status and logged data as retrieved from cloudwatch
    :param job_id:
    :param max_log_lines the maximum lines of log output to retrieve from cloudwatch per 5 second interval. This limits
    the amount of data retrieved.
    :param limit parameter used in cloudwatch fetch
    """
    running = False
    start_time = 0
    status = ''
    sleep_duration = 4

    while True:
        job_description = _batch_client.describe_jobs(jobs=[job_id])
        status = job_description['jobs'][0]['status']
        log_stream_name = job_description['jobs'][0]['container']['logStreamName']
        job_name = pass # todo

        (start_ts, log_lines) = _get_logs(log_group_name, log_stream_name, start_time, max_log_lines=max_log_lines)
        start_ts += 1

        print('-' * 80)
        if len(log_lines) > 0:
            print(('\n'.join(log_lines)))

        print('-' * 80)
        print(('Job [%s - %s] %s' % (job_name, job_id, status)))

        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            fail_reason = job_description['jobs'][0]['reason']
            print(('Fail Reason: %s' % fail_reason))
            # todo
            # Sleep to make sure logs reach cw
            # todo print the tail of the logs.
            break

        time.sleep(sleep_duration)
    pass


def _get_logs(log_group_name: str, log_stream_name: str, start_time: int):
    result = []
    kwargs = {'logGroupName': log_group_name,
              'logStreamName': log_stream_name,
              'startTime': start_time,
              'startFromHead': True}

    last_ts = 0
    while True:
        log_events = _cw_client.get_log_events(**kwargs)

        for event in log_events['events']:
            last_ts = event['timestamp']
            timestamp = datetime.utcfromtimestamp(last_ts / 1000.0).isoformat()
            result.append('[%s] %s' % ((timestamp + ".000")[:23] + 'Z', event['message']))

        next_token = log_events.get('nextForwardToken', None)
        if next_token and kwargs.get('nextToken') != next_token:
            kwargs['nextToken'] = next_token
        else:
            break
    return last_ts, result
