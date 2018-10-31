import boto3
from datetime import datetime
import logging
import os
from utils import update_job_definition_from_file, submit_job, track_job, set_batch_client, set_cloudwatch_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
)


def submit_example_aws_batch_job():
    boto3.setup_default_session(region_name='us-east-1')
    set_batch_client(boto3.client('batch'))
    set_cloudwatch_client(boto3.client('logs'))

    job_queue = "my_queue_here"
    job_name = f"example_job-{datetime.now().isoformat()}"
    print(f"JobName: {job_name}")

    command_args = {
        '-arg1': "value1",
        '-arg2': "value2"
    }
    command = []
    for k, v in command_args.items():
        command.append(k)
        command.append(v)
    # actual command is a list: ['-arg1','value1','-arg2','value2']

    job_def_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "example_batch_job_definition.json")
    job_def = update_job_definition_from_file(job_def_file)

    job_id = submit_job(command, job_definition_arn=job_def['jobDefinitionArn'], job_name=job_name, job_queue=job_queue)
    track_job(job_id)


if __name__ == '__main__':
    submit_example_aws_batch_job()
