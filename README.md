# aws-batch-python-utils
Python utility module for simplifying the use of AWS Batch, submitting jobs and tracking progress.

With it you can:
- keep your job definition in a json file, which you can keep in version control (git, etc
- easily submit jobs, track job progress and inspect the job's logged lines which can be printed to stdout.

# Usage
Example code can be found in `/examples`

Step 1: define the job in a json file that is versioned in git:
Step 2: execute `update_job_definition_from_file(...)`, which will push the local definition to AWS Batch if it's not already there
Step 3: submit the job with `submit_job(...)`
Step 4: track job progress with `track_job(job_id)`, which polls aws batch and cloudwatch for job status and recently logged lines

``` 
job_def_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "example_batch_job_definition.json")
job_def = update_job_definition_from_file(job_def_file)

job_id = submit_job(command, job_definition_arn=job_def['jobDefinitionArn'], job_name=job_name, job_queue=job_queue)
track_job(job_id)
```

