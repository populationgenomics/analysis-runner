#!/usr/bin/env python3

from cpg_utils.config import get_config
from cpg_utils.hail_batch import get_batch

def main():
    config = get_config()
    output_dir = config['workflow']['output_prefix']
    
    b = get_batch()
    j = b.new_bash_job()
    j.command('gcloud storage cp gs://cpg-common-test-upload/test_file.txt ./test_file.txt')
    b.run(wait=True)
    
    secret_file = get_batch().read_input('./test_file.txt')

    for job_id in ['1', '2', '3']:

        job = get_batch().new_job(name=f'Job {job_id}')

        job.command('cp {secret_file} $HOME')

        job.command('cat $HOME/test_file.txt')

    get_batch().run(wait=False)


if __name__ == '__main__':
    main()
