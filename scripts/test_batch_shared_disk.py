#!/usr/bin/env python3

from cpg_utils.config import get_config
from cpg_utils.hail_batch import get_batch, authenticate_cloud_credentials_in_job

def main():
    config = get_config()
    output_dir = config['workflow']['output_prefix']
    
    b = get_batch()
    j = b.new_bash_job(name='Test shared disk')
    j.image(get_config()['workflow']['driver_image'])
    authenticate_cloud_credentials_in_job(j)
    j.command(f'gcloud storage cp gs://cpg-common-test-upload/test_file.txt {j.ofile}')

    for job_id in ['1', '2', '3']:

        job = get_batch().new_job(name=f'Job {job_id}')

        job.command(f'cp {j.ofile} $HOME')

        job.command('cat $HOME/test_file.txt')

    get_batch().run(wait=False)


if __name__ == '__main__':
    main()
