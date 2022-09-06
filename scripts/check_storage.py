from cpg_utils.workflows.batch import get_batch

b = get_batch('Check HGDP storage')
j = b.new_job('Check HGDP storage')
j.command('gsutil du -sh gs://cpg-hgdp-main/gvcf/')
j.command('gsutil du -sh gs://cpg-hgdp-main/cram/')
j.command('gsutil du -sh gs://cpg-hgdp-main/mt/oceania.mt')
j.command('gsutil du -sh gs://cpg-hgdp-main/mt/oceania_eur.mt')
b.run()
