import hailtop.batch as hb

sb = hb.ServiceBackend()
b = hb.Batch(backend=sb)

j = b.new_job('first job')
j.command(f'echo "Hello, there" > {j.outfile}')

j2 = b.new_job('second job')
j2.command(f'cat {j.outfile}')

b.run(wait=False)