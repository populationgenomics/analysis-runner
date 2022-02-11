import hailtop.batch as hb
import click


@click.command()
@click.option('--name-to-print')
def main(name_to_print):

    sb = hb.ServiceBackend()
    b = hb.Batch(backend=sb)

    j = b.new_job('first job')
    j.command(f'echo "Hello, {name_to_print}" > {j.outfile}')

    j2 = b.new_job('second job')
    j2.command(f'cat {j.outfile}')

    b.run(wait=False)


if __name__ == "__main__":
    main()