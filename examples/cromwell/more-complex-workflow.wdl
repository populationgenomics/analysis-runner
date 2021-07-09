version development

import 'tools/tools-for-complex-workflow.wdl' as T

workflow ComplexWorkflow {
    input {
        Int seed = 0
    }

    call T.GenerateRandomInteger as random {
        input:
          seed=seed
    }

    scatter (int in range(random.out)) {

        call T.inner_complex as inner {
            input:
              inps=['String1_~{int}', 'String2_~{int}']
        }
        scatter (inp in inner.out) {
            call T.Md5OfString as md5 {
                input:
                  inp=sep('_', [inp])
            }
        }

        call T.echo as joined {
            input:
              inp=sep("_", md5.out)
        }
    }

    output {
        String out = sep("--", joined.out)
    }
}
