version development

workflow inner_complex {
    input {
        Array[String] inps
    }

    scatter (inp in inps) {
        call echo as print {
            input:
                inp=inp
        }
    }

    output {
        Array[String] out = print.out
    }
}

task echo {
  input {
    String inp
    Boolean? include_newline
  }
  command <<<
    echo \
      ~{if (defined(include_newline) && select_first([include_newline])) then "-n" else ""} \
      '~{inp}'
  >>>
  runtime {
    cpu: 1
    disks: "local-disk 10 SSD"
    docker: "ubuntu@sha256:1d7b639619bdca2d008eca2d5293e3c43ff84cbee597ff76de3b7a7de3e84956"
    memory: "1G"
  }
  output {
    String out = read_string(stdout())
  }
}

task Md5OfString {
  input {
    String inp
  }

  command <<<
    echo '~{inp}' | md5sum
  >>>

  runtime {
    cpu: 1
    disks: "local-disk 10 SSD"
    docker: "ubuntu@sha256:1d7b639619bdca2d008eca2d5293e3c43ff84cbee597ff76de3b7a7de3e84956"
    memory: "1G"
  }
  output {
    String out = read_string(stdout())
  }
}


task GenerateRandomInteger {
    input {
        Int seed = 0
    }

    command <<<
        cat <<EOT >> script.py
        import random
        seed = ~{seed}
        if seed != 0:
            random.seed(seed)
        print(random.randint(3, 10))
        EOT
        python script.py
    >>>

    output {
        Int out = read_int(stdout())
    }

    runtime {
        cpu: 1
        disks: "local-disk 10 HDD"
        docker: "python:3"
        memory: "1G"

    }
}
