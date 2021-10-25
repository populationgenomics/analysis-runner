version development

workflow hello {
  input {
    Array[String] inps
  }
  scatter (inp in inps) {
      call echo as print {
        input:
          inp=inp
      }
      call GenerateFileWithSecondary as generateSecondaries {
        input:
          contents="Has secondary: " + inp
      }

      String j = read_string(print.out)
  }

  output {
    Array[File] outs = print.out
    String joined_out = sep("; ", j)

    Array[File] out_txts = generateSecondaries.out_txt
    Array[File] out_txt_md5s = generateSecondaries.out_txt_md5
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
    File out = stdout()
  }
}

task GenerateFileWithSecondary {
    input {
        String contents
    }
    command <<<
        md5=$(echo '~{contents}' | md5sum | awk '{ print $1 }')
        echo '~{contents}' > "$md5.txt"
        echo "$md5" > "$md5.txt.md5"
    >>>

    output {
        File out_txt = glob("*.txt")[0]
        File out_txt_md5 = glob("*.txt.md5")[0]
    }
}
