version development

import "tools/echo.wdl" as E
import "tools/md5sum.wdl" as M

workflow hello {
  input {
    String? inp = "Hello, world!"
    File inpf
    String prefix
  }
  call E.echo as print {
    input:
      inp = select_first([inp, "Hello, world!"])
  }
  call M.md5sum as md5 {
    input:
      in_file = inpf,
      prefix = prefix
  }
  output {
    String out = print.out
    File md5_res = md5.out_file
  }
}
