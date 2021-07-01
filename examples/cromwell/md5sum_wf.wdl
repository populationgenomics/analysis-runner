version development

import "tools/md5sum.wdl" as M

workflow md5sum {
  input {
    File inpf
    String prefix
  }
  call M.md5sum as md5 {
    input:
      in_file = inpf,
      prefix = prefix
  }
  output {
    File md5_res = md5.out_file
  }
}
