version development

import "tools/echo.wdl" as E

workflow hello {
  input {
    String? inp = "Hello, worlds!"
  }
  call E.echo as print {
    input:
      inp = select_first([inp, "Hello, world!"])
  }
  output {
    String out = print.out
  }
}
