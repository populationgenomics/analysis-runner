#!/usr/bin/env Rscript

require(tidyverse)
require(glue)

gcs_outdir <- Sys.getenv("OUTPUT") # propagated from analysis-runner command
mtcars_tsv <- "mtcars.tsv"
mtcars_plot <- "mtcars_disp_vs_hp.png"
d <- mtcars # data.frame available by default

# scatterplot
p <- d %>%
  ggplot(aes(x = disp, y = hp)) +
  geom_point()

# save plot and data
ggsave(mtcars_plot, p)
readr::write_tsv(d, file = mtcars_tsv)

# required to get gcloud + gsutil working properly
system("gcloud -q auth activate-service-account --key-file=/gsa-key/key.json")

# copy to GCS bucket
system(glue("gsutil cp {mtcars_tsv} {mtcars_plot} {gcs_outdir}"))
cat(glue("[{date()}] Finished successfully!"))
