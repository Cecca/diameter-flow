library(tidyverse)

args = commandArgs(trailingOnly=TRUE)
directory = args[1]
main_file <- file.path(directory, "main.csv")
counts_file <- file.path(directory, "counters.csv")

main_data <- read_csv(main_file)

read_csv(counts_file) %>%
  group_by(sha, counter) %>%
  summarise(count = sum(count)) %>%
  inner_join(main_data) %>%
  arrange(date) %>%
  filter(counter == "UpdatedNodes") %>%
  ungroup() %>%
  select(count) %>%
  tail() %>%
  print()