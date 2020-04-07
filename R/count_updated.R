library(tidyverse)

args = commandArgs(trailingOnly=TRUE)
directory = args[1]
main_file <- file.path(directory, "main.csv")
counts_file <- file.path(directory, "counters.csv")

main_data <- read_csv(main_file)
counters_data <- read_csv(counts_file)
print(main_data)

read_csv(counts_file) %>%
  group_by(sha, counter) %>%
  summarise(count = sum(count)) %>%
  inner_join(main_data) %>%
  arrange(desc(date)) %>%
  select(sha, parameters, count, counter) %>%
  spread(counter, count) %>%
  tail() %>%
  print()

print("Center progression")
main_data %>%
  arrange(date) %>%
  tail(1) %>%
  inner_join(counters_data) %>%
  filter(counter %in% c("Centers", "Uncovered")) %>%
  spread(counter, count) %>%
  arrange(outer_iter) %>%
  select(parameters, outer_iter, inner_iter, Centers, Uncovered) %>%
  print()
  
