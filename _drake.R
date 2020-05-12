source("R/packages.R")  # loads packages
source("R/data_info.R")
source("R/functions.R") # defines the create_plot() function
source("R/plan.R")      # creates the drake plan

drake_config(plan, verbose=2, lock_envir = FALSE)