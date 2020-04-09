plan <- drake_plan(
    main_data = read_csv(here("remote-reports/main.csv.bz2")) %>%
        mutate(diameter = if_else(algorithm == "Bfs",
                                  diameter * 2,
                                  diameter)),
    counters_data = read_csv(here("remote-reports/counters.csv.bz2")),

    # Plots
    plot_main = ggplot(main_data, aes(diameter, 
                                      total_time_ms, 
                                      color=algorithm)) +
        geom_point() +
        facet_wrap(vars(dataset),
                   scales="free") +
        theme_bw(),

    dashboard = rmarkdown::render(
        knitr_in("R/dashboard.Rmd"),
        output_file = file_out("dashboard.html"),
        quiet = T,
        #template = "flex_dashboard",
        #package = "flexdashboard"
    )
)
