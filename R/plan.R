plan <- drake_plan(
    main_data = read_csv(here("remote-reports/main.csv.bz2")) %>%
        mutate(diameter = if_else(algorithm == "Bfs",
                                  diameter * 2,
                                  diameter)),
    counters_data = read_csv(here("remote-reports/counters.csv.bz2")),

    centers_data = counters_data %>%
        filter(counter %in% c("Centers", "Uncovered")) %>%
        group_by(sha, outer_iter, counter) %>%
        summarise(count = sum(count)) %>%
        ungroup() %>%
        spread(counter, count) %>%
        select(iteration = outer_iter, everything()) %>%
        inner_join(main_data) %>%
        separate(parameters, into = c("radius", "base"))
    ,

    # Plots
    plot_diam_vs_time_interactive = vl_chart(autosize = "fit")  %>%
        vl_add_data(values = main_data) %>%
        vl_mark_point(tooltip = TRUE) %>%
        vl_encode_x(field = "diameter", type = "quantitative") %>%
        vl_encode_y(field = "total_time_ms", type = "quantitative") %>%
        vl_encode_color(field = "algorithm", type = "nominal") %>%
        vl_facet_wrap(field = "dataset", type = "nominal") %>%
        vl_resolve_scale_x("independent") %>%
        vl_resolve_scale_y("independent"),

    plot_centers_interactive = vl_chart(autosize = "fit") %>%
        vl_add_data(values = centers_data) %>%
        vl_mark_point(tooltip = T) %>%
        vl_mark_line(tooltip = F) %>%
        vl_encode_x(field = "iteration", type = "quantitative") %>%
        vl_encode_y(field = "Centers", type = "quantitative") %>%
        vl_encode_color(field = "radius", type = "nominal") %>%
        vl_facet_row(field = "base", type = "nominal") %>%
        vl_facet_column(field = "dataset", type = "nominal") %>%
        vl_resolve_scale_y("independent")
    ,

    plot_uncovered_interactive = vl_chart(autosize = "fit") %>%
        vl_add_data(values = centers_data) %>%
        vl_mark_point(tooltip = T) %>%
        vl_mark_line(tooltip = F) %>%
        vl_encode_x(field = "iteration", type = "quantitative") %>%
        vl_encode_y(field = "Uncovered", type = "quantitative") %>%
        vl_encode_color(field = "radius", type = "nominal") %>%
        vl_facet_row(field = "base", type = "nominal") %>%
        vl_facet_column(field = "dataset", type = "nominal") %>%
        vl_resolve_scale_y("independent") %>%
        vl_add_interval_selection(selection_name = "zoom",
                                  bind = "scales", type = "interval")
    ,

    dashboard = rmarkdown::render(
        knitr_in("R/dashboard.Rmd"),
        output_file = file_out("dashboard.html"),
        quiet = T
    )
)
