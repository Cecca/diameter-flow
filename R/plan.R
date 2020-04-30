con <- db_connection()

plan <- drake_plan(
    main_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        select(-hosts, everything()) %>%
        collect() %>%
        mutate(diameter = if_else(algorithm %in% c("Bfs", "DeltaStepping"),
                                  as.integer(2 * diameter),
                                  diameter)),

    diameter_range = table_main(con, file_in("diameter-results.sqlite")) %>%
        filter(algorithm %in% c("Bfs", "DeltaStepping")) %>%
        collect() %>%
        group_by(dataset) %>%
        summarise(
            lower_bound = max(diameter),
            upper_bound = min(2 * diameter)
        )
    ,

    centers_data = table_counters(con, file_in("diameter-results.sqlite")) %>%
        filter(counter %in% c("Centers", "Uncovered")) %>%
        group_by(sha, outer_iter, counter) %>%
        summarise(count = sum(count, na.rm = TRUE)) %>%
        ungroup() %>%
        collect() %>%
        spread(counter, count) %>%
        select(iteration = outer_iter, everything()) %>%
        inner_join(main_data) %>%
        separate(parameters, into = c("radius", "base")) %>%
        mutate(radius = as.integer(radius),
               base = as.integer(base))
    ,

    # Plots
    plot_diam_vs_time_interactive =
        do_plot_diam_vs_time_interactive(main_data),

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
        vl_scale_y(type = "log") %>%
        vl_add_interval_selection(selection_name = "zoom",
                                  bind = "scales", type = "interval")
    ,

    dashboard = rmarkdown::render(
        knitr_in("R/dashboard.Rmd"),
        output_file = file_out("dashboard.html"),
        output_dir = here("."),
        quiet = T
    )
)
