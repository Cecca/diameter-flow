con <- db_connection()

plan <- drake_plan(
    main_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        select(-hosts, everything()) %>%
        collect() %>%
        filter(!(dataset %in% c("sk-2005"))) %>%
        mutate(diameter = if_else(algorithm %in% c("Bfs", "DeltaStepping"),
                                  as.integer(2 * diameter),
                                  diameter)) %>%
        add_graph_type(),

    data_info_table = semi_join(data_info, main_data) %>%
        arrange(max_weight, num_edges),

    tex_data_info_table = {
        file_conn <- file(file_out("export/datasets.tex"))
        n_unweighted <- data_info_table %>% filter(max_weight == 1) %>% count()
        n_weighted <- data_info_table %>% filter(max_weight > 1) %>% count()
        data_info_table %>%
            select(dataset, nodes = num_nodes, edges = num_edges) %>%
            kable(format = 'latex', booktabs = TRUE, linesep = "") %>%
            kable_styling() %>%
            pack_rows(index = c("Unweighted" = n_unweighted,
                                "Weighted" = n_weighted)) %>%
            writeLines(file_conn)
        close(file_conn)
    },

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

    data_auxiliary_graph_size =
        table_counters(con, file_in("diameter-results.sqlite")) %>%
        filter(counter == "Centers") %>%
        group_by(sha) %>%
        summarise(centers = sum(count, na.rm=TRUE)) %>%
        ungroup() %>%
        collect() %>%
        inner_join(main_data) %>%
        inner_join(data_info) %>%
        separate(parameters, into = c("radius", "base"), convert = T)
    ,

    # Plots
    plot_static_diam_vs_time =
        static_diam_vs_time((main_data)) %>%
        ggsave(filename = file_out("export/diam_vs_time.png"),
               width = 8,
               height = 8),

    plot_static_param_dependency_time =
        static_param_dependency_time(main_data) %>%
        ggsave(filename = file_out("export/dep_time.png"),
               width = 8,
               height = 4),

    plot_static_param_dependency_diam =
        static_param_dependency_diam(main_data) %>%
        ggsave(filename = file_out("export/dep_diam.png"),
               width = 8,
               height = 4),

    plot_static_auxiliary_graph_size =
        static_auxiliary_graph_size(data_auxiliary_graph_size) %>%
        ggsave(filename = file_out("export/dep_size.png"),
               width = 8,
               height = 4),

    dashboard = rmarkdown::render(
        knitr_in("R/dashboard.Rmd"),
        output_file = file_out("dashboard.html"),
        output_dir = here("."),
        quiet = T
    )
)
