con <- db_connection()

plan <- drake_plan(
    main_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        collect() %>%
        mutate(n_hosts = str_count(hosts, "eridano")) %>%
        # Select 12 hosts and the sequential algorithm
        filter(n_hosts %in% c(12, 0)) %>%
        filter(!(dataset %in% c("sk-2005", "USA-E"))) %>%
        mutate(diameter = if_else(algorithm %in% c("Bfs", "DeltaStepping"),
                                  as.integer(2 * diameter),
                                  diameter)) %>%
        filter(!killed) %>%
        mutate(
            total_time = set_units(total_time_ms, "ms") %>% set_units("s"),
            final_diameter_time = set_units(final_diameter_time_ms, "ms") %>% set_units("s"),
        ) %>%
        select(-hosts, -total_time_ms, -final_diameter_time_ms)
    ,

    bounds_data = {
        upper <- main_data %>% 
            filter(algorithm == "DeltaStepping") %>%
            group_by(dataset) %>%
            summarise(diameter_upper = min(diameter))
        # print(upper)
        lower <- main_data %>% 
            filter(algorithm %in% c("DeltaStepping", "Sequential")) %>%
            group_by(dataset) %>%
            summarise(diameter_lower = max(if_else(algorithm == "Sequential",
                                               diameter,
                                               as.integer(diameter / 2)))) %>%
            drop_na()
        # print(lower)
        times <- main_data %>%
            group_by(dataset) %>%
            summarise(total_time = mean(total_time))
        # print(times)
        inner_join(upper, lower) %>%
            inner_join(times) %>%
            mutate(dataset = case_when(
                str_detect(dataset, "sk-2005") ~ "sk-2005",
                str_detect(dataset, "twitter-2010") ~ "twitter-2010",
                str_detect(dataset, "USA") ~ "USA"
            )) %>%
            drop_na()
    },

    plot_diameter_time = main_data %>%
        filter(dataset %in% c("sk-2005-lcc-rweight", 
                              "twitter-2010-lcc-rweight", 
                              #"USA", 
                              #"USA-x5", 
                              "USA-x10")) %>%
        mutate(dataset = case_when(
            str_detect(dataset, "sk-2005") ~ "sk-2005",
            str_detect(dataset, "twitter-2010") ~ "twitter-2010",
            str_detect(dataset, "USA") ~ dataset
        )) %>%
        mutate(description = case_when(
            str_detect(dataset, "sk-2005") ~ "web",
            str_detect(dataset, "twitter-2010") ~ "social",
            str_detect(dataset, "USA") ~ "roads"
        )) %>%
        ggplot(aes(diameter, total_time, color=algorithm, shape=algorithm)) +
        # geom_errorbar(aes(xmin=diameter_lower, xmax=diameter_upper,
        #                   y=total_time),
        #               data=bounds_data,
        #               inherit.aes=F,
        #               width=0.5) +
        geom_mark_rect(aes(x=diameter, 
                           y=total_time, 
                           group=dataset, 
                           label=dataset,
                           description=description),
                       inherit.aes=F,
                       ) +
        geom_point_interactive(aes(tooltip=str_c("time", total_time,
                                                 "\ndiameter", diameter,
                                                 "\nparameters", parameters,
                                                 sep=" "))) +
        scale_y_unit() +
        scale_color_brewer(type="qual", palette="Dark2") +
        theme_bw() +
        theme(legend.position="top")
    ,

    figure_diam_vs_time = ggsave("export/diam_vs_time.png",
                                    plot_diameter_time,
                                    width=6,
                                    height=4),

    interactive_diam_vs_time = girafe(ggobj=plot_diameter_time,
                                      width_svg=7,
                                      height_svg=5),

    plot_time = main_data %>%
        filter(dataset %in% c("sk-2005-lcc-rweight", 
                              "twitter-2010-lcc-rweight", 
                              "USA-x10")) %>%
        mutate(dataset = case_when(
            str_detect(dataset, "sk-2005") ~ "sk-2005",
            str_detect(dataset, "twitter-2010") ~ "twitter-2010",
            str_detect(dataset, "USA") ~ dataset
        )) %>%
        replace_na(list(final_diameter_time = 0)) %>%
        group_by(dataset, algorithm) %>%
        slice(which.min(total_time)) %>%
        ungroup() %>%
        ggplot(aes(x=dataset, y=total_time, fill=algorithm)) +
        geom_col(position="dodge", width=0.4) +
        # geom_col(aes(y=final_diameter_time, fill=algorithm), 
        #          width=0.4,
        #          color="black",
        #          show.legend=F,
        #          position="dodge") +
        scale_y_unit(breaks=c(0, 1000,2000,3000)) +
        scale_fill_manual(values=list(
            "DeltaStepping" = "gray",
            "RandClusterGuess" = "#B34537"
        )) +
        theme_tufte() +
        theme(legend.position="top",
              panel.grid.major.x=element_blank(),
              panel.grid.minor.x=element_blank(),
              panel.grid.minor.y=element_blank(),
              panel.grid.major.y=element_line(color="white", size=.5),
            #   panel.background = element_rect(fill = NA),
              panel.ontop = TRUE)
    ,

    plot_diameter = main_data %>%
        filter(dataset %in% c("sk-2005-lcc-rweight", 
                              "twitter-2010-lcc-rweight", 
                              "USA-x10")) %>%
        mutate(dataset = case_when(
            str_detect(dataset, "sk-2005") ~ "sk-2005",
            str_detect(dataset, "twitter-2010") ~ "twitter-2010",
            str_detect(dataset, "USA") ~ dataset
        )) %>%
        replace_na(list(final_diameter_time = 0)) %>%
        group_by(dataset, algorithm) %>%
        slice(which.min(total_time)) %>%
        ungroup() %>%
        ggplot(aes(x=dataset, y=diameter, fill=algorithm)) +
        geom_col(position="dodge", width=0.4) +
        # geom_point(position=position_dodge(1)) +
        # geom_linerange(aes(ymin=0, ymax=diameter),
        #                position=position_dodge(1)) +
        # # scale_fill_brewer(type="seq", palette="Dark2") +
        scale_fill_manual(values=list(
            "DeltaStepping" = "gray",
            "RandClusterGuess" = "#B34537"
        )) +
        theme_tufte() +
        theme(legend.position="top",
              panel.grid.major.x=element_blank(),
              panel.grid.minor.x=element_blank(),
              panel.grid.minor.y=element_blank(),
              panel.grid.major.y=element_line(color="white", size=.5),
            #   panel.background = element_rect(fill = NA),
              panel.ontop = TRUE)
    ,

    figure_time = ggsave("export/time.png", plot_time, width=4,height=4),
    figure_diam = ggsave("export/diameter.png", plot_diameter, width=4,height=4),

    # Use only entries that report the time it takes to 
    # time_dependency_data = table_main(con, file_in("diameter-results.sqlite")) %>%
    #     collect() %>%
    #     drop_na(final_diameter_time_ms) %>%
    #     filter(hosts == "eridano11.fast:10000__eridano12.fast:10000__eridano13.fast:10000__eridano14.fast:10000__eridano15.fast:10000__eridano16.fast:10000__eridano17.fast:10000__eridano18.fast:10000__eridano19.fast:10000__eridano21.fast:10000__eridano22.fast:10000__eridano23.fast:10000__eridano25.fast:10000") %>%
    #     select(-hosts, everything()) %>%
    #     filter(!(dataset %in% c("sk-2005", "USA-E"))) %>%
    #     mutate(diameter = if_else(algorithm %in% c("Bfs", "DeltaStepping"),
    #                               as.integer(2 * diameter),
    #                               diameter)) %>%
    #     add_graph_type() %>%
    #     drop_na(graph_type) %>%
    #     mutate(final_diameter_frac = final_diameter_time_ms / total_time_ms),

    # scalability_data = table_main(con, file_in("diameter-results.sqlite")) %>%
    #     collect() %>%
    #     filter((dataset == "USA" & parameters == "10000:2") | (dataset == "uk-2005-lcc" & parameters == "4:2")) %>%
    #     mutate(num_hosts = str_count(hosts, "__") + 1)
    # ,

    # scalability_n_data = table_main(con, file_in("diameter-results.sqlite")) %>%
    #     collect() %>%
    #     filter(((dataset %in% c("USA", "USA-x2", "USA-x4", "USA-x8", "USA-x16")) & (parameters == "10000:2")) |
    #             ((str_detect(dataset, "uk-2014-host-lcc")) & (parameters == "16:2"))) %>%
    #     mutate(scale_factor = as.integer(str_extract(dataset, "\\d+$")) %>%
    #                             replace_na(1),
    #            dataset = str_extract(dataset, "USA|uk-2014-host-lcc"))
    # ,

    # data_info_table = semi_join(data_info, main_data) %>%
    #     arrange(max_weight, num_edges),

    # tex_data_info_table = {
    #     file_conn <- file(file_out("export/datasets.tex"))
    #     n_unweighted <- data_info_table %>% filter(max_weight == 1) %>% count()
    #     n_weighted <- data_info_table %>% filter(max_weight > 1) %>% count()
    #     data_info_table %>%
    #         select(dataset, nodes = num_nodes, edges = num_edges) %>%
    #         kable(format = 'latex', booktabs = TRUE, linesep = "") %>%
    #         kable_styling() %>%
    #         pack_rows(index = c("Unweighted" = n_unweighted,
    #                             "Weighted" = n_weighted)) %>%
    #         writeLines(file_conn)
    #     close(file_conn)
    # },

    # diameter_range = table_main(con, file_in("diameter-results.sqlite")) %>%
    #     filter(algorithm %in% c("Bfs", "DeltaStepping")) %>%
    #     collect() %>%
    #     group_by(dataset) %>%
    #     summarise(
    #         lower_bound = max(diameter),
    #         upper_bound = min(2 * diameter)
    #     )
    # ,

    # centers_data = table_counters(con, file_in("diameter-results.sqlite")) %>%
    #     filter(counter %in% c("Centers", "Uncovered")) %>%
    #     group_by(sha, outer_iter, counter) %>%
    #     summarise(count = sum(count, na.rm = TRUE)) %>%
    #     ungroup() %>%
    #     collect() %>%
    #     spread(counter, count) %>%
    #     select(iteration = outer_iter, everything()) %>%
    #     inner_join(main_data) %>%
    #     separate(parameters, into = c("radius", "base")) %>%
    #     mutate(radius = as.integer(radius),
    #            base = as.integer(base))
    # ,

    # data_auxiliary_graph_size =
    #     table_counters(con, file_in("diameter-results.sqlite")) %>%
    #     filter(counter == "Centers") %>%
    #     group_by(sha) %>%
    #     summarise(centers = sum(count, na.rm=TRUE)) %>%
    #     ungroup() %>%
    #     collect() %>%
    #     inner_join(main_data) %>%
    #     inner_join(data_info) %>%
    #     separate(parameters, into = c("radius", "base"), convert = T)
    # ,

    # data_auxiliary_graph_size_raw =
    #     table_counters(con, file_in("diameter-results.sqlite")) %>%
    #     filter(counter == "Centers") %>%
    #     group_by(sha) %>%
    #     summarise(centers = sum(count, na.rm=TRUE)) %>%
    #     ungroup() %>%
    #     collect()
    # ,

    # parameter_dependency_data = time_dependency_data %>% 
    #     inner_join(data_auxiliary_graph_size_raw) %>%
    #     inner_join(data_info) %>%
    #     separate(parameters, into = c("radius", "base"), convert = T)
    # ,

    # # Plots
    # plot_static_diam_vs_time =
    #     static_diam_vs_time((main_data)) %>%
    #     ggsave(filename = file_out("export/diam_vs_time.png"),
    #            width = 8,
    #            height = 8),
    # plot_interactive_diam_vs_time = girafe(ggobj = static_diam_vs_time((main_data)), width_svg=8, height_svg=8),

    # plot_static_param_dependency_time =
    #     static_param_dependency_time(parameter_dependency_data) %>%
    #     ggsave(filename = file_out("export/dep_time.png"),
    #            width = 8,
    #            height = 4),
    # plot_interactive_param_dependency_time = 
    #     girafe(ggobj = static_param_dependency_time(parameter_dependency_data),
    #            width_svg=8, height_svg=8),

    # plot_static_param_dependency_diam =
    #     static_param_dependency_diam(main_data) %>%
    #     ggsave(filename = file_out("export/dep_diam.png"),
    #            width = 8,
    #            height = 4),
    # # plot_interactive_param_dependency_diam = girafe(ggobj = static_param_dependency_diam(main_data)),

    # plot_static_auxiliary_graph_size =
    #     static_auxiliary_graph_size(data_auxiliary_graph_size) %>%
    #     ggsave(filename = file_out("export/dep_size.png"),
    #            width = 8,
    #            height = 4),
    # # plot_interactive_auxiliary_graph_size = girafe(ggobj = static_auxiliary_graph_size(data_auxiliary_graph_size)),

    # plot_static_scalability =
    #     do_scalability_plot(scalability_data) %>%
    #     ggsave(filename = file_out("export/scalability.png"),
    #            width = 8,
    #            height= 2),

    # plot_static_scalability_n =
    #     do_scalability_n_plot(scalability_n_data) %>%
    #     ggsave(filename = file_out("export/scalability_n.png"),
    #            width = 8,
    #            height= 2),

    # plot_interactive_scalability_n =
    #     ggiraph(ggobj=do_scalability_n_plot(scalability_n_data),
    #             width_svg=8, height_svg=4),

    dashboard = rmarkdown::render(
        knitr_in("R/dashboard.Rmd"),
        output_file = file_out("dashboard.html"),
        output_dir = here("."),
        quiet = T
    )
)
