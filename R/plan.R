con <- db_connection()

plan <- drake_plan(
    killed_dstepping = table_main(con, file_in("diameter-results.sqlite")) %>%
        filter(killed, algorithm == "DeltaStepping") %>%
        collect(),


    main_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        filter(!killed) %>%
        filter(algorithm %in% c("RandClusterGuess", "DeltaStepping", "Sequential")) %>%
        collect() %>%
        bind_rows(killed_dstepping) %>%
        mutate(n_hosts = str_count(hosts, "eridano")) %>%
        mutate(algorithm = if_else(algorithm == "RandClusterGuess", "ClusterDiameter", algorithm)) %>%
        # Select 12 hosts and the sequential algorithm
        filter(n_hosts %in% c(12, 0)) %>%
        filter(!(dataset %in% c("sk-2005", "USA-E"))) %>%
        mutate(diameter = if_else(algorithm %in% c("Bfs", "DeltaStepping"),
                                  as.integer(2 * diameter),
                                  diameter)) %>%
        mutate(
            total_time = set_units(total_time_ms, "ms") %>% set_units("s"),
            final_diameter_time = set_units(final_diameter_time_ms, "ms") %>% set_units("s"),
        ) %>%
        select(-hosts, -total_time_ms, -final_diameter_time_ms)
    ,

    scalability_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        filter(!killed) %>%
        filter(algorithm %in% c("RandClusterGuess"),
               dataset %in% c("USA", "USA-x10"),
               parameters == "10000000:2950,10") %>%
        collect() %>%
        mutate(n_hosts = str_count(hosts, "eridano")) %>%
        mutate(algorithm = if_else(algorithm == "RandClusterGuess", "ClusterDiameter", algorithm)) %>%
        # Select 12 hosts and the sequential algorithm
        mutate(
            total_time = set_units(total_time_ms, "ms") %>% set_units("s"),
            final_diameter_time = set_units(final_diameter_time_ms, "ms") %>% set_units("s"),
        ) %>%
        select(-hosts, -total_time_ms, -final_diameter_time_ms)
    ,

    diameter_insight_data = table_main(con, file_in("diameter-results.sqlite")) %>%
        filter(sha %in% c("f5da6e", "f556bd", "bfe3e5", "2525cd", "be685f")) %>%
        collect() %>%
        mutate(
            total_time = set_units(total_time_ms, "ms") %>% set_units("s"),
            final_diameter_time = set_units(final_diameter_time_ms, "ms") %>% set_units("s"),
            clustering_time = total_time - final_diameter_time,
        ) %>%
        separate(parameters, into=c("radius", "base"), convert=T) %>%
        mutate(iteration_radius = case_when(
            radius == 295 ~ 290,
            radius == 2950 ~ 2900,
            radius == 29500 ~ 29000,
            radius == 295000 ~ 290000,
            radius == 2950000 ~ 2900000,
        )) %>%
        select(iteration_radius, diameter)
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

    parameter_dep_data = main_data %>%
        filter(dataset == "USA", 
               algorithm == "ClusterDiameter",
               parameters == "100000:29,10") %>%
        inner_join(table_rand_cluster_iterations(con, file_in("diameter-results.sqlite")) %>% collect()) %>%
        mutate(duration = set_units(duration_ms, "ms")) %>%
        select(-duration_ms, -diameter) %>%
        inner_join(diameter_insight_data)
    ,

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
                                    height=4, dpi=300),

    interactive_diam_vs_time = girafe(ggobj=plot_diameter_time,
                                      width_svg=7,
                                      height_svg=5),

    plot_time = main_data %>%
        filter(dataset %in% c("sk-2005-lcc-rweight", 
                              "twitter-2010-lcc-rweight", 
                              "USA-x10")) %>%
        mutate(killed=as.logical(killed)) %>%
        mutate(total_time = if_else(killed, 5*3600.0, as.double(drop_units(total_time))),
               final_diameter_time = drop_units(final_diameter_time)) %>%
        mutate(label=if_else(killed, 
                             "timed out > 5h", 
                             scales::number(total_time, suffix=" s"))) %>%
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
        #          fill="#351410",
        #          show.legend=F,
        #          position="dodge") +
        geom_text(aes(label=label,
                       y=total_time+500),
                   position=position_dodge(.8),
                   color="black",
                   size=3,
                   show.legend=FALSE) +
        scale_y_continuous("time", breaks=c(0,3000,6000,9000,12000,15000)) +
        scale_fill_manual(values=list(
            "DeltaStepping" = "gray",
            "ClusterDiameter" = "#B34537"
        )) +
        # annotate("text",
        #          label="timed out",
        #          hjust=1,
        #          x=2.5,
        #          y=14000) +
        theme_tufte() +
        theme(legend.position="none",
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
            "ClusterDiameter" = "#B34537"
        )) +
        theme_tufte() +
        theme(legend.position=c(.8, .8),
              panel.grid.major.x=element_blank(),
              panel.grid.minor.x=element_blank(),
              panel.grid.minor.y=element_blank(),
              panel.grid.major.y=element_line(color="white", size=.5),
            #   panel.background = element_rect(fill = NA),
              panel.ontop = TRUE)
    ,

    figure_time = ggsave("export/time.png", plot_time, width=4,height=4, dpi=300),
    figure_diam = ggsave("export/diameter.png", plot_diameter, width=4,height=4, dpi=300),

    plot_parameter_dep_time = {
        plotdata <- parameter_dep_data %>%
            mutate(duration = set_units(duration, "s")) %>%
            filter(iteration_radius >= 290) %>%
            arrange(iteration_radius) %>%
            mutate(iteration_radius_multiple = iteration_radius / 2900,
                   duration = drop_units(duration))
        durations <- plotdata %>% select(duration) %>% pull()

        ggplot(plotdata, aes(x=iteration_radius_multiple, y=duration)) +
            geom_point() +
            geom_line() +
            # geom_linerange(aes(ymin=0,
            #                    ymax=duration)) +
            geom_rangeframe(color="black") +
            # geom_text_repel(aes(label=cumulative_time %>% drop_units())) +
            scale_y_continuous(trans="log10", 
                               label=scales::number_format(),
                               breaks=durations) +
            # scale_y_unit(trans="log10",
            #              breaks=c(237, 570, 1452, 4688, 13065)) +
            scale_x_continuous(trans="log10",
                            labels=c("0.1", "1", "10", "100", "1000"),
                            breaks=c(0.1, 1, 10,100,1000)) +
            labs(x="radius guess, as multiple of the average edge weight",
                 y="duration (s)") +
            theme_tufte()
    }
    ,

    plot_parameter_dep_diameter = {
        plotdata <- parameter_dep_data %>%
            mutate(duration = set_units(duration, "s")) %>%
            filter(iteration_radius >= 290) %>%
            arrange(iteration_radius) %>%
            mutate(iteration_radius_multiple = iteration_radius / 2900,
                   duration = drop_units(duration))

        ggplot(plotdata, aes(x=iteration_radius_multiple, y=diameter)) +
            geom_point() +
            geom_line() +
            geom_rangeframe(color="black") +
            scale_y_continuous(#trans="log10", 
                               label=scales::number_format(scale=0.0000001)) +
            scale_x_continuous(trans="log10",
                            labels=c("0.1", "1", "10", "100", "1000"),
                            breaks=c(0.1, 1, 10,100,1000)) +
            labs(x="radius guess, as multiple of the average edge weight",
                 y="diameter (millions)") +
            theme_tufte()
    }
    ,

    plot_parameter_dep_time_cumulative = {
        plotdata <- parameter_dep_data %>%
            mutate(duration = set_units(duration, "s")) %>%
            filter(iteration_radius >= 290) %>%
            arrange(iteration_radius) %>%
            mutate(iteration_radius_multiple = iteration_radius / 2900,
                cumulative_time = cumsum(duration) %>% drop_units())

        ggplot(plotdata, aes(x=iteration_radius_multiple, y=cumulative_time)) +
            geom_point() +
            geom_line() +
            geom_rangeframe(color="black") +
            # geom_text_repel(aes(label=cumulative_time %>% drop_units())) +
            scale_y_continuous(trans="log10",
                         breaks=c(237, 570, 1452, 4688, 13065)) +
            scale_x_continuous(trans="log10",
                            labels=c("0.1", "1", "10", "100", "1000"),
                            breaks=c(0.1, 1, 10,100,1000)) +
            labs(x="radius guess, as multiple of the average edge weight",
                 y="cumulative time (s)") +
            theme_tufte()
    }
    ,

    plot_parameter_dep_size = parameter_dep_data %>%
        mutate(duration = set_units(duration, "s")) %>%
        filter(iteration_radius >= 290) %>%
        arrange(iteration_radius) %>%
        mutate(iteration_radius_multiple = iteration_radius / 2900) %>%
        ggplot(aes(x=iteration_radius_multiple, y=num_centers)) +
        geom_point() +
        geom_line() +
        geom_rangeframe(color="black") +
        # geom_text_repel(aes(label=cumulative_time %>% drop_units())) +
        # scale_y_unit(breaks=c(237, 570, 1452, 4688, 13065)) +
        scale_x_continuous(trans="log10", labels=c("0.1", "1", "10", "100", "1000"),
                           breaks=c(0.1,1,10,100,1000)) +
        scale_y_continuous(trans="log10") +
        labs(x="radius guess, as multiple of the average edge weight",
             y="size of the auxiliary graph (nodes)") +
        theme_tufte()
    ,

    figure_parameter_dep = {
        p <- plot_grid(
            plot_parameter_dep_time + theme(axis.title.x = element_blank()), 
            # plot_parameter_dep_time_cumulative, 
            plot_parameter_dep_size,# + theme(axis.title.x = element_blank()),
            plot_parameter_dep_diameter + theme(axis.title.x = element_blank()),
            align="h",
            ncol=3)
        ggsave("export/param_dep.png", p, width=8, height=3, dpi=300)
    },

    data_scalability = main_data %>%
        filter(dataset %in% c("USA", "USA-x10", "USA-x5"),
               !killed,
               parameters %in% c("10000000:20000,10", "", "2950000000")) %>%
        mutate(scale = as.integer(str_extract(dataset, "\\d+"))) %>%
        replace_na(list(scale = 1)) %>%
        group_by(scale, algorithm, parameters) %>%
        summarise(total_time = mean(total_time)) %>%
        ungroup()
    ,

    plot_scalability_n = data_scalability %>%
        mutate(total_time=drop_units(total_time)) %>%
        ggplot(aes(scale, total_time, color=algorithm, shape=algorithm)) +
        geom_point() +
        geom_line() +
        geom_label_repel(aes(label=scales::number(total_time,
                                                  suffix=" s")),
                         show.legend=F) +
        
        geom_rangeframe(color="black") +
        scale_x_continuous(breaks = c(1,5,10),
                           labels = c("USA", "USA-x5", "USA-x10")) +
        scale_color_manual(values=c(
            "DeltaStepping" = "#808080",
            "Sequential" = "#4C4CFF",
            "ClusterDiameter" = "#B34537"
        )) +
        labs(x="dataset",
             y="time (s)") +
        theme_tufte() +
        theme(legend.position = c(.8, .8),
            #   axis.line.y.left = element_line(size=.5),
            #   axis.line.x.bottom = element_line(size=.5),
        )
    ,

    figure_scalability_n =
        ggsave("export/scalability_n.png", plot_scalability_n, width=8, height=3, dpi=300)
    ,

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
