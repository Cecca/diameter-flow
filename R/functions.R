db_connection <- function() {
    DBI::dbConnect(RSQLite::SQLite(), dbname = "diameter-results.sqlite")
}

conn_builder <- function(path) {
    print("Building new connection")
    # function() {
        DBI::dbConnect(RSQLite::SQLite(), dbname = path)
    # }
}

# The path argument is just a trick to make drake notice that something happened
table_main <- function(con, path) {
    print(paste("Referencing main table", path))
    tbl(con, "main")
}

# The path argument is just a trick to make drake notice that something happened
table_counters <- function(con, path) {
    print(paste("Referencing counters table", path))
    tbl(con, "counters")
}

scale_color_category10 <- function() {
    scale_color_manual(
        values = c(
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
            "#17becf"
        )
    )
}

scale_color_algorithm <- function() {
    scale_color_manual(
        values = c(
            "Bfs" = "#1f77b4",
            "HyperBall" = "#ff7f0e",
            "RandCluster" = "#2ca02c",
            "DeltaStepping" = "#d62728"
        )
    )
}

add_graph_type <- function(data) {
    data %>%
        mutate(graph_type = case_when(
            (dataset %in% c("livejournal", "friendster")) ~ "social",
            (dataset %in% c("sk-2005", "uk-2014-host-lcc", "uk-2005-lcc", "sk-2005-lcc")) ~ "web",
            (dataset %in% c("USA-E", "USA-W", "USA-CTR", "USA")) ~ "roads"
        ))
}

static_diam_vs_time <- function(to_plot) {
    to_plot <- to_plot %>%
        mutate(total_time = total_time_ms / 1000) %>%
        group_by(graph_type, dataset, algorithm, parameters) %>%
        summarise(
            total_time = mean(total_time),
            diameter = mean(diameter)
        )

    subplot <- function (data) {
        bounds <- data %>%
            filter(algorithm %in% c("Bfs", "DeltaStepping")) %>%
            group_by(graph_type, dataset) %>%
            summarise(
                diameter_lower = max(diameter / 2),
                diameter_upper = min(diameter)
            )
        ggplot(data, aes()) +
            geom_rect(mapping = aes(xmin = diameter_lower,
                                    xmax = diameter_upper,
                                    ymin = 0,
                                    ymax = Inf),
                    data = bounds,
                    alpha = 0.6,
                    fill = "lightgray") +
            geom_point_interactive(aes(x = diameter, y = total_time, color = algorithm, 
                                       tooltip=str_c("total time:", scales::number(total_time, accuracy=.1, suffix="s"), "diameter:", diameter, sep=" "))) +
            facet_wrap(vars(dataset), scales = "free",
                    ncol = 4) +
            scale_color_algorithm() +
            # scale_y_log10() +
            labs(x = "diameter",
                y = "total time (s)") +
            theme_bw() +
            theme(legend.position = "top")
    }

    plot_grid(
        to_plot %>% filter(graph_type == "web") %>% subplot(),
        to_plot %>% filter(graph_type == "social") %>% subplot(),
        to_plot %>% filter(graph_type == "roads") %>% subplot(),
        ncol = 1,
        labels = c("Web", "Social", "Roads")
    )
}

static_param_dependency_time <- function(to_plot) {
    to_plot <- to_plot %>%
        filter(algorithm == "RandCluster") %>%
            separate(parameters, into = c("radius", "base"), convert = TRUE) %>%
            filter(base == 2) %>%
            mutate(total_time = total_time_ms / 1000)

    ggplot(to_plot, aes(x = radius, y = total_time)) +
        stat_summary() +
        geom_line(stat = "summary") +
        facet_wrap(vars(dataset), scales = "free", ncol = 4) +
        scale_color_category10() +
        scale_x_continuous(trans="log2") +
        labs(x = "radius",
             y = "total time (s)") +
        theme_bw() +
        theme(legend.position = "top")
}

static_param_dependency_diam <- function(to_plot) {
    to_plot <- to_plot %>%
        filter(algorithm == "RandCluster") %>%
            separate(parameters, into = c("radius", "base"), convert = TRUE) %>%
            mutate(total_time = total_time_ms / 1000) %>%
        filter(base == 2) %>%
        group_by(dataset, radius, base) %>%
        summarise(diameter = mean(diameter))

    ggplot(to_plot, aes(x = radius, y = diameter)) +
        geom_point() +
        geom_line() +
        facet_wrap(vars(dataset), scales = "free", ncol = 4) +
        scale_color_category10() +
        scale_x_continuous(trans="log2") +
        labs(x = "radius",
             y = "diameter") +
        theme_bw() +
        theme(legend.position = "top")
}

static_auxiliary_graph_size <- function(to_plot) {
    to_plot <- to_plot %>%
        mutate(fraction = centers / num_nodes) %>%
        filter(base == 2) %>%
        group_by(dataset, radius, base) %>%
        summarise(fraction = mean(fraction))

    ggplot(to_plot, aes(x = radius,
                        y = fraction)) +
        geom_point() +
        geom_line() +
        facet_wrap(vars(dataset), scales = "free", ncol = 4) +
        scale_color_category10() +
        scale_x_continuous(trans = "log2") +
        labs(x = "radius",
             y = "fraction of input nodes") +
        theme_bw() +
        theme(legend.position = "top")

}
