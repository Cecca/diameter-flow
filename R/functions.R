theme_set(theme_bw())

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
            "HyperBall" = "#1f77b4",
            "Bfs" = "#ff7f0e",
            "RandCluster" = "#2ca02c",
            "DeltaStepping" = "#d62728"
        )
    )
}

add_graph_type <- function(data) {
    data %>%
        mutate(graph_type = case_when(
            (dataset %in% c("livejournal", "orkut")) ~ "social",
            (dataset %in% c("uk-2014-host-lcc", "uk-2005-lcc", "sk-2005-lcc")) ~ "web",
            (dataset %in% c("USA-E", "USA-W", "USA-CTR", "USA")) ~ "roads",
            (dataset %in% c("mesh-1000")) ~ "synthetic"
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
        max_diam <- data %>%
            ungroup() %>%
            summarise(max(diameter)) %>%
            pull()
        p <- ggplot(data, aes()) +
            geom_rect(mapping = aes(xmin = diameter_lower,
                                    xmax = diameter_upper,
                                    ymin = 0,
                                    ymax = Inf),
                    data = bounds,
                    alpha = 0.6,
                    fill = "lightgray") +
            geom_point_interactive(aes(x = diameter, y = total_time, color = algorithm, 
                                       tooltip=str_c("total time:", scales::number(total_time, accuracy=.1, suffix="s"), 
                                                     "diameter:", diameter, 
                                                     "parameters:", parameters, 
                                                     sep=" "))) +
            facet_wrap(vars(dataset), 
                       scales = "free_y",
                       ncol = 4) +
            scale_color_algorithm() +
            # scale_y_log10() +
            labs(x = "diameter",
                y = "total time (s)") +
            theme_bw() +
            theme(legend.position = "none")

        if (max_diam > 10000) {
            p <- p +
              scale_x_continuous(labels=scales::number_format(scale=0.0000001, accuracy=1))  +
              labs(x=TeX("diameter $\\cdot 10^7$"))
        }

        p
    }

    legend_plot <- tribble(
        ~algorithm,
        "Bfs",
        "HyperBall",
        "RandCluster" ,
        "DeltaStepping" 
    ) %>% ggplot(aes(color=algorithm)) +
    geom_point(x=0, y=0) +
    scale_color_algorithm() +
    theme_void() +
    theme(legend.position="top")
    
    legend <- get_legend(legend_plot)
    
    plot_grid(
        legend,
        to_plot %>% filter(graph_type == "web") %>% subplot(),
        plot_grid(
            to_plot %>% filter(graph_type == "social") %>% subplot(),
            to_plot %>% filter(graph_type == "synthetic") %>% subplot(),
            rel_widths = c(2,1),
            labels = c("Social", "Synthetic")
        ),
        to_plot %>% filter(graph_type == "roads") %>% subplot(),
        ncol = 1,
        labels = c("", "Web", "", "Roads"),
        rel_heights = c(0.2, 1, 1, 1)
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

do_scalability_plot <- function(to_plot) {
    to_plot %>%
    mutate(total_time = total_time_ms / 1000) %>%
    ggplot(aes(x=num_hosts, y=total_time, group=num_hosts)) +
        stat_summary(fun.data=mean_cl_boot,
                     geom="pointrange") +
        facet_wrap(vars(dataset)) +
        theme_bw()
}

do_scalability_n_plot <- function(to_plot) {
    to_plot %>%
    mutate(total_time = total_time_ms / 1000) %>%
    ggplot(aes(x=scale_factor, y=total_time)) +
        stat_summary(fun.data=mean_cl_boot,
                     geom="pointrange") +
        facet_wrap(vars(dataset)) +
        theme_bw()
}

