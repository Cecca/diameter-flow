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

do_plot_diam_vs_time_interactive <- function(to_plot) {
    bounds <- to_plot %>%
        filter(algorithm %in% c("Bfs", "DeltaStepping")) %>%
        group_by(dataset) %>%
        mutate(
            diameter_lower = max(diameter),
            diameter_upper = min(2 * diameter)
        )

    bands <- function() {
        vl_chart() %>%
        vl_filter(filter = "datum.algorithm == 'Bfs' | datum.algorithm == 'DeltaStepping'") %>%
        vl_calculate(calculate = "datum.diameter/2", as="diameter2") %>%
        vl_mark_rect(opacity = 0.4, color = "lightgray") %>%
        vl_encode_x(field = "diameter", type = "quantitative",
                    aggregate = "min") %>%
        vl_encode_x2(field = "diameter2",
                     aggregate = "max")
    }


    scatter <- function() {
        vl_chart()  %>%
        vl_mark_point(tooltip = TRUE,
                      filled = TRUE) %>%
        vl_encode_x(field = "diameter",
                    aggregate = "mean",
                    type = "quantitative") %>%
        vl_encode_y(field = "total_time_ms",
                    aggregate = "mean",
                    type = "quantitative") %>%
        vl_scale_y(type = "log") %>%
        vl_encode_detail(field = "parameters",
                         type = "nominal") %>%
        vl_encode_color(field = "algorithm",
                        type = "nominal",
                        scale = list(scheme = "category10"))
    }

    vl_layer(bands(), scatter()) %>%
        vl_add_data(to_plot) %>%
        vl_facet_wrap(field = "dataset", type = "nominal") %>%
        vl_resolve_scale_x("independent") %>%
        vl_resolve_scale_y("independent")
}

static_diam_vs_time <- function(to_plot) {
    # lower_bounds <- to_plot %>%
    #     filter(algorithm != "RandCluster") %>%
    #     mutate(diameter_lower = if_else(algorithm == "HyperBall",
    #                                     as.integer(diameter),
    #                                     as.integer(diameter / 2))) %>%
    #     group_by(dataset) %>%
    #     summarise(diameter_lower = max(diameter_lower))

    # upper_bounds <- to_plot %>%
    #     filter(algorithm != "HyperBall") %>%
    #     filter(algorithm != "RandCluster") %>%
    #     group_by(dataset) %>%
    #     summarise(diameter_upper = min(diameter))

    # bounds <- inner_join(lower_bounds, upper_bounds)
    # print(bounds)

    bounds <- to_plot %>%
        filter(algorithm %in% c("Bfs", "DeltaStepping")) %>%
        group_by(dataset) %>%
        summarise(
            diameter_lower = max(diameter / 2),
            diameter_upper = min(diameter)
        )

    to_plot <- to_plot %>%
        mutate(total_time = total_time_ms / 1000) %>%
        group_by(dataset, algorithm, parameters) %>%
        summarise(
            total_time = mean(total_time),
            diameter = mean(diameter)
        )

    ggplot(to_plot, aes()) +
        geom_rect(mapping = aes(xmin = diameter_lower,
                                xmax = diameter_upper,
                                ymin = 0,
                                ymax = Inf),
                  data = bounds,
                  alpha = 0.6,
                  fill = "lightgray") +
        geom_point(aes(x = diameter, y = total_time, color = algorithm)) +
        facet_wrap(vars(dataset), scales = "free",
                   ncol = 4) +
        scale_color_category10() +
        scale_y_log10() +
        labs(x = "diameter",
             y = "total time (s)") +
        theme_bw() +
        theme(legend.position = "top")
}

static_param_dependency_time <- function(to_plot) {
    to_plot <- to_plot %>%
        filter(algorithm == "RandCluster") %>%
            separate(parameters, into = c("radius", "base"), convert = TRUE) %>%
            mutate(total_time = total_time_ms / 1000)

    ggplot(to_plot, aes(x = radius, y = total_time, color = factor(base))) +
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
        group_by(dataset, radius, base) %>%
        summarise(diameter = mean(diameter))

    ggplot(to_plot, aes(x = radius, y = diameter, color = factor(base))) +
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
        group_by(dataset, radius, base) %>%
        summarise(fraction = mean(fraction))

    ggplot(to_plot, aes(x = radius,
                        y = fraction,
                        color = factor(base))) +
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
