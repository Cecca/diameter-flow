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
                    aggregate = "max") %>%
        vl_encode_x2(field = "diameter2",
                     aggregate = "min")
    }


    scatter <- function() {
        vl_chart()  %>%
        vl_mark_point(tooltip = TRUE) %>%
        vl_encode_x(field = "diameter",
                    aggregate = "mean",
                    type = "quantitative") %>%
        vl_encode_y(field = "total_time_ms",
                    aggregate = "mean",
                    type = "quantitative") %>%
        vl_encode_detail(field = "parameters",
                         type = "nominal") %>%
        vl_encode_color(field = "algorithm", type = "nominal")
    }

    vl_layer(bands(), scatter()) %>%
        vl_add_data(to_plot) %>%
        vl_facet_wrap(field = "dataset", type = "nominal") %>%
        vl_resolve_scale_x("independent") %>%
        vl_resolve_scale_y("independent")
}
