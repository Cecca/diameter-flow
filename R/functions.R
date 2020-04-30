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
