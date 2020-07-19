use crate::Config;
use chrono::prelude::*;

use rusqlite::*;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

use std::time::Duration;

pub struct Reporter {
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, outer and inner iteration counters, and count, no longer used
    counters: Vec<(String, u32, u32, u64)>,
    // Table with iteration, radius, duration, and size of the graph
    rand_cluster_guesses: Vec<(u32, u32, Duration, u32)>,
    diameter: Option<u32>,
    duration: Option<Duration>,
    final_approx_time: Option<Duration>,
    killed: bool,
}

impl Reporter {
    pub fn new(config: Config) -> Self {
        Self {
            date: Utc::now(),
            config: config,
            counters: Vec::new(),
            rand_cluster_guesses: Vec::new(),
            diameter: None,
            duration: None,
            final_approx_time: None,
            killed: false,
        }
    }

    pub fn set_result(&mut self, diameter: u32, elapsed: Duration) {
        self.diameter.replace(diameter);
        self.duration.replace(elapsed);
    }

    pub fn set_final_approx_time(&mut self, final_approx_time: Duration) {
        self.final_approx_time.replace(final_approx_time);
    }

    // pub fn append_counter(&mut self, event: CountEvent, count: u64) {
    //     let (outer, inner) = event.iterations();
    //     self.counters.push((event.as_string(), outer, inner, count));
    // }

    pub fn append_rand_cluster_iteration(
        &mut self,
        iteration: u32,
        radius: u32,
        duration: Duration,
        num_centers: u32,
    ) {
        self.rand_cluster_guesses
            .push((iteration, radius, duration, num_centers));
    }

    fn sha(&self) -> String {
        let datestr = self.date.to_rfc2822();
        let mut sha = Sha256::new();
        sha.input(datestr);
        // I know that the following is implementation-dependent, but I just need
        // to have a identifier to join different tables created in this run.
        sha.input(format!("{:?}", self.config));
        sha.input(format!("{:?}", self.counters));

        format!("{:x}", sha.result())[..6].to_owned()
    }

    fn get_db_path() -> std::path::PathBuf {
        let mut path = dirs::home_dir().expect("unable to get home directory");
        path.push("diameter-results.sqlite");
        path
    }

    pub fn already_run(&self) -> Option<String> {
        if self.config.rerun {
            return None;
        }
        let dbpath = Self::get_db_path();
        let conn = Connection::open(dbpath).expect("error connecting to the database");
        conn.query_row(
            "SELECT sha FROM main WHERE
                seed == ?1 AND 
                threads == ?2 AND 
                hosts == ?3 AND 
                dataset == ?4 AND
                algorithm == ?5 AND 
                parameters == ?6 AND
                offline == ?7",
            params![
                format!("{}", self.config.seed()),
                self.config.threads.unwrap_or(1) as u32,
                self.config.hosts_string(),
                self.config.dataset,
                self.config.algorithm.name(),
                self.config.algorithm.parameters_string(),
                self.config.offline
            ],
            |row| row.get(0),
        )
        .optional()
        .unwrap_or(None)
    }

    pub fn killed(&mut self, time: Duration) {
        self.killed = true;
        self.duration.replace(time);
    }

    pub fn report(&self) {
        let sha = self.sha();
        let dbpath = Self::get_db_path();
        let mut conn = Connection::open(dbpath).expect("error connecting to the database");
        create_tables_if_needed(&conn);

        if self.killed {
            conn.execute(
            "INSERT INTO main ( sha, date, seed, threads, hosts, dataset, algorithm, parameters, diameter, total_time_ms, offline, final_diameter_time_ms, killed )
                VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13 )",
                params![
                    sha,
                    self.date.to_rfc3339(),
                    format!("{}", self.config.seed()),
                    self.config.threads.unwrap_or(1) as u32,
                    self.config.hosts_string(),
                    self.config.dataset,
                    self.config.algorithm.name(),
                    self.config.algorithm.parameters_string(),
                    -1,
                    self.duration.expect("missing total time").as_millis() as u32,
                    self.config.offline,
                    self.final_approx_time.map(|dur| dur.as_millis() as u32),
                    true
                ],
            )
            .expect("error inserting into main table");
        }

        let tx = conn.transaction().expect("problem starting transaction");

        {
            // Insert into main table
            tx.execute(
                "INSERT INTO main ( sha, date, seed, threads, hosts, dataset, algorithm, parameters, diameter, total_time_ms, offline, final_diameter_time_ms )
                 VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12 )",
                params![
                    sha,
                    self.date.to_rfc3339(),
                    format!("{}", self.config.seed()),
                    self.config.threads.unwrap_or(1) as u32,
                    self.config.hosts_string(),
                    self.config.dataset,
                    self.config.algorithm.name(),
                    self.config.algorithm.parameters_string(),
                    self.diameter.expect("missing diameter"),
                    self.duration.expect("missing total time").as_millis() as u32,
                    self.config.offline,
                    self.final_approx_time.map(|dur| dur.as_millis() as u32)
                ],
            )
            .expect("error inserting into main table");

            // Insert into counters table
            let mut stmt = tx
                .prepare(
                    "INSERT INTO counters ( sha, counter, outer_iter, inner_iter, count
                    ) VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                )
                .expect("failed to prepare statement");
            for (name, outer, inner, count) in self.counters.iter() {
                stmt.execute(params![sha, name, outer, inner, *count as u32])
                    .expect("Failure to insert into counters table");
            }

            if !self.rand_cluster_guesses.is_empty() {
                let mut stmt = tx.prepare(
                    "INSERT INTO rand_cluster_iterations (sha, iteration, iteration_radius, duration_ms, num_centers)
                    VALUES (?1, ?2, ?3, ?4, ?5)",
                ).expect("failed to prepare statement");

                for (iteration, iteration_radius, duration, num_centers) in
                    self.rand_cluster_guesses.iter()
                {
                    stmt.execute(params![
                        sha,
                        iteration,
                        iteration_radius,
                        duration.as_millis() as u32,
                        num_centers
                    ])
                    .expect("failed to execute statement");
                }
            }
        }

        tx.commit().expect("error committing insertions");
        conn.close().expect("error inserting into the database");
    }
}

fn bump(conn: &Connection, ver: u32) {
    conn.pragma_update(None, "user_version", &ver)
        .expect("error updating version");
}

fn create_tables_if_needed(conn: &Connection) {
    let version: u32 = conn
        .query_row(
            "SELECT user_version FROM pragma_user_version",
            params![],
            |row| row.get(0),
        )
        .unwrap();
    info!("Current database version is {}", version);

    if version < 1 {
        info!("applying changes for version 1");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS main (
            sha      TEXT PRIMARY KEY,
            date     TEXT NOT NULL,
            seed     TEXT NOT NULL,
            threads  INTEGER NOT NULL,
            hosts    TEXT NOT NULL,
            dataset  TEXT NOT NULL,
            algorithm TEXT NOT NULL,
            parameters TEXT NOT NULL,
            diameter INTEGER NOT NULL,
            total_time_ms  INTEGER NOT NULL
            )",
            params![],
        )
        .expect("Error creating main table");

        conn.execute(
            "CREATE VIEW IF NOT EXISTS main_recent AS
            SELECT sha, max(date) AS date, seed, threads, hosts, dataset, algorithm, parameters, diameter, total_time_ms
            FROM main
            GROUP BY seed, threads, hosts, dataset, algorithm, parameters",
            params![]
        )
        .expect("Error creating the main_recent view");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS counters (
            sha       TEXT NOT NULL,
            counter   TEXT NOT NULL,
            outer_iter INTEGER NOT NULL,
            inner_iter INTEGER NOT NULL,
            count     INTEGER NOT NULL,
            FOREIGN KEY (sha) REFERENCES main (sha)
            )",
            params![],
        )
        .expect("error creating counters table");

        bump(conn, 1);
    }

    if version < 2 {
        info!("applying changes for version 2");
        conn.execute(
            "ALTER TABLE main ADD COLUMN
            offline    BOOL NOT NULL DEFAULT FALSE
            ",
            params![],
        )
        .expect("Error creating main table");

        bump(conn, 2);
    }

    if version < 3 {
        info!("applying changes for version 3");

        conn.execute("DROP VIEW main_recent", params![])
            .expect("error dropping view");
        conn.execute(
            "CREATE VIEW IF NOT EXISTS main_recent AS
            SELECT sha, max(date) AS date, seed, threads, hosts, dataset, offline, algorithm, parameters, diameter, total_time_ms 
            FROM main
            GROUP BY seed, threads, hosts, dataset, offline, algorithm, parameters",
            params![]
        )
        .expect("Error creating the main_recent view");

        bump(conn, 3);
    }

    if version < 4 {
        info!("applying changes for version 4");

        conn.execute(
            "ALTER TABLE main ADD final_diameter_time_ms INT64 DEFAULT NULL",
            NO_PARAMS,
        )
        .expect("Error changing the table");

        bump(conn, 4);
    }

    if version < 5 {
        info!("applying changes for version 5");

        conn.execute(
            "CREATE TABLE rand_cluster_iterations (
                sha       TEXT NOT NULL,
                iteration   INTEGER NOT NULL,
                iteration_radius  INTEGER NOT NULL,
                duration_ms     INTEGER NOT NULL,
                num_centers    INTEGER NOT NULL,
                FOREIGN KEY (sha) REFERENCES main (sha)
            )",
            NO_PARAMS,
        )
        .expect("Error creating table rand_cluster_iterations");

        bump(conn, 5);
    }

    if version < 6 {
        info!("applying changes for version 6");

        conn.execute(
            "ALTER TABLE main ADD killed BOOLEAN DEFAULT FALSE",
            NO_PARAMS,
        )
        .expect("Error creating table rand_cluster_iterations");

        bump(conn, 6);
    }

    info!("database schema up tp date");
}
