use crate::logging::*;
use crate::Config;
use chrono::prelude::*;
use flate2::write::GzEncoder;
use flate2::Compression;
use rusqlite::*;
use rusqlite::{params, Connection, Result as SQLResult};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Result as IOResult, Write};
use std::path::Path;
use std::time::Duration;

pub struct Reporter {
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, outer and inner iteration counters, and count
    counters: Vec<(String, u32, u32, u64)>,
    diameter: Option<u32>,
    duration: Option<Duration>,
}

impl Reporter {
    pub fn new(config: Config) -> Self {
        Self {
            date: Utc::now(),
            config: config,
            counters: Vec::new(),
            diameter: None,
            duration: None,
        }
    }

    pub fn set_result(&mut self, diameter: u32, elapsed: Duration) {
        self.diameter.replace(diameter);
        self.duration.replace(elapsed);
    }

    pub fn append_counter(&mut self, event: CountEvent, count: u64) {
        let (outer, inner) = event.iterations();
        self.counters.push((event.as_string(), outer, inner, count));
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

    pub fn report(&self) {
        let sha = self.sha();
        let dbpath = Self::get_db_path();
        let mut conn = Connection::open(dbpath).expect("error connecting to the database");
        create_tables_if_needed(&conn);

        let tx = conn.transaction().expect("problem starting transaction");

        {
            // Insert into main table
            tx.execute(
                "INSERT INTO main ( sha, date, seed, threads, hosts, dataset, algorithm, parameters, diameter, total_time_ms, offline )
                 VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11 )",
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
                    self.config.offline
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

    info!("database schema up tp date");
}
