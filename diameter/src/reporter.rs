use crate::logging::*;
use crate::Config;
use chrono::prelude::*;
use sha2::{Digest, Sha256};

use std::fs::File;
use std::io::{Result as IOResult, Write};
use std::path::Path;
use std::time::Duration;

pub struct Reporter {
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, worker id, outer and inner iteration counters, and count
    counters: Vec<(String, usize, u32, u32, u64)>,
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

    pub fn append_counter(&mut self, event: CountEvent, worker_id: usize, count: u64) {
        let (outer, inner) = event.iterations();
        self.counters
            .push((event.as_string(), worker_id, outer, inner, count));
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

    fn with_header<P: AsRef<Path> + std::fmt::Debug>(path: P, header: &str) -> IOResult<File> {
        use std::fs::OpenOptions;
        if !path.as_ref().is_file() {
            println!("File {:?} does not exist, writing header", path);
            let mut f = OpenOptions::new().create(true).write(true).open(path)?;
            writeln!(f, "{}", header);
            Ok(f)
        } else {
            OpenOptions::new().create(true).append(true).open(path)
        }
    }

    pub fn report<P: AsRef<Path>>(&self, path: P) -> IOResult<()> {
        let sha = self.sha();
        let dir = path.as_ref().to_path_buf();
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir)?;
        }

        // Write configuration and parameters
        let mut main_path = dir.clone();
        main_path.push("main.csv");
        let mut writer = Self::with_header(
            main_path,
            "sha,seed,threads,hosts,dataset,algorithm,main,diameter,total_time_ms",
        )?;
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{}",
            sha,
            self.config.seed(),
            self.config.threads.unwrap_or(1),
            self.config.hosts_string(),
            self.config.dataset,
            self.config.algorithm.name(),
            self.config.algorithm.parameters_string(),
            self.diameter.expect("missing diameter"),
            self.duration.expect("missing total time").as_millis(),
        )?;

        // Write counters
        let mut counters_path = dir.clone();
        counters_path.push("counters.csv");
        let mut writer = Self::with_header(
            counters_path,
            "sha,counter,worker,outer_iter,inner_iter,count",
        )?;
        for (name, worker, outer, inner, count) in self.counters.iter() {
            writeln!(
                writer,
                "{},{},{},{},{},{}",
                sha, name, worker, outer, inner, count
            )?;
        }

        Ok(())
    }
}
