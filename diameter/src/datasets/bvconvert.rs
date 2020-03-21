// Convert files in the webgraph compressed format into textual
// adjacency lists, by wrapping the Java code provided by WebGraph authors
//
// This is not the most efficient method by several measures. However,
// it is far easier
use flate2::read::GzDecoder;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;

use std::io::Write;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tar::Archive;

pub fn maybe_download_file(url: &str, dest: PathBuf) -> PathBuf {
    if !dest.exists() {
        println!("Downloading {} to {:?}", url, dest);
        let mut resp = reqwest::get(url).expect("problem while getting the url");
        assert!(resp.status().is_success());
        let mut out = File::create(&dest).expect("failed to create file");
        std::io::copy(&mut resp, &mut out).expect("failed to copy content");
    }
    dest
}

fn unpack_entries<I, S: AsRef<str>>(tar_path: &PathBuf, directory: &PathBuf, entries: I)
where
    I: IntoIterator<Item = S>,
{
    let entry_set: HashSet<String> = entries.into_iter().map(|e| e.as_ref().to_owned()).collect();
    let file = File::open(tar_path).expect("error opening tar");
    let mut archive = Archive::new(GzDecoder::new(file));
    let entries = archive.entries().expect("Problem retrieving entries");
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let path = e.path().expect("problem getting path").clone().to_owned();
            let path_str = path.to_str().to_owned();
            if let Some(path_str) = path_str {
                entry_set.contains(path_str)
            } else {
                false
            }
        })
        .for_each(|mut entry| {
            let path = entry.path().unwrap().file_name().unwrap().to_owned();
            let mut out_path = PathBuf::from(directory);
            out_path.push(path);
            entry.unpack(&out_path).expect("error unpacking the entry");
        });
}

fn get_jars(directory: &PathBuf) {
    let webgraph_url = "http://webgraph.di.unimi.it/webgraph-3.6.3-bin.tar.gz";
    let dependencies_url = "http://webgraph.di.unimi.it/webgraph-deps.tar.gz";

    let webgraph_tar = maybe_download_file(
        webgraph_url,
        PathBuf::from_iter(&["java", "webgraph.tar.gz"]),
    );
    let dependencies_tar = maybe_download_file(
        dependencies_url,
        PathBuf::from_iter(&["java", "dependencies.tar.gz"]),
    );

    unpack_entries(
        &webgraph_tar,
        directory,
        &["webgraph-3.6.3/webgraph-3.6.3.jar"],
    );
    unpack_entries(
        &dependencies_tar,
        directory,
        &[
            "webgraph-3.6.3/webgraph-3.6.3.jar",
            "dsiutils-2.6.2.jar",
            "fastutil-8.3.0.jar",
            "jsap-2.1.jar",
            "slf4j-api-1.7.26.jar",
        ],
    );
}

pub fn read<F>(graph_path: &PathBuf, mut action: F)
where
    F: FnMut((u32, u32)),
{
    println!("Reading {:?}", graph_path);
    let java_dir = PathBuf::from("java");
    if !java_dir.is_dir() {
        std::fs::create_dir(&java_dir).expect("Problems creating directory");
    }
    let mut graph_path = graph_path.clone();
    graph_path.set_extension("");
    let java_binary = include_bytes!("../../../java/BVGraphToEdges.class");
    let file = File::create("BVGraphToEdges.class").expect("Problem creating clas file");
    let mut writer = BufWriter::new(file);
    writer
        .write_all(java_binary)
        .expect("Problem writing class file");
    get_jars(&java_dir);
    let mut child = Command::new("java")
        .args(&["-classpath",
                ".:webgraph-3.6.3.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:slf4j-api-1.7.26.jar",
                "BVGraphToEdges",
                graph_path.to_str().unwrap()])
        .current_dir(&java_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("java command failed");

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(status.success());
                // Finish copying the stream
                let stdout = child.stdout.as_mut().unwrap();
                let reader = BufReader::new(stdout);
                let mut cnt = 0;
                for line in reader.lines() {
                    let line = line.unwrap();
                    let mut tokens = line.split_whitespace();
                    let src = tokens
                        .next()
                        .expect("no source in line")
                        .parse::<u32>()
                        .expect("could not parse source");
                    let dst = tokens
                        .next()
                        .expect("no destination in line")
                        .parse::<u32>()
                        .expect("could not parse destination");
                    action((src, dst));
                    cnt += 1;
                }

                let stderr = child.stderr.as_mut().unwrap();
                std::io::copy(stderr, &mut std::io::stderr()).expect("Failure piping the output");
                println!(
                    "Reading input from compressed file complete ({} lines in the last batch)",
                    cnt
                );
                return;
            }
            Ok(None) => {
                println!("Reading some output");
                let stdout = child.stdout.as_mut().unwrap();
                let reader = BufReader::new(stdout);
                let mut cnt = 0;
                for line in reader.lines() {
                    let line = line.unwrap();
                    let mut tokens = line.split_whitespace();
                    let src = tokens
                        .next()
                        .expect("no source in line")
                        .parse::<u32>()
                        .expect("could not parse source");
                    let dst = tokens
                        .next()
                        .expect("no destination in line")
                        .parse::<u32>()
                        .expect("could not parse destination");
                    action((src, dst));
                    cnt += 1;
                }
                println!("Read {} lines in this batch", cnt);

                let stderr = child.stderr.as_mut().unwrap();
                std::io::copy(stderr, &mut std::io::stderr()).expect("Failure piping the output");
            }
            Err(e) => panic!("error attempting to wait: {}", e),
        }
    }
}
