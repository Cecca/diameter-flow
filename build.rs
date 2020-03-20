extern crate flate2;
extern crate tar;
use flate2::read::GzDecoder;
use std::collections::HashSet;
use std::fs::File;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::process::Command;
use tar::Archive;

fn maybe_download_file(url: &str, dest: PathBuf) -> PathBuf {
    if !dest.exists() {
        println!("cargo:warning=Downloading {}", url);
        let mut resp = reqwest::get(url).expect("problem while getting the url");
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
    // let java_path = "java/BVGraphToEdges.java";
    // let binary_path = "java/BVGraphToEdges.class";
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

fn main() {
    get_jars(&PathBuf::from("java"));
    Command::new("javac")
        .args(&["-classpath",
                "webgraph-3.6.3.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:slf4j-api-1.7.26.jar",
                "BVGraphToEdges.java"])
        .current_dir("java")
        .spawn()
        .expect("java compilation failed");
}
