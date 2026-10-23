use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn push_if_exists(dirs: &mut Vec<PathBuf>, candidate: PathBuf) {
    if candidate.exists() {
        dirs.push(candidate);
    }
}

fn include_from_repo_vendor() -> Option<PathBuf> {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")?;
    let include = PathBuf::from(manifest_dir)
        .join("..")
        .join("..")
        .join("third_party")
        .join("protobuf")
        .join("include");
    include.exists().then_some(include)
}

fn include_from_env(var_name: &str) -> Option<PathBuf> {
    env::var_os(var_name).map(PathBuf::from).filter(|p| p.exists())
}

fn include_from_protoc_env() -> Option<PathBuf> {
    let protoc = env::var_os("PROTOC")?;
    let protoc = PathBuf::from(protoc);
    let parent = protoc.parent()?;
    let include = parent.join("..").join("include");
    include.exists().then_some(include)
}

fn include_from_winget() -> Option<PathBuf> {
    let local_app_data = env::var_os("LOCALAPPDATA")?;
    let packages = PathBuf::from(local_app_data)
        .join("Microsoft")
        .join("WinGet")
        .join("Packages");

    let entries = fs::read_dir(packages).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("Google.Protobuf_") {
            let include = entry.path().join("include");
            if include.exists() {
                return Some(include);
            }
        }
    }

    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    let mut include_dirs = vec![PathBuf::from("proto")];

    if let Some(path) = include_from_repo_vendor() {
        // Prefer workspace-local protobuf includes (ASCII path) to avoid
        // Windows toolchain issues with non-ASCII user profile paths.
        push_if_exists(&mut include_dirs, path);
    } else {
        if let Some(path) = include_from_env("PROTOBUF_INCLUDE") {
            push_if_exists(&mut include_dirs, path);
        }
        if let Some(path) = include_from_env("PROTOC_INCLUDE") {
            push_if_exists(&mut include_dirs, path);
        }
        if let Some(path) = include_from_protoc_env() {
            push_if_exists(&mut include_dirs, path);
        }
        if let Some(path) = include_from_winget() {
            push_if_exists(&mut include_dirs, path);
        }
    }

    include_dirs.sort();
    include_dirs.dedup();

    let include_refs: Vec<&Path> = include_dirs.iter().map(|p| p.as_path()).collect();

    prost_build::Config::new()
        .file_descriptor_set_path(out_dir.join("persistence_descriptor.bin"))
        .compile_protos(&["proto/persistence.proto", "proto/snapshot.proto"], &include_refs)?;

    Ok(())
}
