use xshell::Shell;

fn main() {
    let matches = clap::Command::new("codegentool")
        .subcommand_required(true)
        .subcommand(clap::Command::new("generate-proto"))
        .subcommand(clap::Command::new("generate-flatbuffers"))
        .subcommand(clap::Command::new("generate-all"))
        .get_matches();

    match matches.subcommand() {
        Some(("generate-proto", _args)) => generate_protobuf(),
        Some(("generate-flatbuffers", _args)) => generate_flatbuffers(),
        Some(("generate-all", _args)) => {
            generate_protobuf();
            generate_flatbuffers();
        }
        _ => panic!("Unknown subcommand {:?}", matches.subcommand()),
    }
}

fn generate_protobuf() {
    let src_dir = std::env::current_dir()
        .expect("current_dir")
        .join("rust")
        .join("amudai-format")
        .join("src");
    let out_dir = src_dir.join("defs");
    assert!(
        src_dir.exists(),
        "generate_protobuf: {} does not exist",
        src_dir.display()
    );

    std::fs::create_dir_all(&out_dir).expect("create_dir_all");

    let proto_dir = std::env::current_dir()
        .expect("current_dir")
        .join("proto_defs")
        .join("shard_format");

    let sh = Shell::new().expect("shell");
    let proto_files = sh
        .read_dir(&proto_dir)
        .expect("read proto dir")
        .into_iter()
        .filter(|p| p.extension().is_some_and(|ext| ext == "proto"))
        .collect::<Vec<_>>();
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&proto_files, &[proto_dir])
        .expect("compile_protos");
}

fn generate_flatbuffers() {
    let src_dir = std::env::current_dir()
        .expect("current_dir")
        .join("rust")
        .join("amudai-format")
        .join("src");
    let out_dir = src_dir.join("defs");
    assert!(
        src_dir.exists(),
        "generate_protobuf: {} does not exist",
        src_dir.display()
    );

    std::fs::create_dir_all(&out_dir).expect("create_dir_all");

    let proto_dir = std::env::current_dir()
        .expect("current_dir")
        .join("proto_defs")
        .join("shard_format");

    let sh = Shell::new().expect("shell");

    for input_fbs in sh
        .read_dir(&proto_dir)
        .expect("read proto dir")
        .into_iter()
        .filter(|p| p.extension().is_some_and(|ext| ext == "fbs"))
    {
        let out_name = input_fbs
            .file_name()
            .expect("fbs file name")
            .to_string_lossy();
        let out_name = format!("{out_name}.rs");
        let out_path = out_dir.join(&out_name);
        sh.cmd("planus")
            .arg("rust")
            .arg("-o")
            .arg(&out_path)
            .arg(&input_fbs)
            .run()
            .expect("generate from fbs");
    }
}
