use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

fn read_casemap_data_file(name: &str) -> Vec<u32> {
    let file = File::open(format!("./data/casemap/{name}")).unwrap();
    let reader = BufReader::new(file);
    let mut values = Vec::<u32>::new();
    let mut vec_idx = 0u32;
    for l in reader.lines() {
        let l = l.unwrap();
        let mut it = l.split_ascii_whitespace();
        let from = it.next().unwrap().parse::<u32>().unwrap();
        let to = it.next().unwrap().parse::<u32>().unwrap();
        while vec_idx < from {
            values.push(0);
            vec_idx += 1;
        }
        values.push(to);
        vec_idx += 1;
        assert!(it.next().is_none());
    }
    values
}

/// Generates static vector that maps between different letter cases.
fn generate_case_mapping_vec(name: &str, buf: &mut String) {
    let values = read_casemap_data_file(name);
    buf.push_str(&format!(
        "pub(crate) const {}_MAP_LEN: usize = {};\r\n",
        name.to_uppercase(),
        values.len()
    ));
    buf.push_str(&format!(
        "pub(crate) static {}_MAP: [u32; {}] = [",
        name.to_uppercase(),
        values.len()
    ));
    for v in values {
        buf.push_str(&format!("{v},"));
    }
    buf.push_str("];\r\n");
}

fn main() {
    let mut buf = String::from("mod case_mapping {\r\n");
    generate_case_mapping_vec("tolower", &mut buf);
    generate_case_mapping_vec("toupper", &mut buf);
    buf.push('}');

    let path = Path::new(&std::env::var("OUT_DIR").unwrap()).join("case_mapping.rs");
    std::fs::write(&path, buf).unwrap();
}
