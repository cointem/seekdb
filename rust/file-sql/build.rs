fn main() {
    cxx_build::bridge("src/lib.rs")
        .std("c++17")
        .include("include")
        .compile("file_sql_cxx");
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::copy(
        "include/file_sql_status.h",
        output.join("file_sql_status.h"),
    )
    .unwrap();
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=include/file_sql_status.h");
}
