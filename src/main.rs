#![feature(try_blocks)]
mod antlr_gen;
mod codegen_common;
mod codegen_gcs;
mod ir;
mod to_ir;

use crate::codegen_common::CodeStruct;
use antlr_gen::lexer::CypherLexer;
use antlr_gen::parser::CypherParser;
use antlr_rust::common_token_stream::CommonTokenStream;
use antlr_rust::token_factory::CommonTokenFactory;
use antlr_rust::InputStream;
use clap::{App, Arg};
use handlebars::Handlebars;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

/* Generates code using templates formatted by handlebars
 * (see https://docs.rs/handlebars/3.5.2/handlebars/)
 * The handlebars templates can use any information found in the codegen object.
 * The formatted output is written to the file in output_filename.
 * Arguments:
 * @code_gen:  a code_gen object that contains information that can be formatted nicely by the handlebars
 * @template_path_name: the path leading to a handlebars template
 * @output_filename: where the output is written
 */
// TODO: make this trait more concrete
fn write_to_handlebars(code_gen: &CodeStruct, template_path: PathBuf, output_filename: PathBuf) {
    let display = template_path.display();
    let mut template_file = match File::open(&template_path) {
        Err(msg) => panic!("Failed to open {}: {}", display, msg),
        Ok(file) => file,
    };

    let mut template_str = String::new();
    match template_file.read_to_string(&mut template_str) {
        Err(msg) => panic!("Failed to read {}: {}", display, msg),
        Ok(_) => log::info!("Successfully read {}", display),
    }

    let handlebars = Handlebars::new();

    let output = handlebars
        .render_template(&template_str, &code_gen)
        .expect("handlebar render failed");

    log::info!("Writing output to: {:?}", output_filename);
    let mut file = File::create(output_filename).expect("file create failed.");
    file.write_all(output.as_bytes()).expect("write failed");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up logging
    let mut builder = env_logger::Builder::from_default_env();
    // do not want timestamp for now
    builder.default_format_timestamp(false);
    // Set default log level to info
    builder.filter_level(log::LevelFilter::Info);
    builder.init();

    let bin_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let template_dir = bin_dir.join("templates");
    let def_query_dir = bin_dir.join("gcs_query/query.cc");
    let app = App::new("Distributed Tracing in GCS");
    let matches = app
        .clone()
        .arg(
            Arg::with_name("query")
                .short("q")
                .long("query")
                .required(true)
                .value_name("FILE")
                .help("Sets the .cql query file to use"),
        )
        .arg(
            Arg::with_name("udf") // UDF is currently unused
                .short("u")
                .long("udf")
                .multiple(true)
                .value_name("UDF_FILE")
                .help("Optionally sets user defined function file to use"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("out-file")
                .value_name("OUT_FILE")
                .default_value(def_query_dir.to_str().unwrap())
                .help("Location and name of the output file."),
        )
        .get_matches();

    let tf = CommonTokenFactory::default();
    // Read query from file specified by command line argument.
    // Clap ensures that all of these are valid so we can safely unwrap
    let query_file = matches.value_of("query").unwrap();
    let filter_out = PathBuf::from(matches.value_of("output").unwrap());
    // Collect UDFs
    let mut udfs = Vec::new();
    if let Some(udf_files) = matches.values_of("udf") {
        for udf_file in udf_files {
            let udf = match std::fs::read_to_string(udf_file) {
                Ok(udf_str) => udf_str,
                Err(err) => {
                    log::error!("Failed to parse UDF file as string: {:?}", err);
                    std::process::exit(-1);
                }
            };
            udfs.push(udf);
        }
    }
    // Start the parsing phase
    let query = match fs::read_to_string(query_file) {
        Ok(query_str) => query_str,
        Err(err) => {
            log::error!("Failed to parse query file as string: {:?}", err);
            std::process::exit(-1);
        }
    };
    let query_stream = InputStream::new_owned(query.into_boxed_str());
    let lexer = CypherLexer::new_with_token_factory(query_stream, &tf);
    let token_source = CommonTokenStream::new(lexer);
    let mut parser = CypherParser::new(token_source);
    let result = parser.oC_Cypher()?;

    let visitor_results = to_ir::visit_result(result);
    let codegen_object = codegen_gcs::generate_code_blocks(visitor_results, udfs);
    let filter_str = "gcs_filter.cc.handlebars";
    write_to_handlebars(&codegen_object, template_dir.join(filter_str), filter_out);
    Ok(())
}
