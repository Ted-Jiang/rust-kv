extern crate clap;

use clap::{App, SubCommand, Arg, ArgMatches};
use std::process;

fn main() {
    let app = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("set a KV")
                .arg(
                    Arg::with_name("KEY").required(true).index(1),
                )
                .arg(
                    Arg::with_name("VALUE").required(true).index(2),
                ),
        ).subcommand(
        SubCommand::with_name("get")
            .about("get value from key")
            .arg(
                Arg::with_name("KEY")
                    .help("The key you want to query.")
                    .required(true)
                    .index(1),
            ),
    ).subcommand(
        SubCommand::with_name("rm").about("Remove a given key").arg(
            Arg::with_name("KEY")
                .help("The key you want to remove.")
                .required(true)
                .index(1),
        ),
    )
        .get_matches();

    match app.subcommand() {
        ("set", Some(args)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("get", Some(args)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("rm", Some(args)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => unreachable!(),
    }
}