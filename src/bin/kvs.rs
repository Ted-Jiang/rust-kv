extern crate clap;
extern crate env_logger;
extern crate simple_logger;

use clap::{App, SubCommand, Arg, ArgMatches, AppSettings};
use std::process;

use kvs::{KvStore, Result, KvsError};
use std::env::current_dir;
use std::process::exit;

fn main() -> Result<()>{
    env_logger::init();

    let app = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
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
            let key = args.value_of("KEY").unwrap();
            let value = args.value_of("VALUE").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(args)) => {
            let key = args.value_of("KEY").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            if let Some(val) = store.get(key.to_string())? {
                println!("{}", val);
            }else {
                println!("Key not found");
            }
        }
        ("rm", Some(args)) => {
            let key = args.value_of("KEY").unwrap();
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(_) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e)
            }
        }
        _ => unreachable!(),
    }

    // main must end with Result
    Ok(())
}