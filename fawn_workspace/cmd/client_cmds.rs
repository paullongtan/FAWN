use clap::{Command, Arg};

pub fn app_commands() -> Vec<Command> {
    vec![
        Command::new("put")
            .about("Store a key-value pair")
            .arg(Arg::new("key").required(true))
            .arg(Arg::new("value").required(true)),

        Command::new("get")
            .about("Retrieve the value for a key")
            .arg(Arg::new("key").required(true)),

        Command::new("delete")
            .about("Delete a key")
            .arg(Arg::new("key").required(true)),
    ]
}
