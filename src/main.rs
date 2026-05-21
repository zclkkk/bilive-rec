#[allow(dead_code)]
mod bilibili;
mod cli;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod error;
#[allow(dead_code)]
mod pipeline;
#[allow(dead_code)]
mod recorder;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod uploader;

use clap::Parser;

use cli::{Cli, Command, StateAction};

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Login => {
            println!("not implemented: login");
        }
        Command::Check { room_url } => {
            println!("not implemented: check {room_url}");
        }
        Command::Record { room_url } => {
            println!("not implemented: record {room_url}");
        }
        Command::Upload { files } => {
            println!("not implemented: upload {:?}", files);
        }
        Command::Run { config } => {
            println!("not implemented: run --config {}", config.display());
        }
        Command::State { action } => match action {
            StateAction::Inspect => {
                println!("not implemented: state inspect");
            }
            StateAction::Recover => {
                println!("not implemented: state recover");
            }
        },
    }
}
