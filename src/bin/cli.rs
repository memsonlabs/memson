use futures::{SinkExt, TryFutureExt};
use memson::json::Json;
use memson::Error;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

/// Entry point for CLI tool.
///
/// The `[tokio::main]` annotation signals that the Tokio runtime should be
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
///
/// `basic_scheduler` is used here to avoid spawning background threads. The CLI
/// tool use case benefits more by being lighter instead of multi-threaded.
#[tokio::main(basic_scheduler)]
async fn main() -> memson::Result<()> {
    // Parse command line arguments
    //let cli = Cli::from_args();
    let host = "127.0.0.1";
    let port = "8080";

    // Get the remote address to connect to
    let addr = format!("{}:{}", host, port);

    let socket = TcpStream::connect(addr).map_err(|_| Error::BadIO).await?;

    // Initialize the connection state. This allocates read/write buffers to
    // perform redis protocol frame parsing.

    let mut lines = Framed::new(socket, LinesCodec::new());
    // Establish a connection
    println!("\n🚀 connecting to memson on {}:{}", host, port);

    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline("memson> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                lines.send(&line).map_err(|_| Error::BadIO).await?;
                if let Some(result) = lines.next().await {
                    match result {
                        Ok(line) => {
                            let val: Json = serde_json::from_str(&line).unwrap();
                            println!("{}", serde_json::to_string_pretty(&val).unwrap());
                        }
                        Err(e) => {
                            println!("error on decoding from socket; error = {:?}", e);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt").unwrap();
    /*
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut input = String::new();
    loop {
        print!("{}", Paint::green("memson> "));
        stdout.flush().map_err(|_| memson::Error::BadIO)?;
        if let Err(err) = stdin.read_line(&mut input) {
            println!("error: {}", err);
            continue;
        };
        if &input == "exit" {
            break;
        }
        input.remove(input.len() - 1);
        lines.send(&input).map_err(|_| Error::BadIO).await?;
        println!("line sent");
        input.clear();
        if let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    println!("{}", line);
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {:?}", e);
                }
            }
        }


    }
    */

    Ok(())
}
