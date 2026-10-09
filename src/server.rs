use std::net::{IpAddr, SocketAddr, Shutdown};
use std::result;
use std::io::{Read, Write};
use std::fmt;
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, Duration};
use std::str;
use getrandom::getrandom;
use std::fmt::Write as OtherWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Interest, Token, Events};

type Result<T> = result::Result<T, ()>;

const PORT: u16 = 6969;
const SAFE_MODE: bool = false;
const BAN_LIMIT: Duration = Duration::from_secs(10*60);
const MESSAGE_RATE: Duration = Duration::from_secs(1);
const SLOWLORIS_LIMIT: Duration = Duration::from_millis(200);
const STRIKE_LIMIT: usize = 10;
// NOTE: the token, a space and the name must fit into a single read buffer of client_read()
const NAME_LIMIT: usize = 24;
const HISTORY_FILE_PATH: &str = "./HISTORY";
// How many of the most recent messages are kept in memory and sent to the newly authorized clients
const HISTORY_LIMIT: usize = 100;

struct Sens<T>(T);

impl<T: fmt::Display> fmt::Display for Sens<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(inner) = self;
        if SAFE_MODE {
            "[REDACTED]".fmt(f)
        } else {
            inner.fmt(f)
        }
    }
}

struct Client {
    conn: TcpStream,
    last_message: SystemTime,
    connected_at: SystemTime,
    authed: bool,
    name: String,
    addr: SocketAddr,
}

enum Sinner {
    Striked(usize),
    Banned(SystemTime),
}

impl Sinner {
    fn new() -> Self {
        Self::Striked(0)
    }

    fn forgive(&mut self) {
        *self = Self::Striked(0)
    }

    fn strike(&mut self) -> bool {
        match self {
            Self::Striked(x) => {
                if *x >= STRIKE_LIMIT {
                    *self = Self::Banned(SystemTime::now());
                    true
                } else {
                    *x += 1;
                    false
                }
            }
            Self::Banned(_) => true,
        }
    }
}

struct History {
    messages: VecDeque<String>,
    file: File,
}

impl History {
    fn open(file_path: &str) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).create(true).open(file_path).map_err(|err| {
            eprintln!("ERROR: could not open chat history file {file_path}: {err}");
        })?;
        let mut messages = VecDeque::new();
        for line in BufReader::new(&file).lines() {
            let line = line.map_err(|err| {
                eprintln!("ERROR: could not read chat history file {file_path}: {err}");
            })?;
            if messages.len() >= HISTORY_LIMIT {
                messages.pop_front();
            }
            messages.push_back(line);
        }
        Ok(Self { messages, file })
    }

    fn push(&mut self, message: String) {
        // NOTE: the whole line is written in one go, so a message never ends up half-saved next to another one
        let _ = self.file.write_all(format!("{message}\n").as_bytes()).map_err(|err| {
            eprintln!("ERROR: could not save message to the chat history: {err}");
        });
        if self.messages.len() >= HISTORY_LIMIT {
            self.messages.pop_front();
        }
        self.messages.push_back(message);
    }
}

struct Server {
    clients: HashMap<Token, Client>,
    sinners: HashMap<IpAddr, Sinner>,
    token: String,
    history: History,
}

impl Server {
    fn new(token: String, history: History) -> Self {
        Self {
            clients: HashMap::new(),
            sinners: HashMap::new(),
            token,
            history,
        }
    }

    fn client_connected(&mut self, mut author: TcpStream, author_addr: SocketAddr, token: Token) {
        let now = SystemTime::now();

        if let Some(sinner) = self.sinners.get_mut(&author_addr.ip()) {
            match sinner {
                Sinner::Banned(banned_at) => {
                    let diff = now.duration_since(*banned_at).unwrap_or_else(|err| {
                        eprintln!("ERROR: ban time check on client connection: the clock might have gone backwards: {err}");
                        Duration::ZERO
                    });
                    if diff < BAN_LIMIT {
                        let secs = (BAN_LIMIT - diff).as_secs_f32();
                        // TODO: probably remove this logging, cause banned MFs may still keep connecting and overflow us with logs
                        println!("INFO: Client {author_addr} tried to connected, but that MF is banned for {secs} secs", author_addr = Sens(author_addr));
                        let _ = writeln!(author, "You are banned MF: {secs} secs left").map_err(|err| {
                            eprintln!("ERROR: could not send banned message to {author_addr}: {err}", author_addr = Sens(author_addr), err = Sens(err));
                        });
                        let _ = author.shutdown(Shutdown::Both).map_err(|err| {
                            eprintln!("ERROR: could not shutdown socket for {author_addr}: {err}", author_addr = Sens(author_addr), err = Sens(err));
                        });
                        return;
                    } else {
                        sinner.forgive()
                    }
                }
                Sinner::Striked(_) => {}
            }
        }

        println!("INFO: Client {author_addr} connected", author_addr = Sens(author_addr));
        self.clients.insert(token, Client {
            conn: author,
            last_message: now - 2*MESSAGE_RATE,
            connected_at: now,
            authed: false,
            name: String::new(),
            addr: author_addr,
        });
    }

    fn client_read(&mut self, token: Token) {
        if let Some(author) = self.clients.get_mut(&token) {
            let author_addr: SocketAddr = author.addr.clone();
            let mut buffer = [0; 64];
            let bytes: Vec<_> = match author.conn.read(&mut buffer) {
                Ok(0) => {
                    // TODO: we need to distinguish between willful client disconnects and banned disconnects
                    // Banned Sinners may try to use this to fill up all the space on the hard drive
                    println!("INFO: Client {author_addr} disconnected", author_addr = Sens(author_addr));
                    // TODO: if the disconnected client was not authorized we may probably want to strike their
                    // IP, because they are probably constantly connecting/disconnecting trying to evade the
                    // strike.
                    self.clients.remove(&token);
                    return;
                }
                Ok(n) => buffer[0..n].iter().cloned().filter(|x| *x >= 32).collect(),
                Err(err) => {
                    if err.kind() != io::ErrorKind::WouldBlock {
                        eprintln!("ERROR: could not read message from {author_addr}: {err}", author_addr = Sens(author_addr), err = Sens(err));
                        self.clients.remove(&token);
                    }
                    return;
                }
            };

            let now = SystemTime::now();
            let diff = now.duration_since(author.last_message).unwrap_or_else(|err| {
                eprintln!("ERROR: message rate check on new message: the clock might have gone backwards: {err}");
                Duration::from_secs(0)
            });
            if diff < MESSAGE_RATE {
                self.strike_ip(author_addr.ip());
                return;
            }
            let text = if let Ok(text) = str::from_utf8(&bytes) {
                text
            } else {
                return
            };
            self.sinners.entry(author_addr.ip()).or_insert(Sinner::new()).forgive();
            author.last_message = now;
            if author.authed {
                let message = format!("{name}: {text}", name = author.name);
                println!("INFO: Client {author_addr} sent message {bytes:?}", author_addr = Sens(author_addr));
                for (client_token, client) in self.clients.iter_mut() {
                    if *client_token != token && client.authed {
                        let _ = writeln!(client.conn, "{message}").map_err(|err| {
                            eprintln!("ERROR: could not broadcast message to all the clients from {author_addr}: {err}", author_addr = Sens(author_addr), err = Sens(err))
                        });
                    }
                }
                self.history.push(message);
            } else {
                self.client_authorize(token, text);
            }
        }
    }

    fn name_error(&self, name: &str) -> Option<String> {
        if name.is_empty() || name.len() > NAME_LIMIT || !name.chars().all(|x| x.is_ascii_alphanumeric() || x == '_' || x == '-') {
            Some(format!("Invalid name! It must be 1-{NAME_LIMIT} characters long and contain only letters, digits, `_` or `-`"))
        } else if self.clients.values().any(|client| client.authed && client.name == name) {
            Some(format!("Name {name} is already taken!"))
        } else {
            None
        }
    }

    // The first message of the client is expected to be `<token> <name>`
    fn client_authorize(&mut self, token: Token, text: &str) {
        let (access_token, name) = text.split_once(' ').unwrap_or((text, ""));
        let name_error = self.name_error(name);
        if let Some(author) = self.clients.get_mut(&token) {
            let author_addr: SocketAddr = author.addr.clone();
            if access_token != self.token {
                // TODO: let the user know that they were banned after this attempt
                println!("INFO: {} failed authorization!", Sens(author_addr));
                let _ = writeln!(author.conn, "Invalid token! Bruh!").map_err(|err| {
                    eprintln!("ERROR: could not notify client {} about invalid token: {}", Sens(author_addr), Sens(err));
                });
                let _ = author.conn.shutdown(Shutdown::Both).map_err(|err| {
                    eprintln!("ERROR: could not shutdown {}: {}", Sens(author_addr), Sens(err));
                });
                self.clients.remove(&token);
                // TODO: each IP strike must be properly documented in the source code giving the reasoning
                // behind it.
                self.strike_ip(author_addr.ip());
                return;
            }

            // NOTE: a bad name is not striked, because only the clients that know the token get that far
            if let Some(name_error) = name_error {
                println!("INFO: {} picked a bad name: {name_error}", Sens(author_addr));
                let _ = writeln!(author.conn, "{name_error}").map_err(|err| {
                    eprintln!("ERROR: could not notify client {} about bad name: {}", Sens(author_addr), Sens(err));
                });
                let _ = author.conn.shutdown(Shutdown::Both).map_err(|err| {
                    eprintln!("ERROR: could not shutdown {}: {}", Sens(author_addr), Sens(err));
                });
                self.clients.remove(&token);
                return;
            }

            author.authed = true;
            author.name = name.to_string();
            println!("INFO: {} authorized as {name}!", Sens(author_addr));
            let mut greeting = String::new();
            for message in self.history.messages.iter() {
                let _ = writeln!(&mut greeting, "{message}");
            }
            let _ = writeln!(&mut greeting, "Welcome to the Club {name}!");
            let _ = author.conn.write_all(greeting.as_bytes()).map_err(|err| {
                eprintln!("ERROR: could not send welcome message to {}: {}", Sens(author_addr), Sens(err));
            });
        }
    }

    fn strike_ip(&mut self, ip: IpAddr) {
        let sinner = self.sinners.entry(ip).or_insert(Sinner::new());
        if sinner.strike() {
            println!("INFO: IP {ip} got banned", ip = Sens(ip));
            self.clients.retain(|_token, client| {
                let addr: SocketAddr = client.addr.clone();
                if addr.ip() == ip {
                    let _ = writeln!(client.conn, "You are banned Sinner!").map_err(|err| {
                        eprintln!("ERROR: could not send banned message to {addr}: {err}", addr = Sens(addr), err = Sens(err));
                    });
                    let _ = client.conn.shutdown(Shutdown::Both).map_err(|err| {
                        eprintln!("ERROR: could not shutdown socket for {addr}: {err}", addr = Sens(addr), err = Sens(err));
                    });
                    return false
                }
                true
            });
        }
    }

    fn update(&mut self, token: Token) {
        self.client_read(token);

        // TODO: keep waiting connections in a separate hash map
        self.clients.retain(|_, client| {
            let addr: SocketAddr = client.addr.clone();
            if !client.authed {
                let now = SystemTime::now();
                let diff = now.duration_since(client.connected_at).unwrap_or_else(|err| {
                    eprintln!("ERROR: slowloris time limit check: the clock might have gone backwards: {err}");
                    SLOWLORIS_LIMIT
                });
                if diff >= SLOWLORIS_LIMIT {
                    // TODO: disconnect everyone from addr.ip()
                    self.sinners.entry(addr.ip()).or_insert(Sinner::new()).strike();
                    let _ = client.conn.shutdown(Shutdown::Both).map_err(|err| {
                        eprintln!("ERROR: could not shutdown socket for {addr}: {err}", addr = Sens(addr), err = Sens(err));
                    });
                    return false;
                }
            }
            true
        });

    }
}

fn generate_token() -> Result<String> {
    let mut buffer = [0; 16];
    let _ = getrandom(&mut buffer).map_err(|err| {
        eprintln!("ERROR: could not generate random access token: {err}");
    })?;

    let mut token = String::new();
    for x in buffer.iter() {
        let _ = write!(&mut token, "{x:02X}");
    }
    Ok(token)
}

fn main() -> Result<()> {
    let token = generate_token()?;
    let token_file_path = "./TOKEN";
    fs::write(token_file_path, token.as_bytes()).map_err(|err| {
        eprintln!("ERROR: could not create token file {token_file_path}: {err}");
    })?;

    println!("INFO: check {token_file_path} file for the token");
    let history = History::open(HISTORY_FILE_PATH)?;
    println!("INFO: chat history is saved to {HISTORY_FILE_PATH}, loaded {n} recent messages", n = history.messages.len());
    let address = format!("0.0.0.0:{PORT}");
    let mut listener = TcpListener::bind(address.parse().unwrap()).map_err(|err| {
        eprintln!("ERROR: could not bind {address}: {err}", address = Sens(&address), err = Sens(err))
    })?;
    let mut poll = Poll::new().map_err(|err| {
        eprintln!("ERROR: could not create Poll object: {err}");
    })?;
    let mut events = Events::with_capacity(1024);
    let mut counter = 0;

    poll.registry().register(&mut listener, Token(counter), Interest::READABLE).map_err(|err| {
        eprintln!("ERROR: Could not register server socket in the Poll object: {err}")
    })?;

    let mut server = Server::new(token, history);

    println!("INFO: listening to {}", Sens(address));
    loop {
        if let Err(err) = poll.poll(&mut events, None) {
            eprintln!("ERROR: Failed to poll: {err}");
            continue;
        }
        for token in events.iter().map(|e| e.token()) {
            match token {
                Token(0) => match listener.accept() {
                    Ok((mut stream, author_addr)) => {
                        counter += 1;
                        let token = Token(counter);
                        match poll.registry().register(&mut stream, token, Interest::READABLE) {
                            Ok(_) => server.client_connected(stream, author_addr, token),
                            Err(err) => eprintln!("ERROR: could not register client socket in the Poll object: {err}"),
                        }
                    }
                    Err(err) => if err.kind() != io::ErrorKind::WouldBlock {
                        eprintln!("ERROR: could not accept connection: {err}")
                    }
                },
                token => server.update(token),
            }
        }
    }
}
