# Keith ShareIt

A peer-to-peer media sharing library designed to facilitate selective file transfer between a desktop (running an interactive TUI) and a phone (logging information).

## Features

* **Peer-to-Peer Connectivity**: Leverages the Iroh network stack for secure and efficient direct connections.
* **Desktop TUI**: An interactive terminal user interface for browsing files and initiating transfers from the desktop.
* **Phone Logger**: On the phone, the application will simply run passively and log its state as files are transferred.
* **Selective File Transfer**: Easily copy specific files from your desktop to your phone.

## Installation

To build the project, ensure you have the Rust toolchain installed, then run:

```bash
cargo build
```

To run the desktop TUI:

```bash
cargo run -- send <path-to-src-dir>
```

To run the phone logger (I use Termux with a Rust environment installed):

```bash
cargo run -- recv -d <path-to-dst-dir>
```

### Using Nix

If you have Nix and direnv installed, you can simply enter the project directory to get a temporary development environment.

## API Documentation

API documentation for the Rust crates can be generated using `cargo doc`:

```bash
cargo doc --open
```