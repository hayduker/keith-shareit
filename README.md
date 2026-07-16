# Keith ShareIt

A peer-to-peer media sharing library designed to facilitate selective file transfer between a desktop (running an interactive TUI) and a phone (logging information).

## Features

* **Peer-to-Peer Connectivity**: Connections between devices are peer-to-peer, thanks to the Iroh ecosystem. By default, `keith-shareit` supports automatic peer discovery and connection using mDNS when devices are on the same LAN. Iroh's ticketing system can also be used to connections across broader networks.
* **Desktop TUI**: On the desktop (the media sender), you will be presented with TUI with an interactive file tree for selective choosing and sending files or directories, as well as a log pane.
* **Phone Logger**: On the phone, the application will simply run passively and log its state as files are transferred.

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