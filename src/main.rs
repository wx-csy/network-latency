use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::thread::Thread;
use std::time::Instant;

use clap::Parser;
use rand::RngCore;

#[derive(Parser, Debug)]
pub enum Opts {
    #[clap(about = "start a network latency test tcp server")]
    TcpServer {
        #[clap(
            default_value = "127.0.0.1:8888",
            about = "the local socket address to listen"
        )]
        socket_addr: SocketAddr,
        #[clap(
            short,
            long,
            default_value = "1048576",
            about = "maximum size of data allowed to receive"
        )]
        max_data_size: usize,
    },
    #[clap(about = "start a network latency test udp server")]
    UdpServer {
        #[clap(
            default_value = "127.0.0.1:8888",
            about = "the local socket address to listen"
        )]
        socket_addr: SocketAddr,
        #[clap(
            short,
            long,
            default_value = "1048576",
            about = "maximum size of data allowed to receive"
        )]
        max_data_size: usize,
    },
    #[clap(about = "start as a tcp worker")]
    TcpClient {
        #[clap(about = "the remote socket address to connect")]
        socket_addr: SocketAddr,
        #[clap(short, long, default_value = "1024", about = "the data size to send")]
        data_size: usize,
        #[clap(
            short,
            long,
            default_value = "100",
            about = "the number of repetitions"
        )]
        repeat: usize,
    },
    #[clap(about = "start as a udp worker")]
    UdpClient {
        #[clap(about = "the remote socket address to connect")]
        remote_addr: SocketAddr,
        #[clap(
            default_value = "127.0.0.1:9999",
            about = "the local socket address to connect"
        )]
        local_addr: SocketAddr,
        #[clap(short, long, default_value = "1024", about = "the data size to send")]
        data_size: usize,
        #[clap(
            short,
            long,
            default_value = "100",
            about = "the number of repetitions"
        )]
        repeat: usize,
    },
}

fn start_tcp_server(addr: SocketAddr, max_data_size: usize) {
    let listener = TcpListener::bind(addr).unwrap();

    fn handle_client(mut stream: TcpStream, max_data_size: usize) {
        let mut buf = vec![0u8; max_data_size];
        while let Ok(size) = stream.read(buf.as_mut_slice()) {
            stream.write_all(&buf[..size]).unwrap();
        }
    }

    for stream in listener.incoming() {
        std::thread::spawn(move || handle_client(stream.unwrap(), max_data_size));
    }
}

fn start_udp_server(addr: SocketAddr, max_data_size: usize) {
    let socket = UdpSocket::bind(addr).unwrap();

    let mut buf = vec![0u8; max_data_size];
    while let Ok((size, peer_addr)) = socket.recv_from(buf.as_mut()) {
        socket.send_to(&buf[..size], peer_addr).unwrap();
    }
}

fn start_tcp_client(addr: SocketAddr, data_size: usize, repeat: usize) {
    let mut stream = TcpStream::connect(addr).unwrap();

    let mut data: Vec<u8> = vec![0; data_size];
    let mut recv_data: Vec<u8> = vec![0; data_size];

    for _ in 0..repeat {
        rand::thread_rng().fill_bytes(data.as_mut_slice());
        let start = Instant::now();
        stream.write_all(data.as_slice()).unwrap();
        stream.read_exact(recv_data.as_mut_slice()).unwrap();
        assert_eq!(data, recv_data);
        eprintln!("{} us elapsed", start.elapsed().as_micros());
    }
    stream.shutdown(Shutdown::Both).unwrap();
}

fn start_udp_client(
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    data_size: usize,
    repeat: usize,
) {
    let socket = UdpSocket::bind(local_addr).unwrap();
    socket.connect(remote_addr).unwrap();

    let mut data: Vec<u8> = vec![0; data_size];
    let mut recv_data: Vec<u8> = vec![0; data_size];

    for _ in 0..repeat {
        rand::thread_rng().fill_bytes(data.as_mut_slice());
        let start = Instant::now();
        socket.send(data.as_slice()).unwrap();
        socket.recv(recv_data.as_mut_slice()).unwrap();
        assert_eq!(data, recv_data);
        eprintln!("{} us elapsed", start.elapsed().as_micros());
    }
}

fn main() {
    match Opts::parse() {
        Opts::TcpServer {
            socket_addr,
            max_data_size,
        } => start_tcp_server(socket_addr, max_data_size),
        Opts::UdpServer {
            socket_addr,
            max_data_size,
        } => start_udp_server(socket_addr, max_data_size),
        Opts::TcpClient {
            socket_addr,
            data_size,
            repeat,
        } => start_tcp_client(socket_addr, data_size, repeat),
        Opts::UdpClient {
            remote_addr,
            local_addr,
            data_size,
            repeat,
        } => start_udp_client(remote_addr, local_addr, data_size, repeat),
    }
}
