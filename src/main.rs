use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::Parser;
use rand::RngCore;

mod grpc;

#[derive(Parser, Debug)]
pub enum Opts {
    #[clap(about = "start a network latency test tcp forwarder")]
    TcpForwarder {
        #[clap(
            default_value = "127.0.0.1:8888",
            about = "the local socket address to listen"
        )]
        local_socket_addr: SocketAddr,
        #[clap(about = "the remote socket address to connect")]
        remote_socket_addr: SocketAddr,
        #[clap(
            short,
            long,
            default_value = "1048576",
            about = "maximum size of data allowed to receive"
        )]
        max_data_size: usize,
    },
    #[clap(about = "start a network latency test udp forwarder")]
    UdpForwarder {
        #[clap(
            default_value = "127.0.0.1:8888",
            about = "the local socket address to listen"
        )]
        local_socket_addr: SocketAddr,
        #[clap(about = "the remote socket address to connect")]
        remote_socket_addr: SocketAddr,
        #[clap(
            short,
            long,
            default_value = "65536",
            about = "maximum size of data allowed to receive"
        )]
        max_data_size: usize,
    },
    #[clap(about = "start a network latency tcp tester")]
    TcpTester {
        #[clap(
            default_value = "127.0.0.1:8888",
            about = "the local socket address to listen"
        )]
        local_socket_addr: SocketAddr,
        #[clap(about = "the remote socket address to connect")]
        remote_socket_addr: SocketAddr,
        #[clap(short, long, default_value = "1024", about = "the data size to send")]
        data_size: usize,
        #[clap(
            short,
            long,
            default_value = "1000",
            about = "the number of repetitions"
        )]
        repeat: usize,
    },

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
            default_value = "65536",
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
            default_value = "1000",
            about = "the number of repetitions"
        )]
        repeat: usize,
    },
    #[clap(about = "start as a udp worker")]
    UdpClient {
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
            default_value = "1000",
            about = "the number of repetitions"
        )]
        repeat: usize,
    },
}

fn start_tcp_forwarder(remote_addr: SocketAddr, local_addr: SocketAddr, max_data_size: usize) {
    let listener = TcpListener::bind(local_addr).unwrap();
    let remote_stream = Arc::new(Mutex::new(TcpStream::connect(remote_addr).unwrap()));

    fn handle_client(
        mut from_stream: TcpStream,
        to_stream: Arc<Mutex<TcpStream>>,
        max_data_size: usize,
    ) {
        let mut buf = vec![0u8; max_data_size];
        while let Ok(size) = from_stream.read(buf.as_mut_slice()) {
            let mut g = to_stream.lock().unwrap();
            g.write_all(&buf[..size]).unwrap();
            g.flush().unwrap();
        }
    }

    for stream in listener.incoming() {
        let remote = remote_stream.clone();
        std::thread::spawn(move || handle_client(stream.unwrap(), remote, max_data_size));
    }
}

fn start_tcp_tester(
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    data_size: usize,
    repeat: usize,
) {
    let listener = TcpListener::bind(local_addr).unwrap();

    let mut recv_stream = listener.incoming().next().unwrap().unwrap();
    let mut send_stream = loop {
        if let Ok(stream) = TcpStream::connect(remote_addr) {
            eprintln!("connected to {:?}", remote_addr);
            break stream;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        eprintln!("trying to connect {:?}", remote_addr);
    };

    let mut data = vec![0u8; data_size];
    let mut buf = vec![0u8; data_size];

    for _ in 0..repeat {
        rand::thread_rng().fill_bytes(data.as_mut_slice());
        let start = Instant::now();
        send_stream.write_all(data.as_slice()).unwrap();
        recv_stream.read_exact(buf.as_mut_slice()).unwrap();
        assert_eq!(data, buf);
        println!("{} us elapsed", start.elapsed().as_micros());
    }
}

fn start_udp_forwarder(remote_addr: SocketAddr, local_addr: SocketAddr, max_data_size: usize) {
    let socket = UdpSocket::bind(local_addr).unwrap();

    let mut buf = vec![0u8; max_data_size];
    while let Ok(size) = socket.recv(buf.as_mut()) {
        socket.send_to(&buf[..size], remote_addr).unwrap();
    }
}

fn start_tcp_server(addr: SocketAddr, max_data_size: usize) {
    let listener = TcpListener::bind(addr).unwrap();

    fn handle_client(mut stream: TcpStream, max_data_size: usize) {
        let mut buf = vec![0u8; max_data_size];
        while let Ok(size) = stream.read(buf.as_mut_slice()) {
            stream.write_all(&buf[..size]).unwrap();
            stream.flush().unwrap();
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
        stream.flush().unwrap();
        stream.read_exact(recv_data.as_mut_slice()).unwrap();
        assert_eq!(data, recv_data);
        println!("{} us elapsed", start.elapsed().as_micros());
    }
    stream.shutdown(Shutdown::Both).unwrap();
}

fn start_udp_client(local_addr: SocketAddr, data_size: usize, repeat: usize) {
    let socket = UdpSocket::bind(local_addr).unwrap();

    let mut data: Vec<u8> = vec![0; data_size];
    let mut recv_data: Vec<u8> = vec![0; data_size];

    for _ in 0..repeat {
        rand::thread_rng().fill_bytes(data.as_mut_slice());
        let start = Instant::now();
        socket.send(data.as_slice()).unwrap();
        socket.recv(recv_data.as_mut_slice()).unwrap();
        assert_eq!(data, recv_data);
        println!("{} us elapsed", start.elapsed().as_micros());
    }
}

fn main() {
    match Opts::parse() {
        Opts::TcpForwarder {
            local_socket_addr,
            remote_socket_addr,
            max_data_size,
        } => start_tcp_forwarder(remote_socket_addr, local_socket_addr, max_data_size),
        Opts::UdpForwarder {
            local_socket_addr,
            remote_socket_addr,
            max_data_size,
        } => start_udp_forwarder(remote_socket_addr, local_socket_addr, max_data_size),
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
            local_addr,
            data_size,
            repeat,
        } => start_udp_client(local_addr, data_size, repeat),
        Opts::TcpTester {
            local_socket_addr,
            remote_socket_addr,
            data_size,
            repeat,
        } => start_tcp_tester(remote_socket_addr, local_socket_addr, data_size, repeat),
    }
}
