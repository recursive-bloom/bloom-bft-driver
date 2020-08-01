use env_logger;
use log::{debug, error, info, warn};
use std::env;
use std::thread;

mod inter_pbft;
mod p2p;

fn main() {
    env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();

    let peers_count = args.get(1).unwrap().parse().unwrap();
    let my_peer_index = args.get(2).unwrap().parse().unwrap();

    info!(
        "peers_count: {}, my_peer_index: {}",
        peers_count, my_peer_index
    );

    let (p2p_tx, p2p_rx) = std::sync::mpsc::channel();
    let (pbft_tx, pbft_rx) = std::sync::mpsc::channel();

    let context = zmq::Context::new();

    let context_clone = context.clone();
    let pbft_t = thread::spawn(move || {
        inter_pbft::start_inter_loop(context_clone, peers_count, my_peer_index, p2p_tx, pbft_rx);
    });
    let context_clone = context.clone();
    let p2p_t = thread::spawn(move || {
        p2p::start_server(context_clone, peers_count, my_peer_index, p2p_rx, pbft_tx);
    });

    pbft_t.join().unwrap();
    p2p_t.join().unwrap();
}
