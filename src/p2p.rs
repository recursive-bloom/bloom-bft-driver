use log::{debug, error, info, warn};
use sawtooth_sdk::messages::consensus::*;
use sawtooth_sdk::messages::validator::{Message, Message_MessageType};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use uuid;
use zmq;

pub fn start_server(
    context: zmq::Context,
    peers_count: usize,
    my_peer_index: usize,
    p2p_rx: Receiver<(Vec<u8>, Vec<u8>)>,
    pbft_tx: Sender<Vec<u8>>,
) {
    let mut all_peers: Vec<[u8; 1]> = vec![];
    for i in 0..peers_count {
        all_peers.push([i as u8]);
    }
    let my_peer_id = all_peers[my_peer_index];

    // listen socket
    let socket = context.socket(zmq::ROUTER).unwrap();
    socket
        .bind(&format!("tcp://127.0.0.1:{}", 6050 + my_peer_index))
        .unwrap();

    // out connect
    let mut out_sockets = vec![];
    for i in my_peer_index + 1..peers_count {
        let out_socket = context.socket(zmq::DEALER).unwrap();
        out_socket.set_identity(&[my_peer_index as u8]).unwrap();

        debug!("connect to {}", i);
        out_socket
            .connect(&format!("tcp://127.0.0.1:{}", 6050 + i))
            .unwrap();
        out_socket.send("connect", 0).unwrap();
        out_sockets.push(out_socket);
    }
    debug!("start_server finish");
    let mut inner_identities = vec![];

    // pool sockets
    let mut pool_items = vec![socket.as_poll_item(zmq::POLLIN)];
    for s in &out_sockets {
        pool_items.push(s.as_poll_item(zmq::POLLIN));
    }

    loop {
        zmq::poll(&mut pool_items, 1).unwrap();
        for i in 0..pool_items.len() {
            if pool_items[i].is_readable() {
                let s;
                if i == 0 {
                    s = &socket;
                } else {
                    s = &out_sockets[i - 1];
                }

                let mut received_parts = s.recv_multipart(0).unwrap();
                let msg_bytes = received_parts.pop().unwrap();
                let zmq_identity;
                if i == 0 {
                    zmq_identity = received_parts.pop().unwrap();
                } else {
                    zmq_identity = vec![(my_peer_index + i) as u8]
                }

                if msg_bytes == b"connect" {
                    if !inner_identities.contains(&zmq_identity) {
                        inner_identities.push(zmq_identity.clone());
                        debug!("connect from: {:?}", zmq_identity);
                    }
                } else {
                    let message: Message = protobuf::parse_from_bytes(&msg_bytes).unwrap();
                    if message.get_message_type()
                        == Message_MessageType::CONSENSUS_NOTIFY_PEER_MESSAGE
                    {
                        let notify_message: ConsensusNotifyPeerMessage =
                            protobuf::parse_from_bytes(message.get_content()).unwrap();
                        let peer_message_header: ConsensusPeerMessageHeader =
                            protobuf::parse_from_bytes(notify_message.get_message().get_header())
                                .unwrap();
                        let message_type = peer_message_header.get_message_type();
                        debug!(
                            "receive p2p message {:?} to pbft from {:?}",
                            message_type, zmq_identity,
                        );
                    } else {
                        debug!(
                            "receive p2p message {:?} to pbft from {:?}",
                            message.get_message_type(),
                            zmq_identity,
                        );
                    }
                    pbft_tx.send(msg_bytes).unwrap();
                }
            }
        }

        // send out data
        match p2p_rx.try_recv() {
            Ok(p2p_data) => {
                let (data, receiver_id) = p2p_data;
                let message: Message = protobuf::parse_from_bytes(&data).unwrap();

                let message_type;
                if message.get_message_type() == Message_MessageType::CONSENSUS_NOTIFY_PEER_MESSAGE
                {
                    let notify_message: ConsensusNotifyPeerMessage =
                        protobuf::parse_from_bytes(message.get_content()).unwrap();
                    let peer_message_header: ConsensusPeerMessageHeader =
                        protobuf::parse_from_bytes(notify_message.get_message().get_header())
                            .unwrap();
                    message_type = peer_message_header.get_message_type().into();
                } else {
                    message_type = format!("{:?}", message.get_message_type());
                }

                if !receiver_id.is_empty() {
                    let receiver_index = receiver_id[0] as usize;
                    if receiver_index < my_peer_index {
                        debug!("send p2p message {:?} to: {:?}", message_type, receiver_id);
                        socket
                            .send_multipart(vec![receiver_id, data.to_vec()], 0)
                            .unwrap();
                    } else {
                        let s = &out_sockets[receiver_index - my_peer_index - 1];
                        debug!("send p2p message {:?} to: {:?}", message_type, receiver_id);
                        s.send(data.clone(), 0).unwrap();
                    }
                    continue;
                }

                for (i, s) in out_sockets.iter().enumerate() {
                    debug!(
                        "send p2p message {:?} to: {:?}",
                        message_type,
                        vec![(my_peer_index + i + 1) as u8]
                    );
                    s.send(data.clone(), 0).unwrap();
                }
                for identity in &inner_identities {
                    debug!(
                        "send p2p message {:?} to: {:?}",
                        message_type,
                        identity.to_vec()
                    );
                    socket
                        .send_multipart(vec![identity.to_vec(), data.to_vec()], 0)
                        .unwrap();
                }
            }
            Err(TryRecvError::Empty) => (),
            Err(err) => debug!("p2p_rx error: {}", err),
        }
    }
}
