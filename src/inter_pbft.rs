use hex;
use log::{debug, error, info, warn};
use protobuf::{self, Message as ProtobufMessage, RepeatedField};
use rand;
use rand::Rng;
use sawtooth_sdk::messages::consensus::*;
use sawtooth_sdk::messages::validator::{Message, Message_MessageType};
use serde_json;
use std::sync::mpsc::{Receiver, Sender};
use zmq;

fn generate_correlation_id() -> String {
    const LENGTH: usize = 16;
    rand::thread_rng().gen_ascii_chars().take(LENGTH).collect()
}

pub fn start_inter_loop(
    context: zmq::Context,
    peers_count: usize,
    my_peer_index: usize,
    p2p_tx: Sender<(Vec<u8>, Vec<u8>)>,
    pbft_rx: Receiver<Vec<u8>>,
) {
    let mut all_peers: Vec<[u8; 1]> = vec![];
    for i in 0..peers_count {
        all_peers.push([i as u8]);
    }
    let my_peer_id = all_peers[my_peer_index];

    let socket = context.socket(zmq::ROUTER).unwrap();
    socket
        .bind(&format!("tcp://127.0.0.1:{}", 5050 + my_peer_index))
        .unwrap();
    let mut pool_items = vec![socket.as_poll_item(zmq::POLLIN)];

    let mut pbft_identity_wrap: Option<Vec<u8>> = None;

    let mut current_block_id = vec![0, 0, 0, 0, 0];
    let mut current_block_number = 0;

    loop {
        zmq::poll(&mut pool_items, 1).unwrap();
        if !pool_items[0].is_readable() {
            if let Some(pbft_identity) = &pbft_identity_wrap {
                if let Ok(data) = pbft_rx.try_recv() {
                    socket
                        .send_multipart(vec![pbft_identity.clone(), data], 0)
                        .unwrap();
                }
            }
            continue;
        }

        let mut received_parts = socket.recv_multipart(0).unwrap();
        let msg_bytes = received_parts.pop().unwrap();
        let zmq_identity = received_parts.pop().unwrap();
        let message: Message = protobuf::parse_from_bytes(&msg_bytes).unwrap();

        pbft_identity_wrap = Some(zmq_identity.clone());

        match message.get_message_type() {
            Message_MessageType::CONSENSUS_REGISTER_REQUEST => {
                let request: ConsensusRegisterRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );
                let mut response = ConsensusRegisterResponse::new();
                response.set_status(ConsensusRegisterResponse_Status::OK);
                let mut chain_head = ConsensusBlock::new();
                chain_head.set_block_id(current_block_id.clone());
                chain_head.set_previous_id(vec![]);
                chain_head.set_signer_id(vec![1]);
                chain_head.set_block_num(current_block_number);
                chain_head.set_payload(vec![]);
                chain_head.set_summary(vec![]);
                response.set_chain_head(chain_head);

                let mut all_peers_field = RepeatedField::new();
                for peer in &all_peers {
                    if *peer == my_peer_id {
                        continue;
                    }

                    let mut cpi = ConsensusPeerInfo::new();
                    cpi.set_peer_id(peer.to_vec());
                    all_peers_field.push(cpi);
                }
                response.set_peers(all_peers_field);

                let mut local_peer_info = ConsensusPeerInfo::new();
                local_peer_info.set_peer_id(my_peer_id.to_vec());
                response.set_local_peer_info(local_peer_info);

                let response = response.write_to_bytes().unwrap();

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_REGISTER_RESPONSE);
                res_message.set_content(response);

                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_SETTINGS_GET_REQUEST => {
                let request: ConsensusSettingsGetRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut response = ConsensusSettingsGetResponse::new();
                response.set_status(ConsensusSettingsGetResponse_Status::OK);

                let all_peers_hex: Vec<String> = all_peers.iter().map(|x| hex::encode(x)).collect();

                let mut entries_field = RepeatedField::new();

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.members".into());
                entry.set_value(serde_json::to_string(&all_peers_hex).unwrap());
                entries_field.push(entry);

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.block_publishing_delay".into());
                entry.set_value("5000".into());
                entries_field.push(entry);

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.idle_timeout".into());
                entry.set_value("30000".into());
                entries_field.push(entry);

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.commit_timeout".into());
                entry.set_value("10000".into());
                entries_field.push(entry);

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.view_change_duration".into());
                entry.set_value("5000".into());
                entries_field.push(entry);

                let mut entry = ConsensusSettingsEntry::new();
                entry.set_key("sawtooth.consensus.pbft.forced_view_change_interval".into());
                entry.set_value("3".into());
                entries_field.push(entry);

                response.set_entries(entries_field);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_SETTINGS_GET_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_INITIALIZE_BLOCK_REQUEST => {
                let request: ConsensusInitializeBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut response = ConsensusInitializeBlockResponse::new();
                response.set_status(ConsensusInitializeBlockResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message
                    .set_message_type(Message_MessageType::CONSENSUS_INITIALIZE_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_SUMMARIZE_BLOCK_REQUEST => {
                let request: ConsensusSummarizeBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut response = ConsensusSummarizeBlockResponse::new();
                response.set_status(ConsensusSummarizeBlockResponse_Status::OK);
                response.set_summary(vec![]);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message
                    .set_message_type(Message_MessageType::CONSENSUS_SUMMARIZE_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_FINALIZE_BLOCK_REQUEST => {
                let request: ConsensusFinalizeBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut block_id = current_block_id.clone();
                for i in (0..block_id.len()).rev() {
                    if block_id[i] < 255 {
                        block_id[i] += 1;
                        break;
                    }
                }

                let mut response = ConsensusFinalizeBlockResponse::new();
                response.set_status(ConsensusFinalizeBlockResponse_Status::OK);
                response.set_block_id(block_id.clone());

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message
                    .set_message_type(Message_MessageType::CONSENSUS_FINALIZE_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(
                        vec![zmq_identity.clone(), res_message.write_to_bytes().unwrap()],
                        0,
                    )
                    .unwrap();

                // BlockNew notify
                let mut consensus_block = ConsensusBlock::new();
                consensus_block.set_block_id(block_id);
                consensus_block.set_previous_id(current_block_id.clone());
                consensus_block.set_signer_id(my_peer_id.to_vec());
                consensus_block.set_block_num(current_block_number + 1);
                consensus_block.set_payload(request.get_data().to_vec());
                consensus_block.set_summary(vec![]);
                let mut response = ConsensusNotifyBlockNew::new();
                response.set_block(consensus_block);

                let mut res_message = Message::new();
                res_message.set_correlation_id(generate_correlation_id());
                res_message.set_message_type(Message_MessageType::CONSENSUS_NOTIFY_BLOCK_NEW);
                res_message.set_content(response.write_to_bytes().unwrap());

                p2p_tx
                    .send((res_message.write_to_bytes().unwrap(), vec![]))
                    .unwrap();
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_CHECK_BLOCKS_REQUEST => {
                let request: ConsensusCheckBlocksRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut response = ConsensusCheckBlocksResponse::new();
                response.set_status(ConsensusCheckBlocksResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_CHECK_BLOCKS_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(
                        vec![zmq_identity.clone(), res_message.write_to_bytes().unwrap()],
                        0,
                    )
                    .unwrap();

                // BlockValid notify
                let mut response = ConsensusNotifyBlockValid::new();
                response.set_block_id(request.get_block_ids().get(0).unwrap().clone());

                let mut res_message = Message::new();
                res_message.set_correlation_id(generate_correlation_id());
                res_message.set_message_type(Message_MessageType::CONSENSUS_NOTIFY_BLOCK_VALID);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_BROADCAST_REQUEST => {
                let request: ConsensusBroadcastRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                // broadcast to all peer
                let mut peer_message = ConsensusPeerMessage::new();
                let mut peer_message_header = ConsensusPeerMessageHeader::new();
                peer_message_header.set_signer_id(my_peer_id.to_vec());
                peer_message_header.set_content_sha512(vec![1, 2, 3]);
                peer_message_header.set_message_type(request.get_message_type().to_string());
                peer_message_header.set_name("pbft".to_string());
                peer_message_header.set_version("1.0".to_string());
                peer_message.set_header(peer_message_header.write_to_bytes().unwrap());
                peer_message.set_header_signature(vec![1, 2, 3]);
                peer_message.set_content(request.get_content().to_vec());

                let mut notify_message = ConsensusNotifyPeerMessage::new();
                notify_message.set_message(peer_message);
                notify_message.set_sender_id(my_peer_id.to_vec());

                let mut p2p_message = Message::new();
                p2p_message.set_correlation_id(generate_correlation_id());
                p2p_message.set_message_type(Message_MessageType::CONSENSUS_NOTIFY_PEER_MESSAGE);
                p2p_message.set_content(notify_message.write_to_bytes().unwrap());
                p2p_tx
                    .send((p2p_message.write_to_bytes().unwrap(), vec![]))
                    .unwrap();

                let mut response = ConsensusBroadcastResponse::new();
                response.set_status(ConsensusBroadcastResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_BROADCAST_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_COMMIT_BLOCK_REQUEST => {
                let request: ConsensusCommitBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                let mut response = ConsensusCommitBlockResponse::new();
                response.set_status(ConsensusCommitBlockResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_COMMIT_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(
                        vec![zmq_identity.clone(), res_message.write_to_bytes().unwrap()],
                        0,
                    )
                    .unwrap();

                // notify CONSENSUS_NOTIFY_BLOCK_COMMIT
                let mut response = ConsensusNotifyBlockCommit::new();
                response.set_block_id(request.get_block_id().to_vec());

                let mut res_message = Message::new();
                res_message.set_correlation_id(generate_correlation_id());
                res_message.set_message_type(Message_MessageType::CONSENSUS_NOTIFY_BLOCK_COMMIT);
                res_message.set_content(response.write_to_bytes().unwrap());
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();

                current_block_id = request.get_block_id().to_vec();
                current_block_number += 1;
            }
            Message_MessageType::CONSENSUS_SEND_TO_REQUEST => {
                let request: ConsensusSendToRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );

                // send to peer
                let mut peer_message = ConsensusPeerMessage::new();
                let mut peer_message_header = ConsensusPeerMessageHeader::new();
                peer_message_header.set_signer_id(my_peer_id.to_vec());
                peer_message_header.set_content_sha512(vec![1, 2, 3]);
                peer_message_header.set_message_type(request.get_message_type().to_string());
                peer_message_header.set_name("pbft".to_string());
                peer_message_header.set_version("1.0".to_string());
                peer_message.set_header(peer_message_header.write_to_bytes().unwrap());
                peer_message.set_header_signature(vec![1, 2, 3]);
                peer_message.set_content(request.get_content().to_vec());

                let mut notify_message = ConsensusNotifyPeerMessage::new();
                notify_message.set_message(peer_message);
                notify_message.set_sender_id(my_peer_id.to_vec());

                let mut p2p_message = Message::new();
                p2p_message.set_correlation_id(generate_correlation_id());
                p2p_message.set_message_type(Message_MessageType::CONSENSUS_NOTIFY_PEER_MESSAGE);
                p2p_message.set_content(notify_message.write_to_bytes().unwrap());
                p2p_tx
                    .send((
                        p2p_message.write_to_bytes().unwrap(),
                        request.get_receiver_id().to_vec(),
                    ))
                    .unwrap();

                let mut response = ConsensusSendToResponse::new();
                response.set_status(ConsensusSendToResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_SEND_TO_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_CANCEL_BLOCK_REQUEST => {
                let request: ConsensusCancelBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );
                let mut response = ConsensusCancelBlockResponse::new();
                response.set_status(ConsensusCancelBlockResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_CANCEL_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_FAIL_BLOCK_REQUEST => {
                let request: ConsensusFailBlockRequest =
                    protobuf::parse_from_bytes(&message.get_content()).unwrap();
                debug!(
                    "get one request: {:?}, {:?}",
                    message.get_message_type(),
                    request
                );
                let mut response = ConsensusFailBlockResponse::new();
                response.set_status(ConsensusFailBlockResponse_Status::OK);

                let mut res_message = Message::new();
                res_message.set_correlation_id(message.get_correlation_id().into());
                res_message.set_message_type(Message_MessageType::CONSENSUS_FAIL_BLOCK_RESPONSE);
                let response = response.write_to_bytes().unwrap();
                res_message.set_content(response);
                socket
                    .send_multipart(vec![zmq_identity, res_message.write_to_bytes().unwrap()], 0)
                    .unwrap();
            }
            Message_MessageType::CONSENSUS_NOTIFY_ACK => {}
            _ => {
                error!(
                    "receive unknown message: {:?}, {}, {}",
                    message.get_message_type(),
                    message.get_correlation_id(),
                    message.get_content().len()
                );
            }
        }
    }
}
