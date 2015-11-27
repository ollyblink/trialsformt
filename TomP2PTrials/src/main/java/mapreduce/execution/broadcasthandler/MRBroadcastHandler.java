package mapreduce.execution.broadcasthandler;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MRBroadcastHandler.class);

	private BlockingQueue<IBCMessage> bcMessages;
	private PeerAddress owner;

	public MRBroadcastHandler() {
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				bcMessages.add(bcMessage);
				logger.info("Received message " + bcMessage.status() + " for task " + bcMessage.sender().inetAddress() + ":"
						+ bcMessage.sender().tcpPort() + ", " + bcMessage);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	public MRBroadcastHandler queue(BlockingQueue<IBCMessage> bcMessages) {
		this.bcMessages = bcMessages;
		return this;
	}

	public MRBroadcastHandler owner(PeerAddress owner) {
		this.owner = owner;
		return this;

	}

	@Override
	public MRBroadcastHandler init(Peer peer) {
		this.owner = peer.peerAddress();
		return (MRBroadcastHandler) super.init(peer);
	}

	public MRBroadcastHandler broadcastListener() {
		return this;
	}
}
