package mapreduce.manager.broadcasthandler;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {
	 private static Logger logger = LoggerFactory.getLogger(MRBroadcastHandler.class);

	private String owner;
	private BlockingQueue<IBCMessage> bcMessages;

	private MRBroadcastHandler() {

	}

	public static MRBroadcastHandler create() {
		return new MRBroadcastHandler();
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) { 
	 
		if (owner == null) {
			logger.info("Owner not set! call owner(String owner)");
		}
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
//			logger.info("Received message with data : " + dataMap);
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
//				logger.info("Message: "+ bcMessage);
				if (owner != null && !bcMessage.sender().equals(owner) && !bcMessages.contains(bcMessage)) {
//					logger.info("Added message");
					bcMessages.add(bcMessage);
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	public MRBroadcastHandler queue(BlockingQueue<IBCMessage> bcMessages) {
		this.bcMessages = bcMessages;
		return this;
	}

	public MRBroadcastHandler owner(String owner) { 
		this.owner = owner;
		return this;
	}
}
