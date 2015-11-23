package mapreduce.execution.broadcasthandler;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.MessageConsumer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MRBroadcastHandler.class);

	private BlockingQueue<IBCMessage> bcMessages;

	public MRBroadcastHandler() {
	}

	public MRBroadcastHandler broadcastListener() {
		return this;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		logger.warn("Received message");
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				bcMessages.add(bcMessage);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	public void queue(BlockingQueue<IBCMessage> bcMessages) {
		this.bcMessages = bcMessages;
	}

}
