package firstdesignidea.execution.broadcasthandler;

import java.io.IOException;
import java.util.NavigableMap;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.storage.IBroadcastDistributor;
import firstdesignidea.storage.IBroadcastListener;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler implements IBroadcastDistributor {

	private IBroadcastListener broadcastListener;

	public MRBroadcastHandler() {
	}

	public MRBroadcastHandler broadcastListener(IBroadcastListener broadcastListener) {
		this.broadcastListener = broadcastListener;
		return this;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				if(this.broadcastListener != null){
					this.broadcastListener.inform(bcMessage);
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // to get from message
		return super.receive(message);
	}

}
