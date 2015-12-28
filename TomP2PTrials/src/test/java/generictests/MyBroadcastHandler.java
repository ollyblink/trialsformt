package generictests;
import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MyBroadcastHandler extends StructuredBroadcastHandler {

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		return super.receive(message);
	}

}
