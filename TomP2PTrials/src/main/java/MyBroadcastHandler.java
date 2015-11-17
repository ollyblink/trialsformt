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

		System.out.println(message.peerSocketAddresses());
		System.out.println("Called BroadCast handler");
		for (DataMap maps : message.dataMapList()) {
			NavigableMap<Number640, Data> dataMap = maps.dataMap();
			for (Number640 nr : dataMap.keySet()) {
				try {
					System.out.println("Received: " + dataMap.get(nr).object());
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch sblock
					e.printStackTrace();
				}
			}
		}

		return super.receive(message);
	}

}
