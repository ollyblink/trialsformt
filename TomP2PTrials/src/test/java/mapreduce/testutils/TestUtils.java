package mapreduce.testutils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import generictests.Example;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.PeerDHT;

public class TestUtils {
	public static IDHTConnectionProvider getTestConnectionProvider() {
		String bootstrapIP = "";
		int bootstrapPort = 4002;
		// DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		List<PeerDHT> peers = SyncedCollectionProvider.syncedArrayList();
		List<MRBroadcastHandler> bcHandlers = SyncedCollectionProvider.syncedArrayList();
		PeerDHT[] peerArray = null;
		try {
			peerArray = Example.createAndAttachPeersDHT(10, bootstrapPort, bcHandlers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Example.bootstrap(peerArray);
		Collections.addAll(peers, peerArray);

		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).externalPeers(peers, bcHandlers);
		return dhtConnectionProvider;
	}
}
