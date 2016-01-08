package mapreduce.testutils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import generictests.Example;
import mapreduce.engine.broadcasting.MRBroadcastHandler;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.PeerDHT;

public class TestUtils {
	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers) {
		return getTestConnectionProvider(port, nrOfPeers, true, null);

	}

	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, PeerDHT master) {
		return getTestConnectionProvider(port, nrOfPeers, true, master);

	}

	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, boolean hasBCHandler, PeerDHT master) {
		String bootstrapIP = "";
		int bootstrapPort = port;
		// DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		List<PeerDHT> peers = SyncedCollectionProvider.syncedArrayList();
		PeerDHT[] peerArray = null;
		MRBroadcastHandler bcHandler = MRBroadcastHandler.create();
		if (!hasBCHandler) {
			bcHandler = null;
		}
		try {
			peerArray = Example.createAndAttachPeersDHT(nrOfPeers, bootstrapPort, bcHandler, master);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Example.bootstrap(peerArray);
		Collections.addAll(peers, peerArray);

		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).externalPeers(peers, bcHandler);
		return dhtConnectionProvider;
	}
}
