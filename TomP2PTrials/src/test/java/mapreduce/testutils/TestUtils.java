package mapreduce.testutils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import generictests.Example;
import mapreduce.manager.broadcasting.MRBroadcastHandler;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.PeerDHT;

public class TestUtils {
	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, Boolean... hasBCHandler) {
		String bootstrapIP = "";
		int bootstrapPort = port;
		// DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		List<PeerDHT> peers = SyncedCollectionProvider.syncedArrayList();
		PeerDHT[] peerArray = null;
		MRBroadcastHandler bcHandler = MRBroadcastHandler.create();
		if (hasBCHandler != null && hasBCHandler.length == 1 && !hasBCHandler[0]) {
			bcHandler = null;
		}
//		System.err.println(bcHandler);
		try {
			peerArray = Example.createAndAttachPeersDHT(nrOfPeers, bootstrapPort, bcHandler);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Example.bootstrap(peerArray);
		Collections.addAll(peers, peerArray);

		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).externalPeers(peers, bcHandler);
		return dhtConnectionProvider;
	}
}
