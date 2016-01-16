package mapreduce.testutils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import generictests.Example;
import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.PeerDHT;

public class TestUtils {
	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, IMessageConsumer messageConsumer) {
		return getTestConnectionProvider(port, nrOfPeers, true, null, messageConsumer);

	}

	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, PeerDHT master, IMessageConsumer messageConsumer) {
		return getTestConnectionProvider(port, nrOfPeers, true, master, messageConsumer);

	}

	public static IDHTConnectionProvider getTestConnectionProvider(int port, int nrOfPeers, boolean hasBCHandler, PeerDHT master, IMessageConsumer messageConsumer) {
		String bootstrapIP = "";
		int bootstrapPort = port;
		// DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		List<PeerDHT> peers = SyncedCollectionProvider.syncedArrayList();
		PeerDHT[] peerArray = null;
		JobCalculationBroadcastHandler bcHandler = JobCalculationBroadcastHandler.create(1);
		if(messageConsumer != null){
			bcHandler.messageConsumer(messageConsumer);
		}
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

		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.create(bootstrapIP, bootstrapPort).externalPeers(peers, bcHandler);
		return dhtConnectionProvider;
	}
}
