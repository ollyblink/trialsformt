package firstdesignidea;

import java.io.IOException;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.server.MRJobExecutor;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.utils.GetOwnIpAddressTest;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) {
		int maxNrOfFinishedPeers = 3;
		try {
			GetOwnIpAddressTest.main(null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int id = 2;
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider().broadcastDistributor(new MRBroadcastHandler());

		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		} else {
			dhtConnectionProvider.port(bootstrapPort);
		}
		MRJobExecutor jobExecutor = MRJobExecutor.newJobExecutor(dhtConnectionProvider).maxNrOfFinishedPeers(maxNrOfFinishedPeers);
		jobExecutor.start();
	}
}
