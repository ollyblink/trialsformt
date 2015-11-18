package firstdesignidea;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.server.MRJobExecutor;
import firstdesignidea.storage.DHTConnectionProvider;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) {
		int id = 2;
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider().broadcastDistributor(new MRBroadcastHandler());

		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		}else{
			dhtConnectionProvider.port(bootstrapPort);
		}
		MRJobExecutor jobExecutor = MRJobExecutor.newJobExecutor().dhtConnectionProvider(dhtConnectionProvider);
		jobExecutor.start();
	}
}
