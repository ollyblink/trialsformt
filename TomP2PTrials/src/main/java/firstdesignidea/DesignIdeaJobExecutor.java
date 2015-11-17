package firstdesignidea;

import firstdesignidea.server.MRJobExecutor;
import firstdesignidea.storage.DHTConnectionProvider;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) {
		int id = 1;
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4001;
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider();

		if (id != 1) {
			dhtConnectionProvider = dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		}
		MRJobExecutor jobExecutor = MRJobExecutor.newJobExecutor().dhtConnectionProvider(dhtConnectionProvider);
		jobExecutor.start();
	}
}
