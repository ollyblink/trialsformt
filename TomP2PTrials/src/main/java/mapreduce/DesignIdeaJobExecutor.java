package mapreduce;

import java.io.IOException;

import mapreduce.execution.broadcasthandler.MRBroadcastHandler;
import mapreduce.server.MRJobExecutor;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.GetOwnIpAddressTest;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) { 
		try {
			GetOwnIpAddressTest.main(null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int id = 2;
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider() ;

		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		} else {
			dhtConnectionProvider.port(bootstrapPort);
		}
		MRJobExecutor jobExecutor = MRJobExecutor.newJobExecutor(dhtConnectionProvider) ;
		jobExecutor.start();
	}
}
