package mapreduce;

import java.io.IOException;

import mapreduce.execution.computation.context.PrintContext;
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
		int id = Integer.parseInt(args[0]);
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		String storageFilePath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/main/java/mapreduce/storage/";
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider();

		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		} else {
			dhtConnectionProvider.port(bootstrapPort);
		}
		MRJobExecutor jobExecutor = MRJobExecutor.newJobExecutor(dhtConnectionProvider).context(PrintContext.newPrintContext()).canExecute(true);
		jobExecutor.start();
	}
}
