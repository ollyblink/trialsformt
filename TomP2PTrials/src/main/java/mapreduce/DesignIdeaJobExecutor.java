package mapreduce;

import java.io.IOException;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.GetOwnIpAddressTest;
import obsolete.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) throws InterruptedException {
		try {
			GetOwnIpAddressTest.main(null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int id = Integer.parseInt(args[0]);
		String bootstrapIP = "192.168.43.65";
		int bootstrapPort = 4000;
		// String storageFilePath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/main/java/mapreduce/storage/";
		DHTConnectionProvider newInstance = DHTConnectionProvider.create(bootstrapIP, bootstrapPort);
		// long waitingTime = 0;
		if (id == 1) {
			newInstance.isBootstrapper(true);
		}
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(newInstance);

		jobExecutor.start();

	}
}
