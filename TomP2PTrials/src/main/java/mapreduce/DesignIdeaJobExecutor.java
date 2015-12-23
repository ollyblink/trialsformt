package mapreduce;

import java.io.IOException;

import mapreduce.execution.computation.context.PseudoStorageContext;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.utils.GetOwnIpAddressTest;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) throws InterruptedException {
		try {
			GetOwnIpAddressTest.main(null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int id = Integer.parseInt(args[0]);
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		// String storageFilePath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/main/java/mapreduce/storage/";
		DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		// long waitingTime = 0;
		if (id == 1) {
			dhtUtils.isBootstrapper(true);
		}
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.newInstance(DHTConnectionProvider.newInstance(dhtUtils))
				.context(PseudoStorageContext.newInstance()).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance());

		jobExecutor.start();

	}
}
