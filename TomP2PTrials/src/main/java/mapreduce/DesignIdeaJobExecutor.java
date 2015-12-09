package mapreduce;

import java.io.IOException;

import mapreduce.execution.computation.context.PrintContext;
import mapreduce.execution.computation.context.WaitingContext;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.storage.DHTConnectionProvider;
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
		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance();
		// long waitingTime = 0;
		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		} else {
			dhtConnectionProvider.port(bootstrapPort);
			// waitingTime = 1;
		}
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.newInstance(dhtConnectionProvider)
				// .context(WaitingContext.newInstance().shouldPrint(false).waitingTime(waitingTime))
				.taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance());

		jobExecutor.start();

	}
}
