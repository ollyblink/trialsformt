package mapreduce.manager.broadcasthandler.messageconsumer;

import org.junit.Test;

import mapreduce.execution.computation.context.DHTStorageContext;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;

public class MRJobExecutorManagerTest {

	@Test
	public void test() throws InterruptedException {
		String bootstrapIP = "192.168.43.65";
		int bootstrapPort = 4000;
		DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		DHTConnectionProvider newInstance = DHTConnectionProvider.newInstance(dhtUtils);
		Thread.sleep(3000);
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.newInstance(newInstance).context(DHTStorageContext.create(newInstance))
				.taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance());
		jobExecutor.start(); 
	}

}
