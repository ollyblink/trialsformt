package firstdesignidea.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.execution.scheduling.ITaskSplitter;
import firstdesignidea.storage.DHTConnectionProvider;

public class MRJobSubmitter implements IJobManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmitter.class);
	private DHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;

	private MRJobSubmitter() {
	}

	public static MRJobSubmitter newMapReduceJobSubmitter() {
		return new MRJobSubmitter();
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {

		dhtConnectionProvider.connect();
		List<Task> tasks = taskSplitter.split(job);

		for (Task task : tasks) {
			dhtConnectionProvider.addTask(job.id(), task);
		}
	}

	@Override
	public MRJobSubmitter dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.dhtConnectionProvider.jobManager(this);
		return this;
	}

	@Override
	public DHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

}
