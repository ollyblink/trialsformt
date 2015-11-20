package firstdesignidea.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.execution.broadcasthandler.broadcastobserver.IBroadcastListener;
import firstdesignidea.execution.datasplitting.ITaskSplitter;
import firstdesignidea.execution.datasplitting.MaxFileSizeTaskSplitter;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.storage.DHTConnectionProvider;

public class MRJobSubmitter implements IBroadcastListener {
	private static final ITaskSplitter DEFAULT_TASK_SPLITTER = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
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
		dhtConnectionProvider().connect();
		logger.warn("Connected.");
		// Split into specified file sizes
		taskSplitter().split(job);
		logger.warn("Splitted tasks.");

		ExecutorService server = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (final Task task : job.tasks()) {
			server.submit(new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub

					for (Object key : task.keys()) {
						try {
							String filePath = (String) key;
							BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
							String line = null;
							String lines = "";
							while ((line = reader.readLine()) != null) {
								lines += line + "\n";
							}
							reader.close();
							dhtConnectionProvider.addDataForTask(task.id(), filePath, lines);
							logger.warn("Added file with path " + filePath);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			});

		}
		server.shutdown();
		while(!server.isTerminated()){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		dhtConnectionProvider.broadcastNewJob(job);
		logger.info("broadcased job");
	}

	public MRJobSubmitter dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		dhtConnectionProvider.broadcastDistributor().broadcastListener(this);
		return this;
	}

	public DHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobSubmitter taskSplitter(ITaskSplitter taskSplitter) {
		this.taskSplitter = taskSplitter;
		return this;
	}

	public ITaskSplitter taskSplitter() {
		if (this.taskSplitter == null) {
			this.taskSplitter = DEFAULT_TASK_SPLITTER;
		}
		return this.taskSplitter;
	}

	@Override
	public void inform(IBCMessage bcMessage) {
		System.out.println("MRJobSubmitter received BC message with status: " + bcMessage.status());
	}

}
