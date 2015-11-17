package firstdesignidea.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.storage.DHTConnectionProvider;

public class MRJobExecutor implements IJobManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);
	private DHTConnectionProvider dhtConnectionProvider;

	private MRJobExecutor() {

	}

	public static MRJobExecutor newJobExecutor() {
		return new MRJobExecutor();
	}

	public MRJobExecutor dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.dhtConnectionProvider.jobManager(this);
		return this;
	}

	public DHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public void start() {
		dhtConnectionProvider.connect();
	}

}
