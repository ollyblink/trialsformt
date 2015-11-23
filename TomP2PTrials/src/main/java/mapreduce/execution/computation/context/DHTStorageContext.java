package mapreduce.execution.computation.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.IDHTConnectionProvider;

public class DHTStorageContext implements IContext {
	private static final String DEFAULT_TASK_ID = "0_0_0_0_0_0_0_0_0_0_0_0_0_0_0_0";

	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private String taskId;

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (taskId == null) {
			this.taskId = DEFAULT_TASK_ID;
			logger.warn("DHTStorageContext::write::No task id set! Default task id used.");
		}
		dhtConnectionProvider.addDataForTask(taskId, keyOut, valueOut);
	}

	public DHTStorageContext taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

}
