package mapreduce.execution.computation.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.jobtask.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;

public class DHTStorageContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;

	private Task task;

	private boolean awaitOnAdd;

	private DHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
	}

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (task == null) {
			logger.warn("No task id set!");
			System.err.println("DHTStorageContext::write::No task id set! Default task id used.");
			return;
		}

		dhtConnectionProvider.addTaskData(task, keyOut, valueOut, awaitOnAdd);
	}

	public DHTStorageContext task(Task task) {
		this.task = task;
		return this;
	}

	public DHTStorageContext awaitOnAdd(boolean awaitOnAdd) {
		this.awaitOnAdd = awaitOnAdd;
		return this;
	}

	public DHTStorageContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public static IContext newDHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		return new DHTStorageContext(dhtConnectionProvider);
	}

}
