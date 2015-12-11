package mapreduce.execution.computation.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.task.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import net.tomp2p.peers.Number160;

public class DHTStorageContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;

	private Task task;

	// private ITaskResultComparator taskResultComparator;

	/**
	 * 
	 * @param dhtConnectionProvider
	 * @param taskResultComparator
	 *            may add certain speed ups such that the task result comparison afterwards becomes faster
	 */
	private DHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
	}

	public static IContext newDHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		return new DHTStorageContext(dhtConnectionProvider);
	}

	// public DHTStorageContext taskResultComparator(ITaskResultComparator taskResultComparator) {
	// this.taskResultComparator = taskResultComparator;
	// return this;
	// }

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (task == null) {
			logger.warn("No task id set!");
			System.err.println("DHTStorageContext::write::No task id set! Default task id used.");
			return;
		}

		this.dhtConnectionProvider.addTaskData(task, keyOut, valueOut);
		// if (this.taskResultComparator != null) {
		// taskResultComparator.enableSpeedUp(this.dhtConnectionProvider, task, keyOut, valueOut, awaitOnAdd);
		// }
	}

	public DHTStorageContext task(Task task) {
		this.task = task;
		return this;
	}

	public DHTStorageContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
