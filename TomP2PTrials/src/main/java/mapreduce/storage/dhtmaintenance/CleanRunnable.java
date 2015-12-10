package mapreduce.storage.dhtmaintenance;

import java.util.List;

import mapreduce.execution.task.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.LocationBean;

public final class CleanRunnable implements Runnable {

	private Task task;
	private LocationBean location;
	private DHTConnectionProvider dhtConnection;

	private CleanRunnable(String bootstrapIP, int bootstrapPort) {
		this.dhtConnection = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).connect();
		
	}

	public static CleanRunnable newInstance(String bootstrapIP, int bootstrapPort) {
		return new CleanRunnable(bootstrapIP, bootstrapPort);
	}

	public CleanRunnable dataToRemove(Task task, LocationBean location) {
		this.task = task;
		this.location = location;
		return this;
	}

	@Override
	public void run() {
		if (task != null && location != null) {
			dhtConnection.removeTaskResultsFor(task, location);
		}
	}

	public void shutdown() {

		this.dhtConnection.shutdown();
	}
}
