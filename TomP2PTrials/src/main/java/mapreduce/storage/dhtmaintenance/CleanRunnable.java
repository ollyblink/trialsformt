package mapreduce.storage.dhtmaintenance;

import mapreduce.execution.task.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public final class CleanRunnable implements Runnable {

	private Task task;
	private Tuple<PeerAddress, Integer> location;
	private DHTConnectionProvider dhtConnection;

	private CleanRunnable(String bootstrapIP, int bootstrapPort) {
		this.dhtConnection = DHTConnectionProvider.newInstance(DHTUtils.newInstance(bootstrapIP, bootstrapPort)).connect();

	}

	public static CleanRunnable newInstance(String bootstrapIP, int bootstrapPort) {
		return new CleanRunnable(bootstrapIP, bootstrapPort);
	}

	public CleanRunnable dataToRemove(Task task, Tuple<PeerAddress, Integer> location) {
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
