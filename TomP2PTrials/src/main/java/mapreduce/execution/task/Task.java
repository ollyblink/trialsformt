package mapreduce.execution.task;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;
import static mapreduce.utils.SyncedCollectionProvider.syncedListMultimap;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ListMultimap;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import net.tomp2p.peers.Number160;

public class Task implements Serializable, Comparable<Task> {

	// private static Logger logger = LoggerFactory.getLogger(Task.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;

	/** the task's key is also its ID */
	private Object key;
	private String jobId;
	private boolean isFinished;

	private ListMultimap<String, BCMessageStatus> executingPeers; // String here: JobSubmitter/Executor id
	private ListMultimap<Number160, String> taskResults; // String here: JobSubmitter/Executor id +"_"+locationIndex
	private ListMultimap<String, Number160> reverseTaskResults;// String here: JobSubmitter/Executor id +"_"+locationIndex

	// /**
	// * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	// * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	// */
	// private Tuple<PeerAddress, Integer> finalDataLocation;
	/**
	 * Rejected data locations that need to be removed
	 */
	private List<String> removableTaskExecutorDomains;// String here: JobSubmitter/Executor id +"_"+locationIndex
	/**
	 * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	 * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	 */
	private List<String> finalTaskExecutorDomains;// String here: JobSubmitter/Executor id +"_"+locationIndex

	private boolean isActive;

	private Task(Object key, String jobId) {
		this.key = key;
		this.jobId = jobId;
		this.executingPeers = syncedListMultimap();
		this.taskResults = syncedListMultimap();
		this.reverseTaskResults = syncedListMultimap();
		this.finalTaskExecutorDomains = syncedArrayList();
		this.removableTaskExecutorDomains = syncedArrayList();
		this.isFinished = false;
	}

	public static Task newInstance(Object key, String jobId) {
		return new Task(key, jobId);
	}

	public String id() {
		return key.toString();
	}

	public String jobId() {
		return jobId;
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public Task isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
	}

	public List<String> removableTaskExecutorDomains() {
		return this.removableTaskExecutorDomains;
	}

	public ListMultimap<String, BCMessageStatus> executingPeers() {
		return this.executingPeers;
	}

	public ListMultimap<Number160, String> taskResults() {
		return taskResults;
	}

	public ListMultimap<String, Number160> reverseTaskResults() {
		return reverseTaskResults;
	}

	public List<String> finalDataLocationDomains() {
		return finalTaskExecutorDomains;
	}

	public Task finalDataLocationDomains(String... finalTaskExecutorDomains) {
		Collections.addAll(this.finalTaskExecutorDomains, finalTaskExecutorDomains);
		return this;
	}

 

	@Override
	public String toString() {
		return "Task [key=" + key + ", jobId=" + jobId + ", isFinished=" + isFinished + ", executingPeers=" + executingPeers + ", taskResults="
				+ taskResults + ", reverseTaskResults=" + reverseTaskResults + ", removableTaskExecutorDomains=" + removableTaskExecutorDomains
				+ ", finalTaskExecutorDomains=" + finalTaskExecutorDomains + ", isActive=" + isActive + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id() == null) ? 0 : id().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		// if (getClass() != obj.getClass())
		// return false;
		Task other = (Task) obj;
		if (id() == null) {
			if (other.id() != null)
				return false;
		} else if (!id().equals(other.id()))
			return false;
		return true;
	}

	@Override
	public int compareTo(Task o) {
		return this.id().compareTo(o.id().toString());
	}

	public void isActive(boolean isActive) {
		this.isActive = isActive;
	}

	public boolean isActive() {
		return this.isActive;
	}
}
