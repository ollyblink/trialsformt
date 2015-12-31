package mapreduce.execution.task;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;
import static mapreduce.utils.SyncedCollectionProvider.syncedListMultimap;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ListMultimap;

import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;

public class Task implements Serializable, Comparable<Task> {

	// private static Logger logger = LoggerFactory.getLogger(Task.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;

	/** the task's key is also its ID */
	private Object key;
	private Tuple<String, Tuple<String, Integer>> jobProcedureDomain;
	// private String jobId;
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
	private List<Tuple<String, Integer>> rejectedExecutorTaskDomainParts;// String here: JobSubmitter/Executor id
	/**
	 * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	 * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	 */
	private List<Tuple<String, Integer>> finalExecutorTaskDomainParts;// String here: JobSubmitter/Executor id

	private volatile boolean isActive;

	private List<Tuple<String, Tuple<String, Integer>>> initialDataLocation;

	private Task(Object key, Tuple<String, Tuple<String, Integer>> jobProcedureDomain) {
		this.key = key;
		this.jobProcedureDomain = jobProcedureDomain;
		this.executingPeers = syncedListMultimap();
		this.taskResults = syncedListMultimap();
		this.reverseTaskResults = syncedListMultimap();
		this.finalExecutorTaskDomainParts = syncedArrayList();
		this.rejectedExecutorTaskDomainParts = syncedArrayList();
		this.initialDataLocation = syncedArrayList();
		this.isFinished = false;
	}

	public static Task create(Object key, Tuple<String, Tuple<String, Integer>> jobProcedureDomain) {
		return new Task(key, jobProcedureDomain);
	}

	public String id() {
		return key.toString();
	}

	public String jobId() {
		return jobProcedureDomain.first();
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public Task isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
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

	public List<Tuple<String, Integer>> finalExecutorTaskDomainParts() {
		return finalExecutorTaskDomainParts;
	}

	public Task addFinalExecutorTaskDomainPart(Tuple<String, Integer> t) {
		if (t != null) {
			this.finalExecutorTaskDomainParts.add(t);
		}
		return this;
	}

	public List<Tuple<String, Integer>> rejectedExecutorTaskDomainParts() {
		return this.rejectedExecutorTaskDomainParts;
	}

	public Task addRejectedExecutorTaskDomainPart(Tuple<String, Integer> t) {
		if (t != null) {
			this.rejectedExecutorTaskDomainParts.add(t);
		}
		return this;
	}

	/**
	 * Puts a new result hash
	 * 
	 * @param sender
	 * @param location
	 * @param resultHash
	 * @return returns the number of times this result hash has been achieved.
	 */
	public int updateResultHash(Tuple<String, Integer> t, Number160 resultHash) {
		String executorTaskDomain = DomainProvider.INSTANCE.executorTaskDomain(Tuple.create(key.toString(), t));
		taskResults.put(resultHash, executorTaskDomain);
		reverseTaskResults.put(executorTaskDomain, resultHash);
		return taskResults.get(resultHash).size();

	}

	public Number160 resultHash(Tuple<String, Integer> t) {
		String executorTaskDomain = DomainProvider.INSTANCE.executorTaskDomain(Tuple.create(key.toString(), t));
		List<Number160> resultHashs = reverseTaskResults.get(executorTaskDomain);
		if (resultHashs.size() > 0) {
			return resultHashs.get(0);
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		return "Task [key=" + key + ", jobId=" + jobProcedureDomain.first() + ", isFinished=" + isFinished + ", executingPeers=" + executingPeers
				+ ", taskResults=" + taskResults + ", reverseTaskResults=" + reverseTaskResults + ", removableTaskExecutorDomains="
				+ rejectedExecutorTaskDomainParts + ", finalTaskExecutorDomains=" + finalExecutorTaskDomainParts + ", isActive=" + isActive + "]";
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

	public Tuple<String, Tuple<String, Integer>> executorTaskDomain(Tuple<String, Integer> executorTaskDomainPart) {
		if (executorTaskDomainPart != null) {
			// if (executingPeers.containsKey(executorTaskDomainPart.first())) {
			// if (executingPeers.get(executorTaskDomainPart.first()).size() >= executorTaskDomainPart.second()) {
			return Tuple.create(key.toString(), executorTaskDomainPart);
			// }
			// }
		}
		return null;
	}

	public String executorTaskDomainString(Tuple<String, Integer> executorTaskDomainPart) {
		Tuple<String, Tuple<String, Integer>> executorTaskDomain = executorTaskDomain(executorTaskDomainPart);
		if (executorTaskDomain != null) {
			return DomainProvider.INSTANCE.executorTaskDomain(executorTaskDomain);
		}
		return null;
	}

	public String concatenationString(Tuple<String, Integer> executorTaskDomainPart) {
		Tuple<String, Tuple<String, Integer>> executorTaskDomain = executorTaskDomain(executorTaskDomainPart);
		if (jobProcedureDomain != null && executorTaskDomain != null) {
			return DomainProvider.INSTANCE.concatenation(jobProcedureDomain, executorTaskDomain);
		}
		return null;
	}

	public void addInitialExecutorTaskDomain(Tuple<String, Tuple<String, Integer>> initialDataLocation) {
		this.initialDataLocation.add(initialDataLocation);

	}

	public List<Tuple<String, Tuple<String, Integer>>> initialExecutorTaskDomain() {
		return this.initialDataLocation;
	}
}
