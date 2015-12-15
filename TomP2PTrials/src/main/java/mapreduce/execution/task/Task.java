package mapreduce.execution.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Task implements Serializable, Comparable<Task> {

	// private static Logger logger = LoggerFactory.getLogger(Task.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;

	private String id;
	private String jobId;
	private boolean isFinished;

	private ListMultimap<PeerAddress, BCMessageStatus> executingPeers;
	private ListMultimap<Number160, Tuple<PeerAddress, Integer>> taskResults;
	private ListMultimap<Tuple<PeerAddress, Integer>, Number160> reverseTaskResults;

	// // CONSIDER ONLY STORING THEIR HASH REPRESENTATION FOR THE DOMAIN AND OTHER KEYS
	// /** Data location to retrieve the data from for this task */
	// private Tuple<PeerAddress, Integer> initialDataLocation;
	/**
	 * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	 * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	 */
	private Tuple<PeerAddress, Integer> finalDataLocation;
	private List<Tuple<PeerAddress, Integer>> dataToRemove;

	private Task(String id, String jobId) {
		this.id = id;
		this.jobId = jobId;
		ArrayListMultimap<PeerAddress, BCMessageStatus> tmp = ArrayListMultimap.create();
		this.executingPeers = Multimaps.synchronizedListMultimap(tmp);
		ArrayListMultimap<Number160, Tuple<PeerAddress, Integer>> tmp2 = ArrayListMultimap.create();
		this.taskResults = Multimaps.synchronizedListMultimap(tmp2);
		ArrayListMultimap<Tuple<PeerAddress, Integer>, Number160> tmp3 = ArrayListMultimap.create();
		this.reverseTaskResults = Multimaps.synchronizedListMultimap(tmp3);
		this.isFinished = false;
		this.finalDataLocation = null;
		this.dataToRemove = Collections.synchronizedList(new ArrayList<>());
	}

	public static Task newInstance(String id, String jobId) {
		return new Task(id, jobId);
	}

	public String id() {
		return id;
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

	public List<Tuple<PeerAddress, Integer>> dataToRemove() {
		return this.dataToRemove;
	}

	public ListMultimap<PeerAddress, BCMessageStatus> executingPeers() {
		return this.executingPeers;
	}

	public ListMultimap<Number160, Tuple<PeerAddress, Integer>> taskResults() {
		return taskResults;
	}

	public ListMultimap<Tuple<PeerAddress, Integer>, Number160> reverseTaskResults() {
		return reverseTaskResults;
	}

	public Task finalDataLocation(Tuple<PeerAddress, Integer> finalDataLocation) {
		this.finalDataLocation = finalDataLocation;
		return this;
	}

	public Tuple<PeerAddress, Integer> finalDataLocation() {
		return finalDataLocation;
	}

	@Override
	public String toString() {
		return "Task [id=" + id + ", jobId=" + jobId + ", isFinished=" + isFinished + ", executingPeers=" + executingPeers + ", taskResults="
				+ taskResults + ", reverseTaskResults=" + reverseTaskResults + ", finalDataLocation=" + finalDataLocation + ", dataToRemove="
				+ dataToRemove + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		if (id == null) {
			if (other.id() != null)
				return false;
		} else if (!id.equals(other.id()))
			return false;
		return true;
	}

	@Override
	public int compareTo(Task o) {
		return this.id.compareTo(o.id);
	}

}
