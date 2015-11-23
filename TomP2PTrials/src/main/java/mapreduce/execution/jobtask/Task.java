package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import net.tomp2p.peers.PeerAddress;

public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;
	private static Logger logger = LoggerFactory.getLogger(Task.class);

	private String id;
	private String jobId;
	private IMapReduceProcedure<?, ?, ?, ?> procedure;
	private List<?> keys;
	private int procedureIndex;
	private Multimap<PeerAddress, JobStatus> executingPeers;

	private Task() {
		executingPeers = ArrayListMultimap.create();
	}

	public static Task newTask() {
		return new Task();
	}

	public String id() {
		return procedureIndex + "_" + id;
	}

	public String jobId() {
		return jobId;
	}

	public IMapReduceProcedure<?, ?, ?, ?> procedure() {
		return this.procedure;
	}

	public List<?> keys() {
		return this.keys;
	}

	public Task keys(List<?> keys) {
		this.keys = Collections.synchronizedList(keys);
		return this;
	}

	public Task id(String id) {
		this.id = id;
		return this;
	}

	public Task jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	/**
	 * 
	 * @param procedure
	 * @param procedureIndex
	 *            specifies which procedure in the queue it is, used for task id
	 * @return
	 */
	public Task procedure(IMapReduceProcedure<?, ?, ?, ?> procedure) {
		this.procedure = procedure;
		return this;
	}

	public Task updateExecutingPeerStatus(PeerAddress peerAddress, JobStatus currentStatus) {
		synchronized (executingPeers) {

			LinkedList<JobStatus> jobStati = new LinkedList<JobStatus>(this.executingPeers.removeAll(peerAddress));
			if (jobStati.size() > 0) {
				if (currentStatus == JobStatus.EXECUTING_TASK) {
					jobStati.addLast(currentStatus);
				} else if (currentStatus == JobStatus.FINISHED_TASK) {
					jobStati.removeLastOccurrence(JobStatus.EXECUTING_TASK);
					jobStati.addLast(currentStatus);
				}
			} else {
				jobStati.addLast(currentStatus);
			}

			this.executingPeers.putAll(peerAddress, jobStati);
			logger.warn("Task::updateExecutingPeerStatus(): " + executingPeers.values());
		}
		return this;
	}

	/**
	 * Check how many peers are currently executing, finished, or failed to execute this task by specifying the status to check as an argument
	 * 
	 * @param statusToCheck
	 *            <code>JobStatus</code> to check how many peers for this task are currently holding it
	 * @return number of peers that were assigned this task and currently hold the specified <code>JobStatus</code>
	 */
	public int numberOfPeersWithStatus(JobStatus statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				Collection<JobStatus> stati = this.executingPeers.get(executingPeer);
				for (JobStatus status : stati) {
					if (status.equals(statusToCheck)) {

						++nrOfPeers;
					}
				}
			}
		}
		return nrOfPeers;
	}

	public int numberOfAssignedPeers() {
		synchronized (executingPeers) {
			return this.executingPeers.keySet().size();
		}
	}

	public Collection<JobStatus> statusForPeer(PeerAddress peerAddress) {
		synchronized (executingPeers) {
			return this.executingPeers.get(peerAddress);
		}
	}

	public Multimap<PeerAddress, JobStatus> get() {
		synchronized (executingPeers) {
			return this.executingPeers;
		}
	}

	@Override
	public String toString() {
		return "Task [executingPeers=" + executingPeers + "]";
	}

}
