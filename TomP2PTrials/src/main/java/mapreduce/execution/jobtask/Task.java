package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
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
	private Multimap<PeerAddress, JobStatus> executingPeers;

	private Task() {
		executingPeers = ArrayListMultimap.create();
	}

	public static Task newTask() {
		return new Task();
	}

	public String id() {
		return id;
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
					int lastIndexOfExecuting = jobStati.lastIndexOf(JobStatus.EXECUTING_TASK);
					if (lastIndexOfExecuting == -1) {// not yet executing this task
						jobStati.addLast(currentStatus);
						logger.info("if-if-if: " + peerAddress + " is now executing task " + id);
					} else {
						// Already executing this task. Nothing to do until it finished
						logger.warn("if-if-else: " + peerAddress + " is already executing task " + id + ". Update ignored.");
					}
				} else if (currentStatus == JobStatus.FINISHED_TASK) {
					int lastIndexOfExecuting = jobStati.lastIndexOf(JobStatus.EXECUTING_TASK);
					if (lastIndexOfExecuting == -1) {// FINISHED_TASK arrived multiple times after another without an EXECUTING_TASK
						// Something went wrong... Should first be Executing before it is Finished. Mark as Failed?
						logger.warn("if-elseif-if: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress
								+ " is already executing task " + id);
					} else { // It was executing and now it finished. This should be the case
						jobStati.removeLastOccurrence(JobStatus.EXECUTING_TASK);
						jobStati.addLast(currentStatus);
						logger.info("if-elseif-else: " + peerAddress + " finished executing task " + id);
					}
				} else {
					// Should never happen
					logger.warn("if-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
				}
			} else { // task was not yet assigned to this peer.
				if (currentStatus == JobStatus.EXECUTING_TASK) {
					jobStati.addLast(currentStatus);
					logger.info("else-if: " + peerAddress + " is now executing task " + id);
				} else if (currentStatus == JobStatus.FINISHED_TASK) {
					// Something went wrong, shouldnt get a finished job before it even started
					logger.warn("else-elseif: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress
							+ " is not yet executing task " + id);
				} else {
					// Should never happen
					logger.warn("else-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
				}
			}
			this.executingPeers.putAll(peerAddress, jobStati);
		}
		return this;
	}

	/**
	 * Check how many peers have the same status multiple times (e.g. FINISHED_TASK)
	 * 
	 * @param statusToCheck
	 *            <code>JobStatus</code> to check how many peers for this task are currently holding it
	 * @return number of peers that were assigned this task and currently hold the specified <code>JobStatus</code>
	 */
	public int numberOfPeersWithMultipleSameStati(JobStatus statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<JobStatus> stati = this.executingPeers.get(executingPeer);
				for (JobStatus status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfStatus;
					}
				}
				if (nrOfStatus > 1) {
					++nrOfPeers;
				}
			}
		}
		return nrOfPeers;
	}

	public int numberOfPeersWithSingleStatus(JobStatus statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<JobStatus> stati = this.executingPeers.get(executingPeer);
				for (JobStatus status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfStatus;
					}
				}
				if (nrOfStatus == 1) {
					++nrOfPeers;
				}
			}
		}
		return nrOfPeers;
	}

	public int numberOfPeersWithAtLeastOneFinishedExecution() {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<JobStatus> stati = this.executingPeers.get(executingPeer);
				for (JobStatus status : stati) {
					if (status.equals(JobStatus.FINISHED_TASK)) {
						++nrOfStatus;
						break;
					}
				}
				if (nrOfStatus == 1) {
					++nrOfPeers;
				}
			}
		}
		return nrOfPeers;
	}

	public int totalNumberOfFinishedExecutions() {
		return countTotalNumber(JobStatus.FINISHED_TASK);
	}
	
	public int totalNumberOfCurrentExecutions(){
		return countTotalNumber(JobStatus.EXECUTING_TASK);
	}

	private int countTotalNumber(JobStatus statusToCheck) {
		int nrOfFinishedExecutions = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				Collection<JobStatus> stati = this.executingPeers.get(executingPeer);
				for (JobStatus status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfFinishedExecutions;
					}
				}
			}
		}
		return nrOfFinishedExecutions;
	}
	 

	public int numberOfSameStatiForPeer(PeerAddress peerAddress, JobStatus statusToCheck) {
		int statiCount = 0;
		synchronized (executingPeers) {
			Collection<JobStatus> stati = this.executingPeers.get(peerAddress);
			for (JobStatus status : stati) {
				if (status.equals(statusToCheck)) {
					++statiCount;
				}
			}
		}
		return statiCount;
	}

	public int numberOfDifferentPeersExecutingTask() {
		synchronized (executingPeers) {
			return this.executingPeers.keySet().size();
		}
	}

	public Collection<JobStatus> statiForPeer(PeerAddress peerAddress) {
		synchronized (executingPeers) {
			return this.executingPeers.get(peerAddress);
		}
	}

	public Set<PeerAddress> allAssignedPeers() {
		synchronized (executingPeers) {
			return executingPeers.keySet();
		}
	}

	@Override
	public String toString() {
		return id;
	}

}
