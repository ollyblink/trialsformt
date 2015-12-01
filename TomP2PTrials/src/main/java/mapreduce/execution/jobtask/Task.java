package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class Task implements Serializable, Comparable<Task> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;
	private static Logger logger = LoggerFactory.getLogger(Task.class);

	private String id;
	private String jobId;
	private IMapReduceProcedure procedure;
	private Multimap<PeerAddress, BCStatusType> executingPeers;
	private Multimap<PeerAddress, BCStatusType> comparingPeers;

	private boolean isFinished;
	private int maxNrOfFinishedWorkers;

	// CONSIDER ONLY STORING THERE HASH REPRESENTATION FOR THE DOMAIN AND OTHER KEYS
	/** This address is the one from the multi map that remains after evaluation of which task result to keep (if there are multiple task results) */
	private PeerAddress dataLocationHashPeerAddress;
	/** this index is the location in the executingPeers multimap above of the JobStatus that is chosen to be kept */
	private int dataLocationHashJobStatusIndex;

	private Task(String jobId) {
		Multimap<PeerAddress, BCStatusType> tmp = ArrayListMultimap.create();
		this.executingPeers = Multimaps.synchronizedMultimap(tmp);
		tmp = ArrayListMultimap.create();
		this.comparingPeers = Multimaps.synchronizedMultimap(tmp);
		this.jobId = jobId;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName()) + "_" + jobId;
	}

	public static Task newInstance(String jobId) {
		return new Task(jobId);
	}

	public String id() {
		return id;
	}

	public String jobId() {
		return jobId;
	}

	public IMapReduceProcedure procedure() {
		return this.procedure;
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public Task isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
	}

	public int maxNrOfFinishedWorkers() {
		if (this.maxNrOfFinishedWorkers == 0) {
			this.maxNrOfFinishedWorkers = 1;
		}
		return maxNrOfFinishedWorkers;
	}

	public Task maxNrOfFinishedWorkers(int maxNrOfFinishedWorkers) {
		this.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;
		return this;
	}

	/**
	 * 
	 * @param procedure
	 * @param procedureIndex
	 *            specifies which procedure in the queue it is, used for task id
	 * @return
	 */
	public Task procedure(IMapReduceProcedure procedure) {
		this.procedure = procedure;
		return this;
	}

	public PeerAddress dataLocationHashPeerAddress() {
		return this.dataLocationHashPeerAddress;
	}

	public Task dataLocationHashPeerAddress(PeerAddress dataLocationHashPeerAddress) {
		this.dataLocationHashPeerAddress = dataLocationHashPeerAddress;
		return this;
	}

	public int dataLocationHashJobStatusIndex() {
		return this.dataLocationHashJobStatusIndex;
	}

	public Task dataLocationHashJobStatusIndex(int dataLocationHashJobStatusIndex) {
		this.dataLocationHashJobStatusIndex = dataLocationHashJobStatusIndex;
		return this;
	}

	public void updateStati(PeerAddress peerAddress, BCStatusType currentStatus) {
		switch (currentStatus) {
		case EXECUTING_TASK:
		case FINISHED_TASK:
			updateTaskStati(peerAddress, currentStatus, executingPeers, Tuple.newInstance(BCStatusType.EXECUTING_TASK, BCStatusType.FINISHED_TASK));
			break;
		case EXECUTING_TASK_COMPARISON:
			updateTaskStati(peerAddress, currentStatus, comparingPeers,
					Tuple.newInstance(BCStatusType.EXECUTING_TASK_COMPARISON, BCStatusType.FINISHED_TASK_COMPARISON));
			break;
		case FINISHED_TASK_COMPARISON:
			updateTaskStati(peerAddress, currentStatus, comparingPeers,
					Tuple.newInstance(BCStatusType.EXECUTING_TASK_COMPARISON, BCStatusType.FINISHED_TASK_COMPARISON));
			break;
		default:
			logger.warn("updateStati(peerAddress, currentStatus): Something wrong in switch: PeerAddress: " + peerAddress + ", current status: "
					+ currentStatus);
			break;
		}
	}

	private void updateTaskStati(PeerAddress peerAddress, BCStatusType currentStatus, Multimap<PeerAddress, BCStatusType> peers,
			Tuple<BCStatusType, BCStatusType> toCheck) {
		synchronized (peers) {
			LinkedList<BCStatusType> jobStati = new LinkedList<BCStatusType>(this.executingPeers.removeAll(peerAddress));
			if (jobStati.size() > 0) {
				if (currentStatus == toCheck.first()) {
					int lastIndexOfExecuting = jobStati.lastIndexOf(toCheck.first());
					if (lastIndexOfExecuting == -1) {// not yet executing this task
						jobStati.addLast(currentStatus);
						logger.info("if-if-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
					} else {
						// Already executing this task. Nothing to do until it finished
						logger.warn("if-if-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is already executing task " + id
								+ ". Update ignored.");
					}
				} else if (currentStatus == toCheck.second()) {
					int lastIndexOfExecuting = jobStati.lastIndexOf(toCheck.first());
					if (lastIndexOfExecuting == -1) {// FINISHED_TASK arrived multiple times after another without an EXECUTING_TASK
						// Something went wrong... Should first be Executing before it is Finished. Mark as Failed?
						logger.warn("if-elseif-if: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress.inetAddress()
								+ ":" + peerAddress.tcpPort() + " is already executing task " + id);
					} else { // It was executing and now it finished. This should be the case
						jobStati.removeLastOccurrence(toCheck.first());
						jobStati.addLast(currentStatus);
						logger.info("if-elseif-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " finished executing task " + id);
					}
				} else {
					// Should never happen
					logger.warn("if-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
				}
			} else { // task was not yet assigned to this peer.
				if (currentStatus == toCheck.first()) {
					jobStati.addLast(currentStatus);
					logger.info("else-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
				} else if (currentStatus == toCheck.second()) {
					// Something went wrong, shouldnt get a finished job before it even started
					logger.warn("else-elseif: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress.inetAddress() + ":"
							+ peerAddress.tcpPort() + " is not yet executing task " + id);
				} else {
					// Should never happen
					logger.warn("else-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
				}
			}
			logger.warn("Current jobstati for peer " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + ": " + jobStati);
			this.executingPeers.putAll(peerAddress, jobStati);
		}
	}

	/**
	 * Check how many peers have the same status multiple times (e.g. FINISHED_TASK)
	 * 
	 * @param statusToCheck
	 *            <code>JobStatus</code> to check how many peers for this task are currently holding it
	 * @return number of peers that were assigned this task and currently hold the specified <code>JobStatus</code>
	 */
	public int numberOfPeersWithMultipleSameStati(BCStatusType statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCStatusType> stati = this.executingPeers.get(executingPeer);
				for (BCStatusType status : stati) {
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

	public int numberOfPeersWithSingleStatus(BCStatusType statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCStatusType> stati = this.executingPeers.get(executingPeer);
				for (BCStatusType status : stati) {
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
				Collection<BCStatusType> stati = this.executingPeers.get(executingPeer);
				for (BCStatusType status : stati) {
					if (status.equals(BCStatusType.FINISHED_TASK)) {
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
		return countTotalNumber(BCStatusType.FINISHED_TASK);
	}

	public int totalNumberOfCurrentExecutions() {
		return countTotalNumber(BCStatusType.EXECUTING_TASK);
	}

	private int countTotalNumber(BCStatusType statusToCheck) {
		int nrOfFinishedExecutions = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				Collection<BCStatusType> stati = this.executingPeers.get(executingPeer);
				for (BCStatusType status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfFinishedExecutions;
					}
				}
			}
		}
		return nrOfFinishedExecutions;
	}

	public int numberOfSameStatiForPeer(PeerAddress peerAddress, BCStatusType statusToCheck) {
		int statiCount = 0;
		synchronized (executingPeers) {
			Collection<BCStatusType> stati = this.executingPeers.get(peerAddress);
			for (BCStatusType status : stati) {
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

	public ArrayList<BCStatusType> statiForPeer(PeerAddress peerAddress) {
		synchronized (executingPeers) {
			return new ArrayList<BCStatusType>(this.executingPeers.get(peerAddress));
		}
	}

	public ArrayList<PeerAddress> allAssignedPeers() {
		synchronized (executingPeers) {
			Set<PeerAddress> peers = executingPeers.keySet();
			return new ArrayList<PeerAddress>(peers);
		}
	}

	@Override
	public String toString() {
		return "Task [id=" + id + ", jobId=" + jobId + ", procedure=" + procedure + /* ", keys=" + keys + */", executingPeers=" + executingPeers
				+ ", isFinished=" + isFinished + "]";
	}

	public void synchronizeFinishedTaskStatiWith(Task receivedTask) {

		synchronized (executingPeers) {
			synchronized (receivedTask) {
				ArrayList<PeerAddress> allAssignedPeers = receivedTask.allAssignedPeers();
				for (PeerAddress peerAddress : allAssignedPeers) {
					// ArrayList<JobStatus> statiForReceivedPeer = new ArrayList<JobStatus>(receivedTask.statiForPeer(peerAddress));
					// ArrayList<JobStatus> jobStatiForPeer = new ArrayList<JobStatus>(this.executingPeers.get(peerAddress));
					// if (jobStatiForPeer.size() > statiForReceivedPeer.size()) {
					// jobStatiForPeer = new ArrayList<JobStatus>();
					// for (int i = 0; i < statiForReceivedPeer.size(); ++i) {
					// jobStatiForPeer.add(statiForReceivedPeer.get(i));
					// }
					// } else if (jobStatiForPeer.size() == statiForReceivedPeer.size()) { // In that case, update all executing to finished...
					//
					// for (int i = 0; i < jobStatiForPeer.size(); ++i) {
					// if (jobStatiForPeer.get(i).equals(JobStatus.FINISHED_TASK)
					// && statiForReceivedPeer.get(i).equals(JobStatus.EXECUTING_TASK)) {
					// jobStatiForPeer.set(i, JobStatus.FINISHED_TASK);
					// }
					// }
					// }
					this.executingPeers.removeAll(peerAddress);
					this.executingPeers.putAll(peerAddress, receivedTask.statiForPeer(peerAddress));
				}
			}

		}
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
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	/**
	 * WARNING only use this for testing purposes
	 * 
	 * @return
	 */
	public Task copyWithoutExecutingPeers() {

		Task taskCopy = new Task("");
		taskCopy.id = id;
		taskCopy.jobId = jobId;
		taskCopy.isFinished = isFinished;
		// taskCopy.keys = keys;// Shallow copy...
		taskCopy.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;
		taskCopy.procedure = procedure;
		Multimap<PeerAddress, BCStatusType> tmp = ArrayListMultimap.create();
		taskCopy.executingPeers = Multimaps.synchronizedMultimap(tmp);
		// for(PeerAddress peerAddress: executingPeers.keySet()){
		// taskCopy.executingPeers.putAll(new PeerAddress(peerAddress.peerId()), executingPeers.get(peerAddress));
		// }
		return taskCopy;
	}

	@Override
	public int compareTo(Task o) {
		return this.id.compareTo(o.id);
	}

}
