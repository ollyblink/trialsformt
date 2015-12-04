package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
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
	private Multimap<PeerAddress, BCMessageStatus> executingPeers;
	private Map<PeerAddress, BCMessageStatus> comparingPeers;

	private boolean isFinished;
	private int maxNrOfFinishedWorkers;

	// CONSIDER ONLY STORING THERE HASH REPRESENTATION FOR THE DOMAIN AND OTHER KEYS
	/** Data location to retrieve the data from for this task */
	private Tuple<PeerAddress, Integer> initialDataLocation;
	/**
	 * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	 * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	 */
	private Tuple<PeerAddress, Integer> finalDataLocation;

	private Task(String jobId) {
		Multimap<PeerAddress, BCMessageStatus> tmp = ArrayListMultimap.create();
		this.executingPeers = Multimaps.synchronizedMultimap(tmp);
		this.comparingPeers = Collections.synchronizedMap(new HashMap<PeerAddress, BCMessageStatus>());
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

	public Task initialDataLocation(Tuple<PeerAddress, Integer> initialDataLocation) {
		this.initialDataLocation = initialDataLocation;
		return this;
	}

	public Task finalDataLocation(Tuple<PeerAddress, Integer> finalDataLocation) {
		this.finalDataLocation = finalDataLocation;
		return this;
	}

	public Tuple<PeerAddress, Integer> initialDataLocation() {
		return initialDataLocation;
	}

	public Tuple<PeerAddress, Integer> finalDataLocation() {
		return finalDataLocation;
	}

	public boolean taskComparisonAssigned() {
		return this.comparingPeers.values().contains(BCMessageStatus.EXECUTING_TASK_COMPARISON);
	}

	public void updateStati(Tuple<PeerAddress, BCMessageStatus> toUpdate) {
		switch (toUpdate.second()) {
		case EXECUTING_TASK:
		case FINISHED_TASK:
			updateTaskStati(toUpdate, executingPeers, Tuple.create(BCMessageStatus.EXECUTING_TASK, BCMessageStatus.FINISHED_TASK));
			break;
		case EXECUTING_TASK_COMPARISON:
			this.comparingPeers.put(toUpdate.first(), toUpdate.second());
			break;
		default:
			logger.warn("updateStati(peerAddress, currentStatus): Something wrong in switch: PeerAddress: " + toUpdate.first() + ", current status: "
					+ toUpdate.second());
			break;
		}
	}

	private void updateTaskStati(Tuple<PeerAddress, BCMessageStatus> toUpdate, Multimap<PeerAddress, BCMessageStatus> peers,
			Tuple<BCMessageStatus, BCMessageStatus> toCheck) {
		synchronized (peers) {
			PeerAddress peerAddress = toUpdate.first();
			BCMessageStatus currentStatus = toUpdate.second();
			LinkedList<BCMessageStatus> jobStati = new LinkedList<BCMessageStatus>(this.executingPeers.removeAll(peerAddress));

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
	public int numberOfPeersWithMultipleSameStati(BCMessageStatus statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCMessageStatus> stati = this.executingPeers.get(executingPeer);
				for (BCMessageStatus status : stati) {
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

	public int numberOfPeersWithSingleStatus(BCMessageStatus statusToCheck) {
		int nrOfPeers = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCMessageStatus> stati = this.executingPeers.get(executingPeer);
				for (BCMessageStatus status : stati) {
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
				Collection<BCMessageStatus> stati = this.executingPeers.get(executingPeer);
				for (BCMessageStatus status : stati) {
					if (status.equals(BCMessageStatus.FINISHED_TASK)) {
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
		return countTotalNumber(BCMessageStatus.FINISHED_TASK);
	}

	public int totalNumberOfCurrentExecutions() {
		return countTotalNumber(BCMessageStatus.EXECUTING_TASK);
	}

	private int countTotalNumber(BCMessageStatus statusToCheck) {
		int nrOfFinishedExecutions = 0;
		synchronized (executingPeers) {
			for (PeerAddress executingPeer : this.executingPeers.keySet()) {
				Collection<BCMessageStatus> stati = this.executingPeers.get(executingPeer);
				for (BCMessageStatus status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfFinishedExecutions;
					}
				}
			}
		}
		return nrOfFinishedExecutions;
	}

	public int numberOfSameStatiForPeer(PeerAddress peerAddress, BCMessageStatus statusToCheck) {
		int statiCount = 0;
		synchronized (executingPeers) {
			Collection<BCMessageStatus> stati = this.executingPeers.get(peerAddress);
			for (BCMessageStatus status : stati) {
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

	public ArrayList<BCMessageStatus> statiForPeer(PeerAddress peerAddress) {
		synchronized (executingPeers) {
			return new ArrayList<BCMessageStatus>(this.executingPeers.get(peerAddress));
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
		return "Task [id=" + id + ", jobId=" + jobId + ", procedure=" + procedure + ", executingPeers=" + executingPeers + ", comparingPeers="
				+ comparingPeers + ", isFinished=" + isFinished + ", maxNrOfFinishedWorkers=" + maxNrOfFinishedWorkers + ", initialDataLocation="
				+ initialDataLocation + ", finalDataLocation=" + finalDataLocation + "]";
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
		Multimap<PeerAddress, BCMessageStatus> tmp = ArrayListMultimap.create();
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
