package mapreduce.execution.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.LocationBean;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
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
	private ListMultimap<PeerAddress, BCMessageStatus> executingPeers;
	private ListMultimap<Number160, Tuple<PeerAddress, Integer>> taskResults;
	private ListMultimap<Tuple<PeerAddress, Integer>, Number160> reverseTaskResults;
	private boolean isFinished;
	private int maxNrOfFinishedWorkers;
	/** The number of same hash results to be achieved */
	private int bestOfMaxNrOfFinishedWorkersWithSameResultHash;

	// CONSIDER ONLY STORING THEIR HASH REPRESENTATION FOR THE DOMAIN AND OTHER KEYS
	/** Data location to retrieve the data from for this task */
	private LocationBean initialDataLocation;
	/**
	 * Data location chosen to be the data that remains in the DHT of all the peers that finished the task in executingPeers (above)... The Integer
	 * value is actually the index in the above multimap of the value (Collection) for that PeerAddress key
	 */
	private LocationBean finalDataLocation;
	private List<LocationBean> dataToRemove;

	private Task(String jobId) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName()) + "_" + jobId;
		this.jobId = jobId;
		this.procedure = null;
		ArrayListMultimap<PeerAddress, BCMessageStatus> tmp = ArrayListMultimap.create();
		this.executingPeers = Multimaps.synchronizedListMultimap(tmp);
		ArrayListMultimap<Number160, Tuple<PeerAddress, Integer>> tmp2 = ArrayListMultimap.create();
		this.taskResults = Multimaps.synchronizedListMultimap(tmp2);
		ArrayListMultimap<Tuple<PeerAddress, Integer>, Number160> tmp3 = ArrayListMultimap.create();
		this.reverseTaskResults = Multimaps.synchronizedListMultimap(tmp3);
		this.maxNrOfFinishedWorkers(1);
		this.isFinished = false;
		this.initialDataLocation = null;
		this.finalDataLocation = null;
		this.dataToRemove = new ArrayList<>();
	}

	private int bestOfMaxNrOfFinishedWorkersWithSameResultHash(int maxNrOfFinishedWorkers) {
		return Math.round((((float) maxNrOfFinishedWorkers) / 2f));
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

		return maxNrOfFinishedWorkers;
	}

	public Task maxNrOfFinishedWorkers(int maxNrOfFinishedWorkers) {
		if (maxNrOfFinishedWorkers < 1) {
			maxNrOfFinishedWorkers = 1;
		}
		this.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;
		int tmpBestOf = bestOfMaxNrOfFinishedWorkersWithSameResultHash(maxNrOfFinishedWorkers);
		if (tmpBestOf > this.bestOfMaxNrOfFinishedWorkersWithSameResultHash) {
			isFinished = false;
			finalDataLocation = null;
		}
		this.bestOfMaxNrOfFinishedWorkersWithSameResultHash = tmpBestOf;

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

	public Task initialDataLocation(LocationBean initialDataLocation) {
		this.initialDataLocation = initialDataLocation;
		return this;
	}

	public Task finalDataLocation(LocationBean finalDataLocation) {
		this.finalDataLocation = finalDataLocation;
		return this;
	}

	public LocationBean initialDataLocation() {
		return initialDataLocation;
	}

	public LocationBean finalDataLocation() {
		return finalDataLocation;
	}

//	public boolean taskComparisonAssigned() {
//		// return this.comparingPeers.values().contains(BCMessageStatus.EXECUTING_TASK_COMPARISON);
//		return false;
//	}

	public void updateStati(TaskResult toUpdate) {
		synchronized (executingPeers) {
			PeerAddress peerAddress = toUpdate.sender();
			BCMessageStatus currentStatus = toUpdate.status();
			LinkedList<BCMessageStatus> jobStati = new LinkedList<BCMessageStatus>(executingPeers.removeAll(peerAddress));

			// int lastIndexOfExecuting = jobStati.lastIndexOf(BCMessageStatus.EXECUTING_TASK);
			boolean containsExecuting = jobStati.contains(BCMessageStatus.EXECUTING_TASK);
			logger.info("updateStati:  received " + currentStatus + ", " + peerAddress.peerId() + " with stati " + jobStati);
			if (currentStatus == BCMessageStatus.FINISHED_TASK && containsExecuting) {
				jobStati.removeLastOccurrence(BCMessageStatus.EXECUTING_TASK);
				jobStati.addLast(currentStatus);
				int locationIndex = jobStati.size() - 1;
				int nrOfResultsWithHash = this.updateResultHash(peerAddress, locationIndex, toUpdate.resultHash);
				boolean bestOfAchieved = nrOfResultsWithHash == this.bestOfMaxNrOfFinishedWorkersWithSameResultHash;
				boolean enoughWorkersFinished = totalNumberOfFinishedExecutions() >= maxNrOfFinishedWorkers;

				if (bestOfAchieved || enoughWorkersFinished) {
					this.isFinished = true;
					this.finalDataLocation = LocationBean.create(Tuple.create(peerAddress, locationIndex), procedure);
					for (PeerAddress p : executingPeers.keySet()) {
						List<BCMessageStatus> list = executingPeers.get(p);
						for (int i = 0; i < list.size(); ++i) {
							LocationBean lB = LocationBean.create(Tuple.create(p, i), procedure);
							if (!lB.equals(finalDataLocation)) {
								this.dataToRemove.add(lB);
							}
						}
					}
				}
			} else if (currentStatus == BCMessageStatus.EXECUTING_TASK && !containsExecuting && !isFinished) {
				jobStati.addLast(currentStatus);
			} else {
				logger.warn("updateStati:Ignored update for: received " + toUpdate + " for job stati of " + peerAddress.peerId() + " with stati "
						+ jobStati);
			}

			executingPeers.putAll(peerAddress, jobStati);
		}
	}

	/**
	 * Puts a new result hash
	 * 
	 * @param sender
	 * @param location
	 * @param resultHash
	 * @return returns the number of times this result hash has been achieved.
	 */
	public int updateResultHash(PeerAddress peerAddress, Integer location, Number160 resultHash) {
		Tuple<PeerAddress, Integer> tuple = Tuple.create(peerAddress, location);
		synchronized (this.taskResults) {
			this.taskResults.put(resultHash, tuple);
		}
		synchronized (this.reverseTaskResults) {
			this.reverseTaskResults.put(tuple, resultHash);
		}
		return this.taskResults.get(resultHash).size();
	}

	public Number160 resultHash(PeerAddress peerAddress, Integer location) {
		return this.reverseTaskResults.get(Tuple.create(peerAddress, location)).get(0);
	}

	// With logging info
	// public void updateStati(Tuple<PeerAddress, BCMessageStatus> toUpdate) {
	// synchronized (executingPeers) {
	// PeerAddress peerAddress = toUpdate.first();
	// BCMessageStatus currentStatus = toUpdate.second();
	// LinkedList<BCMessageStatus> jobStati = new LinkedList<BCMessageStatus>(executingPeers.removeAll(peerAddress));
	//
	// if (jobStati.size() > 0) {
	// if (currentStatus == BCMessageStatus.EXECUTING_TASK) {
	// int lastIndexOfExecuting = jobStati.lastIndexOf(BCMessageStatus.EXECUTING_TASK);
	// if (lastIndexOfExecuting == -1) {// not yet executing this task
	// jobStati.addLast(currentStatus);
	// logger.info("if-if-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
	// } else {
	// // Already executing this task. Nothing to do until it finished
	// logger.warn("if-if-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is already executing task " + id
	// + ". Update ignored.");
	// }
	// } else if (currentStatus == BCMessageStatus.FINISHED_TASK) {
	// int lastIndexOfExecuting = jobStati.lastIndexOf(BCMessageStatus.EXECUTING_TASK);
	// if (lastIndexOfExecuting == -1) {// FINISHED_TASK arrived multiple times after another without an EXECUTING_TASK
	// // Something went wrong... Should first be Executing before it is Finished. Mark as Failed?
	// logger.warn("if-elseif-if: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress.inetAddress()
	// + ":" + peerAddress.tcpPort() + " is already executing task " + id);
	// } else { // It was executing and now it finished. This should be the case
	// jobStati.removeLastOccurrence(BCMessageStatus.EXECUTING_TASK);
	// jobStati.addLast(currentStatus);
	// logger.info("if-elseif-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " finished executing task " + id);
	// }
	// } else {
	// // Should never happen
	// logger.warn("if-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
	// }
	// } else { // task was not yet assigned to this peer.
	// if (currentStatus == BCMessageStatus.EXECUTING_TASK) {
	// jobStati.addLast(currentStatus);
	// logger.info("else-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
	// } else if (currentStatus == BCMessageStatus.FINISHED_TASK) {
	// // Something went wrong, shouldnt get a finished job before it even started
	// logger.warn("else-elseif: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress.inetAddress() + ":"
	// + peerAddress.tcpPort() + " is not yet executing task " + id);
	// } else {
	// // Should never happen
	// logger.warn("else-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
	// }
	// }
	// logger.warn("Current jobstati for peer " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + ": " + jobStati);
	// executingPeers.putAll(peerAddress, jobStati);
	// }
	// }

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
		return "Task [id=" + id + ", jobId=" + jobId + ", procedure=" + procedure + ", executingPeers=" + executingPeers + ", isFinished="
				+ isFinished + ", maxNrOfFinishedWorkers=" + maxNrOfFinishedWorkers + ", initialDataLocation=" + initialDataLocation
				+ ", finalDataLocation=" + finalDataLocation + "]";
	}

	public void synchronizeFinishedTaskStatiWith(Task receivedTask) {

		synchronized (executingPeers) {
			synchronized (receivedTask) {
				ArrayList<PeerAddress> allAssignedPeers = receivedTask.allAssignedPeers();
				for (PeerAddress peerAddress : allAssignedPeers) {
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
		taskCopy.maxNrOfFinishedWorkers(maxNrOfFinishedWorkers);
		taskCopy.procedure = procedure;
		// for(PeerAddress peerAddress: executingPeers.keySet()){
		// taskCopy.executingPeers.putAll(new PeerAddress(peerAddress.peerId()), executingPeers.get(peerAddress));
		// }
		return taskCopy;
	}

	@Override
	public int compareTo(Task o) {
		return this.id.compareTo(o.id);
	}

	public void removeStatusForPeerAt(PeerAddress peerAddress, int jobStatusIndexToRemove) {
		ArrayList<BCMessageStatus> statiForPeer = new ArrayList<BCMessageStatus>(this.executingPeers.removeAll(peerAddress));
		statiForPeer.remove(jobStatusIndexToRemove);

		if (statiForPeer.size() > 0) {
			synchronized (this.executingPeers) {
				this.executingPeers.putAll(peerAddress, statiForPeer);
			}
		}

	}

	public void executingPeers(Collection<Tuple<PeerAddress, BCMessageStatus>> executingPeers) {
		if (executingPeers != null) {
			synchronized (this.executingPeers) {
				this.executingPeers.clear();
				for (Tuple<PeerAddress, BCMessageStatus> tuple : executingPeers) {
					this.executingPeers.put(tuple.first(), tuple.second());
				}
			}
		}
	}

	public List<LocationBean> dataToRemove() {
		return this.dataToRemove;
	}

}
