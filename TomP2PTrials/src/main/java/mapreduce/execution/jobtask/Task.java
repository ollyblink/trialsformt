package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.utils.IDCreator;
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
	private boolean isFinished;
	private int maxNrOfFinishedWorkers;

	private Task(String jobId) {
		Multimap<PeerAddress, JobStatus> tmp = ArrayListMultimap.create();
		executingPeers = Multimaps.synchronizedMultimap(tmp);
		this.jobId = jobId;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName()) + "_" + jobId;
	}

	public static Task newTask(String jobId) {
		return new Task(jobId);
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
						logger.info("if-if-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
					} else {
						// Already executing this task. Nothing to do until it finished
						logger.warn("if-if-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is already executing task " + id
								+ ". Update ignored.");
					}
				} else if (currentStatus == JobStatus.FINISHED_TASK) {
					int lastIndexOfExecuting = jobStati.lastIndexOf(JobStatus.EXECUTING_TASK);
					if (lastIndexOfExecuting == -1) {// FINISHED_TASK arrived multiple times after another without an EXECUTING_TASK
						// Something went wrong... Should first be Executing before it is Finished. Mark as Failed?
						logger.warn("if-elseif-if: Something wrong: JobStatus was " + currentStatus + " but peer with " + peerAddress.inetAddress()
								+ ":" + peerAddress.tcpPort() + " is already executing task " + id);
					} else { // It was executing and now it finished. This should be the case
						jobStati.removeLastOccurrence(JobStatus.EXECUTING_TASK);
						jobStati.addLast(currentStatus);
						logger.info("if-elseif-else: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " finished executing task " + id);
					}
				} else {
					// Should never happen
					logger.warn("if-else: Wrong JobStatus detected: JobStatus was " + currentStatus);
				}
			} else { // task was not yet assigned to this peer.
				if (currentStatus == JobStatus.EXECUTING_TASK) {
					jobStati.addLast(currentStatus);
					logger.info("else-if: " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + " is now executing task " + id);
				} else if (currentStatus == JobStatus.FINISHED_TASK) {
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

	public int totalNumberOfCurrentExecutions() {
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
		return "Task [id=" + id + ", jobId=" + jobId + ", procedure=" + procedure + ", keys=" + keys + ", executingPeers=" + executingPeers
				+ ", isFinished=" + isFinished + "]";
	}

	public void synchronizeFinishedTaskStatiWith(Task receivedTask) {
		logger.info("before update");
		for (PeerAddress p : allAssignedPeers()) {
			logger.info("task " + id() + "<" + p.peerId() + ", " + p.inetAddress() + ":" + p.tcpPort() + ">," + statiForPeer(p));
		}

		synchronized (executingPeers) {
			synchronized (receivedTask) {
				Set<PeerAddress> allAssignedPeers = receivedTask.allAssignedPeers();
				for (PeerAddress peerAddress : allAssignedPeers) {
					ArrayList<JobStatus> statiForReceivedPeer = new ArrayList<JobStatus>(receivedTask.statiForPeer(peerAddress));
					ArrayList<JobStatus> jobStatiForPeer = new ArrayList<JobStatus>(this.executingPeers.get(peerAddress));
					if (jobStatiForPeer.size() < statiForReceivedPeer.size()) {
						jobStatiForPeer = new ArrayList<JobStatus>();
						for (int i = 0; i < statiForReceivedPeer.size(); ++i) {
							jobStatiForPeer.add(statiForReceivedPeer.get(i));
						}
					} else if (jobStatiForPeer.size() == statiForReceivedPeer.size()) { // In that case, update all executing to finished...

						for (int i = 0; i < jobStatiForPeer.size(); ++i) {
							if (jobStatiForPeer.get(i).equals(JobStatus.EXECUTING_TASK)
									&& statiForReceivedPeer.get(i).equals(JobStatus.FINISHED_TASK)) {
								jobStatiForPeer.set(i, JobStatus.FINISHED_TASK);
							}
						}
					}
					this.executingPeers.removeAll(peerAddress);
					this.executingPeers.putAll(peerAddress, jobStatiForPeer);
				}
			}

			logger.info("after update");
			for (PeerAddress p : allAssignedPeers()) {
				logger.info("task " + id() + "<" + p.peerId() + ", " + p.inetAddress() + ":" + p.tcpPort() + ">," + statiForPeer(p));
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
	 * @return
	 */
	public Task copyWithoutExecutingPeers() {

		Task taskCopy = new Task("");
		taskCopy.id = id;
		taskCopy.jobId = jobId;
		taskCopy.isFinished = isFinished;
		taskCopy.keys = keys;// Shallow copy...
		taskCopy.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;
		taskCopy.procedure = procedure;
		Multimap<PeerAddress, JobStatus> tmp = ArrayListMultimap.create();
		taskCopy.executingPeers = Multimaps.synchronizedMultimap(tmp);
		// for(PeerAddress peerAddress: executingPeers.keySet()){
		// taskCopy.executingPeers.putAll(new PeerAddress(peerAddress.peerId()), executingPeers.get(peerAddress));
		// }
		return taskCopy;
	}

}
