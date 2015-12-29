package mapreduce.execution.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Tasks {
	private static Logger logger = LoggerFactory.getLogger(Tasks.class);

	/**
	 * Check how many peers have the same status multiple times (e.g. FINISHED_TASK)
	 * 
	 * @param statusToCheck
	 *            <code>JobStatus</code> to check how many peers for this task are currently holding it
	 * @return number of peers that were assigned this task and currently hold the specified <code>JobStatus</code>
	 */
	public static int numberOfPeersWithMultipleSameStati(Task task, BCMessageStatus statusToCheck) {
		int nrOfPeers = 0;
		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		synchronized (executingPeers) {
			for (String executingPeer : executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCMessageStatus> stati = executingPeers.get(executingPeer);
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

	public static int numberOfPeersWithSingleStatus(Task task, BCMessageStatus statusToCheck) {
		int nrOfPeers = 0;
		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		synchronized (executingPeers) {
			for (String executingPeer : executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCMessageStatus> stati = executingPeers.get(executingPeer);
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

	public static int numberOfPeersWithAtLeastOneFinishedExecution(Task task) {
		int nrOfPeers = 0;
		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		synchronized (executingPeers) {
			for (String executingPeer : executingPeers.keySet()) {
				int nrOfStatus = 0;
				Collection<BCMessageStatus> stati = executingPeers.get(executingPeer);
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

	public static int totalNumberOfFinishedExecutions(Task task) {
		return countTotalNumber(task, BCMessageStatus.FINISHED_TASK);
	}

	public static int totalNumberOfCurrentExecutions(Task task) {
		return countTotalNumber(task, BCMessageStatus.EXECUTING_TASK);
	}

	public static void synchronizeFinishedTaskStatiWith(Task currentTask, Task receivedTask) {
		synchronized (currentTask.executingPeers()) {
			synchronized (receivedTask) {
				ArrayList<String> allAssignedPeers = allAssignedPeers(receivedTask);
				for (String peerAddress : allAssignedPeers) {
					currentTask.executingPeers().removeAll(peerAddress);
					currentTask.executingPeers().putAll(peerAddress, statiForPeer(receivedTask, peerAddress));
				}
			}

		}
	}

	public static ArrayList<BCMessageStatus> statiForPeer(Task task, String peerAddress) {
		synchronized (task.executingPeers()) {
			return new ArrayList<BCMessageStatus>(task.executingPeers().get(peerAddress));
		}
	}

	public static ArrayList<String> allAssignedPeers(Task task) {
		synchronized (task.executingPeers()) {
			return new ArrayList<String>(task.executingPeers().keySet());
		}
	}

	private static int countTotalNumber(Task task, BCMessageStatus statusToCheck) {
		int nrOfFinishedExecutions = 0;
		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		synchronized (executingPeers) {
			for (String executingPeer : executingPeers.keySet()) {
				Collection<BCMessageStatus> stati = executingPeers.get(executingPeer);
				for (BCMessageStatus status : stati) {
					if (status.equals(statusToCheck)) {
						++nrOfFinishedExecutions;
					}
				}
			}
		}
		return nrOfFinishedExecutions;
	}

	public static int numberOfSameStatiForPeer(Task task, String peerAddress, BCMessageStatus statusToCheck) {
		int statiCount = 0;
		synchronized (task.executingPeers()) {
			Collection<BCMessageStatus> stati = task.executingPeers().get(peerAddress);
			for (BCMessageStatus status : stati) {
				if (status.equals(statusToCheck)) {
					++statiCount;
				}
			}
		}
		return statiCount;
	}

	public static int numberOfDifferentPeersExecutingOrFinishedTask(Task task) {
		synchronized (task.executingPeers()) {
			return task.executingPeers().keySet().size();
		}
	}

	public static void updateStati(Task task, TaskResult toUpdate, int maxNrOfFinishedWorkers) {
		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		synchronized (executingPeers) {
			String sender = toUpdate.sender();
			BCMessageStatus currentStatus = toUpdate.status();
			LinkedList<BCMessageStatus> jobStati = new LinkedList<BCMessageStatus>(executingPeers.removeAll(sender));

			// int lastIndexOfExecuting = jobStati.lastIndexOf(BCMessageStatus.EXECUTING_TASK);
			boolean containsExecuting = jobStati.contains(BCMessageStatus.EXECUTING_TASK);
			logger.info("updateStati:  received " + currentStatus + ", " + sender + " with stati " + jobStati);
			if (currentStatus == BCMessageStatus.FINISHED_TASK && containsExecuting) {
				jobStati.removeLastOccurrence(BCMessageStatus.EXECUTING_TASK);
				jobStati.addLast(currentStatus);
				int locationIndex = jobStati.size() - 1;
				int nrOfResultsWithHash = task.updateResultHash(Tuple.create(sender, locationIndex), toUpdate.resultHash);
				boolean bestOfAchieved = nrOfResultsWithHash == bestOfMaxNrOfFinishedWorkersWithSameResultHash(maxNrOfFinishedWorkers);
				boolean enoughWorkersFinished = totalNumberOfFinishedExecutions(task) >= maxNrOfFinishedWorkers;
				logger.info("bestOfAchieved || enoughWorkersFinished: " + bestOfAchieved + "||" + enoughWorkersFinished+"?"
						+ (bestOfAchieved || enoughWorkersFinished));
				if (bestOfAchieved || enoughWorkersFinished) {
					task.isFinished(true);
					task.addFinalExecutorTaskDomainPart(Tuple.create(sender, locationIndex));
					for (String p : executingPeers.keySet()) {
						List<BCMessageStatus> list = executingPeers.get(p);
						for (int i = 0; i < list.size(); ++i) {
							Tuple<String, Integer> toRemove = Tuple.create(p, i);
							if (!task.finalExecutorTaskDomainParts().contains(toRemove)) {
								synchronized (task.rejectedExecutorTaskDomainParts()) {
									task.addRejectedExecutorTaskDomainPart(toRemove);
								}
							}
						}
					}
				}
			} else if (currentStatus == BCMessageStatus.EXECUTING_TASK && !containsExecuting && !task.isFinished()) {
				jobStati.addLast(currentStatus);
			} else {
				logger.warn("updateStati:Ignored update for: received " + toUpdate + " for job stati of " + sender + " with stati " + jobStati);
			}

			executingPeers.putAll(sender, jobStati);
		}
	}

	public static int bestOfMaxNrOfFinishedWorkersWithSameResultHash(int maxNrOfFinishedWorkers) {
		return Math.round((((float) maxNrOfFinishedWorkers) / 2f));
	}

	public static void removeStatusForPeerAt(Task task, String peerAddress, int jobStatusIndexToRemove) {
		ArrayList<BCMessageStatus> statiForPeer = null;
		synchronized (task.executingPeers()) {
			statiForPeer = new ArrayList<BCMessageStatus>(task.executingPeers().removeAll(peerAddress));
		}
		if (statiForPeer != null) {
			statiForPeer.remove(jobStatusIndexToRemove);
			if (statiForPeer.size() > 0) {
				synchronized (task.executingPeers()) {
					task.executingPeers().putAll(peerAddress, statiForPeer);
				}
			}
		}
	}

	public static void executingPeers(Task task, Collection<Tuple<String, BCMessageStatus>> executingPeers) {
		if (executingPeers != null) {
			synchronized (task.executingPeers()) {
				task.executingPeers().clear();
				for (Tuple<String, BCMessageStatus> tuple : executingPeers) {
					task.executingPeers().put(tuple.first(), tuple.second());
				}
			}
		}
	}
}
