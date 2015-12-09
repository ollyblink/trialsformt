package mapreduce.execution.task.taskexecutorscleaner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.LocationBean;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class TaskExecutorsCleaner implements IJobCleaner {

	@Override
	public Multimap<Task, LocationBean> cleanUp(final Job job) {
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());

		Multimap<Task, LocationBean> allToRemoveFromDHT = ArrayListMultimap.create();
		for (final Task task : tasks) {
			Multimap<Task, LocationBean> toRemoveFromDHT = ArrayListMultimap.create();
			int total = task.totalNumberOfCurrentExecutions() + task.totalNumberOfFinishedExecutions();
			// only keep exactly max nr of finished workers in the job
			Collection<Tuple<PeerAddress, BCMessageStatus>> toKeep = new ArrayList<Tuple<PeerAddress, BCMessageStatus>>();
			int cnt = 0;
			int sum = toKeep.size() + toRemoveFromDHT.values().size();
			int maxCnt = 0;
			while (sum != total) {
				for (final PeerAddress peerAddress : task.allAssignedPeers()) {

					ArrayList<BCMessageStatus> statiForPeer = task.statiForPeer(peerAddress);
					if (maxCnt < statiForPeer.size()) {
						maxCnt = statiForPeer.size();
					}
					if (cnt >= statiForPeer.size()) {
						continue;
					}
					if ((!statiForPeer.get(cnt).equals(BCMessageStatus.EXECUTING_TASK)) && toKeep.size() < task.maxNrOfFinishedWorkers()) {
						toKeep.add(Tuple.create(peerAddress, statiForPeer.get(cnt)));
					} else {
						toRemoveFromDHT.put(task, LocationBean.create(Tuple.create(peerAddress, cnt), task.procedure()));
					}
					sum = toKeep.size() + toRemoveFromDHT.values().size();
				}
				++cnt;
				if (maxCnt < cnt) {
					break;
				}
			}
			task.executingPeers(toKeep);
			allToRemoveFromDHT.putAll(toRemoveFromDHT);
		}

		return allToRemoveFromDHT;
	}

	public static TaskExecutorsCleaner newInstance() {
		return new TaskExecutorsCleaner();
	}

}
