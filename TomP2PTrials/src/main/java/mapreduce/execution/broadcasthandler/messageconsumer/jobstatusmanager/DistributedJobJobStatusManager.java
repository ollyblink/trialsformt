package mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;

public class DistributedJobJobStatusManager extends AbstractJobStatusManager {

	public static DistributedJobJobStatusManager newInstance() {
		return new DistributedJobJobStatusManager();
	}

	private DistributedJobJobStatusManager() {

	}

	@Override
	protected void manageMessages() {
		Multimap<String, DistributedJobBCMessage> map = ArrayListMultimap.create();
		for (IBCMessage msg : bcMessages) {
			DistributedJobBCMessage djm = (DistributedJobBCMessage) msg;
			map.put(djm.job().id(), djm);
		}

		for (String jobId : map.keySet()) {
			List<DistributedJobBCMessage> distributedJobBCMessagesForJob = new ArrayList<DistributedJobBCMessage>(map.get(jobId));
			Collections.sort(distributedJobBCMessagesForJob, new Comparator<DistributedJobBCMessage>() {

				@Override
				public int compare(DistributedJobBCMessage o1, DistributedJobBCMessage o2) {
					return o1.creationTime().compareTo(o2.creationTime());
				}
			});
			System.err.println(">> KEEPING " + distributedJobBCMessagesForJob.get(0).job().id() + " from "
					+ distributedJobBCMessagesForJob.get(0).sender().peerId().toString() + " with time "
					+ distributedJobBCMessagesForJob.get(0).creationTime());

			if (distributedJobBCMessagesForJob.size() > 1) {
				for (int i = 1; i < distributedJobBCMessagesForJob.size(); ++i) {
					System.err.println(">> CANCEL AND REMOVE " + distributedJobBCMessagesForJob.get(i).job().id() + " from "
							+ distributedJobBCMessagesForJob.get(i).sender().peerId().toString() + " with time "
							+ distributedJobBCMessagesForJob.get(i).creationTime());
					bcMessages.remove(distributedJobBCMessagesForJob.get(i));
				}
			}
		}

	}

}
