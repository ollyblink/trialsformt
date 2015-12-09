package mapreduce.utils;

import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public enum DomainProvider {
	INSTANCE;
	public String domain(Task task, PeerAddress peerAddress) {
		Number160 peerId = peerAddress.peerId();
		int jobStatusIndex = task.statiForPeer(peerAddress).size() - 1;
		if (jobStatusIndex == -1 && task.initialDataLocation() != null) { // means there's none assigned for that peer... either a problem, or take
																			// initial data location instead
			jobStatusIndex = task.initialDataLocation().second();
		}
		String domain = task.id() + task.procedure().getClass().getSimpleName() + peerId + jobStatusIndex;
		return domain;
	}

	public String domain(Task task, PeerAddress peerAddress, Integer jobStatusIndex) {
		return task.id() + task.procedure().getClass().getSimpleName() + peerAddress.peerId() + jobStatusIndex;
	}
}
