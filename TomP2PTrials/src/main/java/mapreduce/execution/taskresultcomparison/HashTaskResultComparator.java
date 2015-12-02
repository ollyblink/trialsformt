package mapreduce.execution.taskresultcomparison;

import java.util.ArrayList;

import mapreduce.execution.jobtask.Task;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class HashTaskResultComparator implements ITaskResultComparator {

	public static HashTaskResultComparator newInstance() {
		return new HashTaskResultComparator();
	}

	private HashTaskResultComparator() {

	}

	@Override
	public Tuple<PeerAddress, Integer> evaluateTaskResults(Task task) {
		Tuple<PeerAddress, Integer> tuple = null;

		ArrayList<PeerAddress> allAssignedPeers = task.allAssignedPeers();
		for (PeerAddress peerAddress : allAssignedPeers) {
			task.statiForPeer(peerAddress);
		}
		return tuple;
	}

	@Override
	public void abortTaskComparison() {
		//Ja nie snaiu, ja nitschewo nie ponimaju
	}

}
