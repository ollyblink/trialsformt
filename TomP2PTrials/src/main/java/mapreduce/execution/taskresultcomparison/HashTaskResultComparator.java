package mapreduce.execution.taskresultcomparison;

import java.util.ArrayList;

import mapreduce.execution.jobtask.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class HashTaskResultComparator implements ITaskResultComparator {
	private IDHTConnectionProvider dhtConnectionProvider;

	public static HashTaskResultComparator newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new HashTaskResultComparator(dhtConnectionProvider);
	}

	private HashTaskResultComparator(IDHTConnectionProvider dhtConnectionProvider) {

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

}
