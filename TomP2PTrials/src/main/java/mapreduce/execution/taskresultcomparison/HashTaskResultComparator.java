package mapreduce.execution.taskresultcomparison;

import java.util.ArrayList;

import mapreduce.execution.jobtask.Task;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
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
		// Ja nie snaiu, ja nitschewo nie ponimaju

	}

	public static void main(String[] args) {
		Number160 hash1 = Number160.createHash("Hello world this is me");
		Number160 hash2 = Number160.createHash("Hello world this is me");
		System.out.println(hash1.xor(hash2)); 
	}

	@Override
	public boolean abortedTaskComparisons() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void abortedTaskComparisons(boolean abortedTaskComparisons) {
		// TODO Auto-generated method stub
		
	}
}
