package mapreduce.execution.taskcomparison;

import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.KeyValuePair;
import net.tomp2p.peers.PeerAddress;

public class HashTaskResultComparisonStrategy<KEY, VALUE> implements ITaskResultComparisonStrategy<KEY, VALUE> {

	@Override
	public void evaluateFinalResult(Multimap<PeerAddress, JobStatus> taskExecutingPeers, Multimap<PeerAddress, KeyValuePair> taskResultsForPeers) {
		// TODO Auto-generated method stub

	}

}
