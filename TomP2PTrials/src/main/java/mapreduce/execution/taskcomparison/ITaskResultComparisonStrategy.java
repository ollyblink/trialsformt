package mapreduce.execution.taskcomparison;

import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.KeyValuePair;
import net.tomp2p.peers.PeerAddress;

public interface ITaskResultComparisonStrategy<KEY, VALUE> {
	public void evaluateFinalResult(Multimap<PeerAddress, JobStatus> taskExecutingPeers, Multimap<PeerAddress, KeyValuePair> taskResultsForPeers);
}
