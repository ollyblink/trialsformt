package mapreduce.execution.taskresultcomparison;

import mapreduce.execution.jobtask.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public interface ITaskResultComparator {
	/**
	 * 
	 * @param task
	 *            is evaluated for equality of the results
	 * @return a tupel containing the resulting peer and jobstatus that should be used in the hash to obtain data from the dht for the next task
	 */
	public Tuple<PeerAddress, Integer> evaluateTaskResults(Task task);
 
}
