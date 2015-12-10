package mapreduce.storage.dhtmaintenance;

import com.google.common.collect.Multimap;

import mapreduce.execution.task.Task;
import mapreduce.utils.IAbortableExecution;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public interface IDHTDataCleaner extends IAbortableExecution{

	public void removeDataFromDHT(Multimap<Task, Tuple<PeerAddress, Integer>> dataToRemove);

}
