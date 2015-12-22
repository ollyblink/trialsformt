package mapreduce.execution.computation.context;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class DHTStorageContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;

	private Task task;

	private IMapReduceProcedure combiner;
	private Set<Object> keys;

	private ListMultimap<Object, Object> tmpKeyValues;

	// private ITaskResultComparator taskResultComparator;

	/**
	 * 
	 * @param dhtConnectionProvider
	 * @param taskResultComparator
	 *            may add certain speed ups such that the task result comparison afterwards becomes faster
	 */
	private DHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		ListMultimap<Object, Object> multimap = ArrayListMultimap.create();
		this.tmpKeyValues = Multimaps.synchronizedListMultimap(multimap);
	}

	public static IContext newDHTStorageContext(DHTConnectionProvider dhtConnectionProvider) {
		return new DHTStorageContext(dhtConnectionProvider);
	}

	// public DHTStorageContext taskResultComparator(ITaskResultComparator taskResultComparator) {
	// this.taskResultComparator = taskResultComparator;
	// return this;
	// }

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (task == null) {
			logger.warn("No task id set!");
			return;
		}

		tmpKeyValues.put(keyOut, valueOut);

//		if (dataLimitAchieved(tmpKeyValues)) {
//			this.dhtConnectionProvider.add(keyOut, valueOut, DomainProvider.INSTANCE.executorTaskDomain(task,
//					Tuple.create(dhtConnectionProvider.peerAddress(), task.executingPeers().get(dhtConnectionProvider.peerAddress()).size() - 1)));
//		}

	}

	public DHTStorageContext task(Task task) {
		this.task = task;
		return this;
	}

	public DHTStorageContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DHTStorageContext combiner(IMapReduceProcedure combiner) {
		this.combiner = combiner;
		return this;
	}

	@Override
	public IMapReduceProcedure combiner() {
		return this.combiner;
	}

	@Override
	public Set<Object> keys() {
		return this.keys;
	}

	public static void main(String[] args) {
		long tb = FileSize.MEGA_BYTE.value() * FileSize.MEGA_BYTE.value();
		System.out.println("ABCDEWMQXY".getBytes().length);
		System.out.println(tb / 10);
	}
}
