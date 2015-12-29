package mapreduce.execution.computation.context;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public abstract class AbstractBaseContext implements IContext {

	protected IMapReduceProcedure combiner;
	protected Task task;
	protected Number160 resultHash;
	protected Set<Object> keys;
	protected ListMultimap<String, Object> keyValues = ArrayListMultimap.create();
	protected boolean combine;

	/** NEXT!!! not the current, because the next procedure will use the keys produced for this procedure */
	protected Tuple<String, Tuple<String, Integer>> subsequentJobProcedureDomain;

	protected AbstractBaseContext() {
		resetResultHash();
	}

	@Override
	public IContext combiner(IMapReduceProcedure combiner) {
		this.combiner = combiner;
		return this;
	}

	@Override
	public void combine() {
		if (this.combiner != null) {
			this.combine = true;
			for (String key : keyValues.keySet()) {
				this.combiner.process(key, keyValues.get(key), this);
			}
		}
	}

	@Override
	public IMapReduceProcedure combiner() {
		return this.combiner;
	}

	@Override
	public IContext task(Task task) {
		this.task = task;
		return this;
	}

	public Task task() {
		return this.task;
	}

	@Override
	public Number160 resultHash() {

		return this.resultHash;
	}

	@Override
	public Set<Object> keys() {
		return this.keys;
	}

	@Override
	public void resetResultHash() {
		this.resultHash = Number160.ZERO;
	}

	@Override
	public void updateResultHash(Object keyOut, Object valueOut) {
		resultHash.xor(Number160.createHash(keyOut.toString())).xor(Number160.createHash(valueOut.toString()));
	}

	@Override
	public TaskUpdateBCMessage broadcastResultHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple<String, Tuple<String, Integer>> subsequentJobProcedureDomain() {
		return this.subsequentJobProcedureDomain;
	}

	@Override
	public AbstractBaseContext subsequentJobProcedureDomain(Tuple<String, Tuple<String, Integer>> subsequentJobProcedureDomain) {
		this.subsequentJobProcedureDomain = subsequentJobProcedureDomain;
		return this;
	}

	@Override
	public IContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		// TODO Auto-generated method stub
		return this;
	}

	public List<FuturePut> futurePutData() {
		return null;
	}

	@Override
	public DHTStorageContext taskExecutor(Tuple<String, Integer> taskExecutor) {
		// TODO Auto-generated method stub
		return null;
	}
}
