package mapreduce.execution.computation.context;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task2;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public abstract class AbstractBaseContext implements IContext {

	protected IExecutable combiner;
	protected Number160 resultHash;
	protected ListMultimap<String, Object> keyValues = ArrayListMultimap.create();
	protected boolean combine;

	/** NEXT!!! not the current, because the next procedure will use the keys produced for this procedure */
	protected Procedure subsequentProcedure;
	protected Task2 task;

	protected AbstractBaseContext() {
		resetResultHash();
	}

	@Override
	public IContext combiner(IExecutable combiner) {
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
	public IExecutable combiner() {
		return this.combiner;
	}

	private void resetResultHash() {
		this.resultHash = Number160.ZERO;
	}

	protected void updateResultHash(Object keyOut, Object valueOut) {
		resultHash.xor(Number160.createHash(keyOut.toString())).xor(Number160.createHash(valueOut.toString()));
	}

	@Override
	public Procedure subsequentProcedure() {
		return this.subsequentProcedure;
	}

	@Override
	public AbstractBaseContext subsequentProcedure(Procedure subsequentProcedure) {
		this.subsequentProcedure = subsequentProcedure;
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

	@Override
	public void write(Object keyOut, Object valueOut) {
		// TODO Auto-generated method stub

	}

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBaseContext task(Task2 task) {
		this.task = task;
		return this;
	}

	@Override
	public Task2 task() { 
		return task;
	}
}
