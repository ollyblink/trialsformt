package mapreduce.execution.computation.context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public class PseudoStoreContext implements IContext {

	private Task task;
	private Number160 resultHash;
	private ListMultimap<Object, Object> storage;
	private IMapReduceProcedure combiner;

	@Override
	public void write(Object keyOut, Object valueOut) {
		synchronized (storage) {

			storage.put(keyOut, valueOut);
			if (resultHash == null) {
				resultHash = Number160.createHash(valueOut.toString());
			} else {
				resultHash = resultHash.xor(Number160.createHash(valueOut.toString()));
			}
		}
	}

	@Override
	public PseudoStoreContext task(Task task) {
		this.task = task;
		return this;
	}

	public static PseudoStoreContext newInstance() {
		return new PseudoStoreContext();
	}

	private PseudoStoreContext() {
		ListMultimap<Object, Object> tmp = ArrayListMultimap.create();
		this.storage = Multimaps.synchronizedListMultimap(tmp);
	}

	public ListMultimap<Object, Object> storage() {
		if (this.combiner != null) { 
//			synchronized (storage) {
				Set<Object> keySet = new HashSet<>(storage.keySet());
				for (Object keyOut : keySet) {
					Collection<Object> valueOut = storage.removeAll(keyOut);
					combiner.process(keyOut, valueOut, this);
				}
//			} 
		}
		return storage;
	}

	public Number160 resultHash() {
		return this.resultHash;
	}

	@Override
	public PseudoStoreContext combiner(IMapReduceProcedure combiner) {
		this.combiner = combiner;
		return this;
	}

	@Override
	public IMapReduceProcedure combiner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Object> keys() {
		// TODO Auto-generated method stub
		return null;
	}
}
