package mapreduce.execution.context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public class PseudoStorageContext extends AbstractBaseContext {

	private ListMultimap<Object, Object> storage;

	@Override
	public void write(Object keyOut, Object valueOut) {
		synchronized (storage) {

			storage.put(keyOut, valueOut);
			updateResultHash(keyOut, valueOut);
		}
	}

	public static PseudoStorageContext newInstance() {
		return new PseudoStorageContext();
	}

	private PseudoStorageContext() {
		ListMultimap<Object, Object> tmp = ArrayListMultimap.create();
		this.storage = Multimaps.synchronizedListMultimap(tmp);
	}

	public ListMultimap<Object, Object> storage() {
		if (this.combiner != null) {
			// synchronized (storage) {
			Set<Object> keySet = new HashSet<>(storage.keySet());
			for (Object keyOut : keySet) {
				Collection<Object> valueOut = storage.removeAll(keyOut);
				combiner.process(keyOut, valueOut, this);
			}
			// }
		}
		return storage;
	}

}
