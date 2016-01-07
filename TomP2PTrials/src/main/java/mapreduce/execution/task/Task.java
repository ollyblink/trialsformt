package mapreduce.execution.task;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public class Task implements IFinishable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Domain (ExecutorTaskDomains) of keys for next procedure */
	private List<ExecutorTaskDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	private int nrOfSameResultHash = 1; // Needs at least one
	/** Key of this task to get the values for */
	private final String key;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	/** final output domain for where this tasks output key/values are stored */
	private IDomain resultOutputDomain;
	/** Set true if this tasks's result keys and values were successfully transferred from executor task domain to executor job procedure domain*/
	private volatile boolean isInProcedureDomain;

	private Task(String key) {
		this.key = key;
		this.outputDomains = SyncedCollectionProvider.syncedArrayList();
	}

	public static Task create(String key) {
		return new Task(key);
	}

	public String key() {
		return this.key;
	}

	public boolean isActive() {
		return this.isActive;
	}

	public Task isActive(boolean isActive) {
		this.isActive = isActive;
		return this;
	}

	@Override
	public boolean isFinished() {
		checkIfFinished();
		return resultOutputDomain != null;
	}

	@Override
	public Task addOutputDomain(IDomain domain) {
		this.outputDomains.add((ExecutorTaskDomain) domain);
		return this;
	}

	public int nextStatusIndexFor(String executor) {
		int counter = 0;
		for (IDomain outputDomain : outputDomains) {
			if (outputDomain.executor().equals(executor)) {
				++counter;
			}
		}
		return counter;
	}

	@Override
	public Number160 calculateResultHash() {
		checkIfFinished(); 
		return (resultOutputDomain == null ? null : resultOutputDomain.resultHash());
	}

	@Override
	public IDomain resultOutputDomain() {
		checkIfFinished();
		return resultOutputDomain;
	}

	private void checkIfFinished() {
		ListMultimap<Number160, ExecutorTaskDomain> results = ArrayListMultimap.create();
		for (ExecutorTaskDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		boolean isFinished = false;
		Number160 r = null;
		for (Number160 resultHash : results.keySet()) {
			if (results.get(resultHash).size() >= nrOfSameResultHash) {
				r = resultHash;
				isFinished = true;
				break;
			}
		}
		if (isFinished) {
			this.resultOutputDomain = results.get(r).get(results.get(r).size() - 1);// Most recent... most likely even data still here...
		} else {
			this.resultOutputDomain = null;
		}
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}

	public boolean isInProcedureDomain() {
		return this.isInProcedureDomain;
	}

	public Task isInProcedureDomain(boolean isInProcedureDomain) {
		this.isInProcedureDomain = isInProcedureDomain;
		return this;
	}

	@Override
	public int nrOfOutputDomains() {
		return outputDomains.size();
	}

	@Override
	public Task nrOfSameResultHash(int nrOfSameResultHash) {
		if (nrOfSameResultHash > 1) {
			this.nrOfSameResultHash = nrOfSameResultHash;
		}

		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Task [outputDomains=" + outputDomains + ", nrOfSameResultHash=" + nrOfSameResultHash + ", key=" + key + ", isActive=" + isActive
				+ ", resultOutputDomain=" + resultOutputDomain + ", isInProcedureDomain=" + isInProcedureDomain + "]";
	}

}
