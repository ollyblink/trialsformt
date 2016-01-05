package mapreduce.execution.task;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.execution.JobProcedureDomain;
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
	private int nrOfSameResultHash;
	/** Key of this task to get the values for */
	private final String key;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	/** final output domain for where this tasks output key/values are stored */
	private IDomain resultOutputDomain;
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

	public void isActive(boolean isActive) {
		this.isActive = isActive;
	}

	@Override
	public boolean isFinished() {
		return resultOutputDomain != null;
	}

	@Override
	public void addOutputDomain(IDomain domain) {
		this.outputDomains.add((ExecutorTaskDomain) domain);
	}

	public int differentExecutors() {
		Set<String> differentExecutors = new HashSet<String>();
		for (IDomain domain : outputDomains) {
			differentExecutors.add(domain.executor());
		}
		return differentExecutors.size();
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
		if (resultOutputDomain == null) {
			isFinished();
			if (resultOutputDomain == null) {
				return null;
			}
		}
		return resultOutputDomain.resultHash();
	}

	@Override
	public IDomain resultOutputDomain() {
		ListMultimap<Number160, ExecutorTaskDomain> results = ArrayListMultimap.create();
		for (ExecutorTaskDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		for (Number160 resultHash : results.keySet()) {
			if (results.get(resultHash).size() >= nrOfSameResultHash) {
				this.resultOutputDomain = results.get(resultHash).get(0);
				break;
			}
		}
		return resultOutputDomain;
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}

	public boolean isInProcedureDomain() {
		return this.isInProcedureDomain;
	}

	public void isInProcedureDomain(boolean isInProcedureDomain) {
		this.isInProcedureDomain = isInProcedureDomain;
	}

	@Override
	public int nrOfOutputDomains() {
		return outputDomains.size();
	}

	@Override
	public Task nrOfSameResultHash(int nrOfSameResultHash) {
		this.nrOfSameResultHash = nrOfSameResultHash;
		return this;
	}

}
