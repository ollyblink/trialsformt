package mapreduce.execution.task;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.execution.procedures.ExecutorTaskDomain;
import net.tomp2p.peers.Number160;

public class Task2 implements IFinishable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Domains of keys for this procedure (this key comes from this domain) */
	private List<ExecutorTaskDomain> inputDomains;
	/** Domain (ExecutorTaskDomains) of keys for next procedure */
	private List<IDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	private int nrOfSameResultHash;
	/** Key of this task to get the values for */
	private final String key;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	/** final output domain for where this tasks output key/values are stored */
	private IDomain resultOutputDomain;

	private Task2(String key) {
		this.key = key;
	}

	public static Task2 create(String key) {
		return new Task2(key);
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
		boolean isFinished = true;
		ListMultimap<Number160, IDomain> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		for (Number160 resultHash : results.keySet()) {
			if (results.get(resultHash).size() >= nrOfSameResultHash) {
				this.resultOutputDomain = results.get(resultHash).get(0);
				isFinished = true;
				break;
			}
		}
		return isFinished;
	}

	@Override
	public void addOutputDomain(IDomain domain) {
		this.outputDomains.add(domain);
	}

	public void addInputDomain(ExecutorTaskDomain domain) {
		this.inputDomains.add(domain);
	}

	public List<ExecutorTaskDomain> inputDomains() {
		return this.inputDomains;
	}

	@Override
	public List<IDomain> outputDomains() {
		return this.outputDomains;
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
				return Number160.ZERO; // Means not yet finished
			}
		}
		return resultOutputDomain.resultHash();
	}

	@Override
	public IDomain resultOutputDomain() {
		return resultOutputDomain;
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}

}
