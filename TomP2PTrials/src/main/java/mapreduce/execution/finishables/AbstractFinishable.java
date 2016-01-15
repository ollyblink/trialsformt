package mapreduce.execution.finishables;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.domains.IDomain;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public abstract class AbstractFinishable implements IFinishable {

	/** final output domain for where this tasks output key/values are stored */
	protected IDomain resultOutputDomain;
	/** Domain (ExecutorTaskDomains) of keys for next procedure */
	protected List<IDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	protected int nrOfSameResultHash = 1; // Needs at least one
	/** Assert that there are multiple output domains received before a task is finished */
	private boolean needsMultipleDifferentDomains;

	public AbstractFinishable() {
		this.outputDomains = SyncedCollectionProvider.syncedArrayList();
	}

	@Override
	public boolean isFinished() {
		checkIfFinished();
		return resultOutputDomain != null;
	}

	@Override
	// Always takes the first one!
	public IDomain resultOutputDomain() {
		checkIfFinished();
		return resultOutputDomain;
	}

	protected void checkIfFinished() {
		ListMultimap<Number160, IDomain> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		boolean isFinished = false;
		Number160 r = null;
		for (Number160 resultHash : results.keySet()) {
			if (results.get(resultHash).size() >= nrOfSameResultHash) {
				if (needsMultipleDifferentDomains) {
					List<IDomain> list = results.get(resultHash);
					Set<IDomain> asSet = new HashSet<>();
					asSet.addAll(list);
					if (asSet.size() < nrOfSameResultHash) {
						continue;
					}
				}
				r = resultHash;
				isFinished = true;
				break;
			}
		}
		if (isFinished) {
			this.resultOutputDomain = results.get(r).get(0);// First just in case something happend at the end...
		} else {
			this.resultOutputDomain = null;
		}
	}

	@Override
	public int nrOfOutputDomains() {
		return outputDomains.size();
	}

	@Override
	public Integer currentMaxNrOfSameResultHash() {
		ListMultimap<Number160, IDomain> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		TreeSet<Integer> max = new TreeSet<>();
		for (Number160 resultHash : results.keySet()) {
			max.add(results.get(resultHash).size());
		}
		if (max.size() > 0) {
			return max.last();
		} else {
			return 0;
		}
	}

	@Override
	public AbstractFinishable nrOfSameResultHash(int nrOfSameResultHash) {
		if (nrOfSameResultHash > 0) {
			this.nrOfSameResultHash = nrOfSameResultHash;
		}

		return this;
	}

	@Override
	public AbstractFinishable addOutputDomain(IDomain domain) {
		if (!isFinished()) {
			this.outputDomains.add(domain);
		}
		return this;
	}

	@Override
	public String toString() {
		return "AbstractFinishable [resultOutputDomain=" + resultOutputDomain + ", outputDomains=" + outputDomains + ", nrOfSameResultHash="
				+ nrOfSameResultHash + ", isFinished()=" + isFinished() + "]";
	}

	@Override
	public AbstractFinishable needsMultipleDifferentDomains(boolean needsMultipleDifferentDomains) {
		this.needsMultipleDifferentDomains = needsMultipleDifferentDomains;
		return this;
	}

}