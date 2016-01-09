package mapreduce.execution.task;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public abstract class AbstractFinishable implements IFinishable {

	/** final output domain for where this tasks output key/values are stored */
	protected IDomain resultOutputDomain;
	/** Domain (ExecutorTaskDomains) of keys for next procedure */
	protected List<IDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	protected int nrOfSameResultHash = 1; // Needs at least one
	

	public AbstractFinishable() {
		this.outputDomains = SyncedCollectionProvider.syncedArrayList();
	}

	@Override
	public boolean isFinished() {
		checkIfFinished();
		return resultOutputDomain != null ;
	}

	@Override
	public AbstractFinishable addOutputDomain(IDomain domain) {
		this.outputDomains.add(domain);
		return this;
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
		ListMultimap<Number160, IDomain> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
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
	public AbstractFinishable nrOfSameResultHash(int nrOfSameResultHash) {
		if (nrOfSameResultHash > 1) {
			this.nrOfSameResultHash = nrOfSameResultHash;
		}

		return this;
	}

}