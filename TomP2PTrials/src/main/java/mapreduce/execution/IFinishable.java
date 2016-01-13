package mapreduce.execution;

import net.tomp2p.peers.Number160;

/**
 * All finishable classes (Job, Procedure, Task) need this
 * 
 * @author Oliver
 *
 */
public interface IFinishable {
	public boolean isFinished();

	public IFinishable needsMultipleDifferentDomains(boolean needsMultipleDifferentDomains);

	public IFinishable addOutputDomain(IDomain domain);

	public Number160 calculateResultHash();

	public IDomain resultOutputDomain();

	public int nrOfOutputDomains();

	public IFinishable nrOfSameResultHash(int nrOfSameResultHash);

	public Integer currentMaxNrOfSameResultHash();

}
