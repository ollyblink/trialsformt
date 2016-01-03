package mapreduce.execution;

import java.util.List;

/**
 * All finishable classes (Job, Procedure, Task) need this
 * @author Oliver
 *
 */
public interface IFinishable {
	public boolean isFinished();
	public void addOutputDomain(IDomain domain);
	public List<IDomain> outputDomains();
	public void calculateResultHash();
}
