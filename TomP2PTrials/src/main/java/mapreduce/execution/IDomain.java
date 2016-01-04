package mapreduce.execution;

import java.io.Serializable;

import net.tomp2p.peers.Number160;

public interface IDomain extends Serializable {

	public Number160 resultHash();

	public void resultHash(Number160 resultHash);

	public String executor();

	public long creationTime();

	public int submissionCount();

	public void incrementSubmissionCount();

	@Override
	public int hashCode();

	@Override
	public boolean equals(Object obj);

	@Override
	public String toString();
}
