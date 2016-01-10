package mapreduce.execution;

import java.io.Serializable;

import net.tomp2p.peers.Number160;

public interface IDomain extends Serializable, Cloneable {

	public Number160 resultHash();

	public IDomain resultHash(Number160 resultHash);

	public String executor();

	@Override
	public int hashCode();

	@Override
	public boolean equals(Object obj);

	@Override
	public String toString();

	Object clone() throws CloneNotSupportedException;

	public Integer procedureIndex();

}
