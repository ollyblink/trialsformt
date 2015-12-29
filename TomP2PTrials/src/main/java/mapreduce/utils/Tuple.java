package mapreduce.utils;

import java.io.Serializable;

import net.tomp2p.peers.PeerAddress;

/**
 * Generic tupel where tupels are needed...
 * 
 * @author ozihler
 *
 * @param <A>
 * @param <B>
 */
public final class Tuple<A extends Comparable<A>, B> implements Serializable, Comparable<Tuple<A, B>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7791705958098258390L;
	private final A first;
	private final B second;

	private Tuple(final A first, final B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<A>, B> Tuple<A, B> create(final A first, final B second) {
		return new Tuple<A, B>(first, second);
	}

	public final A first() {
		return first;
	}

	public final B second() {
		return second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
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
		Tuple other = (Tuple) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Tuple [first=" + first + ", second=" + second + "]";
	}

	@Override
	public int compareTo(Tuple<A, B> o) {
		return first.compareTo(o.first);
	}

}
