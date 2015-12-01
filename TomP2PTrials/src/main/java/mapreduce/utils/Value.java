package mapreduce.utils;

import java.io.Serializable;
import java.util.Random;

/**
 * To avoid problem of add().list() in dht that doesn't work
 * 
 * @author ozihler
 *
 */
public class Value implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6638101010868136679L;
	private static final Random RND = new Random();
	private long id;
	private Object value;

	public Value(Object value) {
		this.value = value;
		this.id = RND.nextLong() * RND.nextLong() * RND.nextLong();
	}

	public Object value() {
		return this.value;
	}

	@Override
	public String toString() {
		return "<" + id + ", " + value + ">";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
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
		Value other = (Value) obj;
		if (id != other.id)
			return false;
		return true;
	}

}
