package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.Random;

public class KeyValuePair<KEY, VALUE> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6638101010868136679L;
	private static final Random RND = new Random();
	private long id;
	private KEY key;
	private VALUE value;

	public KeyValuePair(KEY key, VALUE value) {
		this.key = key;
		this.value = value;
		this.id = RND.nextLong();
	}

	public KEY key() {
		return this.key;
	}

	public VALUE value() {
		return this.value;
	}

	@Override
	public String toString() {
		return "<" + id + ", " + key + ", " + value + ">";
	}
 

}
