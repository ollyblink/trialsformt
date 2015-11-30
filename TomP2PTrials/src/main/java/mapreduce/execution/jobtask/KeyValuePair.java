package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.Random;

public class KeyValuePair implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6638101010868136679L;
	private static final Random RND = new Random();
	private long id;
	private Object key;
	private Object value;

	public KeyValuePair(Object key, Object value) {
		this.key = key;
		this.value = value;
		this.id = RND.nextLong();
	}

	public Object key() {
		return this.key;
	}

	public Object value() {
		return this.value;
	}

	@Override
	public String toString() {
		return "<" + id + ", " + key + ", " + value + ">";
	}

}
