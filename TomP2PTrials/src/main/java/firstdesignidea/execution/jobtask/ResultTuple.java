package firstdesignidea.execution.jobtask;

public class ResultTuple<Key, Value> {
	Key key;
	Value value;

	public ResultTuple(Key key, Value value) {
		this.key = key;
		this.value = value;
	}

	public Key key() {
		return key;
	}

	public void key(Key key) {
		this.key = key;
	}

	public Value value() {
		return value;
	}

	public void value(Value value) {
		this.value = value;
	}
}
