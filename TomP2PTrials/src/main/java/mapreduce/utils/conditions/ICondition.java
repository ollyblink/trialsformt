package mapreduce.utils.conditions;

public interface ICondition<T> {
	public boolean metBy(T t);
}
