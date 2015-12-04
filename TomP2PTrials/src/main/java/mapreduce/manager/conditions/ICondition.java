package mapreduce.manager.conditions;

public interface ICondition<T> {
	public boolean metBy(T t);
}
