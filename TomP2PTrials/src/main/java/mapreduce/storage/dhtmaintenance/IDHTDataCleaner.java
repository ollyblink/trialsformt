package mapreduce.storage.dhtmaintenance;

import com.google.common.collect.Multimap;

import mapreduce.execution.task.Task;
import mapreduce.storage.LocationBean;
import mapreduce.utils.IAbortableExecution;

public interface IDHTDataCleaner extends IAbortableExecution{

	public void removeDataFromDHT(Multimap<Task, LocationBean> dataToRemove);

}
