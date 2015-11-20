package firstdesignidea.execution.computation.context;

import firstdesignidea.storage.IDHTConnectionProvider;

public class DHTStorageContext implements IContext {

	private IDHTConnectionProvider dhtConnectionProvider;
	
	
	@Override
	public void write(Object keyOut, Object valueOut) {
//		dhtConnectionProvider.addDataForTask(taskId, keyOut, valueOut);
	}

}
