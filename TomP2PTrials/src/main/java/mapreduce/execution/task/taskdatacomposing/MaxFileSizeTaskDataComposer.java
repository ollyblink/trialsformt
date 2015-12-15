package mapreduce.execution.task.taskdatacomposing;

import java.nio.charset.Charset;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposer implements ITaskDataComposer {
	private FileSize maxFileSize = FileSize.THIRTY_TWO_KILO_BYTE;
	private String fileEncoding = "UTF-8";
	private String splitValue = "\n";

	private long fileSizeCounter = 0;
	private String data = "";

	public static MaxFileSizeTaskDataComposer create() {
		return new MaxFileSizeTaskDataComposer();
	}

	@Override
	public String append(String value) {
		String nextValue = value + this.splitValue;
		long lineSize;
		lineSize = fileSizeCounter + (nextValue).getBytes(Charset.forName(this.fileEncoding)).length;

		if (lineSize >= maxFileSize.value()) {
			String result = data;
			reset();
			return result;
		} else {
			this.data += nextValue;
			this.fileSizeCounter = lineSize;
		}
		return null;
	}

	private void reset() {
		this.fileSizeCounter = 0;
		this.data = "";
	}
	@Override
	public String fileEncoding(){
		return this.fileEncoding;
	}

	public MaxFileSizeTaskDataComposer fileEncoding(String fileEncoding) {
		this.fileEncoding = fileEncoding;
		return this;
	}

	public MaxFileSizeTaskDataComposer maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public MaxFileSizeTaskDataComposer splitValue(String splitValue) {
		this.splitValue = splitValue;
		return this;
	}

}
