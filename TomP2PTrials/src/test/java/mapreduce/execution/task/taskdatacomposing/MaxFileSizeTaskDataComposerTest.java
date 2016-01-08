package mapreduce.execution.task.taskdatacomposing;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.junit.Test;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposerTest {

	@Test
	public void test() {
		MaxFileSizeTaskDataComposer instance = MaxFileSizeTaskDataComposer.create();
		instance.maxFileSize(FileSize.THIRTY_TWO_BYTES);
		instance.fileEncoding("UTF-8");
		instance.splitValue(" ");
		String value = null;
		String toAdd = "Text";
		long counter = 0;
		while ((value = instance.append(toAdd)) == null) {
			++counter;
		}

		String compare = "";
		for (int i = 0; i < counter; ++i) {
			compare += (toAdd + " ");
		}
		// Asserts that the file size of the appended value stays below the specified limit
		assertEquals(compare, value);
		assertEquals(counter * (toAdd + " ").getBytes(Charset.forName("UTF-8")).length, value.getBytes(Charset.forName("UTF-8")).length);
		assertEquals(true, FileSize.THIRTY_TWO_BYTES.value() >= value.getBytes(Charset.forName("UTF-8")).length);

		// Assert that all the data is still stored and may be retrieved
		value = instance.currentValues();
		compare = (toAdd + " ");

		assertEquals(compare, value);
		assertEquals((toAdd + " ").getBytes(Charset.forName("UTF-8")).length, value.getBytes(Charset.forName("UTF-8")).length);
		assertEquals(true, FileSize.THIRTY_TWO_BYTES.value() >= value.getBytes(Charset.forName("UTF-8")).length);

	}

}
