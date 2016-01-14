package mapreduce.execution.task.taskdatacomposing;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposerTest {

	@Test
	public void testCompose() {
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

	@Test
	public void testSplit() {
		MaxFileSizeTaskDataComposer instance = MaxFileSizeTaskDataComposer.create();
		instance.maxFileSize(FileSize.EIGHT_BYTES);
		instance.fileEncoding("UTF-8");
		instance.splitValue(" ");
		String[] results = { "this is ", "a test t", "his is a", " test" };

		String toSplit = "this is a test this is a test";
		List<String> splits = instance.splitToSize(toSplit);
		for (int i = 0; i < splits.size(); ++i) {
			assertEquals(results[i], splits.get(i));
		}
	}

}
