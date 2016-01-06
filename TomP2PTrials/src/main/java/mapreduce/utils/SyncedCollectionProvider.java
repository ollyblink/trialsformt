package mapreduce.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class SyncedCollectionProvider {

	public static <T> List<T> syncedArrayList() {
		return Collections.synchronizedList(new ArrayList<T>());
	}

	public static <K, V> Map<K, V> syncedHashMap() {
		return Collections.synchronizedMap(new HashMap<K, V>());
	}

	public static <K, V> SortedMap<K, V> syncedTreeMap() {
		return Collections.synchronizedSortedMap(new TreeMap<K, V>());
	}

	public static <K, V> ListMultimap<K, V> syncedListMultimap() {
		ArrayListMultimap<K, V> tmp3 = ArrayListMultimap.create();
		return Multimaps.synchronizedListMultimap(tmp3);

	}

	public static <T> SortedSet<T> syncedTreeSet() {
		return Collections.synchronizedSortedSet(new TreeSet<T>());
	}
}
