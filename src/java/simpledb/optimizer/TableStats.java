package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	private Map<Integer, IntHistogram> fieldIndexWithIntHistogram;
	private Map<Integer, StringHistogram> fieldIndexWithStringHistogram;
	private TupleDesc tupleDesc;

	private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

	static final int IO_COST_PER_PAGE = 1000;
	private int tupleCount;
	private int ioCostPerPage;

	public static TableStats getTableStats(String tablename) {
		return statsMap.get(tablename);
	}

	public static void setTableStats(String tablename, TableStats stats) {
		statsMap.put(tablename, stats);
	}

	public static void setStatsMap(Map<String, TableStats> s) {
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
			e.printStackTrace();
		}

	}

	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}

	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);
		}
		System.out.println("Done.");
	}

	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 *
	 * @param tableid       The table over which to compute statistics
	 * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
	 *                      sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		this.ioCostPerPage = ioCostPerPage;
		fieldIndexWithIntHistogram = new HashMap<>();
		fieldIndexWithStringHistogram = new HashMap<>();
		DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
		DbFileIterator dbFileIterator = dbFile.iterator(new TransactionId());
		TupleDesc tupleDesc = dbFile.getTupleDesc();
		this.tupleDesc = tupleDesc;
		Map<Integer, IntFieldHistogram> fieldIndexWithIntHistogram = new HashMap<>();
		Map<Integer, StringFieldHistogram> fieldIndexWithStringHistogram = new HashMap<>();
		try {
			dbFileIterator.open();
			while (dbFileIterator.hasNext()) {
				Tuple tuple = dbFileIterator.next();
				tupleCount += 1;
				for (int i = 0; i < tupleDesc.numFields(); i++) {
					if (tuple.getField(i) instanceof IntField) {
						IntField intField = (IntField) tuple.getField(i);
						if (fieldIndexWithIntHistogram.containsKey(i)) {
							IntFieldHistogram intFieldHistogram = fieldIndexWithIntHistogram.get(i);
							intFieldHistogram.values.add(intField.getValue());
							intFieldHistogram.max = Math.max(intField.getValue(), intFieldHistogram.max);
							intFieldHistogram.min = Math.min(intField.getValue(), intFieldHistogram.min);
						} else {
							IntFieldHistogram intFieldHistogram = new IntFieldHistogram();
							intFieldHistogram.values.add(intField.getValue());
							intFieldHistogram.max = intField.getValue();
							intFieldHistogram.min = intField.getValue();
							fieldIndexWithIntHistogram.put(i, intFieldHistogram);
						}
					} else {
						StringField stringField = ((StringField) tuple.getField(i));
						if (fieldIndexWithStringHistogram.containsKey(i)) {
							StringFieldHistogram stringFieldHistogram = fieldIndexWithStringHistogram.get(i);
							stringFieldHistogram.values.add(stringField.getValue());
						} else {
							StringFieldHistogram stringFieldHistogram = new StringFieldHistogram();
							stringFieldHistogram.values.add(stringField.getValue());
							fieldIndexWithStringHistogram.put(i, stringFieldHistogram);
						}
					}
				}
			}
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
		fieldIndexWithIntHistogram.forEach((index, intFieldHistogram) -> {
			this.fieldIndexWithIntHistogram.put(index, new IntHistogram(intFieldHistogram.getBuckets(), intFieldHistogram.min, intFieldHistogram.max));
			for (int v : intFieldHistogram.values) {
				this.fieldIndexWithIntHistogram.get(index).addValue(v);
			}
		});
		fieldIndexWithStringHistogram.forEach((index, stringFieldHistogram) -> {
			this.fieldIndexWithStringHistogram.put(index, new StringHistogram(stringFieldHistogram.values.size()));
			for (String v : stringFieldHistogram.values) {
				this.fieldIndexWithStringHistogram.get(index).addValue(v);
			}
		});
	}

	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * <p>
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 *
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		return ioCostPerPage * (tupleCount * tupleDesc.getSize() / (BufferPool.getPageSize() * 1.0));
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 *
	 * @param selectivityFactor The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 * selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		// some code goes here
		return (int) (tupleCount * selectivityFactor);
	}

	/**
	 * The average selectivity of the field under op.
	 *
	 * @param field the index of the field
	 * @param op    the operator in the predicate
	 *              The semantic of the method is that, given the table, and then given a
	 *              tuple, of which we do not know the value of the field, return the
	 *              expected selectivity. You may estimate this value from the histograms.
	 */
	public double avgSelectivity(int field, Predicate.Op op) {
		// some code goes here
		return 1.0;
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 *
	 * @param field    The field over which the predicate ranges
	 * @param op       The logical operation in the predicate
	 * @param constant The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 * predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		if (fieldIndexWithIntHistogram.containsKey(field)) {
			return fieldIndexWithIntHistogram.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
		} else {
			return fieldIndexWithStringHistogram.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
		}
	}

	/**
	 * return the total number of tuples in this table
	 */
	public int totalTuples() {
		return tupleCount;
	}

	private static class IntFieldHistogram {
		List<Integer> values;
		int min;
		int max;

		IntFieldHistogram() {
			values = new ArrayList<>();
			min = Integer.MAX_VALUE;
			max = Integer.MIN_VALUE;
		}

		public int getBuckets() {
			if (values.size() > (max - min + 1)) {
				return max - min + 1;
			}
			return (max - min + 1) / values.size();
		}
	}

	private static class StringFieldHistogram {
		List<String> values;

		StringFieldHistogram() {
			values = new ArrayList<>();
		}
	}
}
