package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * 对string类型的单字段做分组聚合
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int groupByFieldIndex;
	private int aggregateFieldIndex;
	private Map<Field, Integer> fieldWithTupleIndex;
	private TupleDesc tupleDesc;
	private List<Tuple> aggregateTuples;
	private OpIterator opIterator;
	private Iterator<Tuple> iterator;

	/**
	 * Aggregate constructor
	 *
	 * @param groupByFieldIndex   the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param groupByFieldType    the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param aggregateFieldIndex the 0-based index of the aggregate field in the tuple
	 * @param op                  aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int groupByFieldIndex, Type groupByFieldType, int aggregateFieldIndex, Op op) {
		this.groupByFieldIndex = groupByFieldIndex;
		this.aggregateFieldIndex = aggregateFieldIndex;
		fieldWithTupleIndex = new HashMap<>();
		tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
		aggregateTuples = new ArrayList<>();
		opIterator = new Operator() {
			@Override
			protected Tuple fetchNext() throws DbException, TransactionAbortedException {
				while (iterator.hasNext()) {
					Tuple tuple = iterator.next();
					// 如果不分组只聚合的话，不能带上分组field
					if (groupByFieldIndex == NO_GROUPING) {
						Field valueField = tuple.getField(1);
						tuple = new Tuple(new TupleDesc(new Type[]{tupleDesc.getFieldType(aggregateFieldIndex)}));
						tuple.setField(0, valueField);
					}
					return tuple;
				}
				return null;
			}

			@Override
			public OpIterator[] getChildren() {
				return new OpIterator[0];
			}

			@Override
			public void setChildren(OpIterator[] children) {

			}

			@Override
			public TupleDesc getTupleDesc() {
				return tupleDesc;
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				iterator = aggregateTuples.iterator();
			}
		};
	}

	/**
	 * 把string类型的field聚合，只做count聚合
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		Field groupByField;
		if (groupByFieldIndex == NO_GROUPING) {
			groupByField = new IntField(Integer.MAX_VALUE);
		} else {
			groupByField = tup.getField(groupByFieldIndex);
		}
		Integer index = fieldWithTupleIndex.get(groupByField);
		if (index == null) {
			Tuple tuple = new Tuple(tupleDesc);
			tuple.setField(0, groupByField);
			tuple.setField(1, new IntField(1));
			fieldWithTupleIndex.put(groupByField, aggregateTuples.size());
			aggregateTuples.add(tuple);
		} else {
			Tuple tuple = aggregateTuples.get(index);
			tuple.setField(1, new IntField(((IntField) tuple.getField(1)).getValue() + 1));
		}
		iterator = aggregateTuples.iterator();
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal,
	 * aggregateVal) if using group, or a single (aggregateVal) if no
	 * grouping. The aggregateVal is determined by the type of
	 * aggregate specified in the constructor.
	 */
	public OpIterator iterator() {
		return opIterator;
	}
}
