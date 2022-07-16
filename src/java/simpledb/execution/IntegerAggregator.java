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
 * 对int类型的单字段做分组聚合
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int groupByFieldIndex;
	private int aggregateFieldIndex;
	private Op op;
	private Map<Field, Integer> fieldWithTupleIndex;
	private Map<Field, Integer> fieldWithSameCount;
	private TupleDesc tupleDesc;
	private List<Tuple> aggregateTuples;
	private OpIterator opIterator;
	private Iterator<Tuple> iterator;

	/**
	 * Aggregate constructor
	 *
	 * @param groupByFieldIndex   the 0-based index of the group-by field in the tuple, or
	 *                            NO_GROUPING if there is no grouping
	 * @param groupByFieldType    the type of the group by field (e.g., Type.INT_TYPE), or null
	 *                            if there is no grouping
	 * @param aggregateFieldIndex the 0-based index of the aggregate field in the tuple
	 * @param op                  the aggregation operator
	 */

	public IntegerAggregator(int groupByFieldIndex, Type groupByFieldType, int aggregateFieldIndex, Op op) {
		this.groupByFieldIndex = groupByFieldIndex;
		this.aggregateFieldIndex = aggregateFieldIndex;
		this.op = op;
		fieldWithTupleIndex = new HashMap<>();
		fieldWithSameCount = new HashMap<>();
		tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
		aggregateTuples = new ArrayList<>();
		opIterator = new Operator() {
			@Override
			protected Tuple fetchNext() throws DbException, TransactionAbortedException {
				while (iterator.hasNext()) {
					Tuple tuple = iterator.next();
					// sum和avg在merge的行为是一样的都是求和，avg在获取聚合结果时要做除法处理
					if (op.equals(Op.AVG)) {
						Field avgField = new IntField(((IntField)tuple.getField(1)).getValue() / fieldWithSameCount.get(getGroupByField(tuple)));
						Field groupByField = getGroupByField(tuple);
						tuple = new Tuple(getTupleDesc());
						tuple.setField(0, groupByField);
						tuple.setField(1, avgField);
					}
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

	private Field getGroupByField(Tuple tup) {
		Field groupByField;
		if (groupByFieldIndex == NO_GROUPING) {
			groupByField = new IntField(Integer.MAX_VALUE);
		} else {
			groupByField = tup.getField(groupByFieldIndex);
		}
		return groupByField;
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		Field aggregateField = tup.getField(aggregateFieldIndex);
		Field groupByField = getGroupByField(tup);
		Integer index = fieldWithTupleIndex.get(groupByField);
		if (index == null) {
			Tuple tuple = new Tuple(tupleDesc);
			tuple.setField(0, groupByField);
			if (op.equals(Op.COUNT)) {
				tuple.setField(1, new IntField(1));
			} else {
				tuple.setField(1, aggregateField);
			}
			fieldWithTupleIndex.put(groupByField, aggregateTuples.size());
			fieldWithSameCount.put(groupByField, 1);
			aggregateTuples.add(tuple);
		} else {
			fieldWithSameCount.put(groupByField, fieldWithSameCount.get(groupByField) + 1);
			Tuple tuple = aggregateTuples.get(index);
			int aggregateFieldValue = ((IntField) aggregateField).getValue();
			int tupleFieldValue = ((IntField) tuple.getField(1)).getValue();
			switch (op) {
				case AVG:
				case SUM: {
					tuple.setField(1, new IntField(aggregateFieldValue + tupleFieldValue));
					break;
				}
				case MIN: {
					tuple.setField(1, new IntField(Math.min(aggregateFieldValue, tupleFieldValue)));
					break;
				}
				case MAX: {
					tuple.setField(1, new IntField(Math.max(aggregateFieldValue, tupleFieldValue)));
					break;
				}
				case COUNT: {
					tuple.setField(1, new IntField(tupleFieldValue + 1));
					break;
				}
			}
		}
		iterator = aggregateTuples.iterator();
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
	 * if using group, or a single (aggregateVal) if no grouping. The
	 * aggregateVal is determined by the type of aggregate specified in
	 * the constructor.
	 */
	public OpIterator iterator() {
		return opIterator;
	}
}
