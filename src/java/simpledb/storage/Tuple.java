package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	private static final long serialVersionUID = 1L;
	private TupleDesc td;
	private RecordId recordId;
	private List<Field> fields;

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td the schema of this tuple. It must be a valid TupleDesc
	 *           instance with at least one field.
	 */
	public Tuple(TupleDesc td) {
		if (td == null || td.numFields() == 0) {
			throw new IllegalArgumentException("Invalid TupleDesc with null or zero field");
		}
		this.td = td;
		fields = new ArrayList<>(td.numFields());
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 * be null.
	 */
	public RecordId getRecordId() {
		return recordId;
	}

	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		this.recordId = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i index of the field to change. It must be a valid index.
	 * @param f new value for the field.
	 */
	public void setField(int i, Field f) {
		if (i < 0 || i >= td.numFields()) {
			throw new IllegalArgumentException(String.format("index of %d is out of fields range", i));
		}
		if (i >= fields.size()) {
			fields.add(f);
		} else {
			fields.set(i, f);
		}
	}

	/**
	 * @param i field index to return. Must be a valid index.
	 * @return the value of the ith field, or null if it has not been set.
	 */
	public Field getField(int i) {
		if (i < 0 || i >= fields.size()) {
			throw new IllegalArgumentException(String.format("index of %d is out of fields range", i));
		}
		return fields.get(i);
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 * <p>
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN
	 * <p>
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < fields.size(); i++) {
			Field field = fields.get(i);
			Object value = null;
			switch (field.getType()) {
				case INT_TYPE: {
					value = ((IntField)field).getValue();
					break;
				}
				case STRING_TYPE: {
					value = ((StringField)field).getValue();
					break;
				}
				default: {
					throw new IllegalArgumentException("un support field type");
				}
			}
			result.append(value.toString());
			if (i < fields.size() - 1) {
				result.append("\t");
			}
		}
		result.append(System.lineSeparator());
		return result.toString();
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		return fields.iterator();
	}

	/**
	 * reset the TupleDesc of this tuple (only affecting the TupleDesc)
	 */
	public void resetTupleDesc(TupleDesc td) {
		// some code goes here
	}
}
