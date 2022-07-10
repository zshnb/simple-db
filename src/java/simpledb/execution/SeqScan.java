package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

	private static final long serialVersionUID = 1L;
	private String alias;
	private DbFile dbFile;
	private TransactionId transactionId;
	private String tableName;
	private DbFileIterator iterator;

	/**
	 * Creates a sequential scan over the specified table as a part of the
	 * specified transaction.
	 *
	 * @param tid        The transaction this scan is running as a part of.
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the returned
	 *                   tupleDesc should have fields with name tableAlias.fieldName
	 *                   (note: this class is not responsible for handling a case where
	 *                   tableAlias or fieldName are null. It shouldn't crash if they
	 *                   are, but the resulting name can be null.fieldName,
	 *                   tableAlias.null, or null.null).
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		transactionId = tid;
		alias = tableAlias;
		dbFile = Database.getCatalog().getDatabaseFile(tableid);
		tableName = Database.getCatalog().getTableName(tableid);
	}

	/**
	 * @return return the table name of the table the operator scans. This should
	 * be the actual name of the table in the catalog of the database
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return Return the alias of the table this operator scans.
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 *
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the returned
	 *                   tupleDesc should have fields with name tableAlias.fieldName
	 *                   (note: this class is not responsible for handling a case where
	 *                   tableAlias or fieldName are null. It shouldn't crash if they
	 *                   are, but the resulting name can be null.fieldName,
	 *                   tableAlias.null, or null.null).
	 */
	public void reset(int tableid, String tableAlias) {
		this.alias = tableAlias;
	}

	public SeqScan(TransactionId tid, int tableId) {
		this(tid, tableId, Database.getCatalog().getTableName(tableId));
	}

	public void open() throws DbException, TransactionAbortedException {
		iterator = dbFile.iterator(transactionId);
		iterator.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor. This prefix
	 * becomes useful when joining tables containing a field(s) with the same
	 * name.  The alias and name should be separated with a "." character
	 * (e.g., "alias.fieldName").
	 *
	 * @return the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		TupleDesc origin = dbFile.getTupleDesc();
		Type[] types = new Type[origin.numFields()];
		String[] fields = new String[origin.numFields()];
		for (int i = 0; i < types.length; i++) {
			types[i] = origin.getFieldType(i);
			fields[i] = String.format("%s.%s", alias, origin.getFieldName(i));
		}
		return new TupleDesc(types, fields);
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		return iterator.hasNext();
	}

	public Tuple next() throws NoSuchElementException,
		TransactionAbortedException, DbException {
		return iterator.next();
	}

	public void close() {
		iterator.close();
	}

	public void rewind() throws DbException, NoSuchElementException,
		TransactionAbortedException {
		iterator.rewind();
	}
}
