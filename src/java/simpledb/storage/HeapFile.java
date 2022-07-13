package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;
    private int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.id = new Random().nextInt();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * 从file中读取指定page，需要计算offset,使用RandomAccessFile读取，offset=pageNumber * pageSize
     * */
    public Page readPage(PageId pid) {
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(offset);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(bytes);
            return new HeapPage((HeapPageId) pid, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, numPages(), id);
    }
}

class HeapFileIterator extends AbstractDbFileIterator {
    private Iterator<Tuple> iterator;
    private HeapPage page;
    private int numPages;
    private int pageNumber;
    private TransactionId tid;
    private int tableId;

    public HeapFileIterator(TransactionId tid, int numPages, int tableId) {
        this.tid = tid;
        this.numPages = numPages;
        this.tableId = tableId;
        pageNumber = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            pageNumber += 1;
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNumber), null);
            iterator = page.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
        }
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNumber), null);
        iterator = page.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
//        page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNumber), null);
        iterator = page.iterator();
    }

    @Override
    public void close() {
        super.close();
        iterator = null;
    }
}
