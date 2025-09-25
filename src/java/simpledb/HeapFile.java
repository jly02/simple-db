package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /** The file backing this heap file. */
    private final File f;

    /** The scheme on this file. */
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            int pgNo = pid.getPageNumber();
            int pgSz = BufferPool.getPageSize();

            // Find start of page
            int off = pgNo * pgSz;

            // Store the bytes of the page
            byte[] data = new byte[pgSz];
            RandomAccessFile rf = new RandomAccessFile(this.f, "r");
            rf.seek(off);
            rf.read(data);
            rf.close();

            // Then we can construct the page
            HeapPage pg = new HeapPage((HeapPageId) pid, data);
            return pg;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // get offset
        int pageNo = page.getId().getPageNumber();
        int pageSz = BufferPool.getPageSize();
        int offset = pageNo * pageSz;

        // write
        RandomAccessFile rf = new RandomAccessFile(this.f, "rw");
        rf.seek(offset);
        rf.write(page.getPageData());
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        BufferPool bp = Database.getBufferPool();
        ArrayList<Page> affectedPgs = new ArrayList<>();
    
        // look for empty page
        for (int i = 0; i < numPages(); i++) {
            HeapPageId hid = new HeapPageId(getId(), i);
            HeapPage pg = (HeapPage) bp.getPage(tid, hid, Permissions.READ_ONLY);
            
            // found, so insert
            if (pg.getNumEmptySlots() > 0) {
                pg = (HeapPage) bp.getPage(tid, hid, Permissions.READ_WRITE);
                affectedPgs.add(pg);
                pg.insertTuple(t);
                return affectedPgs;
            }
        }
    
        // no room, make new page
        HeapPageId hid = new HeapPageId(getId(), numPages());
        HeapPage pg = new HeapPage(hid, HeapPage.createEmptyPageData());
        affectedPgs.add(pg);
        pg.insertTuple(t);
        writePage(pg);
        return affectedPgs;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        BufferPool bp = Database.getBufferPool();
        ArrayList<Page> affectedPgs = new ArrayList<>();
    
        // find tuple location
        PageId pid = t.getRecordId().getPageId();
        HeapPage pg = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
        affectedPgs.add(pg);
        pg.deleteTuple(t);
        return affectedPgs;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        /** An iterator along a single page. */
        private Iterator<Tuple> it;

        /** Other internal information. */
        private TransactionId tid;
        private HeapPage page;
        private boolean open;
        private int pgNo;

        private HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.open = false;
            this.pgNo = 0;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.open = true;
            this.pgNo = 0;

            HeapPageId hid = new HeapPageId(getId(), pgNo);
            this.page = (HeapPage) Database.getBufferPool()
                                        .getPage(tid, hid, Permissions.READ_ONLY);

            this.it = this.page.iterator();

            // in case the page is empty
            while (!it.hasNext()) {
                this.pgNo++;
                if (pgNo < numPages()) {
                    hid = new HeapPageId(getId(), pgNo);
                    this.page = (HeapPage) Database.getBufferPool()
                                        .getPage(tid, hid, Permissions.READ_ONLY);

                    this.it = page.iterator();
                } else {
                    return;
                }
            }
        }

        public void close() {
            super.close();
            open = false;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (!this.open || it == null) {
                return null;
            }

            if (it.hasNext()) {
                return it.next();
            }
            // in case the page is empty
            while (!it.hasNext()) {
                this.pgNo++;
                if (pgNo < numPages()) {
                    HeapPageId hid = new HeapPageId(getId(), pgNo);
                    this.page = (HeapPage) Database.getBufferPool()
                                        .getPage(tid, hid, Permissions.READ_ONLY);

                    this.it = page.iterator();
                } else {
                    return null;
                }
            }

            return it.next();   
        }
    }

}

