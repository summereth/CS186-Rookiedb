package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);
        // append commit record and flush logs
        CommitTransactionLogRecord logRecord = new CommitTransactionLogRecord(transNum, xactEntry.lastLSN);
        long commitLSN = logManager.appendToLog(logRecord);
        logManager.flushToLSN(commitLSN);
        // update xact table
        xactEntry.lastLSN = commitLSN;
        xactEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        return commitLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);
        // append abort record
        AbortTransactionLogRecord logRecord = new AbortTransactionLogRecord(transNum, xactEntry.lastLSN);
        long abortLSN = logManager.appendToLog(logRecord);
        // update xact table
        xactEntry.lastLSN = abortLSN;
        xactEntry.transaction.setStatus(Transaction.Status.ABORTING);

        return abortLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);
        if (xactEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0); // roll back to the beginning of the xact
        }
        // remove xact from the xact table
        transactionTable.remove(transNum);
        // append end log
        EndTransactionLogRecord logRecord = new EndTransactionLogRecord(transNum, xactEntry.lastLSN);
        long endLSN = logManager.appendToLog(logRecord);
        // update xact status
        xactEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        return endLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * The general idea is starting the LSN of the most recent record that hasn't
     * been undone:
     * - while the current LSN is greater than the LSN we're rolling back to
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Flush if necessary
     *       - Update the dirty page table if necessary in the following cases:
     *          - You undo an update page record (this is the same as applying
     *            the original update in reverse, which would dirty the page)
     *          - You undo alloc page page record (note that freed pages are no
     *            longer considered dirty)
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5_part1) implement the rollback logic described above
        while (currentLSN > LSN) {
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                currentLSN = undoLog(logRecord);

                // For testing
//                System.out.println("XACT Table: " + transactionEntry);
//                if (logRecord.getPageNum().isPresent()) {
//                    System.out.println("DPT: { pageNum: " + logRecord.getPageNum().get() + ", recLSN: " + dirtyPageTable.getOrDefault(logRecord.getPageNum().get(), -1L) + " }");
//                }
            } else {
                currentLSN = logRecord.getPrevLSN().orElse(-1L);
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5_part1): implement
        assert pageNum != 0; // This method is never called on a log page
        assert transactionTable.containsKey(transNum);
        TransactionTableEntry xactEntry = transactionTable.get(transNum);

        // check if the log can be written in one page
        if (after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            // break the log into 2 logs
            LogRecord undoOnly = new UpdatePageLogRecord(transNum, pageNum, xactEntry.lastLSN, pageOffset, before, null);
            long undoLSN = logManager.appendToLog(undoOnly);
            LogRecord redoOnly = new UpdatePageLogRecord(transNum, pageNum, undoLSN, pageOffset, null, after);
            xactEntry.lastLSN = logManager.appendToLog(redoOnly);
            if (!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, undoLSN);
            }
        } else {
            LogRecord logRecord = new UpdatePageLogRecord(transNum, pageNum, xactEntry.lastLSN, pageOffset, before, after);
            xactEntry.lastLSN = logManager.appendToLog(logRecord);
            if (!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, xactEntry.lastLSN);
            }
        }
        xactEntry.touchedPages.add(pageNum);

        // For testing
//        System.out.println("XACT Table: " + xactEntry);
//        System.out.println("DPT: { pageNum: " + pageNum + ", recLSN: " + dirtyPageTable.getOrDefault(pageNum, -1L) + " }");

        return xactEntry.lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5_part1): implement
        rollbackToLSN(transNum, LSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();
        Map<Long, List<Long>> chkptTouchedPages = new HashMap<>();

        // TODO(proj5_part1): generate end checkpoint record(s) for DPT and transaction table
        List<Long> dirtyPages = new ArrayList<>(dirtyPageTable.keySet());
        List<Long> transactions = new ArrayList<>(transactionTable.keySet());
        int dirtyPageIdx = 0, txnIdx = 0;

        if (dirtyPages.isEmpty() && transactions.isEmpty()) {
            EndCheckpointLogRecord logRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable, chkptTouchedPages);
            logManager.appendToLog(logRecord);
            // Ensure checkpoint is fully flushed before updating the master record
            logManager.flushToLSN(logRecord.getLSN());
            return;
        }

        while (dirtyPageIdx < dirtyPages.size() || txnIdx < transactions.size()) {

            // First, iterate through the dirtyPageTable and copy the entries.
            // Stop if the current record would cause the end checkpoint record to be too large.
            while (dirtyPageIdx < dirtyPages.size() && EndCheckpointLogRecord.fitsInOneRecord(dirtyPageIdx + 1, 0, 0, 0)) {
                chkptDPT.put(dirtyPages.get(dirtyPageIdx), dirtyPageTable.get(dirtyPages.get(dirtyPageIdx)));
                dirtyPageIdx++;
            }

            // Then, iterate through transactionTable
            int numTouchedPages = 0;
            if (txnIdx < transactions.size()) numTouchedPages = transactionTable.get(transactions.get(txnIdx)).touchedPages.size();
            while (txnIdx < transactions.size()
                    && EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), txnIdx + 1, txnIdx + 1, numTouchedPages)) {

                long transNum = transactions.get(txnIdx);
                TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                chkptTxnTable.put(transNum,
                        new Pair<>(transactionEntry.transaction.getStatus(), transactionEntry.lastLSN));
                chkptTouchedPages.put(transNum, new ArrayList<>());
                for (long touchedPage : transactionEntry.touchedPages) {
                    chkptTouchedPages.get(transNum).add(touchedPage);
                }

                txnIdx++;
                if (txnIdx < transactions.size()) numTouchedPages += transactionTable.get(transactions.get(txnIdx)).touchedPages.size();
            }
            EndCheckpointLogRecord logRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable, chkptTouchedPages);
            logManager.appendToLog(logRecord);
            // Ensure checkpoint is fully flushed before updating the master record
            logManager.flushToLSN(logRecord.getLSN());
            chkptDPT.clear();
            chkptTxnTable.clear();
            chkptTouchedPages.clear();
        }

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.cleanDPT();

        return () -> {
            this.restartUndo();
            this.checkpoint();
        };
    }

    private void endingTransactionsAfterAnalysis() {
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionEntry = entry.getValue();
            if (transactionEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                // all transactions in the COMMITTING state should be ended (cleanup(), state set to COMPLETE, end
                // transaction record written, and removed from the transaction table).
                transactionEntry.transaction.cleanup();
                end(transNum);
            } else if (transactionEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                // all transactions in the RUNNING state should be moved into the RECOVERY_ABORTING state, and an
                // abort transaction record should be written.
                abort(transNum);
                transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            } else if (transactionEntry.transaction.getStatus() == Transaction.Status.COMPLETE) {
                transactionTable.remove(transNum);
            }
        }
    }

    private void analyzePageOperationRelatedLog(LogRecord logRecord, TransactionTableEntry transactionEntry) {
        long pageNum = logRecord.getPageNum().get();
        // Add to touchedPages
        transactionEntry.touchedPages.add(pageNum);
        // Acquire X lock
        acquireTransactionLock(transactionEntry.transaction, getPageLockContext(pageNum), LockType.X);
        // Update DPT
        if (logRecord.getType() == LogType.UPDATE_PAGE || logRecord.getType() == LogType.UNDO_UPDATE_PAGE) {
            // UpdatePage/UndoUpdatePage both may dirty a page in memory, without flushing changes to disk.
            if (!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, logRecord.getLSN());
            }
        } else if (logRecord.getType() == LogType.FREE_PAGE || logRecord.getType() == LogType.UNDO_ALLOC_PAGE) {
            // FreePage/UndoAllocPage both make their changes visible on disk immediately, and can be seen
            // as flushing the freed page to disk (remove page from DPT)
            dirtyPageTable.remove(pageNum);
        }
        // No need to update DPT for AllocPage/UndoFreePage
    }

    private void analyzeEndCheckPointLog(EndCheckpointLogRecord logRecord) {
        Map<Long, Long> chkptDPT = logRecord.getDirtyPageTable();
        Map<Long, Pair<Transaction. Status, Long>> chkptTxnTable = logRecord.getTransactionTable();
        Map<Long, List<Long>> chkptTouchedPages = logRecord.getTransactionTouchedPages();

        // update DPT with recLSN from endpoint's DPT
        for (long pageNum : chkptDPT.keySet()) {
            dirtyPageTable.put(pageNum, chkptDPT.get(pageNum));
        }

        // update txnTable with endpoint's txnTable
        for (long transNum : chkptTxnTable.keySet()) {
            if (!transactionTable.containsKey(transNum)) {
                Transaction transaction = newTransaction.apply(transNum);
                startTransaction(transaction);
            }
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            transactionEntry.lastLSN =
                    Math.max(transactionEntry.lastLSN, chkptTxnTable.get(transNum).getSecond());
            if (transactionStatusTransitionable(transactionEntry.transaction.getStatus(), chkptTxnTable.get(transNum).getFirst())) {
                transactionEntry.transaction.setStatus(chkptTxnTable.get(transNum).getFirst());
            }
        }

        // update touchedPages and acquire X locks
        for (long transNum : chkptTouchedPages.keySet()) {
            if (transactionTable.containsKey(transNum)) {
                for (long touchedPage : chkptTouchedPages.get(transNum)) {
                    transactionTable.get(transNum).touchedPages.add(touchedPage);
                    acquireTransactionLock(transactionTable.get(transNum).transaction, getPageLockContext(touchedPage), LockType.X);
                }
            }
        }
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (free/undoalloc always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     * - The status's in the transaction table should be updated if its possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> committing is a possible transition,
     *   but committing -> running is not.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING. Remove
     * transactions in the COMPLETE state from the transaction table.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5_part2): implement
        Iterator<LogRecord> ite = logManager.scanFrom(LSN);
        while (ite.hasNext()) {
            LogRecord logRecord = ite.next();
            // Transaction operations related log
            if (logRecord.getTransNum().isPresent()) {
                long transNum = logRecord.getTransNum().get();
                if (!transactionTable.containsKey(transNum)) {
                    Transaction transaction = newTransaction.apply(transNum);
                    startTransaction(transaction);
                }
                TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                transactionEntry.lastLSN =
                        Math.max(transactionEntry.lastLSN, logRecord.getLSN());

                // Page operations related log
                if (logRecord.getPageNum().isPresent()) {
                    analyzePageOperationRelatedLog(logRecord, transactionEntry);
                }

                // Transaction status related log
                if (logRecord.getType() == LogType.COMMIT_TRANSACTION) {
                    // transaction table updated before
                    // update transaction status
                    transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                } else if (logRecord.getType() == LogType.ABORT_TRANSACTION) {
                    // transaction table updated before
                    // update transaction status
                    transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else if (logRecord.getType() == LogType.END_TRANSACTION) {
                    transactionEntry.transaction.cleanup();
                    transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                }
            }

            // Checkpoint record
            if (logRecord.getType() == LogType.BEGIN_CHECKPOINT) {
                BeginCheckpointLogRecord beginRecord = (BeginCheckpointLogRecord) logRecord;
                if (beginRecord.getMaxTransactionNum().isPresent()) {
                    updateTransactionCounter.accept(beginRecord.getMaxTransactionNum().get());
                }
            }
            if (logRecord.getType() == LogType.END_CHECKPOINT) {
                analyzeEndCheckPointLog((EndCheckpointLogRecord) logRecord);
            }
        }

        endingTransactionsAfterAnalysis();
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5_part2): implement
        Set<LogType> partTypes = Set.of(LogType.ALLOC_PART, LogType.UNDO_ALLOC_PART, LogType.FREE_PART, LogType.UNDO_FREE_PART);
        Set<LogType> allocPageTypes = Set.of(LogType.ALLOC_PAGE, LogType.UNDO_FREE_PAGE);
        Set<LogType> modPageTypes = Set.of(LogType.FREE_PAGE, LogType.UNDO_ALLOC_PAGE, LogType.UPDATE_PAGE, LogType.UNDO_UPDATE_PAGE);

        long LSN = Long.MAX_VALUE;
        for (long recLSN : dirtyPageTable.values()) {
            LSN = Math.min(LSN, recLSN);
        }
        Iterator<LogRecord> ite = logManager.scanFrom(LSN);
        while (ite.hasNext()) {
            LogRecord logRecord = ite.next();
            if (!logRecord.isRedoable()) continue;

            if (partTypes.contains(logRecord.getType()) || allocPageTypes.contains(logRecord.getType())) {
                logRecord.redo(diskSpaceManager, bufferManager);
            } else if (modPageTypes.contains(logRecord.getType())) {
                assert logRecord.getPageNum().isPresent();
                long pageNum = logRecord.getPageNum().get();
                if (!dirtyPageTable.containsKey(pageNum) || dirtyPageTable.get(pageNum) > logRecord.getLSN()) continue;

                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                try {
                    long pageLSN = page.getPageLSN();
                    if (pageLSN < logRecord.getLSN()) {
                        logRecord.redo(diskSpaceManager, bufferManager);
                    }
                } finally {
                    page.unpin();
                }
            }
        }

    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5_part2): implement
        // create priority queue sorted on lastLSN of all aborting transactions.
        PriorityQueue<LogRecord> toUndo = new PriorityQueue<>(
                (a, b) -> Long.compare(b.getLSN(), a.getLSN())
        );
        for (TransactionTableEntry txnEntry : transactionTable.values()) {
            if (txnEntry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                toUndo.offer(logManager.fetchLogRecord(txnEntry.lastLSN));
            }
        }

        // work on largest LSN in the priority queue
        while (!toUndo.isEmpty()) {
            LogRecord logRecord = toUndo.poll();
            if (logRecord.isUndoable()) {
                undoLog(logRecord);
            }
            long nextLSN = logRecord.getUndoNextLSN().orElse(logRecord.getPrevLSN().orElse(0L));
            if (nextLSN == 0L && logRecord.getTransNum().isPresent()) {
                end(logRecord.getTransNum().get());
            } else {
                toUndo.offer(logManager.fetchLogRecord(nextLSN));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager. THIS IS SLOW
     * and should only be used during recovery.
     */
    private void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType typfromInte of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }

    /**
     * Returns if current status can transition into transitioning status.
     * Transactions will always advance through states in one of two ways:
     * running -> committing -> complete
     * running -> aborting -> complete
     */
    private boolean transactionStatusTransitionable(Transaction.Status currentStatus, Transaction.Status transitioningStatus) {
        if (transitioningStatus == Transaction.Status.COMPLETE && currentStatus != Transaction.Status.RUNNING) return true;
        if (currentStatus == Transaction.Status.RUNNING && transitioningStatus != Transaction.Status.COMPLETE) return true;
        return false;
    }

    /**
     * Performs undo on given log record lsn, and returns the next lsn to undo or -1L
     */
    private long undoLog(LogRecord logRecord) {
        assert logRecord.getTransNum().isPresent();
        TransactionTableEntry txnEntry = transactionTable.get(logRecord.getTransNum().get());
        // write CLR and flush if necessary
        Pair<LogRecord, Boolean> undoRes = logRecord.undo(txnEntry.lastLSN);
        long undoLSN = logManager.appendToLog(undoRes.getFirst());
        if (undoRes.getSecond()) {
            logManager.flushToLSN(undoLSN);
        }
        // update lastLSN in xact table
        txnEntry.lastLSN = undoLSN;
        // update DPT if necessary
        if (logRecord.getType() == LogType.UPDATE_PAGE) {
            assert logRecord.getPageNum().isPresent();
            dirtyPageTable.putIfAbsent(logRecord.getPageNum().get(), undoLSN);
        }
        if (logRecord.getType() == LogType.ALLOC_PAGE) {
            dirtyPageTable.remove(logRecord.getPageNum().get());
        }
        // perform undo
        undoRes.getFirst().redo(diskSpaceManager, bufferManager);

        return undoRes.getFirst().getUndoNextLSN().orElse(-1L);
    }
}
