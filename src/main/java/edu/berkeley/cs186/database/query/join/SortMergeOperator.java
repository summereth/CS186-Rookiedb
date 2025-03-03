package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = checkSchemaForColumn(leftSource.getSchema(), leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = checkSchemaForColumn(rightSource.getSchema(), rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If
         * there are no more records to return, a NoSuchElementException should
         * be thrown.
         *
         * @throws NoSuchElementException if there are no more records to yield
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement
            // leftSource and rightSource is already sorted by calling prepareLeft()/Right() in constructor

            if (leftRecord == null) { // see case line 190
                throw new NoSuchElementException();
            }

            // CASE 1: no matches under current two pointer
            if (!this.marked) {
                while (compare(leftRecord, rightRecord) > 0) {
                    if (!rightIterator.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    rightRecord = rightIterator.next();
                }
                while (compare(leftRecord, rightRecord) < 0) {
                    if (!leftIterator.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    leftRecord = leftIterator.next();
                }
                // find the start of matches
                this.marked = true;
                rightIterator.markPrev();
            }
            // CASE 2: find matches already, search for pairs between <li, <rj, rj+1, rj+2...>
            if (compare(leftRecord, rightRecord) == 0) {
                // CASE 2a: match. rj is still in range -> update nextRecord and advance rightRecord or leftRecord
                this.nextRecord = leftRecord.concat(rightRecord);
                if (rightIterator.hasNext()) {
                    // move to rj+1 is there is any
                    rightRecord = rightIterator.next();
                } else {
                    // else advance li and reset rj
                    rightIterator.reset();
                    rightRecord = rightIterator.next();
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                }
            } else {
                // CASE 2b: doesn't match. rj is out of range -> reset rightRecord and advance leftRecord, call fetchNextRecord() again
                rightIterator.reset();
                rightRecord = rightIterator.next();
                leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                this.marked = false; // end of <li, <rj,rj+1,...> search
                fetchNextRecord();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
