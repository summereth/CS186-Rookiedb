package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.categories.Proj4Part1Tests;
import edu.berkeley.cs186.database.categories.Proj4Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockManagerPartI {
    private LoggingLockManager lockman;
    private TransactionContext[] transactions;
    private ResourceName dbResource;
    private ResourceName[] tables;

    // 2 seconds per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                2000 * TimeoutScaling.factor)));

    /**
     * Given a LockManager lockman, checks if transaction holds a Lock specified
     * by type on the resource specified by name
     */
    static boolean holds(LockManager lockman, TransactionContext transaction, ResourceName name,
                         LockType type) {
        List<Lock> locks = lockman.getLocks(transaction);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.name == name && lock.lockType == type) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void setUp() {
        // Sets up a LockManager, 8 possible transactions, a database resource
        // and 8 possible tables for use in tests
        lockman = new LoggingLockManager();
        transactions = new TransactionContext[8];
        dbResource = new ResourceName(new Pair<>("database", 0L));
        tables = new ResourceName[transactions.length];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransactionContext(lockman, i);
            tables[i] = new ResourceName(dbResource, new Pair<>("table" + i, (long) i));
        }
    }


    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLock() {
        /**
         * Transaction 0 acquires an S lock on table0
         * Transaction 1 acquires an X lock on table1
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], tables[0], LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], tables[1], LockType.X));

        // Transaction 0 should have an S lock on table0
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));

        // table0 should only have an S lock from Transaction 0
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.S, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // Transaction 1 should have an X lock on table1
        assertEquals(LockType.X, lockman.getLockType(transactions[1], tables[1]));

        // table1 should only have an X lock from Transaction 1
        List<Lock>expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.X, 1L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLockFail() {
        DeterministicRunner runner = new DeterministicRunner(1);
        TransactionContext t0 = transactions[0];

        // Transaction 0 acquires an X lock on dbResource
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
        try {
            // Transaction 0 attempts to acquire another X lock on dbResource
            runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
            fail("Attempting to acquire a duplicate lock should throw a " +
                 "DuplicateLockRequestException.");
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleaseLock() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 0 releases its lock on dbResource
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.X);
            lockman.release(transactions[0], dbResource);
        });

        // Transaction 0 should have no lock on dbResource
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));

        // There should be no locks on dbResource
        assertEquals(Collections.emptyList(), lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleConflict() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 1 attempts to acquire an X lock on dbResource but
         *   blocks due to a conflict with Transaction 0's X lock
         *
         * After this:
         *   Transaction 0 should have an X lock on dbResource
         *   Transaction 1 should have no lock on dbResource
         *   Transaction 0 should not be blocked
         *   Transaction 1 should be blocked (waiting to acquire an X lock on dbResource)
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));

        // Lock checks
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.NL, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks = Collections.singletonList(new Lock(dbResource, LockType.X, 0L));
        assertEquals(expectedDbLocks, lockman.getLocks(dbResource));

        // Block checks
        assertFalse(transactions[0].getBlocked());
        assertTrue(transactions[1].getBlocked());

        /**
         * Transaction 0 releases its lock on dbResource
         * Transaction 1 should unblock, and acquire an X lock on dbResource
         *
         * After this:
         *   Transaction 0 should have no lock on dbResource
         *   Transaction 1 should have an X lock dbResource
         *   Both transactions should be unblocked
         *
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // Lock checks
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks2 = Collections.singletonList(new Lock(dbResource, LockType.X, 1L));
        assertEquals(expectedDbLocks2, lockman.getLocks(dbResource));

        // Block checks
        assertFalse(transactions[0].getBlocked());
        assertFalse(transactions[1].getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testReleaseUnheldLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        TransactionContext t1 = transactions[0];
        try {
            runner.run(0, () -> lockman.release(t1, dbResource));
            fail("Releasing a lock on a resource you don't hold a lock on " +
                 "should throw a NoLockHeldException");
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

}

