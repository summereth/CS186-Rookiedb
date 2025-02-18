package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // Error checking
        readonlyErrorCheck();
        multigranularityErrorCheck(transaction, lockType);

        lockman.acquire(transaction, name, lockType);
        if (parentContext() != null) {
            parentContext().numChildLocks.put(
                    transaction.getTransNum(),
                    parentContext().numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1
            );
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // Error checking
        readonlyErrorCheck();
        // releasing locks in bottom-up order
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("Request invalid: releasing locks on children first");
        }

        lockman.release(transaction, name);
        if (parentContext() != null) {
            assert parentContext().numChildLocks.containsKey(transaction.getTransNum());
            parentContext().numChildLocks.put(
                    transaction.getTransNum(),
                    parentContext().numChildLocks.get(transaction.getTransNum()) - 1
            );
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // Error checking
        readonlyErrorCheck();
        multigranularityErrorCheck(transaction, newLockType);

        // Special case: promotion to SIX (from IS/IX/S), simultaneously release all descendant locks of type S/IS
        if (newLockType == LockType.SIX) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("Request invalid: already has SIX lock on ancestor");
            }
            for (ResourceName sisChild : sisDescendants(transaction)) {
                lockman.release(transaction, sisChild);
            }
        }
        lockman.promote(transaction, this.name, newLockType);
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // Error checking
        readonlyErrorCheck();
        LockType currentLockType = lockman.getLockType(transaction, this.name);
        if (currentLockType == LockType.NL) {
            throw new NoLockHeldException("Request invalid: transaction has no lock at this level");
        }

        // no need to change
        if (currentLockType == LockType.S || currentLockType == LockType.X) {
            return;
        }
        LockType escalatedLockType = LockType.substitutable(LockType.S, currentLockType) ? LockType.S : LockType.X;
        List<ResourceName> descendantsWithLocks = new ArrayList<>();
        for (Long childName : children.keySet()) {
            LockContext child = childContext(childName);
            LockType childLockType = child.lockman.getLockType(transaction, child.name);
            if (childLockType != LockType.NL) {
                descendantsWithLocks.add(child.name);
                if (!LockType.substitutable(escalatedLockType, childLockType)) {
                    escalatedLockType = LockType.X;
                }
            }
        }
        numChildLocks.put(transaction.getTransNum(), 0);
        lockman.acquireAndRelease(transaction, this.name, escalatedLockType, descendantsWithLocks);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, this.name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType explicitLockType = getExplicitLockType(transaction);
        if ((explicitLockType.equals(LockType.NL) || explicitLockType.isIntent()) && parentContext() != null) {
            LockType parentEffectiveLockType = parentContext().getEffectiveLockType(transaction);
            if (!parentEffectiveLockType.isIntent()) {
                return parentEffectiveLockType;
            } else if (parentEffectiveLockType.equals(LockType.SIX)) {
                return LockType.S;
            }
        }
        return explicitLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext parent = parentContext();
        while (parent != null) {
            if (lockman.getLockType(transaction, parent.name) == LockType.SIX) {
                return true;
            }
            parent = parent.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (long child : children.keySet()) {
            LockContext childContext = childContext(child);
            LockType childLockType = lockman.getLockType(transaction, childContext.name);
            if (childLockType == LockType.IS || childLockType == LockType.S) {
                sisDescendants.add(childContext.name);
            }
        }
        return sisDescendants;
    }

    /**
     * Helper method to raise UnsupportedOperationException if context is readonly
     *
     * @throws UnsupportedOperationException if context is readonly
     */
    private void readonlyErrorCheck() {
        if (this.readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
    }

    /**
     * Check if acquiring given lockType on `name` by given transaction violates multigranularity constraints.
     *
     * @throws InvalidLockException if the request would violate multigranularity constraints.
     */
    private void multigranularityErrorCheck(TransactionContext transaction, LockType lockType) {
        if (parentContext() != null) {
            LockType lockOnParent = lockman.getLockType(transaction, parentContext().name);
            if (lockType == LockType.S || lockType == LockType.IS) {
                if (hasSIXAncestor(transaction)) {
                    throw new InvalidLockException("Request invalid: Redundant to acquire an IS/S lock when an ancestor has SIX");
                }
                if (lockOnParent != LockType.IS && lockOnParent != LockType.IX) {
                    throw new InvalidLockException("Request invalid: To get S or IS lock on a node, must hold IS or IX on parent node");
                }
            }
            if (lockType == LockType.X || lockType == LockType.IX) {
                if (lockOnParent != LockType.IX && lockOnParent != LockType.SIX) {
                    throw new InvalidLockException("Request invalid: To get X or IX on a node, must hold IX or SIX on parent node.");
                }
            }
        }
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

