package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type -> do nothing
     * - The current lock type is IX and the requested lock is S -> promote to SIX
     * - The current lock type is an intent lock -> acquire requested lock by escalating and promoting
     * - None of the above -> acquire requested lock by promoting
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return; // The current lock type can effectively substitute the requested type -> do nothing
        }

        // Step 1: ensure we have the appropriate locks on ancestors
        ensureAncestorLockHeld(transaction, parentContext, requestType);

        // Step 2: acquire the lock on the resource
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            if (!LockType.substitutable(lockContext.getExplicitLockType(transaction), requestType)) {
                lockContext.promote(transaction, requestType);
            }
        } else if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    private static void ensureAncestorLockHeld(TransactionContext transaction, LockContext parentContext, LockType childRequestType) {
        if (childRequestType.equals(LockType.NL) || parentContext == null) return;
        ensureAncestorLockHeld(transaction, parentContext.parentContext(), childRequestType);

        Map<LockType, LockType> intentLockMap = Map.of(LockType.S, LockType.IS, LockType.X, LockType.IX);
        if (parentContext.getExplicitLockType(transaction) == LockType.NL) {
            parentContext.acquire(transaction, intentLockMap.get(childRequestType));
        } else if (!LockType.substitutable(
                parentContext.getExplicitLockType(transaction),
                intentLockMap.get(childRequestType))) {
            parentContext.promote(transaction, intentLockMap.get(childRequestType));
        }
    }
}
