package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a == NL || b == NL) {
            return true;
        }
        if (a == X || b == X) {
            return false;
        }
        if (a == IS || b == IS) {
            return true;
        }
        if (a == IX && b == IX || a == S && b == S) {
            return true;
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     *
     * S(A) can read A and all descendants of A.
     * X(A) can read and write A and all descendants of A.
     * IS(A) can request shared and intent-shared locks on all children of A.
     * IX(A) can request any lock on all children of A.
     * SIX(A) can do anything that having S(A) or IX(A) lets it do, except requesting S, IS, or SIX** locks on children of A, which would be redundant.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType == NL) return true;
        switch (parentLockType) {
            case NL: return false;
            case S: return childLockType == S;
            case X: return childLockType == X;
            case IS: return childLockType == IS || childLockType == S;
            case IX: return true;
            case SIX: return childLockType != S && childLockType != IS && childLockType != SIX;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (required == NL) return true;
        if (substitute == NL) return false;
        if (required == substitute) return true;
        if (substitute == X) return true;
        switch (substitute) {
            case S:
            case IX:
                return required == IS;
            case IS: return false;
            case SIX: return required != X;
        }

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

