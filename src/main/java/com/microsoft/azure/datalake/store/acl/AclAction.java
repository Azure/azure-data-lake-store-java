/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;

/**
 * Specifies the possible combinations of actions allowed in an ACL.
 *
 */
public enum AclAction {
    NONE          ("---"),
    EXECUTE       ("--x"),
    WRITE         ("-w-"),
    WRITE_EXECUTE ("-wx"),
    READ          ("r--"),
    READ_EXECUTE  ("r-x"),
    READ_WRITE    ("rw-"),
    ALL           ("rwx");

    private final String rwx;
    private static final AclAction[] values = AclAction.values();

    AclAction(String rwx) {
        this.rwx = rwx;
    }

    /**
     * returns the Unix rwx string representation of the {@code AclAction}
     * @return string representation of the {@code AclAction}
     */
    public String toString() {
        return this.rwx;
    }

    /**
     * static method that returns the Unix rwx string representation of the supplied {@code AclAction}
     * @param action the {@code AclAction} enum value to convert to string
     *
     * @return string representation of the {@code AclAction}
     */
    public static String toString(AclAction action) {
        return action.rwx;
    }

    /**
     * Returns an {@code AclAction} enum value represented by the supplied Unix rwx permission string
     * @param rwx the string containing the unix permission in rwx form
     *
     * @return The {@code AclAction} enum value corresponding to the string
     */
    public static AclAction fromRwx(String rwx) {
        if (rwx==null) throw new IllegalArgumentException("access specifier is null");
        rwx = rwx.trim().toLowerCase();
        for (AclAction a: values) {
            if (a.rwx.equals(rwx)) { return a; }
        }
        throw new IllegalArgumentException(rwx + " is not a valid access specifier");
    }

    /**
     * Checks to see if the supplied string is a valid unix rwx permission string
     * @param input the string to check
     *
     * @return true if the string is a valid rwx permission string, false otherwise
     */
    public static boolean isValidRwx(String input) {
        try {
            fromRwx(input);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    /**
     * Returns an {@code AclAction} enum value represented by the supplied Octal digit
     * @param perm the octal digit representing the permission
     *
     * @return The {@code AclAction} enum value corresponding to the octal digit
     */
    public static AclAction fromOctal(int perm) {
        if (perm <0 || perm>7) throw new IllegalArgumentException(perm + " is not a valid access specifier");
        return values[perm];
    }

    /**
     * returns the octal representation of the {@code AclAction}
     * @return octal representation of the {@code AclAction}
     */
    public int toOctal() {
        return this.ordinal();
    }

    /**
     * static method that returns the octal representation of the supplied {@code AclAction}
     * @param action the {@code AclAction} enum value to convert to octal
     *
     * @return octal representation of the {@code AclAction}
     */
    public static int toOctal(AclAction action) {
        return action.ordinal();
    }


}
