/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;

import java.util.LinkedList;
import java.util.List;

/**
 * Contains one ACL entry. An ACL entry consists of a scope (access or default),
 * the type of the ACL (user, group, other or mask), the name of the user or group
 * associated with this ACL (can be blank to specify the default permissions for
 * users and groups, and must be blank for mask entries), and the action permitted
 * by this ACL entry.
 * <P>
 * An ACL for an object consists of a {@code List} of Acl entries.
 * </P>
 * <p>
 * This class also provides a number of convenience methods for converting ACL entries
 * and ACLs to and back from strings.
 *  </P>
 */
public class AclEntry {
    /**
     * {@link AclScope} specifying scope of the Acl entry (access or default)
     */
    public AclScope scope;

    /**
     * {@link AclType} specifying the type of the Acl entry (user, group, other or mask)
     */
    public AclType type;

    /**
     * String specifying the name of the user or group associated with this Acl entry. Can be
     * blank to specify the default permissions for users and groups, and must be blank
     * for mask entries.
     */
    public String name;


    /**
     * {@link AclAction} enum specifying the action permitted  by this Acl entry. Has
     * convenience methods to create value from unix-style permission string (e.g.,
     * {@code AclAction.fromRwx("rw-")}), or from unix Octal permission (e.g.,
     * {@code AclAction.fromOctal(6)}).
     */
    public AclAction action;

    public AclEntry() {

    }

    /**
     * creates and Acl Entry from the supplied scope, type, name and action
     * @param scope {@link AclScope} specifying scope of the Acl entry (access or default)
     * @param type {@link AclType} specifying the type of the Acl entry (user, group, other or mask)
     * @param name String specifying the name of the user or group associated with this Acl entry
     * @param action {@link AclAction} specifying the action permitted  by this Acl entry
     */
    public AclEntry(AclScope scope, AclType type, String name, AclAction action) {
        if (scope == null) throw new IllegalArgumentException("AclScope is null");
        if (type == null ) throw new IllegalArgumentException("AclType is null");
        if (type == AclType.MASK && name != null && !name.trim().equals(""))
                throw new IllegalArgumentException("mask should not have user/group name");
        if (type == AclType.OTHER && name != null && !name.trim().equals(""))
            throw new IllegalArgumentException("ACL entry type 'other' should not have user/group name");

        this.scope = scope;
        this.type = type;
        this.name = name;
        this.action = action;
    }

    /**
     * Parses an Acl entry from its posix string form. For example: {@code "default:user:bob:r-x"}
     * @param entryString Acl entry string to parse
     *
     * @return {@link AclEntry} parsed from the string
     *
     * @throws IllegalArgumentException if the string is invalid
     */
    public static AclEntry parseAclEntry(String entryString) throws IllegalArgumentException {
        return parseAclEntry(entryString, false);
    }

    /**
     * Parses a single Acl entry from its posix string form. For example: {@code "default:user:bob:r-x"}.
     * <P>
     * If the Acl string will be used to remove an existing acl from a file or folder, then the permission
     * level does not need to be specified. Passing false to the {@code removeAcl} argument tells the parser
     * to accept such strings.
     * </P>
     * @param entryString Acl entry string to parse
     * @param removeAcl boolean specifying whether to parse this string like a remove Acl (that is, with
     *                  permission optional)
     *
     * @return {@link AclEntry} parsed from the string
     *
     * @throws IllegalArgumentException if the string is invalid
     */
    public static AclEntry parseAclEntry(String entryString, boolean removeAcl) throws IllegalArgumentException {
        if (entryString == null || entryString.equals("")) return null;
        AclEntry aclEntry = new AclEntry();
        String aclString = entryString.trim();

        // check if this is default ACL
        int fColonPos = aclString.indexOf(":");
        String firstToken = aclString.substring(0, fColonPos).toLowerCase().trim();
        if (firstToken.equals("default")) {
            aclEntry.scope = com.microsoft.azure.datalake.store.acl.AclScope.DEFAULT;
            aclString = aclString.substring(fColonPos+1);
        } else {
            aclEntry.scope = com.microsoft.azure.datalake.store.acl.AclScope.ACCESS;
        }

        // remaining string should have 3 entries (or 2 for removeacl)
        String[] parts = aclString.split(":", 5);  // without the 5, java discards trailing empty strings
        if (parts.length <2 || parts.length >3) throw new IllegalArgumentException("invalid aclEntryString " + entryString);
        if (parts.length == 2 && !removeAcl) throw new IllegalArgumentException("invalid aclEntryString " + entryString);

        // entry type (user/group/other/mask)
        try {
            aclEntry.type = AclType.valueOf(parts[0].toUpperCase().trim());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid ACL AclType in " + entryString);
        } catch (NullPointerException ex) {
            throw new IllegalArgumentException("ACL Entry AclType missing in " + entryString);
        }

        // user/group name
        aclEntry.name = parts[1].trim();
        if (aclEntry.type == AclType.MASK && !aclEntry.name.equals(""))
                throw new IllegalArgumentException("mask entry cannot contain user/group name: " + entryString);
        if (aclEntry.type == AclType.OTHER && !aclEntry.name.equals(""))
            throw new IllegalArgumentException("entry of type 'other' should not contain user/group name: " + entryString);

        // permission (rwx)
        if (!removeAcl) {
            try {
                aclEntry.action = AclAction.fromRwx(parts[2]);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid ACL action in " + entryString);
            } catch (NullPointerException ex) {
                throw new IllegalArgumentException("ACL action missing in " + entryString);
            }
        }
        return aclEntry;
    }

    /**
     * Returns the posix string form of this acl entry. For example: {@code "default:user:bob:r-x"}.

     *  @return the posix string form of this acl entry
     */
    public String toString() {
        return this.toString(false);
    }

    /**
     * Returns the posix string form of this acl entry. For example: {@code "default:user:bob:r-x"}.
     * <P>
     * If the Acl string will be used to remove an existing acl from a file or folder, then the permission
     * level does not need to be specified. Passing true to the {@code removeAcl} argument omits the permission
     * level in the output string.
     * </P>
     *
     * @param removeAcl passing true will result in an acl entry string with no permission specified
     *
     * @return the string form of the {@code AclEntry}
     */
    public String toString(boolean removeAcl) {
        StringBuilder str = new StringBuilder();
        if (this.scope == null) throw new IllegalArgumentException("Acl Entry has no scope");
        if (this.type == null) throw new IllegalArgumentException("Acl Entry has no type");

        if (this.scope == com.microsoft.azure.datalake.store.acl.AclScope.DEFAULT) str.append("default:");

        str.append(this.type.toString().toLowerCase());
        str.append(":");

        if (this.name!=null) str.append(this.name);

        if (this.action != null && !removeAcl) {
            str.append(":");
            str.append(this.action.toString());
        }
        return str.toString();
    }

    /**
     * parses a posix acl spec string into a {@link List} of {@code AclEntry}s.
     * @param aclString the acl spec string tp parse
     *
     * @return {@link List}{@code <AclEntry>} represented by the acl spec string
     *
     */
    public static List<AclEntry> parseAclSpec(String aclString) throws IllegalArgumentException {
        if (aclString == null || aclString.trim().equals("")) return new LinkedList<AclEntry>();

        aclString = aclString.trim();
        String car,   // the first entry
                cdr;  // the rest of the list after first entry
        int commaPos = aclString.indexOf(",");
        if (commaPos < 0) {
            car = aclString;
            cdr = null;
        } else {
            car = aclString.substring(0, commaPos).trim();
            cdr = aclString.substring(commaPos+1);
        }
        LinkedList<AclEntry> aclSpec = (LinkedList<AclEntry>) parseAclSpec(cdr);
        if (!car.equals("")) {
            aclSpec.addFirst(parseAclEntry(car));
        }
        return aclSpec;
    }

    /**
     * converts a {@link List}{@code <AclEntry>} to its posix aclspec string form
     * @param list {@link List}{@code <AclEntry>} to covert to string
     *
     * @return posix acl spec string
     */
    public static String aclListToString(List<AclEntry> list) {
        return aclListToString(list, false);
    }

    /**
     * converts a {@link List}{@code <AclEntry>} to its posix aclspec string form.
     * <P>
     * If the aclspec string will be used to remove an existing acl from a file or folder, then the permission
     * level does not need to be specified. Passing true to the {@code removeAcl} argument omits the permission
     * level in the output string.
     * </P>
     *
     * @param list {@link List}{@code <AclEntry>} to covert to string
     * @param removeAcl removeAcl passing true will result in an aclspec with no permission specified
     *
     * @return posix acl spec string
     */
    public static String aclListToString(List<AclEntry> list, boolean removeAcl) {
        if (list == null || list.size() == 0) return "";
        String separator = "";
        StringBuilder output = new StringBuilder();

        for (AclEntry entry : list) {
            output.append(separator);
            output.append(entry.toString(removeAcl));
            separator = ",";
        }
        return output.toString();
    }
}
