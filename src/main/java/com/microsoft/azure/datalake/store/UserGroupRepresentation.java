package com.microsoft.azure.datalake.store;


/**
 * Enum specifying how user and group objects should be represented in calls that return user and group ID.
 */
public enum UserGroupRepresentation {

    /**
     * Object ID (OID), which is a GUID representing the ID of the user or group. The OID is immutable even if
     * the name of the user or group changes.
     */
    OID,

    /**
     * User Principal Name of the group or user, which is the human-friendly username. Since users and groups are
     * stored internally as OID, using the UPN involves an additional lookup into the directory.
     */
    UPN
}
