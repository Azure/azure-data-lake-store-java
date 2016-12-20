package com.microsoft.azure.datalake.store;


/**
 * Enum specifying how to interpret the expiry time specified in setExpiry call.
 */
public enum ExpiryOption {
    /**
     * No expiry. ExpireTime is ignored.
     */
    NeverExpire,
    /**
     * Interpret as miliseconds from now. ExpireTime is an integer in milliseconds representing the expiration date
     * relative to when file expiration is updated
     */
    RelativeToNow,
    /**
     * Interpet as milliseconds from the file's creation date+time. ExpireTime is an integer in milliseconds
     * representing the expiration date relative to file creation
     */
    RelativeToCreationDate,
    /**
     * Interpret as date/time. ExpireTime is an integer in milliseconds, as a Unix timestamp relative
     * to 1/1/1970 00:00:00
     */
    Absolute
}
