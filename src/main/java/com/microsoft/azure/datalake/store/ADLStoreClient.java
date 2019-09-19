/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.acl.AclStatus;
import com.microsoft.azure.datalake.store.oauth2.*;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import com.microsoft.azure.datalake.store.retrypolicies.NonIdempotentRetryPolicy;
import com.microsoft.azure.datalake.store.SSLSocketFactoryEx.SSLChannelMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * {@code ADLStoreClient} class represents a client to Azure Data Lake. It can be used to perform operations on files
 * and directories.
 *
 */
public class ADLStoreClient {

    private final String accountFQDN;
    private String accessToken;
    private final AccessTokenProvider tokenProvider;
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store"); // package-default logging policy
    private static final AtomicLong clientIdCounter = new AtomicLong(0);
    private final long clientId;

    private String userAgentString;
    private String tiHeader = null;
    private String proto = "https";
    private boolean enableRemoteExceptions = false;
    private String pathPrefix = null;
    private int readAheadQueueDepth = -1;  // no preference set by caller, use default in ADLFileInputStream
    volatile boolean disableReadAheads = false;
    int timeout = 60000; // internal scope, available to Input and Output Streams
    private boolean alterCipherSuits = true;
    private SSLChannelMode sslChannelMode = SSLChannelMode.Default;
	
    private int maxRetries = 4;
	private int exponentialRetryInterval = 1000;
	private int exponentialFactor = 4;
	
    private static String sdkVersion = null;
    static {
        InputStream is = ADLStoreClient.class.getResourceAsStream("/adlsdkversion.properties");
        if (is == null) {
            sdkVersion = "SDKVersionNotKnown";
        } else {
            Properties prop = new Properties();
            try {
                prop.load(is);
            } catch (IOException ex) {
                sdkVersion = "SDKVersionUnknown";
            }
            try {
                is.close();
            } catch (IOException ex) {
                // swallow
            }
            if (sdkVersion == null) sdkVersion = prop.getProperty("sdkversion", "SDKVersionMissing");
        }



        // This stinks to set this system-wide property from within SDK. However, Oracle JDK has a flaw in that
        // HttpURLConnection automatically and silently retries POST requests, which is clearly against HTTP semantics.
        // See details here:
        // http://koenserneels.blogspot.com/2016/01/pola-and-httpurlconnection.html
        // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6382788
        System.setProperty("sun.net.http.retryPost", "false");
    }

    private static String userAgent =
            String.format("%s-%s/%s-%s/%s/%s-%s",
                    "ADLSJavaSDK",
                    sdkVersion,
                    System.getProperty("os.name").replaceAll(" ", ""),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vendor").replaceAll(" ", ""),
                    System.getProperty("java.version")
            );



    // private constructor, references should be obtained using the createClient factory method
    private ADLStoreClient(String accountFQDN, String accessToken, long clientId, AccessTokenProvider tokenProvider) {
        this.accountFQDN = accountFQDN;
        this.accessToken = "Bearer " + accessToken;
        this.tokenProvider = tokenProvider;
        this.clientId = clientId;
        this.userAgentString = userAgent;
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param token {@link AzureADToken} object that contains the AAD token to use
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, AzureADToken token) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }
        if (token == null || token.accessToken == null || token.accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.trace("ADLStoreClient {} created for {} using SDK version {}", clientId, accountFQDN, sdkVersion);
        return new ADLStoreClient(accountFQDN, token.accessToken, clientId, null);
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param accessToken String containing the AAD access token to be used
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, String accessToken) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (accessToken == null || accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.trace("ADLStoreClient {} created for {} using SDK version {}", clientId, accountFQDN, sdkVersion);
        return new ADLStoreClient(accountFQDN, accessToken, clientId, null);
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link AccessTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, AccessTokenProvider tokenProvider) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (tokenProvider == null) {
            throw new IllegalArgumentException("token provider is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.trace("ADLStoreClient {} created for {} using SDK version {}", clientId, accountFQDN, sdkVersion);
        return new ADLStoreClient(accountFQDN, null, clientId, tokenProvider);
    }


    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link ClientCredsTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, ClientCredsTokenProvider tokenProvider) {
        // just a convenience overload, for easy discoverability in IDE's autocompletion.
        return createClient(accountFQDN, (AccessTokenProvider) tokenProvider);
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link RefreshTokenBasedTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, RefreshTokenBasedTokenProvider tokenProvider) {
        // just a convenience overload, for easy discoverability in IDE's autocompletion.
        return createClient(accountFQDN, (AccessTokenProvider) tokenProvider);    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link UserPasswordTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, UserPasswordTokenProvider tokenProvider) {
        // just a convenience overload, for easy discoverability in IDE's autocompletion.
        return createClient(accountFQDN, (AccessTokenProvider) tokenProvider);    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link DeviceCodeTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, DeviceCodeTokenProvider tokenProvider) {
        // just a convenience overload, for easy discoverability in IDE's autocompletion.
        return createClient(accountFQDN, (AccessTokenProvider) tokenProvider);    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link DeviceCodeTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, MsiTokenProvider tokenProvider) {
        // just a convenience overload, for easy discoverability in IDE's autocompletion.
        return createClient(accountFQDN, (AccessTokenProvider) tokenProvider);    }


	/**
	 * Set parameters for exponential backoff
	 * @param maxRetries Maximum number of retry attempts
	 * @param exponentialRetryInterval Initial retry interval, in milliseconds
	 * @param exponentialFactor Retry interval is geometrically increased by this factor on each attempt
	 */
	public void setExponentialBackoffParameters(int maxRetries, int exponentialRetryInterval, int exponentialFactor) {
		this.maxRetries = maxRetries;
		this.exponentialRetryInterval = exponentialRetryInterval;
		this.exponentialFactor = exponentialFactor;
	}
	
	public ExponentialBackoffPolicy makeExponentialBackoffPolicy() {
		return new ExponentialBackoffPolicy(maxRetries, 0, exponentialRetryInterval, exponentialFactor);
	}
	
    /* ----------------------------------------------------------------------------------------------------------*/

    /*
    *
    * Methods that apply to Files only
    *
    */

    /**
     * create a file. If {@code overwriteIfExists} is false and the file already exists,
     * then an exceptionis thrown.
     * The call returns an {@link ADLFileOutputStream} that can then be written to.
     *
     *
     * @param path full pathname of file to create
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @return  {@link ADLFileOutputStream} to write to
     * @throws IOException {@link ADLException} is thrown if there is an error in creating the file
     */
    public ADLFileOutputStream createFile(String path, IfExists mode) throws IOException {
        return createFile(path, mode, null, true);
    }

    /**
     * create a file. If {@code overwriteIfExists} is false and the file already exists,
     * then an exceptionis thrown.
     * The call returns an {@link ADLFileOutputStream} that can then be written to.
     *
     *
     * @param path full pathname of file to create
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @param octalPermission permissions for the file, as octal digits (For Example, {@code "755"})
     * @param createParent if true, then parent directories of the file are created if they are missing.
     * @return  {@link ADLFileOutputStream} to write to
     * @throws IOException {@link ADLException} is thrown if there is an error in creating the file
     */
    public ADLFileOutputStream createFile(String path, IfExists mode, String octalPermission, boolean createParent) throws IOException {
        if (octalPermission != null && !octalPermission.equals("") && !Core.isValidOctal(octalPermission)) {
                throw new IllegalArgumentException("Invalid directory permissions specified: " + octalPermission);
        }
        if (log.isTraceEnabled()) {
            log.trace("create file for client {} for file {}", this.getClientId(), path);
        }
        String leaseId = UUID.randomUUID().toString();
        boolean overwrite = (mode==IfExists.OVERWRITE);
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = overwrite ? makeExponentialBackoffPolicy() : new NonIdempotentRetryPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.create(path, overwrite, octalPermission, null, 0, 0, leaseId,
            leaseId, createParent, SyncFlag.DATA, this, opts, resp);
        if (!resp.successful) {
            if(!(overwrite && resp.httpResponseCode == 403 && resp.remoteExceptionName.contains("FileAlreadyExistsException"))){
                throw this.getExceptionFromResponse(resp, "Error creating file " + path);
            }
        }
        return new ADLFileOutputStream(path, this, true, leaseId);
    }



    /**
     * Opens a file for read and returns an {@link ADLFileInputStream} to read the file
     * contents from.
     *
     * @param path full pathname of file to read
     * @return {@link ADLFileInputStream} to read the file contents from.
     * @throws IOException {@link ADLException} is thrown if there is an error in opening the file
     */
    public ADLFileInputStream getReadStream(String path) throws IOException {
        DirectoryEntry de = getDirectoryEntry(path);
        if (de.type == DirectoryEntryType.FILE) {
            return new ADLFileInputStream(path, de, this);
        } else {
            throw new ADLException("Path is not a file: " + path);
        }
    }

    /**
     * appends to an existing file.
     *
     * @param path full pathname of file to append to
     * @return {@link ADLFileOutputStream} to write to. The contents written to this stream
     *         will be appended to the file.
     * @throws IOException {@link ADLException} is thrown if there is an error in opening the file for append
     */
    public ADLFileOutputStream getAppendStream(String path) throws IOException {
        String leaseId = UUID.randomUUID().toString();
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.append(path, -1, null, 0, 0, leaseId, leaseId, SyncFlag.DATA, this, opts,
                resp);
        if (!resp.successful) {
            throw this.getExceptionFromResponse(resp, "Error appending to file " + path);
        }
        return new ADLFileOutputStream(path, this, false, leaseId);
    }

    /**
     * Concatenate the specified list of files into this file. The target should not exist.
     * The source files will be deleted if the concatenate succeeds.
     *
     *
     * @param path full pathname of the destination to concatenate files into
     * @param fileList {@link List} of strings containing full pathnames of the files to concatenate.
     *                Cannot be null or empty.
     * @return returns true if the call succeeds
     * @throws IOException {@link ADLException} is thrown if there is an error in concatenating files
     */
    public boolean concatenateFiles(String path, List<String> fileList) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout + (fileList.size() * 500); // timeout proportional to number of files
        OperationResponse resp = new OperationResponse();
        Core.concat(path, fileList, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error concatenating files into " + path);
        }
        return true;
    }

    /**
     * Sets the expiry time on a file.
     *
     * @param path path of the file to set expiry on
     * @param expiryOption {@link ExpiryOption} value specifying how to interpret the passed in time
     * @param expiryTimeMilliseconds time duration in milliseconds
     * @throws IOException if there is an error in setting file expiry
     */
    public void setExpiryTime(String path, ExpiryOption expiryOption, long expiryTimeMilliseconds) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.setExpiryTime(path, expiryOption, expiryTimeMilliseconds, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error concatenating files into " + path);
        }
    }

    /*
    *
    * Methods that apply to Directories only
    *
    */

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     *
     * @param path full pathname of directory to enumerate
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path) throws IOException {
        return enumerateDirectory(path, (UserGroupRepresentation) null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     *
     * @param path full pathname of directory to enumerate
     * @param oidOrUpn {@link UserGroupRepresentation} enum specifying whether to return user and group information as
     *                                                OID or UPN
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, UserGroupRepresentation oidOrUpn) throws IOException  {
        return enumerateDirectory(path, Integer.MAX_VALUE, null, null, oidOrUpn);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, int maxEntriesToRetrieve) throws IOException  {
        return enumerateDirectory(path, maxEntriesToRetrieve, null, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, String startAfter) throws IOException  {
        return enumerateDirectory(path, Integer.MAX_VALUE, startAfter, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, int maxEntriesToRetrieve, String startAfter) throws IOException  {
        return enumerateDirectory(path, maxEntriesToRetrieve, startAfter, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, String startAfter, String endBefore) throws IOException  {
        return enumerateDirectory(path, Integer.MAX_VALUE, startAfter, endBefore);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path,
                                                   int maxEntriesToRetrieve,
                                                   String startAfter,
                                                   String endBefore)
            throws IOException {
        return enumerateDirectory(path, maxEntriesToRetrieve, startAfter, endBefore, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @param oidOrUpn {@link UserGroupRepresentation} enum specifying whether to return user and group information as
     *                                                OID or UPN
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path,
                                                   int maxEntriesToRetrieve,
                                                   String startAfter,
                                                   String endBefore,
                                                   UserGroupRepresentation oidOrUpn)
            throws IOException {
        ArrayList<DirectoryEntry> deList = new ArrayList<DirectoryEntry>();
        int pagesize = 4000;
        int numEntriesToRequest;
        ArrayList<DirectoryEntry> list;
        String continuationToken;
        boolean eol = (maxEntriesToRetrieve <= 0); // eol=end-of-list

        while (!eol) {
            numEntriesToRequest = Math.min(maxEntriesToRetrieve, pagesize);
            DirectoryEntryListWithContinuationToken directoryEntryListWithContinuationToken = enumerateDirectoryInternal(path, numEntriesToRequest,
                    startAfter, endBefore, oidOrUpn);
            continuationToken = directoryEntryListWithContinuationToken.getContinuationToken();
            list = (ArrayList<DirectoryEntry>) directoryEntryListWithContinuationToken.getEntries();
            if (list == null || list.size() == 0) break; // return what we have so far
            int size = list.size();
            deList.addAll(list);
            startAfter = continuationToken;
            maxEntriesToRetrieve -= size;
            eol = (maxEntriesToRetrieve <= 0) || (continuationToken == "");
        }
        return deList;
    }

    private DirectoryEntryListWithContinuationToken enumerateDirectoryInternal(String path,
                                                            int maxEntriesToRetrieve,
                                                            String startAfter,
                                                            String endBefore,
                                                            UserGroupRepresentation oidOrUpn)
            throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = 2 * this.timeout;  // double the default timeout
        OperationResponse resp = new OperationResponse();
        DirectoryEntryListWithContinuationToken dirEnt  = Core.listStatusWithToken(path, startAfter, endBefore, maxEntriesToRetrieve, oidOrUpn, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error enumerating directory " + path);
        }
        return dirEnt;
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @param path full pathname of directory to create
     * @return returns {@code true} if the call succeeded
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory(String path) throws IOException  {
        return createDirectory(path, null);
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @param path full pathname of directory to create
     * @param octalPermission permissions for the directory, as octal digits (for example, {@code "755"}). Can be null.
     * @return returns {@code true} if the call succeeded
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory(String path, String octalPermission) throws IOException  {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.mkdirs(path, octalPermission, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error creating directory " + path);
        }
        return succeeded;
    }

    /**
     * deletes a directory and all it's child directories and files recursively.
     *
     * @param path full pathname of directory to delete
     * @return returns {@code true} if the call succeeded
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean deleteRecursive(String path) throws IOException {
        if (path.equals("/")) throw new IllegalArgumentException("Cannot delete root directory tree");

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(path, true, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error deleting directory tree " + path);
        }

        return succeeded;
    }

    /**
     * removes all default acl entries from a directory. The access ACLs for the directory itself are
     * not modified.
     *
     * @param path full pathname of directory to remove default ACLs from
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void removeDefaultAcls(String path) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.removeDefaultAcl(path, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error removing default ACLs for directory " + path);
        }
    }

    /*
    *
    * Methods that apply to both Files and Directoties
    *
    */

    /**
     * rename a file or directory.
     *
     * @param path full pathname of file or directory to rename
     * @param newName the new name of the file
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String path, String newName) throws IOException {
        return rename(path, newName, false);
    }

    /**
     * rename a file or directory.
     *
     * @param path full pathname of file or directory to rename
     * @param newName the new name of the file/directory
     * @param overwrite overwrite destination if it already exists. If the
     *                  destination is a non-empty directory, then the call
     *                  fails rather than overwrite the directory.
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String path, String newName, boolean overwrite) throws IOException {
        if (path==null || path.equals("")) throw new IllegalArgumentException("Path cannot be null or empty");
        if (path.equals("/")) throw new IllegalArgumentException("Cannot rename root directory");

        // In case of self rename operation, HDFS semantics expects true if file, false if directory.
        // Renaming self is not a common operation. but more obvious to contract test validation.
        if(path.equals(newName)) {
            return getDirectoryEntry(path).type == DirectoryEntryType.FILE;
        }

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.rename(path, newName, overwrite, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error renaming file " + path);
        }
        return succeeded;
    }

    /**
     * delete the file or directory.
     *
     * @param path full pathname of file or directory to delete
     * @return returns {@code true} if the call succeeded
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean delete(String path) throws IOException {
        if (path.equals("/")) throw new IllegalArgumentException("Cannot delete root directory");

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(path, false, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error deleting directory " + path);
        }

        return succeeded;
    }

    /**
     * Gets the directory metadata about this file or directory.
     *
     * @param path full pathname of file or directory to get directory entry for
     * @return {@link DirectoryEntry} containing the metadata for the file/directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public DirectoryEntry getDirectoryEntry(String path) throws IOException {
        return getDirectoryEntry(path, null);
    }

    /**
     * Gets the directory metadata about this file or directory.
     *
     * @param path full pathname of file or directory to get directory entry for
     * @param oidOrUpn {@link UserGroupRepresentation} enum specifying whether to return user and group information as
     *                                                OID or UPN
     * @return {@link DirectoryEntry} containing the metadata for the file/directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public DirectoryEntry getDirectoryEntry(String path, UserGroupRepresentation oidOrUpn) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        DirectoryEntry dirEnt  = Core.getFileStatus(path, oidOrUpn, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error getting info for file " + path);
        }
        return dirEnt;
    }

    /**
     * Gets the content summary of a file or directory.
     * @param path full pathname of file or directory to query
     * @return {@link ContentSummary} containing summary of information about the file or directory
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public ContentSummary getContentSummary(String path) throws IOException {
        return (new ContentSummaryProcessor()).getContentSummary(this, path);
    }


    /**
     * sets the owning user and group of the file. If the user or group are {@code null}, then they are not changed.
     * It is illegal to pass both user and owner as {@code null}.
     *
     * @param path full pathname of file or directory to set owner/group for
     * @param owner the ID of the user, or {@code null}
     * @param group the ID of the group, or {@code null}
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void setOwner(String path, String owner, String group) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.setOwner(path, owner, group, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error setting owner for file " + path);
        }
    }

    /**
     * sets one or both of the times (Modified and Access time) of the file or directory
     *
     * @param path full pathname of file or directory to set times for
     * @param atime Access time as a long
     * @param mtime Modified time as a long
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void setTimes(String path, Date atime, Date mtime) throws IOException {
        long atimeLong = (atime == null)? -1 : atime.getTime();
        long mtimeLong = (mtime == null)? -1 : mtime.getTime();

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.setTimes(path, atimeLong, mtimeLong, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error setting times for file " + path);
        }
    }

    /**
     * Check that a file or directory exists.
     *
     * @param filename path to check
     * @return true if the path exists, false otherwise
     * @throws IOException thrown on error
     */
    public boolean checkExists(String filename) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        try {
            getDirectoryEntry(filename);
        } catch (ADLException ex) {
            if (ex.httpResponseCode == 404) return false;
            else throw ex;
        }
        return true;
    }

    /**
     * Creates an empty file.
     *
     * @param filename name of file to create.
     * @throws IOException thrown on error
     */
    public void createEmptyFile(String filename) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        OutputStream out = createFile(filename, IfExists.FAIL);
        out.close();
    }

    /**
     * Sets the permissions of the specified file ro directory. This sets the traditional unix read/write/execute
     * permissions for the file/directory. To set Acl's use the
     * {@link  #setAcl(String, List) setAcl} call.
     *
     * @param path full pathname of file or directory to set permissions for
     * @param octalPermissions the permissions to set, in unix octal form. For example, '644'.
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void setPermission(String path, String octalPermissions) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.setPermission(path, octalPermissions, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error setting times for " + path);
        }
    }

    /**
     * checks whether the calling user has the required permissions for the file/directory . The permissions
     * to check should be specified in the rwx parameter, as a unix permission string
     * (for example, {@code "r-x"}).
     *
     * @param path full pathname of file or directory to check access for
     * @param rwx the permission to check for, in rwx string form. The call returns true if the caller has
     *            all the requested permissions. For example, specifying {@code "r-x"} succeeds if the caller has
     *            read and execute permissions.
     * @return true if the caller has the requested permissions, false otherwise
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public boolean checkAccess(String path, String rwx) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.checkAccess(path, rwx, this, opts, resp);
        if (!resp.successful) {
            if (resp.httpResponseCode == 401 || resp.httpResponseCode == 403) return false;
            throw getExceptionFromResponse(resp, "Error checking access for " + path);
        }
        return true;
    }

    /**
     * Modify the acl entries for a file or directory. This call merges the supplied list with
     * existing ACLs. If an entry with the same scope, type and user already exists, then the permissions
     * are replaced. If not, than an new ACL entry if added.
     *
     * @param path full pathname of file or directory to change ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to add or modify
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void modifyAclEntries(String path, List<AclEntry> aclSpec) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.modifyAclEntries(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error modifying ACLs for " + path);
        }
    }

    /**
     * Sets the ACLs for a file or directory. If the file or directory already has any ACLs
     * associated with it, then all the existing ACLs are removed before adding the specified
     * ACLs.
     *
     * @param path full pathname of file or directory to set ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to set
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void setAcl(String path, List<AclEntry> aclSpec) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.setAcl(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error setting ACLs for " + path);
        }
    }

    /**
     * Removes the specified ACL entries from a file or directory.
     *
     * @param path full pathname of file or directory to remove ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s to remove
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void removeAclEntries(String path, List<AclEntry> aclSpec) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.removeAclEntries(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error removing ACLs for " + path);
        }
    }

    /**
     * Removes all acl entries from a file or directory.
     *
     * @param path full pathname of file or directory to remove ACLs for
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public void removeAllAcls(String path) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        Core.removeAcl(path, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error removing all ACLs for file " + path);
        }
    }

    /**
     * Queries the ACLs and permissions for a file or directory.
     *
     * @param path full pathname of file or directory to query
     * @return {@link AclStatus} object containing the ACL and permission info
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public AclStatus getAclStatus(String path)
            throws IOException {
        return getAclStatus(path, null);
    }

    /**
     * Queries the ACLs and permissions for a file or directory.
     *
     * @param path full pathname of file or directory to query
     * @param oidOrUpn {@link UserGroupRepresentation} enum specifying whether to return user and group information as
     *                                                OID or UPN
     * @return {@link AclStatus} object containing the ACL and permission info
     * @throws IOException {@link ADLException} is thrown if there is an error
     */
    public AclStatus getAclStatus(String path, UserGroupRepresentation oidOrUpn)
            throws IOException {
        AclStatus status = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = makeExponentialBackoffPolicy();
        opts.timeout = this.timeout;
        OperationResponse resp = new OperationResponse();
        status = Core.getAclStatus(path, oidOrUpn, this, opts, resp);
        if (!resp.successful) {
            throw getExceptionFromResponse(resp, "Error getting  ACL Status for " + path);
        }
        return status;
    }


    /* ----------------------------------------------------------------------------------------------------------*/
    /* Methods to set client's behavior                                                                          */
    /* ----------------------------------------------------------------------------------------------------------*/
    /**
     * Sets the options to configure the behavior of this client.
     * @param o {@link ADLStoreOptions} object that specifies the options to set
     * @throws IOException if there is an error in creating the file path prefix specified
     */
    public synchronized void setOptions(ADLStoreOptions o) throws IOException {
        if (o.getFilePathPrefix() != null) this.setFilePathPrefix(o.getFilePathPrefix());
        if (o.isUsingInsecureTransport()) this.setInsecureTransport();
        if (o.isThrowingRemoteExceptionsEnabled()) this.enableThrowingRemoteExceptions();
        if (o.getUserAgentSuffix() != null) this.setUserAgentSuffix(o.getUserAgentSuffix());
        if (o.getReadAheadQueueDepth() >= 0 ) this.readAheadQueueDepth = o.getReadAheadQueueDepth();
        if (o.getDefaultTimeout() > 0) this.timeout = o.getDefaultTimeout();
        this.alterCipherSuits = o.shouldAlterCipherSuits();
        this.sslChannelMode = o.getSSLChannelMode();
    }


    /**
     * update token on existing client.
     * This is useful if the client is expected to be used over long time, and token has expired.
     *
     * @param token The OAuth2 Token
     */
    public synchronized void updateToken(AzureADToken token) {
        log.trace("AAD Token Updated for client client {} for account {}", clientId, accountFQDN);
        this.accessToken = "Bearer " + token.accessToken;
    }

    /**
     * update token on existing client.
     * This is useful if the client is expected to be used over long time, and token has expired.
     *
     * @param accessToken The AAD Token string
     */
    public synchronized void updateToken(String accessToken) {
        log.trace("AAD Token Updated for client client {} for account {}", clientId, accountFQDN);
        this.accessToken = "Bearer " + accessToken;
    }

    /* ----------------------------------------------------------------------------------------------------------*/
    /* Private and internal methods                                                                              */
    /* ----------------------------------------------------------------------------------------------------------*/

    /**
     * Gets the Queue depth used for read-aheads in {@link ADLFileInputStream}
     * @return the queue depth
     */
    synchronized int getReadAheadQueueDepth() {
        return this.readAheadQueueDepth;
    }


    /**
     * gets the AAD access token associated with this client
     * @return String containing the AAD Access token
     * @throws IOException thrown if a token provider is being used and the token provider has problem getting token
     */
    synchronized String getAccessToken() throws IOException {
        if (tokenProvider != null ) {
            return "Bearer " + tokenProvider.getToken().accessToken;
        } else {
            return accessToken;
        }
    }


    /**
     * gets the Azure Data Lake Store account name associated with this client
     * @return the account name
     */
    String getAccountName() {
        return accountFQDN;
    }

    private synchronized void setUserAgentSuffix(String userAgentSuffix) {
        if (userAgentSuffix != null && !userAgentSuffix.trim().equals("")) {
            this.userAgentString = userAgent + "/" + userAgentSuffix;
            log.trace("ADLStoreClient {} created for {} changed User-Agent to {}", clientId, accountFQDN, userAgentString);
            if (userAgentSuffix.startsWith("hadoop-azure-datalake")) {
                int pos = userAgentSuffix.indexOf('/');
                if (pos > 0 && pos < (userAgentSuffix.length() - 1)) {
                    tiHeader = userAgentSuffix.substring(pos+1);
                } else {
                    tiHeader = null;
                }
            } else {
                tiHeader = null;
            }
        }
    }

    synchronized String getTiHeaderValue() {
        return tiHeader;
    }

    /**
     * Gets the HTTP User-Agent string that will be used for requests made from this client.
     * @return User-Agent string
     */
    synchronized String getUserAgent() {
        return userAgentString;
    }

    private synchronized void setInsecureTransport() {
        proto = "http";
    }

    boolean shouldAlterCipherSuits() {
        return this.alterCipherSuits;
    }

    /**
     * get the http prefix ({@code http} or {@code https}) that will be used for
     * connections used by thei client.
     * @return Sytring containing the HTTP protocol used ({@code http} or {@code https})
     */
    synchronized String getHttpPrefix() {
        return proto;
    }

    /**
     * Gets a unique long associated with this instance of {@code ADLStoreClient}
     *
     * @return a unique long associated with this instance of {@code ADLStoreClient}
     */
    long getClientId() {
        return this.clientId;
    }

    private synchronized void enableThrowingRemoteExceptions() {
        enableRemoteExceptions = true;
    }

    synchronized boolean remoteExceptionsEnabled() {
        return enableRemoteExceptions;
    }

    private synchronized void setFilePathPrefix(String prefix) throws IOException {
        if (prefix==null || prefix.equals("")) throw new IllegalArgumentException("prefix cannot be empty or null");
        if (prefix.equals("/")) return; // no prefix

        if (prefix.contains("//")) throw new IllegalArgumentException("prefix cannot contain empty path element " + prefix);
        if (prefix.charAt(0) != '/') prefix = "/" + prefix;
        if (prefix.charAt(prefix.length()-1) == '/') prefix = prefix.substring(0, prefix.length()-1);

        try {
            pathPrefix = (new URI(null, null, prefix, null)).toASCIIString();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("Invalid path prefix: " + prefix);
        }
    }

    /**
     * gets the SSL Channel mode for HTTPS calls made by methods in
     * ADLStoreClient objects
     * @return SSLChannelMode enum values as String
     */
    public SSLChannelMode getSSLChannelMode() {
        return this.sslChannelMode;
    }

    /**
     * gets the default timeout for HTTP calls made by methods in ADLStoreClient objects
     * @return default timeout, in Milliseconds
     */
    public int getDefaultTimeout() {
        return this.timeout;
    }

    /**
     * Gets the file path prefix used for this client.
     *
     * @return the path prefix (URL encoded)
     */
    synchronized String getFilePathPrefix() {
        return pathPrefix;
    }

    /* ----------------------------------------------------------------------------------------------------------*/
    /* Utility methods                                                                                                     */
    /* ----------------------------------------------------------------------------------------------------------*/

    /**
     * creates an {@link ADLException} from {@link OperationResponse}.
     *
     * @param resp the {@link OperationResponse} to convert to exception
     * @param defaultMessage message to use if the inner exception does not have a text message.
     * @return the {@link ADLException}, or {@code null} if the {@code resp.successful} is {@code true}
     */
    public IOException getExceptionFromResponse(OperationResponse resp, String defaultMessage) {
        String messageSuffix = " [ServerRequestId:" + resp.requestId + "]";
        if (remoteExceptionsEnabled() &&
                resp.remoteExceptionJavaClassName !=null &&
                !resp.remoteExceptionJavaClassName.equals("")) {
            return getRemoteException(resp.remoteExceptionJavaClassName, resp.remoteExceptionMessage + messageSuffix);
        } else {
            String msg = resp.message == null ? defaultMessage : defaultMessage + "\n" + resp.message;
            if (resp.ex != null) {
                msg = msg + "\nOperation " + resp.opCode + " failed with exception " + resp.ex.getClass().getName() + " : " + resp.ex.getMessage();
            } else if (resp.httpResponseCode > 0) {
                msg = msg + "\nOperation " + resp.opCode + " failed with HTTP" + resp.httpResponseCode + " : " + resp.remoteExceptionName;
            }
            msg = msg +
                    "\nLast encountered exception thrown after " +
                    (resp.numRetries+1) +
                    " tries. ";
            if (resp.exceptionHistory != null) msg = msg + "[" + resp.exceptionHistory + "]";
            msg = msg + "\n" + messageSuffix;

            ADLException ex = new ADLException(msg);
            ex.httpResponseCode = resp.httpResponseCode;
            ex.httpResponseMessage = resp.httpResponseMessage;
            ex.requestId = resp.requestId;
            ex.numRetries = resp.numRetries;
            ex.lastCallLatency = resp.lastCallLatency;
            ex.responseContentLength = resp.responseContentLength;
            ex.remoteExceptionName = resp.remoteExceptionName;
            ex.remoteExceptionMessage = resp.remoteExceptionMessage;
            ex.remoteExceptionJavaClassName = resp.remoteExceptionJavaClassName;
            ex.initCause(resp.ex);
            return ex;
        }
    }

    private static IOException getRemoteException(String className, String message) {
        try {
            Class clazz = Class.forName(className);
            if (!IOException.class.isAssignableFrom(clazz)) { return new IOException(message); }
            Constructor c = clazz.getConstructor(String.class);
            return (IOException) c.newInstance(message);
        } catch (Exception ex) {
            return new IOException(message);
        }
    }


}
