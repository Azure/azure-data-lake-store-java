# Changes to the SDK
### Version 2.3.9
1. Fix the setters in adlstoreoptions to return instance of adlstoreoptions
2. Fetch MSI token directly from AAD if first fetch from MSI cache is the same expiring token.
3. Fix getcontentsummary to do enumerates using continuation token

### Version 2.3.8
1. Add logging for token acquisition.
2. Allow user configured retry count, interval, and exponential factor for ExponentialBackoffPolicy

### Version 2.3.7
1. Implement create with overwrite using conditional delete. Enable this based on config.

### Version 2.3.6
1. Added configuration option to choose SSL channel mode
   (AdlStoreOptions.setSSLChannelMode(String mode) with possible mode values
   being - (a) OpenSSL (b) Default_JSSE (c) Default
   [(c) is the default choice if the config is not present or is invalid.
   When set to (c), connection will be created in OpenSSL mode and will
   default to Default_JSSE on any failure.]

### Version 2.3.5
1. Updated wildfly openssl version and removed shading of the package
2. Fix bug in ordering in json parsing for liststatus response

### Version 2.3.4
1. Updated enumerateDirectory to use continuationToken
2. Updated api-version to 2018-09-01
3. Update version of jackson.core and maven-javadoc
4. separate out append and closehandle for SyncFlag close to avoid race condition on retries

### Version 2.3.3
1. Source files list will go in json format for concat operation to handle special characters in source filenames
2. Prevent FileAlreadyExists exception for create with overwrite 
3. Update com.fasterxml.jackson.core:jackson-core to 2.7.9 to avoid security vulnerability
4. Disable wildfly logs to the console.

### Version 2.3.2
1. Add special handling for 404 errors when requesting tokens from MSI
2. Fix liststatus response parsing when filestatus object contains array in one field.
3. Use wildfly openssl native binding with Java. This is a workaround to https://bugs.openjdk.java.net/browse/JDK-8046943 issue. 2X performance boost over HTTPS.

### Version 2.3.1
1. Made the default queue depth to zero (basically disabling read-ahead by default). Set readahead queue depth 
   using ADLStoreOptions.setReadAheadQueueDepth() to enable read-ahead

### Version 2.3.0-preview2
1. Made timeouts more aggressive, and made the ADLStoreClient's default timeout configurable (ADLStoreOptions.setDefaultTimeout)
2. Do automatic retry for the case where a read requests succeeds, but then reading the content stream a fails because of network issue

### Version 2.3.0-preview1
1. ADLInputStream can now optionally do read-ahead (on by default, with queue depth=4). Added 
   configuration option to ADLStoreOptions to set the queue depth for read-ahead.
2. Changed REST API version to 2017-08-01 (required for read-aheads)
3. Internal bug fixes


### Version 2.2.8
1. Increase default timeout to 60 seconds

### Version 2.2.7
1. Bugfix to IMDS-based MSI
2. More text added to AAD token acquisition exception message

### Version 2.2.6
1. Changed implementation of the MSI token provider to use the new IMDS REST endpoint required by AAD 
2. Made AAD token acquisition more robust - added timeout, retry, disable keep-alive, improved exception text

### Version 2.2.5
1. Made HTTP 429 error retry-able on non-idempotent calls, since HTTP429 does not make a state change on the server

### Version 2.2.4
1. Made timeouts more aggressive, and made the ADLStoreClient's default timeout configurable (`ADLStoreOptions.setDefaultTimeout`)
2. Do automatic retry for the case where a read requests succeeds, but then reading the content stream a fails because of network issue

### Version 2.2.3
1. Made port number and tenant Guid optional for MsiTokenProvider.

### Version 2.2.2
1. (internal-only fix, no user impact) Change MSI token acquisition call to add HTTP header (Metadata: true)

### Version 2.2.1
1. Added support for DeviceCode auth flow
2. Added support for acquiring token using Azure VM's MSI service
3. Switched all internal TokenProviders to use https://datalake.azure.net/ as "resource" in AAD Tokens instead of 
   https://management.core.windows.net/
4. Misc robustness fixes in ADLStoreClient.GetContentSummary

### Version 2.1.5
1. Fixed bug in ADLFileOutputStream, where calling close() right after calling flush() would not release the lease 
   on the file, locking the file out for 10 mins
2. Added server trace ID to exception messages, so failures are easier to troubleshoot for customer-support calls
3. Changed exception handling for token fetching; exceptions from the token fetching process were previously getting 
   replaced with a generic (unhelpful) error message

### Version 2.1.4
1. Fixed bug in Core.listStatus() for expiryTime parsing

### Version 2.1.2
1. Changed implementation of ADLStoreClient.getContentSummary to do the directory enumeration on client side rather than
   server side. This makes the call more performant and reliable.
2. Removed short-circuit check in Core.concurrentAppend, which bypassed sending append to server for a 0-length append.

### Version 2.1.1
1. Added setExpiry method
2. Core.concat has an additional parameter called deleteSourceDirectory, to address specific needs for tool-writers
3. enumerateDirectory and getDirectoryEntry (liststatus and getfilestatus in Core) now return two additional 
   fields: aclBit and expiryTime
4. enumerateDirectory, getDirectoryEntry and getAclStatus now take additional parameter for 
   UserGroupRepresentation (OID or UPN)
5. enumerateDirectory now does paging on the client side, to avoid timeouts on lerge directories (no change 
   to API interface)

### Version 2.0.11
- Initial Release


