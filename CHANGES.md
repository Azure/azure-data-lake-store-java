# Changes to the SDK

### Version 2.3.0-preview1
1. ADLInputStream can now optionally do read-ahead (on by default, with queue depth=4). Added 
   configuration option to ADLStoreOptions to set the queue depth for read-ahead.

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


