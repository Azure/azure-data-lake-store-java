# Changes to the SDK

### Version 2.1.1
1. added setExpiry method
2. Core.concat has an additional parameter called deleteSourceDirectory, to address specific needs for tool-writers
3. enumerateDirectory and getDirectoryEntry (liststatus and getfilestatus in Core) now return two additional fields: aclBit and expiryTime
4. enumerateDirectory, getDirectoryEntry and getAclStatus now take additional parameter for UserGroupRepresentation (OID or UPN)
5. enumerateDirectory now does paging on the client side, to avoid timeouts on lerge directories (no change to API interface)

### Version 2.0.11
- Initial Release


