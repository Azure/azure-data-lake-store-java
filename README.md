# Azure Datalake Store client for Java
 - For an introduction to Azure Data Lake, see here: https://azure.microsoft.com/en-us/services/data-lake-store/
 
 - For getting started introduction to the SDK, see here: https://github.com/Azure-samples/data-lake-store-java-upload-download-get-started 
  
 - For the SDK Javadoc, see here: http://azure.github.io/azure-data-lake-store-java/javadoc
 
## Dependency maintenance
- To refresh dependencies to the latest stable releases, run `mvn versions:use-latest-releases -DincludeScope=compile "-Dmaven.version.ignore=.*-alpha.*,.*-beta.*,.*-rc.*"` from the project root.
- Review the resulting changes in `pom.xml` before committing to confirm no prerelease versions slipped through.