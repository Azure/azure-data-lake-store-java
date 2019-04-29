package com.microsoft.azure.datalake.store;

import java.util.List;

class DirectoryEntryListWithContinuationToken {
    private String continuationToken;
    private List<DirectoryEntry> entries;

    String getContinuationToken() {
        return continuationToken;
    }
    void setContinuationToken(String continuationToken){
        this.continuationToken = continuationToken;
    }

    List<DirectoryEntry> getEntries() {
        return entries;
    }

    void setEntries(List<DirectoryEntry> entries) {
        this.entries = entries;
    }

    DirectoryEntryListWithContinuationToken(String continuationToken, List<DirectoryEntry> entries){
        this.continuationToken = continuationToken;
        this.entries = entries;
    }
    DirectoryEntryListWithContinuationToken(){
        continuationToken = "";
        entries = null;
    }


}