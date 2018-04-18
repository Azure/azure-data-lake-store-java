package com.microsoft.azure.datalake.store;


enum ReadBufferStatus {
    NOT_AVAILABLE,  // buffers sitting in readaheadqueue have this stats
    READING_IN_PROGRESS,  // reading is in progress on this buffer. Buffer should be in inProgressList
    AVAILABLE,  // data is available in buffer. It should be in completedList
    READ_FAILED  // read completed, but failed.
}