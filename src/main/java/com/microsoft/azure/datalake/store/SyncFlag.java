/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */


package com.microsoft.azure.datalake.store;

/**
 * Indicator flags to backend during append.
 * Optionally indicates what to do after completion of the concurrent append.
 * DATA indicates that more data will be sent immediately by the client, the file handle should
 * remain open/locked, and file metadata (including file length, last modified time) should NOT
 * get updated.
 * METADATA indicates that more data will be sent immediately by the client, the file handle should
 * remain open/locked, and file metadata should get updated. CLOSE indicates that the client is
 * done sending data, the file handle should be closed/unlocked, and file metadata should get
 * updated.
 * Possible values include: 'DATA', 'METADATA', 'CLOSE'
 */
public enum SyncFlag {
  /**
   * No update is required, and hold lease. - Performant operation.
   */
  DATA,

  /**
   * Update metdata of the file after the given data is appended to file.
   */
  METADATA,

  /**
   * Update metdata of the file after the given data is appended to file.
   * And close file handle. Once the file handle is closed, lease on the
   * file is released if the stream is opened with leaseid.
   */
  CLOSE
}
