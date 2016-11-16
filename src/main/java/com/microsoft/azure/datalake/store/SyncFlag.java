/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */


package com.microsoft.azure.datalake.store;

/**
 * Indicator flags to backend during append.
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
