/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.*;

/**
 * Utility methods to enable one-liners for simple functionality.
 *
 * The methods are all based on calls to the SDK methods, these are
 * just convenience methods for common tasks.
 */
public class Utils {

    private ADLStoreClient client;

    Utils(ADLStoreClient client) {
        this.client = client;
    }

    /**
     * Uploads the contents of a local file to an Azure Data Lake file.
     *
     * @param filename path of file to upload to
     * @param localFilename path to local file
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @throws IOException thrown on error
     */
    public void upload(String filename, String localFilename, IfExists mode) throws IOException  {
        if (localFilename == null || localFilename.trim().equals(""))
            throw new IllegalArgumentException("localFilename cannot be null");

        try (FileInputStream in = new FileInputStream(localFilename)){
            upload(filename, in, mode);
        }
    }

    /**
     * Uploads an {@link InputStream} to an Azure Data Lake file.
     *
     * @param filename path of file to upload to
     * @param in {@link InputStream} whose contents will be uploaded
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @throws IOException thrown on error
     */
    public void upload(String filename, InputStream in, IfExists mode) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");
        if (in == null) throw new IllegalArgumentException("InputStream cannot be null");

        try (ADLFileOutputStream out = client.createFile(filename, mode)) {
            int bufSize = 4 * 1000 * 1000;
            out.setBufferSize(bufSize);
            byte[] buffer = new byte[bufSize];
            int n;

            while ((n = in.read(buffer)) != -1) {
                out.write(buffer, 0, n);
            }
        }
        in.close();
    }

    /**
     * Uploads the contents of byte array to an Azure Data Lake file.
     *
     * @param filename path of file to upload to
     * @param contents byte array containing the bytes to upload
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @throws IOException thrown on error
     */
    public void upload(String filename, byte[] contents, IfExists mode) throws IOException  {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        if (contents.length <= 4 * 1024 * 1024) { // if less than 4MB, then do a direct CREATE in a single operation
            boolean overwrite = (mode==IfExists.OVERWRITE);
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = overwrite ? new ExponentialBackoffPolicy() : new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            Core.create(filename, overwrite, null, contents, 0, contents.length, null, null, true, SyncFlag.CLOSE, client, null, opts, resp);
            if (!resp.successful) {
                throw client.getExceptionFromResponse(resp, "Error creating file " + filename);
            }
        } else {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(contents)){
                upload(filename, bis, mode);
            }
        }
    }
}
