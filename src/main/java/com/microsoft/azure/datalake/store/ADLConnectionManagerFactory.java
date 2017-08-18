/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import java.util.Stack;

/**
 * {@code ADLConnectionManagerFactory} is used to get HttpClientConnectionManager
 *
 */
public class ADLConnectionManagerFactory {

    private static HttpClientConnectionManager poolingConnectionManager = new PoolingHttpClientConnectionManager();
    private static Stack<HttpClientConnectionManager> connectionManagers = new Stack<>();

    private static int connectionManagerCount = 0;
    private static final int maxConnectionManagers = 10;

    public static synchronized HttpClientConnectionManager getConnectionManager() {
        if (!connectionManagers.isEmpty()) {
            return connectionManagers.pop();
        }

        if (connectionManagerCount < maxConnectionManagers) {
            connectionManagerCount++;
            return new BasicHttpClientConnectionManager();
        }

        return poolingConnectionManager;
    }

    public static synchronized void returnConnectionManager(HttpClientConnectionManager connectionManager) {
        if (connectionManager instanceof BasicHttpClientConnectionManager) {
            connectionManagers.push(connectionManager);
        }
    }
}