/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.util.Stack;

/**
 * {@code HttpContextStore} is used to store and retrieve HttpContext objects
 *
 */
public class HttpContextStore {

    private static int maxHttpContexts = 10;
    private static Stack<HttpContext> httpContexts = new Stack<>();

    static {
        for (int i = 0; i < maxHttpContexts; ++i) {
            BasicHttpContext httpContext = new BasicHttpContext();
            httpContext.setAttribute(HttpClientContext.USER_TOKEN, httpContext);
            httpContexts.push(httpContext);
        }
    }

    public synchronized static HttpContext getHttpContext() {
        if (!httpContexts.empty()) {
            return httpContexts.pop();
        }

        return null;
    }

    public synchronized static void releaseHttpContext(HttpContext httpContext) {
        if (httpContext != null) {
            httpContexts.push(httpContext);
        }
    }
}