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
import java.util.UUID;

/**
 * {@code HttpContextStore} is used to store and retrieve HttpContext objects
 *
 */
public class HttpContextStore {

    private static Stack<HttpContext> httpContexts = new Stack<>();

    private static HttpContext getNewHttpContext() {
        BasicHttpContext httpContext = new BasicHttpContext();
        httpContext.setAttribute(HttpClientContext.USER_TOKEN, UUID.randomUUID().toString());
        return httpContext;
    }

    public static HttpContext getHttpContext() {
        synchronized(HttpContextStore.class) {
            if (!httpContexts.empty()) {
                return httpContexts.pop();
            }
        }

        return getNewHttpContext();
    }

    public static void releaseHttpContext(HttpContext httpContext) {
        if (httpContext == null) return;

        synchronized(HttpContextStore.class) {
            httpContexts.push(httpContext);
        }
    }
}