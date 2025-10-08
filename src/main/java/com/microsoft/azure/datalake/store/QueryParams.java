/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Hashtable;


/**
 * Internal class for SDK's internal use. DO NOT USE.
 */
public class QueryParams {

    private Hashtable<String, String> params = new Hashtable<String, String>();
    Operation op = null;
    String apiVersion = null;
    String separator = "";
    String serializedString = null;

    /**
     * Creates an empty query parameter collection.
     */
    public QueryParams() {
    }

    /**
     * Adds a name/value pair to the query string.
     *
     * @param name parameter name to include.
     * @param value parameter value associated with the name.
     */
    public void add(String name, String value) {
        params.put(name, value);
        serializedString = null;
    }

    /**
     * Sets the ADLS operation whose name should be included in the query string.
     *
     * @param op operation to encode.
     */
    public void setOp(Operation op) {
        this.op = op;
        serializedString = null;
    }

    /**
     * Sets the API version that should be appended to the query string.
     *
     * @param apiVersion service API version identifier.
     */
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        serializedString = null;
    }

    /**
     * Serializes the configured parameters into a URL query string suitable for HTTP calls.
     *
     * @return serialized representation of the query parameters.
     */
    public String serialize()  {
        if (serializedString == null) {
            StringBuilder sb = new StringBuilder();

            if (op != null) {
                sb.append(separator);
                sb.append("op=");
                sb.append(op.name);
                separator = "&";
            }

            for (String name : params.keySet()) {
                try {
                    sb.append(separator);
                    sb.append(URLEncoder.encode(name, "UTF-8"));
                    sb.append('=');
                    sb.append(URLEncoder.encode(params.get(name), "UTF-8"));
                    separator = "&";
                } catch (UnsupportedEncodingException ex) {
                }
            }

            if (apiVersion != null) {
                sb.append(separator);
                sb.append("api-version=");
                sb.append(apiVersion);
                separator = "&";
            }
            serializedString = sb.toString();
        }
        return serializedString;
    }
}
