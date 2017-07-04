/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;

/**
 *
 * Shows the login message for device code to user. The default implementation shows on the console.
 * Subclasses can override the {@link DeviceCodeCallback#showDeviceCodeMessage(DeviceCodeInfo)} method to
 * display the message in a different way, appropriate for the context the program is running in.
 *
 */
class DeviceCodeCallback {

    private static DeviceCodeCallback defaultInstance = new DeviceCodeCallback();

    /**
     * Show the message to the user, instructing them to log in using the browser.
     * This method displays the message on standard output; subclasses may display
     * it differently.
     *
     * @param dcInfo {@link DeviceCodeInfo} object containing the info to display
     */
    public void showDeviceCodeMessage(DeviceCodeInfo dcInfo) {
        System.out.println(dcInfo.message);
    }

    /**
     * Returns an instance of the default {@link DeviceCodeCallback}
     *
     * @return an instance of the default {@link DeviceCodeCallback}
     */
    public static DeviceCodeCallback getDefaultInstance() {
        return defaultInstance;
    }

}

