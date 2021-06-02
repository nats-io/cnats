// Copyright 2015-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "natsp.h"

static const char *statusText[] = {
    "OK",

    "Error",
    "Protocol Error",
    "IO Error",
    "Line too long",

    "Connection Closed",
    "No server available for connection",
    "Stale Connection",
    "Secure Connection not available",
    "Secure Connection Required",
    "Connection Disconnected",
    "Authentication Violation",

    "Not Permitted",
    "Not Found",

    "TCP Address missing",

    "Invalid Subject",
    "Invalid Argument",
    "Invalid Subscription",
    "Invalid Timeout",

    "Illegal State",

    "Slow Consumer, messages dropped",

    "Maximum Payload Exceeded",
    "Maximum Messages Delivered",

    "Insufficient Buffer",

    "No Memory",

    "System Error",

    "Timeout",

    "Initialization Failed",
    "Not Initialized",

    "SSL Error",

    "Not Supported By Server",

    "Not Yet Connected",

    "Draining in progress",

    "Invalid queue name",

    "No responders available for request",

    "JetStream not enabled",
    "Stream name is required",
    "Invalid stream name",
};

const char*
natsStatus_GetText(natsStatus s) {
    return statusText[(int) s];
}
