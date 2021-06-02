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

#ifndef MSG_H_
#define MSG_H_

#include "status.h"
#include "gc.h"

#define HDR_LINE_PRE        "NATS/1.0"
#define HDR_LINE_PRE_LEN    (8)
#define HDR_LINE            HDR_LINE_PRE _CRLF_
#define HDR_LINE_LEN        (10)
#define STATUS_HDR          "Status"
#define DESCRIPTION_HDR     "Description"
#define NO_RESP_STATUS      "503"
#define HDR_STATUS_LEN      (3)

struct __natsMsg;

struct __natsMsg
{
    natsGCItem          gc;

    // The message is allocated as a single memory block that contains
    // this structure and enough space for the headers/payload.
    // Headers, if any, start after the 'next' pointer, followed by
    // the message payload.
    const char          *subject;
    const char          *reply;
    char                *hdr;
    natsStrHash         *headers;
    const char          *data;
    int                 dataLen;
    int                 hdrLen;

    // subscription (needed when delivery done by connection)
    struct __natsSubscription *sub;

    bool                hdrLift;
    // True if the reply is set after the message was created and
    // therefore msg->reply points to a memory location outside
    // of the message object and needs to be freed separately.
    bool                freeRply;

    // Must be last field!
    struct __natsMsg    *next;

    // Nothing after this: the message headers/payload goes there.

};

struct __natsHeaderValue;

typedef struct __natsHeaderValue
{
    char                        *value;
    bool                        needFree;
    struct __natsHeaderValue    *next;

} natsHeaderValue;

int
natsMsgHeader_encodedLen(natsMsg *msg);

natsStatus
natsMsgHeader_encode(natsBuffer *buf, natsMsg *msg);

void
natsMsg_init(natsMsg *msg,
             const char *subject, const char *reply,
             const char *data, int dataLen);

natsStatus
natsMsg_create(natsMsg **newMsg,
               const char *subject, int subjLen,
               const char *reply, int replyLen,
               const char *buf, int bufLen, int hdrLen);

void
natsMsg_freeHeaders(natsMsg *msg);

// This needs to follow the nats_FreeObjectCb prototype (see gc.h)
void
natsMsg_free(void *object);

#endif /* MSG_H_ */
