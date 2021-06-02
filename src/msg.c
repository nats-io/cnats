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

// Do this after including natsp.h in order to have some of the
// GNU specific flag set first.
#include <string.h>
#include <ctype.h>

#include "mem.h"
#include "msg.h"

int
natsMsgHeader_encodedLen(natsMsg *msg)
{
    natsStrHashIter iter;
    char            *key = NULL;
    void            *p   = NULL;
    int             hl   = 0;

    // Special case: if msg->hdrLift is true, it means that this is a message
    // that was received but never lifted (so for sure no header was added,
    // modified or removed. So return the current len of the encoded headers.
    if (msg->hdrLift)
        return msg->hdrLen;

    // Here it could be that a message was created, some headers added but
    // then all were removed before the send. Returning 0 here means that
    // the publish will send PUB instead of HPUB. We could choose to return
    // hdrLinePreLen + _CRLF_LEN_ + _CRLF_LEN_ instead, which means that
    // we would encode "NATS/1.0\r\n\r\n" only.
    if (msg->headers == NULL)
        return 0;

    hl = HDR_LINE_LEN;
    natsStrHashIter_Init(&iter, msg->headers);
    while (natsStrHashIter_Next(&iter, &key, &p))
    {
        natsHeaderValue *v = (natsHeaderValue*) p;
        natsHeaderValue *c;

        for (c = v; c != NULL; c = c->next)
        {
            hl += (int) strlen(key) + 2; // 2 for ": "
            hl += (int) strlen(c->value) + _CRLF_LEN_;
        }
    }
    natsStrHashIter_Done(&iter);
    hl += _CRLF_LEN_;

    return hl;
}

natsStatus
natsMsgHeader_encode(natsBuffer *buf, natsMsg *msg)
{
    natsStrHashIter iter;
    natsStatus      s    = NATS_OK;
    char            *key = NULL;
    void            *p   = NULL;

    // See explanation in natsMsgHeader_encodedLen()
    if (msg->hdrLift)
    {
        natsBuf_Append(buf, (const char*) msg->hdr, msg->hdrLen);
        return NATS_UPDATE_ERR_STACK(s);
    }

    // Based on decision in natsMsgHeader_encodedLen(),
    // getting here with NULL headers is likely a bug.
    if (msg->headers == NULL)
        return nats_setError(NATS_ERR, "%s", "trying to encode headers while there is none");

    s = natsBuf_Append(buf, HDR_LINE, HDR_LINE_LEN);
    if (s == NATS_OK)
    {
        natsStrHashIter_Init(&iter, msg->headers);
        while ((s == NATS_OK) && natsStrHashIter_Next(&iter, &key, &p))
        {
            natsHeaderValue *v = (natsHeaderValue*) p;
            natsHeaderValue *c;

            for (c = v; (s == NATS_OK) && (c != NULL); c = c->next)
            {
                s = natsBuf_Append(buf, (const char*) key, (int) strlen(key));
                if (s == NATS_OK)
                    s = natsBuf_Append(buf, ": ", 2);
                if (s == NATS_OK)
                {
                    int vl  = (int) strlen(c->value);
                    int pos = natsBuf_Len(buf);

                    s = natsBuf_Append(buf, (const char*) c->value, vl);
                    if (s == NATS_OK)
                    {
                        char *ch = natsBuf_Data(buf)+pos;
                        int  i;

                        for (i=0; i<vl; i++)
                        {
                            if ((*ch == '\r') || (*ch == '\n'))
                                *ch = ' ';
                            ch++;
                        }
                    }
                }
                if (s == NATS_OK)
                    s = natsBuf_Append(buf, _CRLF_, _CRLF_LEN_);
            }
        }
        natsStrHashIter_Done(&iter);
    }
    if (s == NATS_OK)
        s = natsBuf_Append(buf, _CRLF_, _CRLF_LEN_);

    return NATS_UPDATE_ERR_STACK(s);
}

void
natsHeaderValue_free(natsHeaderValue *v, bool all)
{
    if (v == NULL)
        return;

    if (v->needFree && (v->value != NULL))
        NATS_FREE(v->value);

    if (all && v->next != NULL)
        natsHeaderValue_free(v->next, all);

    NATS_FREE(v);
}

natsStatus
natsHeaderValue_create(natsHeaderValue **retV, const char *value, bool makeCopy)
{
    natsStatus      s   = NATS_OK;
    char            *cv = (char*) value;
    natsHeaderValue *v  = NULL;

    *retV = NULL;

    v = NATS_MALLOC(sizeof(natsHeaderValue));
    if (v == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    if (makeCopy && value != NULL)
    {
        DUP_STRING(s, cv, value);
        if (s != NATS_OK)
        {
            NATS_FREE(v);
            return NATS_UPDATE_ERR_STACK(s);
        }
    }
    v->value    = cv;
    v->needFree = makeCopy;
    v->next     = NULL;
    *retV       = v;

    return NATS_OK;
}

static natsStatus
_checkMsgAndKey(natsMsg *msg, const char *key)
{
    if (msg == NULL)
        return nats_setError(NATS_INVALID_ARG, "%s", "message cannot be NULL");

    if ((key == NULL) || (key[0] == '\0'))
        return nats_setError(NATS_INVALID_ARG, "%s", "key cannot be NULL nor empty");

    return NATS_OK;
}

static char*
_moveToLF(char *end, char *ptr)
{
    while (ptr != end)
    {
        if ((*ptr == '\r') && (*(ptr+1) == '\n'))
            return ++ptr;
        else
            ptr++;
    }
    return ptr;
}

static natsStatus
_processKeyValue(int line, natsMsg *msg, char *endPtr, char **pPtr, char **lastKey)
{
    natsStatus      s    = NATS_OK;
    char            *ptr = *pPtr;
    char            *col = NULL;
    char            *key = NULL;
    char            *val = NULL;
    natsHeaderValue *v   = NULL;
    bool            ml   = false;
    char            *start;
    char            *endval;

    start = ptr;
    if (*ptr == '\r')
    {
        if ((++ptr == endPtr) || ((*ptr == '\n') && (++ptr == endPtr)))
        {
            *pPtr = ptr;
            return NATS_OK;
        }
        return nats_setError(NATS_PROTOCOL_ERROR, "invalid start of a key: %s", start);
    }
    if (isspace(*ptr))
    {
        if (line == 0)
            return nats_setError(NATS_PROTOCOL_ERROR, "key cannot start with a space: %s", ptr);

        key = *lastKey;
        ml = true;
    }
    else
    {
        col = strchr((const char*) ptr, (int) ':');
        if (col == NULL)
            return nats_setError(NATS_PROTOCOL_ERROR, "column delimiter not found: %s", ptr);

        // Replace column char with \0 to terminate the key string.
        key = ptr;
        ptr = col+1;
        (*col) = '\0';
    }

    while ((ptr != endPtr) && (isspace(*ptr)))
        ptr++;

    if (ptr == endPtr)
        return nats_setError(NATS_PROTOCOL_ERROR, "no value found for key %s", key);

    val = ptr;
    // Now find the \r\n for this value
    ptr = _moveToLF(endPtr, ptr);
    if (ptr == endPtr)
        return nats_setError(NATS_PROTOCOL_ERROR, "no CRLF found for value of key %s", key);

    // Trim right spaces and set to \0 to terminate the value string.
    endval = ptr;
    // Backtrack to \r and any space characters. Make sure we don't go
    // past the beginning of the value pointer.
    endval--;
    if (*endval == '\r')
        endval--;
    while ((endval != val) && (isspace(*endval)))
        endval--;
    endval++;
    *(endval) = '\0';

    if (ml)
    {
        char *newValue = NULL;

        natsHeaderValue *cur = natsStrHash_Get(msg->headers, key);
        if (cur == NULL)
            return nats_setError(NATS_PROTOCOL_ERROR, "unable to process folding lines for key %s", key);

        for (; cur->next != NULL; )
            cur = cur->next;

        if (nats_asprintf(&newValue, "%s %s", cur->value, val) == -1)
            return nats_setDefaultError(NATS_NO_MEMORY);

        if (cur->needFree)
            NATS_FREE(cur->value);

        cur->value    = newValue;
        cur->needFree = true;
    }
    else
    {
        s = natsHeaderValue_create(&v, (const char*) val, false);
        if (s == NATS_OK)
        {
            natsHeaderValue *cur = natsStrHash_Get(msg->headers, key);
            if (cur != NULL)
            {
                for (; cur->next != NULL; )
                    cur = cur->next;

                cur->next = v;
            }
            else
                s = natsStrHash_Set(msg->headers, (char*) key, false, (void*) v, NULL);
        }
    }

    if (s == NATS_OK)
    {
        ptr++;
        *pPtr = ptr;
        *lastKey = key;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_liftHeaders(natsMsg *msg, bool setOrAdd)
{
    natsStatus s       = NATS_OK;
    char       *ptr    = NULL;
    char       *sts    = NULL;
    char       *endPtr = NULL;
    char       *lk     = NULL;
    int        i;

    // If there is no header map and hdrLift is false, and this is not
    // an action to set or add a header, then simply return.
    if (!setOrAdd && (msg->headers == NULL) && !msg->hdrLift)
        return NATS_OK;

    // For set or add operations, possibly create the headers map.
    if (msg->headers == NULL)
    {
        s = natsStrHash_Create(&(msg->headers), 4);
        if (s != NATS_OK)
            return NATS_UPDATE_ERR_STACK(s);
    }

    // In all cases, if there is no need to lift, we are done.
    if (!msg->hdrLift)
        return NATS_OK;

    // If hdrLen is less than what we need for NATS/1.0\r\n, then
    // clearly this is a bad header.
    if ((msg->hdrLen < HDR_LINE_LEN) || (strstr(msg->hdr, HDR_LINE_PRE) != msg->hdr))
        return nats_setError(NATS_PROTOCOL_ERROR, "header prefix missing: %s", msg->hdr);

    endPtr = (char*) (msg->hdr + msg->hdrLen);

    sts = (char*) (msg->hdr + HDR_LINE_PRE_LEN);
    while ((sts != endPtr) && (*sts == ' '))
        sts++;

    ptr = sts;
    ptr = _moveToLF(endPtr, ptr);
    if (ptr != endPtr)
    {
        char *stsEnd = ptr;

        ptr++;
        while ((stsEnd != sts) && (*stsEnd != '\r'))
            stsEnd--;

        // Terminate the status.
        *stsEnd = '\0';
    }

    if (ptr == endPtr)
        return nats_setError(NATS_PROTOCOL_ERROR, "early termination of headers: %s", msg->hdr);

    for (i=0; ((s == NATS_OK) && (ptr != endPtr)); i++)
        s = _processKeyValue(i, msg, endPtr, &ptr, &lk);

    if (s == NATS_OK)
    {
        // At this point we have had no protocol error lifting the header
        // so we clear this flag so that we don't attempt to lift again.
        msg->hdrLift = false;

        // Furthermore, we need the flag to be cleared should we need to
        // add the no responders header (otherwise we would recursively
        // try to lift headers).
        // If adding the field fails, it is likely due to memory issue,
        // so it is fine to keep "hdrLift" as false.

        // Check if we have an inlined status.
        if ((sts != NULL) && (*sts != '\0'))
        {
            // There could be a description...
            if (strlen(sts) > HDR_STATUS_LEN)
            {
                char *desc = (char*) (sts + HDR_STATUS_LEN);
                char descb = 0;

                // Save byte that starts the description
                descb = *desc;
                // Replace with '\0' to end the status.
                *desc = '\0';

                // Set status value (this will make a copy)
                s = natsMsgHeader_Set(msg, STATUS_HDR, (const char*) sts);
                if (s == NATS_OK)
                {
                    char *desce = NULL;

                    // Restore character of starting description
                    *desc = descb;
                    // Trim left spaces
                    while ((*desc != '\0') && isspace((int) *desc))
                        desc++;

                    // If we are not at the end of description
                    if (*desc != '\0')
                    {
                        // Go to end of description and walk back to trim right.
                        desce = (char*) (desc + (int) strlen(desc) - 1);
                        while ((desce != desc) && isspace((int) *desce))
                        {
                            *desce = '\0';
                            desce--;
                        }
                    }
                    // If there is a description, set the value (this will make a copy)
                    if (*desc != '\0')
                        s = natsMsgHeader_Set(msg, DESCRIPTION_HDR, (const char*) desc);
                }
            }
            else
                s = natsMsgHeader_Set(msg, STATUS_HDR, (const char*) sts);
        }
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Set(natsMsg *msg, const char *key, const char *value)
{
    natsStatus  s = NATS_OK;

    if ((s = _checkMsgAndKey(msg, key)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((s = _liftHeaders(msg, true)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (s == NATS_OK)
    {
        natsHeaderValue *v = NULL;

        s = natsHeaderValue_create(&v, value, true);
        if (s == NATS_OK)
        {
            void *p = NULL;

            s = natsStrHash_Set(msg->headers, (char*) key, true, (void*) v, &p);
            if (s != NATS_OK)
                natsHeaderValue_free(v, false);
            else if (p != NULL)
            {
                natsHeaderValue *old = (natsHeaderValue*) p;
                natsHeaderValue_free(old, true);
            }
        }
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Add(natsMsg *msg, const char *key, const char *value)
{
    natsStatus  s = NATS_OK;

    if ((s = _checkMsgAndKey(msg, key)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((s = _liftHeaders(msg, true)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (s == NATS_OK)
    {
        natsHeaderValue *v = NULL;

        s = natsHeaderValue_create(&v, value, true);
        if (s == NATS_OK)
        {
            natsHeaderValue *cur = natsStrHash_Get(msg->headers, (char*) key);
            if (cur != NULL)
            {
                for (; cur->next != NULL; )
                    cur = cur->next;

                cur->next = v;
            }
            else
                s = natsStrHash_Set(msg->headers, (char*) key, true, (void*) v, NULL);
        }
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Get(natsMsg *msg, const char *key, const char **value)
{
    natsStatus      s   = NATS_OK;
    natsHeaderValue *v  = NULL;

    if ((s = _checkMsgAndKey(msg, key)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (value == NULL)
        return nats_setError(NATS_INVALID_ARG, "%s", "value cannot be NULL");

    *value = NULL;

    if ((s = _liftHeaders(msg, false)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((msg->headers == NULL) || natsStrHash_Count(msg->headers) == 0)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    v = natsStrHash_Get(msg->headers, (char*) key);
    if (v == NULL)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    *value = (const char*) v->value;

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Values(natsMsg *msg, const char *key, const char* **values, int *count)
{
    natsStatus      s       = NATS_OK;
    int             c       = 0;
    natsHeaderValue *cur    = NULL;
    const char*     *strs   = NULL;
    natsHeaderValue *v      = NULL;

    if ((s = _checkMsgAndKey(msg, key)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((values == NULL) || (count == NULL))
        return nats_setError(NATS_INVALID_ARG, "%s", "value or count cannot be NULL");

    *values = NULL;
    *count  = 0;

    if ((s = _liftHeaders(msg, false)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((msg->headers == NULL) || natsStrHash_Count(msg->headers) == 0)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    v = natsStrHash_Get(msg->headers, (char*) key);
    if (v == NULL)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    for (cur=v; cur != NULL; cur = cur->next)
        c++;

    strs = NATS_CALLOC(c, sizeof(char*));
    if (strs == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    else
    {
        int i = 0;

        for (cur=v; cur != NULL; cur = cur->next)
            strs[i++] = (const char*) cur->value;

        *values = strs;
        *count  = c;
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Keys(natsMsg *msg, const char* **keys, int *count)
{
    natsStatus  s     = NATS_OK;
    const char* *strs = NULL;
    int         c     = 0;

    if (msg == NULL)
        return nats_setError(NATS_INVALID_ARG, "%s", "message cannot be NULL");

    if ((keys == NULL) || (count == NULL))
        return nats_setError(NATS_INVALID_ARG, "%s", "keys or count cannot be NULL");

    *keys  = NULL;
    *count = 0;

    if ((s = _liftHeaders(msg, false)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((msg->headers == NULL) || (c = natsStrHash_Count(msg->headers)) == 0)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    strs = NATS_CALLOC(c, sizeof(char*));
    if (strs == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    else
    {
        natsStrHashIter iter;
        char            *hk = NULL;
        int             i;

        natsStrHashIter_Init(&iter, msg->headers);
        for (i=0; natsStrHashIter_Next(&iter, &hk, NULL); i++)
        {
            strs[i] = (const char*) hk;
        }
        natsStrHashIter_Done(&iter);

        *keys  = strs;
        *count = c;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsMsgHeader_Delete(natsMsg *msg, const char *key)
{
    natsStatus      s  = NATS_OK;
    natsHeaderValue *v = NULL;

    if ((s = _checkMsgAndKey(msg, key)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((s = _liftHeaders(msg, false)) != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((msg->headers == NULL) || natsStrHash_Count(msg->headers) == 0)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    v = natsStrHash_Remove(msg->headers, (char*) key);
    if (v == NULL)
        return NATS_NOT_FOUND; // normal error, so don't update error stack

    natsHeaderValue_free(v, true);

    return s;
}

void
natsMsg_freeHeaders(natsMsg *msg)
{
    natsStrHashIter iter;
    void            *p = NULL;

    if (msg->headers == NULL)
        return;

    natsStrHashIter_Init(&iter, msg->headers);
    for (;natsStrHashIter_Next(&iter, NULL, &p);)
    {
        natsHeaderValue *v = (natsHeaderValue *)p;
        natsHeaderValue_free(v, true);
    }
    natsStrHash_Destroy(msg->headers);
}

void
natsMsg_free(void *object)
{
    natsMsg *msg;

    if (object == NULL)
        return;

    msg = (natsMsg*) object;

    if (msg->freeRply && (msg->reply != NULL))
        NATS_FREE((char*) msg->reply);

    natsMsg_freeHeaders(msg);

    NATS_FREE(msg);
}

void
natsMsg_Destroy(natsMsg *msg)
{
    if (msg == NULL)
        return;

    if (natsGC_collect((natsGCItem *) msg))
        return;

    natsMsg_free((void*) msg);
}

const char*
natsMsg_GetSubject(const natsMsg *msg)
{
    if (msg == NULL)
        return NULL;

    return (const char*) msg->subject;
}

const char*
natsMsg_GetReply(const natsMsg *msg)
{
    if (msg == NULL)
        return NULL;

    return (const char*) msg->reply;
}

const char*
natsMsg_GetData(const natsMsg *msg)
{
    if (msg == NULL)
        return NULL;

    return (const char*) msg->data;
}

int
natsMsg_GetDataLength(const natsMsg *msg)
{
    if (msg == NULL)
        return 0;

    return msg->dataLen;
}

natsStatus
natsMsg_create(natsMsg **newMsg,
               const char *subject, int subjLen,
               const char *reply, int replyLen,
               const char *buf, int bufLen, int hdrLen)
{
    natsMsg     *msg      = NULL;
    char        *ptr      = NULL;
    int         bufSize   = 0;
    int         dataLen   = bufLen;
    bool        hasHdrs   = (hdrLen > 0 ? true : false);

    bufSize  = subjLen;
    bufSize += 1;
    if (replyLen > 0)
    {
        bufSize += replyLen;
        bufSize += 1;
    }
    bufSize += bufLen;
    bufSize += 1;
    if (hasHdrs)
        bufSize++;

    msg = NATS_MALLOC(sizeof(natsMsg) + bufSize);
    if (msg == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    // To be safe, we could 'memset' the message up to sizeof(natsMsg),
    // but since we are explicitly initializing most of the fields, we save
    // on that call, but we need to make sure what we initialize all fields!!!

    // That being said, we memset the 'gc' structure to protect us in case
    // some fields are later added to this 'external' structure and we forget
    // about updating this initialization code.
    memset(&(msg->gc), 0, sizeof(natsGCItem));

    msg->hdr     = NULL;
    msg->hdrLen  = 0;
    msg->hdrLift = false;
    msg->headers = NULL;
    msg->sub     = NULL;
    msg->freeRply= false;
    msg->next    = NULL;

    ptr = (char*) (((char*) &(msg->next)) + sizeof(msg->next));

    msg->subject = (const char*) ptr;
    memcpy(ptr, subject, subjLen);
    ptr += subjLen;
    *(ptr++) = '\0';

    if (replyLen > 0)
    {
        msg->reply = (const char*) ptr;
        memcpy(ptr, reply, replyLen);
        ptr += replyLen;
        *(ptr++) = '\0';
    }
    else
    {
        msg->reply = NULL;
    }

    if (hasHdrs)
    {
        msg->hdr = ptr;
        memcpy(ptr, buf, hdrLen);
        ptr += hdrLen;
        *(ptr++) = '\0';

        msg->hdrLen  = hdrLen;
        msg->hdrLift = true;
        dataLen -= hdrLen;
        buf += hdrLen;
    }
    msg->data    = (const char*) ptr;
    msg->dataLen = dataLen;
    memcpy(ptr, buf, dataLen);
    ptr += dataLen;
    *(ptr) = '\0';

    // Setting the callback will trigger garbage collection when
    // natsMsg_Destroy() is invoked.
    msg->gc.freeCb = natsMsg_free;

    *newMsg = msg;

    return NATS_OK;
}

// Used internally to initialize a message structure, generally defined on the stack,
// that will then be passed as a reference to publish functions.
void
natsMsg_init(natsMsg *msg, const char *subj, const char *reply, const char *data, int dataLen)
{
    memset(msg, 0, sizeof(natsMsg));
    msg->subject = subj;
    msg->reply = reply;
    msg->data = data;
    msg->dataLen = dataLen;
}

natsStatus
natsMsg_Create(natsMsg **newMsg, const char *subj, const char *reply,
               const char *data, int dataLen)
{
    natsStatus  s = NATS_OK;

    if ((subj == NULL)
        || (subj[0] == '\0')
        || ((reply != NULL) && (reply[0] == '\0')))
    {
        return nats_setDefaultError(NATS_INVALID_ARG);
    }

    s = natsMsg_create(newMsg,
                       subj, (int) strlen(subj),
                       reply, (reply == NULL ? 0 : (int) strlen(reply)),
                       data, dataLen, -1);

    return NATS_UPDATE_ERR_STACK(s);
}

bool
natsMsg_IsNoResponders(natsMsg *m)
{
    const char *val = NULL;

    // To be a "no responders" message, it has to be of 0 length,
    // and have a "Status" header with "503" as a value.
    return ((m != NULL)
                && (natsMsg_GetDataLength(m) == 0)
                && (natsMsgHeader_Get(m, STATUS_HDR, &val) == NATS_OK)
                && (val != NULL)
                && (strncmp(val, NO_RESP_STATUS, HDR_STATUS_LEN) == 0));
}
