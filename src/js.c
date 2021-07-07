// Copyright 2021 The NATS Authors
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

#include "js.h"
#include "mem.h"
#include "conn.h"
#include "util.h"

#ifdef DEV_MODE
// For type safety

void natsJS_lock(natsJS *js)   { natsMutex_Lock(js->mu);   }
void natsJS_unlock(natsJS *js) { natsMutex_Unlock(js->mu); }

static void _retain(natsJS *js)  { js->refs++; }
static void _release(natsJS *js) { js->refs--; }

#else

#define _retain(js)         ((js)->refs++)
#define _release(js)        ((js)->refs--)

#endif // DEV_MODE


const char*      jsDefaultAPIPrefix      = "$JS.API";
const int64_t    jsDefaultRequestWait    = 5000;
const int64_t    jsDefaultStallWait      = 200;
const char       *jsDigits               = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const int        jsBase                  = 62;

#define jsReplyTokenSize    (8)
#define jsReplyPrefixLen    (NATS_INBOX_PRE_LEN + (jsReplyTokenSize) + 1)

static void
_destroyOptions(natsJSOptions *o)
{
    NATS_FREE((char*) o->Prefix);
    NATS_FREE((char*) o->Stream.Purge.Subject);
}

static void
_freeContext(natsJS *js)
{
    natsConnection *nc = NULL;

    natsStrHash_Destroy(js->pm);
    natsSubscription_Destroy(js->rsub);
    _destroyOptions(&(js->opts));
    NATS_FREE(js->rpre);
    natsCondition_Destroy(js->cond);
    natsMutex_Destroy(js->mu);
    nc = js->nc;
    NATS_FREE(js);

    natsConn_release(nc);
}

void
natsJS_release(natsJS *js)
{
    bool doFree;

    natsMutex_Lock(js->mu);
    doFree = (--(js->refs) == 0);
    natsMutex_Unlock(js->mu);

    if (doFree)
        _freeContext(js);
}

static void
natsJS_unlockAndRelease(natsJS *js)
{
    bool doFree;

    doFree = (--(js->refs) == 0);
    natsMutex_Unlock(js->mu);

    if (doFree)
        _freeContext(js);
}

void
natsJS_DestroyContext(natsJS *js)
{
    if (js == NULL)
        return;

    natsJS_lock(js);
    if (js->rsub != NULL)
    {
        natsSubscription_Destroy(js->rsub);
        js->rsub = NULL;
    }
    if ((js->pm != NULL) && natsStrHash_Count(js->pm) > 0)
    {
        natsStrHashIter iter;
        void            *v = NULL;

        natsStrHashIter_Init(&iter, js->pm);
        while (natsStrHashIter_Next(&iter, NULL, &v))
        {
            natsMsg *msg = (natsMsg*) v;
            natsStrHashIter_RemoveCurrent(&iter);
            natsMsg_Destroy(msg);
        }
    }
    natsJS_unlockAndRelease(js);
}

natsStatus
natsJSOptions_Init(natsJSOptions *opts)
{
    if (opts == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(opts, 0, sizeof(natsJSOptions));
    return NATS_OK;
}

// Parse the JSON represented by the NATS message's payload and returns the JSON object.
// Unmarshal the API response.
natsStatus
natsJS_unmarshalResponse(natsJSApiResponse *ar, nats_JSON **new_json, natsMsg *resp)
{
    nats_JSON   *json = NULL;
    nats_JSON   *err  = NULL;
    natsStatus  s;

    memset(ar, 0, sizeof(natsJSApiResponse));

    s = nats_JSONParse(&json, natsMsg_GetData(resp), natsMsg_GetDataLength(resp));
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    // Check if there is an "error" field.
    s = nats_JSONGetObject(json, "error", &err);
    if ((s == NATS_OK) && (err != NULL))
    {
        s = nats_JSONGetInt(err, "code", &(ar->Error.Code));
        IFOK(s, nats_JSONGetUInt16(err, "err_code", &(ar->Error.ErrCode)));
        IFOK(s, nats_JSONGetStr(err, "description", &(ar->Error.Description)));
    }

    if (s == NATS_OK)
        *new_json = json;
    else
        nats_JSONDestroy(json);

    return NATS_UPDATE_ERR_STACK(s);
}

void
natsJS_freeApiRespContent(natsJSApiResponse *ar)
{
    if (ar == NULL)
        return;

    NATS_FREE(ar->Type);
    NATS_FREE(ar->Error.Description);
}

static natsStatus
_copyPurgeOptions(natsJS *js, struct natsJSOptionsStreamPurge *o)
{
    natsStatus                      s   = NATS_OK;
    struct natsJSOptionsStreamPurge *po = &(js->opts.Stream.Purge);

    po->Sequence = o->Sequence;
    po->Keep     = o->Keep;

    if (!nats_IsStringEmpty(o->Subject))
    {
        po->Subject = NATS_STRDUP(o->Subject);
        if (po->Subject == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_copyStreamInfoOptions(natsJS *js, struct natsJSOptionsStreamInfo *o)
{
    js->opts.Stream.Info.DeletedDetails = o->DeletedDetails;
    return NATS_OK;
}

natsStatus
natsJS_NewContext(natsJS **new_js, natsConnection *nc, natsJSOptions *opts)
{
    natsJS      *js = NULL;
    natsStatus  s;

    if ((new_js == NULL) || (nc == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (opts != NULL)
    {
        if (opts->Wait < 0)
            return nats_setError(NATS_INVALID_ARG, "option 'Wait' (%" PRId64 ") cannot be negative", opts->Wait);
        if (opts->PublishAsync.StallWait < 0)
            return nats_setError(NATS_INVALID_ARG, "option 'PublishAsyncStallWait' (%" PRId64 ") cannot be negative", opts->PublishAsync.StallWait);
    }

    js = (natsJS*) NATS_CALLOC(1, sizeof(natsJS));
    if (js == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    js->refs = 1;
    // Retain the NATS connection and keep track of it so that if we
    // detroy the context, in case of failure to fully initialize,
    // we properly release the NATS connection.
    natsConn_retain(nc);
    js->nc = nc;

    s = natsMutex_Create(&(js->mu));
    if (s == NATS_OK)
    {
        // If Domain is set, use domain to create prefix.
        if ((opts != NULL) && !nats_IsStringEmpty(opts->Domain))
        {
            if (nats_asprintf((char**) &(js->opts.Prefix), "$JS.%.*s.API",
                natsJS_lenWithoutTrailingDot(opts->Domain), opts->Domain) < 0)
            {
                s = nats_setDefaultError(NATS_NO_MEMORY);
            }
        }
        else if ((opts == NULL) || nats_IsStringEmpty(opts->Prefix))
        {
            js->opts.Prefix = NATS_STRDUP(jsDefaultAPIPrefix);
            if (js->opts.Prefix == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        else if (nats_asprintf((char**) &(js->opts.Prefix), "%.*s",
                natsJS_lenWithoutTrailingDot(opts->Prefix), opts->Prefix) < 0)
        {
                s = nats_setDefaultError(NATS_NO_MEMORY);
        }
    }
    if ((s == NATS_OK) && (opts != NULL))
    {
        struct natsJSOptionsPublishAsync *pa = &(js->opts.PublishAsync);

        pa->MaxPending          = opts->PublishAsync.MaxPending;
        pa->ErrHandler          = opts->PublishAsync.ErrHandler;
        pa->ErrHandlerClosure   = opts->PublishAsync.ErrHandlerClosure;
        pa->StallWait           = opts->PublishAsync.StallWait;
        js->opts.Wait           = opts->Wait;
    }
    if (js->opts.Wait == 0)
        js->opts.Wait = jsDefaultRequestWait;
    if (js->opts.PublishAsync.StallWait == 0)
        js->opts.PublishAsync.StallWait = jsDefaultStallWait;
    if ((s == NATS_OK) && (opts != NULL))
    {
        s = _copyPurgeOptions(js, &(opts->Stream.Purge));
        IFOK(s, _copyStreamInfoOptions(js, &(opts->Stream.Info)));
    }

    if (s == NATS_OK)
        *new_js = js;
    else
        natsJS_DestroyContext(js);

    return NATS_UPDATE_ERR_STACK(s);
}

int
natsJS_lenWithoutTrailingDot(const char *str)
{
    int l = (int) strlen(str);

    if (str[l-1] == '.')
        l--;
    return l;
}

natsStatus
natsJS_setOpts(natsConnection **nc, bool *freePfx, natsJS *js, natsJSOptions *opts, natsJSOptions *resOpts)
{
    natsStatus s = NATS_OK;

    *freePfx = false;
    natsJSOptions_Init(resOpts);

    if ((opts != NULL) && !nats_IsStringEmpty(opts->Domain))
    {
        char *pfx = NULL;
        if (nats_asprintf(&pfx, "$JS.%.*s.API",
                natsJS_lenWithoutTrailingDot(opts->Domain), opts->Domain) < 0)
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        else
        {
            resOpts->Prefix = pfx;
            *freePfx        = true;
        }
    }
    if (s == NATS_OK)
    {
        struct natsJSOptionsStreamPurge *po = &(js->opts.Stream.Purge);

        natsJS_lock(js);
        // If not set above...
        if (resOpts->Prefix == NULL)
            resOpts->Prefix = (opts == NULL || nats_IsStringEmpty(opts->Prefix)) ? js->opts.Prefix : opts->Prefix;

        // Take provided one or default to context's.
        resOpts->Wait = (opts == NULL || opts->Wait <= 0) ? js->opts.Wait : opts->Wait;

        // Purge options
        if (opts != NULL)
        {
            struct natsJSOptionsStreamPurge *opo = &(opts->Stream.Purge);

            // If any field is set, use `opts`, otherwise, we will use the
            // context's purge options.
            if ((opo->Subject != NULL) || (opo->Sequence > 0) || (opo->Keep > 0))
                po = opo;
        }
        memcpy(&(resOpts->Stream.Purge), po, sizeof(*po));

        // Stream info options
        resOpts->Stream.Info.DeletedDetails = (opts == NULL ? js->opts.Stream.Info.DeletedDetails : opts->Stream.Info.DeletedDetails);

        *nc = js->nc;
        natsJS_unlock(js);
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJSPubOptions_Init(natsJSPubOptions *opts)
{
    if (opts == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(opts, 0, sizeof(natsJSPubOptions));
    return NATS_OK;
}

natsStatus
natsJS_Publish(natsJSPubAck **new_puback, natsJS *js, const char *subj, const void *data, int dataLen,
               natsJSPubOptions *opts, natsJSErrCode *errCode)
{
    natsStatus s;
    natsMsg    msg;

    natsMsg_init(&msg, subj, NULL, (const char*) data, dataLen);
    s = natsJS_PublishMsg(new_puback, js, &msg, opts, errCode);
    natsMsg_freeHeaders(&msg);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_setHeadersFromOptions(natsMsg *msg, natsJSPubOptions *opts)
{
    natsStatus  s        = NATS_OK;
    char        temp[64] = {'\0'};

    if (!nats_IsStringEmpty(opts->MsgId))
        s = natsMsgHeader_Set(msg, jsMsgIdHdr, opts->MsgId);

    if ((s == NATS_OK) && !nats_IsStringEmpty(opts->ExpectLastMsgId))
        s = natsMsgHeader_Set(msg, jsExpectedLastMsgIdHdr, opts->ExpectLastMsgId);

    if ((s == NATS_OK) && !nats_IsStringEmpty(opts->ExpectStream))
        s = natsMsgHeader_Set(msg, jsExpectedStreamHdr, opts->ExpectStream);

    if ((s == NATS_OK) && (opts->ExpectLastSeq > 0))
    {
        snprintf(temp, sizeof(temp), "%" PRIu64, opts->ExpectLastSeq);
        s = natsMsgHeader_Set(msg, jsExpectedLastSeqHdr, temp);
    }

    if ((s == NATS_OK) && (opts->ExpectLastSubjectSeq > 0))
    {
        snprintf(temp, sizeof(temp), "%" PRIu64, opts->ExpectLastSubjectSeq);
        s = natsMsgHeader_Set(msg, jsExpectedLastSubjSeqHdr, temp);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_checkMaxWaitOpt(int64_t *new_ttl, natsJSPubOptions *opts)
{
    int64_t ttl;

    if ((ttl = opts->MaxWait) < 0)
        return nats_setError(NATS_INVALID_ARG, "option 'MaxWait' (%" PRId64 ") cannot be negative", ttl);

    *new_ttl = ttl;
    return NATS_OK;
}

natsStatus
natsJS_PublishMsg(natsJSPubAck **new_puback,natsJS *js, natsMsg *msg,
                  natsJSPubOptions *opts, natsJSErrCode *errCode)
{
    natsStatus          s       = NATS_OK;
    int64_t             ttl     = 0;
    nats_JSON           *json   = NULL;
    natsMsg             *resp   = NULL;
    natsJSApiResponse   ar;

    if (errCode != NULL)
        *errCode = 0;

    if ((js == NULL) || (msg == NULL) || nats_IsStringEmpty(msg->subject))
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (opts != NULL)
    {
        s = _checkMaxWaitOpt(&ttl, opts);
        IFOK(s, _setHeadersFromOptions(msg, opts));
        if (s != NATS_OK)
            return NATS_UPDATE_ERR_STACK(s);
    }

    // As it would be for a NATS connection, if the context has been destroyed,
    // the memory is invalid and accessing any field of the context could cause
    // a SEGFAULT. But assuming the context is still valid, we can access its
    // options and the NATS connection without locking since they are immutable
    // and the NATS connection has been retained when getting the JS context.

    // If not set through options, default to the context's Wait value.
    if (ttl == 0)
        ttl = js->opts.Wait;

    IFOK_JSR(s, natsConnection_RequestMsg(&resp, js->nc, msg, ttl));
    if (s == NATS_OK)
        s = natsJS_unmarshalResponse(&ar, &json, resp);
    if (s == NATS_OK)
    {
        if (natsJS_apiResponseIsErr(&ar))
        {
             if (errCode != NULL)
                *errCode = (int) ar.Error.ErrCode;
            s = nats_setError(NATS_ERR, "%s", ar.Error.Description);
        }
        else if (new_puback != NULL)
        {
            // The user wants the natsJSPubAck object back, so we need to unmarshal it.
            natsJSPubAck *pa = NULL;

            pa = (natsJSPubAck*) NATS_CALLOC(1, sizeof(natsJSPubAck));
            if (pa == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
            else
            {
                s = nats_JSONGetStr(json, "stream", &(pa->Stream));
                IFOK(s, nats_JSONGetULong(json, "seq", &(pa->Sequence)));
                IFOK(s, nats_JSONGetBool(json, "duplicate", &(pa->Duplicate)));

                if (s == NATS_OK)
                    *new_puback = pa;
                else
                    natsJSPubAck_Destroy(pa);
            }
        }
        natsJS_freeApiRespContent(&ar);
        nats_JSONDestroy(json);
    }
    natsMsg_Destroy(resp);
    return NATS_UPDATE_ERR_STACK(s);
}

void
natsJSPubAck_Destroy(natsJSPubAck *pa)
{
    if (pa == NULL)
        return;

    NATS_FREE(pa->Stream);
    NATS_FREE(pa);
}

static void
_handleAsyncReply(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const char      *subject    = natsMsg_GetSubject(msg);
    char            *id         = NULL;
    natsJS          *js         = NULL;
    natsMsg         *pmsg       = NULL;
    bool            freeMsg     = true;
    char            *rplyToFree = NULL;
    char            errTxt[256] = {'\0'};
    struct natsJSOptionsPublishAsync *opa = NULL;
    natsJSPubAckErr pae;

    if ((subject == NULL) || (int) strlen(subject) <= jsReplyPrefixLen)
    {
        natsMsg_Destroy(msg);
        return;
    }

    id = (char*) (subject+jsReplyPrefixLen);
    js = (natsJS*) closure;

    natsJS_lock(js);

    pmsg = natsStrHash_Remove(js->pm, id);
    if (pmsg == NULL)
    {
        natsMsg_Destroy(msg);
        natsJS_unlock(js);
        return;
    }

    opa = &(js->opts.PublishAsync);
    if (opa->ErrHandler != NULL)
    {
        natsStatus s = NATS_OK;

        memset(&pae, 0, sizeof(natsJSPubAckErr));

        // Check for no responders
        if (natsMsg_IsNoResponders(msg))
        {
            s = NATS_NO_RESPONDERS;
        }
        else
        {
            nats_JSON           *json = NULL;
            natsJSApiResponse   ar;

            // Now unmarshal the API response and check if there was an error.

            s = natsJS_unmarshalResponse(&ar, &json, msg);
            if ((s == NATS_OK) && natsJS_apiResponseIsErr(&ar))
            {
                pae.Err     = NATS_ERR;
                pae.ErrCode = (int) ar.Error.ErrCode;
                snprintf(errTxt, sizeof(errTxt), "%s", ar.Error.Description);
            }
            natsJS_freeApiRespContent(&ar);
            nats_JSONDestroy(json);
        }
        if (s != NATS_OK)
        {
            pae.Err = s;
            snprintf(errTxt, sizeof(errTxt), "%s", natsStatus_GetText(pae.Err));
        }

        // We will invoke CB only if there is any kind of error.
        if (pae.Err != NATS_OK)
        {
            // Associate the message with the pubAckErr object.
            pae.Msg = pmsg;
            // And the error text.
            pae.ErrText = errTxt;

            // We need to clear the "reply" subject from the original message,
            // which was added during the publish async call, otherwise user
            // would not be able to resend it if desired.
            if (pmsg->reply != NULL)
            {
                // However, we can't free it since the "id" points to a token
                // in the reply subject. So just keep track of it and free
                // only at the end.
                if (pmsg->freeRply)
                    rplyToFree = (char*) pmsg->reply;
                pmsg->reply = NULL;
            }

            natsJS_unlock(js);

            (opa->ErrHandler)(js, &pae, opa->ErrHandlerClosure);

            natsJS_lock(js);

            // If the user took ownership of the message (by resending it),
            // then we should not destroy the message at the end of this callback.
            freeMsg = (pae.Msg != NULL);
        }
    }

    // Now that the callback has returned, decrement the number of pending messages.
    js->pmcount--;

    // If there are callers waiting for async pub completion, or stalled async
    // publish calls and we are now below max pending, broadcast to unblock them.
    if (((js->pacw > 0) && (js->pmcount == 0))
        || ((js->stalled > 0) && (js->pmcount <= opa->MaxPending)))
    {
        natsCondition_Broadcast(js->cond);
    }
    natsJS_unlock(js);

    if (freeMsg)
        natsMsg_Destroy(pmsg);
    NATS_FREE(rplyToFree);
    natsMsg_Destroy(msg);
}

static void
_subComplete(void *closure)
{
    natsJS_release((natsJS*) closure);
}

static natsStatus
_newAsyncReply(char **new_id, natsJS *js, natsMsg *msg)
{
    natsStatus  s           = NATS_OK;

    // Create the internal objects if it is the first time that we are doing
    // an async publish.
    if (js->rsub == NULL)
    {
        s = natsCondition_Create(&(js->cond));
        IFOK(s, natsStrHash_Create(&(js->pm), 64));
        if (s == NATS_OK)
        {
            js->rpre = NATS_MALLOC(jsReplyPrefixLen+1);
            if (js->rpre == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
            else
            {
                char tmp[NATS_INBOX_PRE_LEN+NUID_BUFFER_LEN+1];

                natsInbox_init(tmp, sizeof(tmp));
                memcpy(js->rpre, tmp, NATS_INBOX_PRE_LEN);
                memcpy(js->rpre+NATS_INBOX_PRE_LEN, tmp+((int)strlen(tmp)-jsReplyTokenSize), jsReplyTokenSize);
                js->rpre[jsReplyPrefixLen-1] = '.';
                js->rpre[jsReplyPrefixLen]   = '\0';
            }
        }
        if (s == NATS_OK)
        {
            char subj[jsReplyPrefixLen + 2];

            snprintf(subj, sizeof(subj), "%s*", js->rpre);
            s = natsConn_subscribeNoPool(&(js->rsub), js->nc, subj, _handleAsyncReply, (void*) js);
            if (s == NATS_OK)
            {
                _retain(js);
                natsSubscription_SetPendingLimits(js->rsub, -1, -1);
                natsSubscription_SetOnCompleteCB(js->rsub, _subComplete, (void*) js);
            }
        }
        if (s != NATS_OK)
        {
            // Undo the things we created so we retry again next time.
            // It is either that or we have to always check individual
            // objects to know if we have to create them.
            NATS_FREE(js->rpre);
            js->rpre = NULL;
            natsStrHash_Destroy(js->pm);
            js->pm = NULL;
            natsCondition_Destroy(js->cond);
            js->cond = NULL;
        }
    }
    if (s == NATS_OK)
    {
        char    reply[jsReplyPrefixLen + jsReplyTokenSize + 1];
        int64_t l;
        int     i;

        memcpy(reply, js->rpre, jsReplyPrefixLen);
        l = nats_Rand64();
        for (i=0; i < jsReplyTokenSize; i++)
        {
		    reply[jsReplyPrefixLen+i] = jsDigits[l%jsBase];
		    l /= jsBase;
	    }
        reply[jsReplyPrefixLen+jsReplyTokenSize] = '\0';

        msg->reply = (const char*) NATS_STRDUP(reply);
        if (msg->reply == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
        else
        {
            msg->freeRply = true;
            *new_id = (char*) (msg->reply+jsReplyPrefixLen);
        }
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_registerPubMsg(natsConnection **nc, char **new_id, natsJS *js, natsMsg *msg)
{
    natsStatus  s       = NATS_OK;
    char        *id     = NULL;
    bool        release = false;
    int         maxp    = 0;

    natsJS_lock(js);

    maxp = js->opts.PublishAsync.MaxPending;

    js->pmcount++;
    s = _newAsyncReply(&id, js, msg);
    if ((s == NATS_OK)
            && (maxp > 0)
            && (js->pmcount > maxp))
    {
        int64_t target = nats_setTargetTime(js->opts.PublishAsync.StallWait);

        _retain(js);

        js->stalled++;
        while ((s != NATS_TIMEOUT) && (js->pmcount > maxp))
            s = natsCondition_AbsoluteTimedWait(js->cond, js->mu, target);
        js->stalled--;

        if (s == NATS_TIMEOUT)
            s = nats_setError(s, "%s", "stalled with too many outstanding async published messages");

        release = true;
    }
    if (s == NATS_OK)
        s = natsStrHash_Set(js->pm, id, false, msg, NULL);
    if (s == NATS_OK)
    {
        *new_id = id;
        *nc     = js->nc;
    }
    else
    {
        js->pmcount--;
    }
    if (release)
        natsJS_unlockAndRelease(js);
    else
        natsJS_unlock(js);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_PublishAsync(natsJS *js, const char *subj, const void *data, int dataLen,
                    natsJSPubOptions *opts)
{
    natsStatus s;
    natsMsg    *msg = NULL;

    s = natsMsg_Create(&msg, subj, NULL, (const char*) data, dataLen);
    IFOK(s, natsJS_PublishMsgAsync(js, &msg, opts));

    // The `msg` pointer will have been set to NULL if the library took ownership.
    natsMsg_Destroy(msg);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_PublishMsgAsync(natsJS *js, natsMsg **msg, natsJSPubOptions *opts)
{
    natsStatus      s   = NATS_OK;
    natsConnection  *nc = NULL;
    char            *id = NULL;

    if ((js == NULL) || (msg == NULL) || (*msg == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (natsMsg_GetReply(*msg) != NULL)
        return nats_setError(NATS_INVALID_ARG, "%s", "reply subject should not be set");

    if (opts != NULL)
    {
        s = _setHeadersFromOptions(*msg, opts);
        if (s != NATS_OK)
            return NATS_UPDATE_ERR_STACK(s);
    }

    // On success, the context will be retained.
    s = _registerPubMsg(&nc, &id, js, *msg);
    if (s == NATS_OK)
    {
        s = natsConnection_PublishMsg(nc, *msg);
        if (s != NATS_OK)
        {
            // The message may or may not have been sent, we don't know for sure.
            // We are going to attempt to remove from the map. If we can, then
            // we return the failure and the user owns the message. If we can't
            // it means that its ack has already been processed, so we consider
            // this call a success. If there was a pub ack failure, it is handled
            // with the error callback, but regardless, the library owns the message.
            natsJS_lock(js);
            // If msg no longer in map, Remove() will return NULL.
            if (natsStrHash_Remove(js->pm, id) == NULL)
                s = NATS_OK;
            else
                js->pmcount--;
            natsJS_unlock(js);
        }
    }

    // On success, clear the pointer to the message to indicate that the library
    // now owns it. If user calls natsMsg_Destroy(), it will have no effect since
    // they would call with natsMsg_Destroy(NULL), which is a no-op.
    if (s == NATS_OK)
        *msg = NULL;

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_PublishAsyncComplete(natsJS *js, natsJSPubOptions *opts)
{
    natsStatus  s       = NATS_OK;
    int64_t     ttl     = 0;
    int64_t     target  = 0;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (opts != NULL)
    {
        s = _checkMaxWaitOpt(&ttl, opts);
        if (s != NATS_OK)
            return NATS_UPDATE_ERR_STACK(s);
    }

    natsJS_lock(js);
    if ((js->pm == NULL) || (js->pmcount == 0))
    {
        natsJS_unlock(js);
        return NATS_OK;
    }
    if (ttl > 0)
        target = nats_setTargetTime(ttl);

    _retain(js);
    js->pacw++;
    while ((s != NATS_TIMEOUT) && (js->pmcount > 0))
    {
        if (target > 0)
            s = natsCondition_AbsoluteTimedWait(js->cond, js->mu, target);
        else
            natsCondition_Wait(js->cond, js->mu);
    }
    js->pacw--;
    natsJS_unlockAndRelease(js);

    return NATS_UPDATE_ERR_STACK(s);
}
