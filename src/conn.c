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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "conn.h"
#include "mem.h"
#include "buf.h"
#include "parser.h"
#include "srvpool.h"
#include "url.h"
#include "opts.h"
#include "util.h"
#include "timer.h"
#include "sub.h"
#include "msg.h"
#include "asynccb.h"
#include "comsock.h"
#include "nkeys.h"
#include "crypto.h"

#define DEFAULT_SCRATCH_SIZE    (512)
#define MAX_INFO_MESSAGE_SIZE   (32768)
#define DEFAULT_FLUSH_TIMEOUT   (10000)

#define NATS_EVENT_ACTION_ADD       (true)
#define NATS_EVENT_ACTION_REMOVE    (false)

#ifdef DEV_MODE
// For type safety

static void _retain(natsConnection *nc)  { nc->refs++; }
static void _release(natsConnection *nc) { nc->refs--; }

void natsConn_Lock(natsConnection *nc)   { natsMutex_Lock(nc->mu);   }
void natsConn_Unlock(natsConnection *nc) { natsMutex_Unlock(nc->mu); }

#else
// We know what we are doing :-)

#define _retain(c)  ((c)->refs++)
#define _release(c) ((c)->refs--)

#endif // DEV_MODE


// CLIENT_PROTO_ZERO is the original client protocol from 2009.
// http://nats.io/documentation/internals/nats-protocol/
#define CLIENT_PROTO_ZERO   (0)

// CLIENT_PROTO_INFO signals a client can receive more then the original INFO block.
// This can be used to update clients on other cluster members, etc.
#define CLIENT_PROTO_INFO   (1)

/*
 * Forward declarations:
 */
static natsStatus
_spinUpSocketWatchers(natsConnection *nc);

static natsStatus
_processConnInit(natsConnection *nc);

static void
_close(natsConnection *nc, natsConnStatus status, bool fromPublicClose, bool doCBs);

static bool
_processOpError(natsConnection *nc, natsStatus s, bool initialConnect);

static natsStatus
_flushTimeout(natsConnection *nc, int64_t timeout);

static bool
_processAuthError(natsConnection *nc, int errCode, char *error);

static int
_checkAuthError(char *error);

/*
 * ----------------------------------------
 */

struct threadsToJoin
{
    natsThread  *readLoop;
    natsThread  *flusher;
    natsThread  *reconnect;
    bool        joinReconnect;

} threadsToJoin;

static void
_initThreadsToJoin(struct threadsToJoin *ttj, natsConnection *nc, bool joinReconnect)
{
    memset(ttj, 0, sizeof(threadsToJoin));

    ttj->joinReconnect = joinReconnect;

    if (nc->readLoopThread != NULL)
    {
        ttj->readLoop = nc->readLoopThread;
        nc->readLoopThread = NULL;
    }

    if (joinReconnect && (nc->reconnectThread != NULL))
    {
        ttj->reconnect = nc->reconnectThread;
        nc->reconnectThread = NULL;
    }

    if (nc->flusherThread != NULL)
    {
        nc->flusherStop = true;
        natsCondition_Signal(nc->flusherCond);

        ttj->flusher = nc->flusherThread;
        nc->flusherThread = NULL;
    }
}

static void
_joinThreads(struct threadsToJoin *ttj)
{
    if (ttj->readLoop != NULL)
    {
        natsThread_Join(ttj->readLoop);
        natsThread_Destroy(ttj->readLoop);
    }

    if (ttj->joinReconnect && (ttj->reconnect != NULL))
    {
        natsThread_Join(ttj->reconnect);
        natsThread_Destroy(ttj->reconnect);
    }

    if (ttj->flusher != NULL)
    {
        natsThread_Join(ttj->flusher);
        natsThread_Destroy(ttj->flusher);
    }
}

static void
_clearServerInfo(natsServerInfo *si)
{
    int i;

    NATS_FREE(si->id);
    NATS_FREE(si->host);
    NATS_FREE(si->version);

    for (i=0; i<si->connectURLsCount; i++)
        NATS_FREE(si->connectURLs[i]);
    NATS_FREE(si->connectURLs);

    NATS_FREE(si->nonce);
    NATS_FREE(si->clientIP);

    memset(si, 0, sizeof(natsServerInfo));
}

static void
_freeConn(natsConnection *nc)
{
    if (nc == NULL)
        return;

    natsTimer_Destroy(nc->ptmr);
    natsBuf_Destroy(nc->pending);
    natsBuf_Destroy(nc->scratch);
    natsBuf_Destroy(nc->bw);
    natsSrvPool_Destroy(nc->srvPool);
    _clearServerInfo(&(nc->info));
    natsCondition_Destroy(nc->flusherCond);
    natsCondition_Destroy(nc->pongs.cond);
    natsParser_Destroy(nc->ps);
    natsThread_Destroy(nc->readLoopThread);
    natsThread_Destroy(nc->flusherThread);
    natsHash_Destroy(nc->subs);
    natsOptions_Destroy(nc->opts);
    if (nc->sockCtx.ssl != NULL)
        SSL_free(nc->sockCtx.ssl);
    NATS_FREE(nc->el.buffer);
    natsConn_destroyRespPool(nc);
    natsInbox_Destroy(nc->respSub);
    natsStrHash_Destroy(nc->respMap);
    natsCondition_Destroy(nc->reconnectCond);
    natsMutex_Destroy(nc->subsMu);
    natsMutex_Destroy(nc->mu);

    NATS_FREE(nc);

    natsLib_Release();
}

void
natsConn_retain(natsConnection *nc)
{
    if (nc == NULL)
        return;

    natsConn_Lock(nc);

    nc->refs++;

    natsConn_Unlock(nc);
}

void
natsConn_release(natsConnection *nc)
{
    int refs = 0;

    if (nc == NULL)
        return;

    natsConn_Lock(nc);

    refs = --(nc->refs);

    natsConn_Unlock(nc);

    if (refs == 0)
        _freeConn(nc);
}

void
natsConn_lockAndRetain(natsConnection *nc)
{
    natsConn_Lock(nc);
    nc->refs++;
}

void
natsConn_unlockAndRelease(natsConnection *nc)
{
    int refs = 0;

    refs = --(nc->refs);

    natsConn_Unlock(nc);

    if (refs == 0)
        _freeConn(nc);
}

natsStatus
natsConn_bufferFlush(natsConnection *nc)
{
    natsStatus  s      = NATS_OK;
    int         bufLen = natsBuf_Len(nc->bw);

    if (bufLen == 0)
        return NATS_OK;

    if (nc->usePending)
    {
        s = natsBuf_Append(nc->pending, natsBuf_Data(nc->bw), bufLen);
    }
    else if (nc->sockCtx.useEventLoop)
    {
        if (!(nc->el.writeAdded))
        {
            nc->el.writeAdded = true;
            s = nc->opts->evCbs.write(nc->el.data, NATS_EVENT_ACTION_ADD);
            if (s != NATS_OK)
                nats_setError(s, "Error processing write request: %d - %s",
                              s, natsStatus_GetText(s));
        }

        return NATS_UPDATE_ERR_STACK(s);
    }
    else
    {
        s = natsSock_WriteFully(&(nc->sockCtx), natsBuf_Data(nc->bw), bufLen);
    }

    natsBuf_Reset(nc->bw);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConn_bufferWrite(natsConnection *nc, const char *buffer, int len)
{
    natsStatus  s = NATS_OK;
    int         offset = 0;
    int         avail  = 0;

    if (len <= 0)
        return NATS_OK;

    if (nc->usePending)
        return natsBuf_Append(nc->pending, buffer, len);

    if (nc->sockCtx.useEventLoop)
    {
        s = natsBuf_Append(nc->bw, buffer, len);
        if ((s == NATS_OK)
            && (natsBuf_Len(nc->bw) >= nc->opts->ioBufSize)
            && !(nc->el.writeAdded))
        {
            nc->el.writeAdded = true;
            s = nc->opts->evCbs.write(nc->el.data, NATS_EVENT_ACTION_ADD);
            if (s != NATS_OK)
                nats_setError(s, "Error processing write request: %d - %s",
                              s, natsStatus_GetText(s));
        }

        return NATS_UPDATE_ERR_STACK(s);
    }

    if (nc->dontSendInPlace)
    {
        s = natsBuf_Append(nc->bw, buffer, len);

        return NATS_UPDATE_ERR_STACK(s);
    }

    // If we have more data that can fit..
    while ((s == NATS_OK) && (len > natsBuf_Available(nc->bw)))
    {
        // If there is nothing in the buffer...
        if (natsBuf_Len(nc->bw) == 0)
        {
            // Do a single socket write to avoid a copy
            s = natsSock_WriteFully(&(nc->sockCtx), buffer + offset, len);

            // We are done
            return NATS_UPDATE_ERR_STACK(s);
        }

        // We already have data in the buffer, check how many more bytes
        // can we fit
        avail = natsBuf_Available(nc->bw);

        // Append that much bytes
        s = natsBuf_Append(nc->bw, buffer + offset, avail);

        // Flush the buffer
        if (s == NATS_OK)
            s = natsConn_bufferFlush(nc);

        // If success, then decrement what's left to send and update the
        // offset.
        if (s == NATS_OK)
        {
            len    -= avail;
            offset += avail;
        }
    }

    // If there is data left, the buffer can now hold this data.
    if ((s == NATS_OK) && (len > 0))
        s = natsBuf_Append(nc->bw, buffer + offset, len);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConn_bufferWriteString(natsConnection *nc, const char *string)
{
    natsStatus s = natsConn_bufferWrite(nc, string, (int) strlen(string));

    return NATS_UPDATE_ERR_STACK(s);
}

// _createConn will connect to the server and do the right thing when an
// existing connection is in place.
static natsStatus
_createConn(natsConnection *nc)
{
    natsStatus  s = NATS_OK;

    // Sets a deadline for the connect process (not just the low level
    // tcp connect. The deadline will be removed when we have received
    // the PONG to our initial PING. See _processConnInit().
    natsSock_InitDeadline(&nc->sockCtx, nc->opts->timeout);

    // Set the IP resolution order
    nc->sockCtx.orderIP = nc->opts->orderIP;

    s = natsSock_ConnectTcp(&(nc->sockCtx), nc->cur->url->host, nc->cur->url->port);
    if (s == NATS_OK)
        nc->sockCtx.fdActive = true;

    // Need to create or reset the buffer even on failure in case we allow
    // retry on failed connect
    if ((s == NATS_OK) || nc->opts->retryOnFailedConnect)
    {
        natsStatus ls = NATS_OK;

        if (nc->bw == NULL)
            ls = natsBuf_Create(&(nc->bw), nc->opts->ioBufSize);
        else
            natsBuf_Reset(nc->bw);

        if (s == NATS_OK)
            s = ls;
    }

    if (s != NATS_OK)
    {
        // reset the deadline
        natsSock_ClearDeadline(&nc->sockCtx);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static void
_clearControlContent(natsControl *control)
{
    NATS_FREE(control->op);
    NATS_FREE(control->args);
}

static void
_initControlContent(natsControl *control)
{
    control->op     = NULL;
    control->args   = NULL;
}

static bool
_isConnecting(natsConnection *nc)
{
    return nc->status == NATS_CONN_STATUS_CONNECTING;
}

static bool
_isConnected(natsConnection *nc)
{
    return ((nc->status == NATS_CONN_STATUS_CONNECTED) || natsConn_isDraining(nc));
}

bool
natsConn_isClosed(natsConnection *nc)
{
    return nc->status == NATS_CONN_STATUS_CLOSED;
}

bool
natsConn_isReconnecting(natsConnection *nc)
{
    return (nc->pending != NULL);
}

bool
natsConn_isDraining(natsConnection *nc)
{
    return ((nc->status == NATS_CONN_STATUS_DRAINING_SUBS) || (nc->status == NATS_CONN_STATUS_DRAINING_PUBS));
}

bool
natsConn_isDrainingPubs(natsConnection *nc)
{
    return nc->status == NATS_CONN_STATUS_DRAINING_PUBS;
}

static natsStatus
_readOp(natsConnection *nc, natsControl *control)
{
    natsStatus  s = NATS_OK;
    char        buffer[MAX_INFO_MESSAGE_SIZE];

    buffer[0] = '\0';

    s = natsSock_ReadLine(&(nc->sockCtx), buffer, sizeof(buffer));
    if (s == NATS_OK)
        s = nats_ParseControl(control, buffer);

    return NATS_UPDATE_ERR_STACK(s);
}

// _processInfo is used to parse the info messages sent
// from the server.
// This function may update the server pool.
static natsStatus
_processInfo(natsConnection *nc, char *info, int len)
{
    natsStatus  s     = NATS_OK;
    nats_JSON   *json = NULL;

    if (info == NULL)
        return NATS_OK;

    _clearServerInfo(&(nc->info));

    s = nats_JSONParse(&json, info, len);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    IFOK(s, nats_JSONGetStr(json, "server_id", &(nc->info.id)));
    IFOK(s, nats_JSONGetStr(json, "version", &(nc->info.version)));
    IFOK(s, nats_JSONGetStr(json, "host", &(nc->info.host)));
    IFOK(s, nats_JSONGetInt(json, "port", &(nc->info.port)));
    IFOK(s, nats_JSONGetBool(json, "auth_required", &(nc->info.authRequired)));
    IFOK(s, nats_JSONGetBool(json, "tls_required", &(nc->info.tlsRequired)));
    IFOK(s, nats_JSONGetLong(json, "max_payload", &(nc->info.maxPayload)));
    IFOK(s, nats_JSONGetArrayStr(json, "connect_urls",
                                 &(nc->info.connectURLs),
                                 &(nc->info.connectURLsCount)));
    IFOK(s, nats_JSONGetInt(json, "proto", &(nc->info.proto)));
    IFOK(s, nats_JSONGetULong(json, "client_id", &(nc->info.CID)));
    IFOK(s, nats_JSONGetStr(json, "nonce", &(nc->info.nonce)));
    IFOK(s, nats_JSONGetStr(json, "client_ip", &(nc->info.clientIP)));
    IFOK(s, nats_JSONGetBool(json, "ldm", &(nc->info.lameDuckMode)));
    IFOK(s, nats_JSONGetBool(json, "headers", &(nc->info.headers)));

    // The array could be empty/not present on initial connect,
    // if advertise is disabled on that server, or servers that
    // did not include themselves in the async INFO protocol.
    // If empty, do not remove the implicit servers from the pool.
    if ((s == NATS_OK) && (nc->info.connectURLsCount > 0))
    {
        bool        added    = false;
        const char  *tlsName = NULL;

        if ((nc->cur != NULL) && (nc->cur->url != NULL) && !nats_HostIsIP(nc->cur->url->host))
            tlsName = (const char*) nc->cur->url->host;

        s = natsSrvPool_addNewURLs(nc->srvPool,
                                   nc->cur->url,
                                   nc->info.connectURLs,
                                   nc->info.connectURLsCount,
                                   tlsName,
                                   &added);
        if ((s == NATS_OK) && added && !nc->initc && (nc->opts->discoveredServersCb != NULL))
            natsAsyncCb_PostConnHandler(nc, ASYNC_DISCOVERED_SERVERS);
    }
    // Process the LDM callback after the above. It will cover cases where
    // we have connect URLs and invoke discovered server callback, and case
    // where we don't.
    if ((s == NATS_OK) && nc->info.lameDuckMode && (nc->opts->lameDuckCb != NULL))
        natsAsyncCb_PostConnHandler(nc, ASYNC_LAME_DUCK_MODE);

    if (s != NATS_OK)
        s = nats_setError(NATS_PROTOCOL_ERROR,
                          "Invalid protocol: %s", nats_GetLastError(NULL));

    nats_JSONDestroy(json);

    return NATS_UPDATE_ERR_STACK(s);
}

// natsConn_processAsyncINFO does the same than processInfo, but is called
// from the parser. Calls processInfo under connection's lock
// protection.
void
natsConn_processAsyncINFO(natsConnection *nc, char *buf, int len)
{
    natsConn_Lock(nc);
    // Ignore errors, we will simply not update the server pool...
    (void) _processInfo(nc, buf, len);
    natsConn_Unlock(nc);
}

#if defined(NATS_HAS_TLS)
static int
_collectSSLErr(int preverifyOk, X509_STORE_CTX* ctx)
{
    SSL             *ssl  = NULL;
    X509            *cert = X509_STORE_CTX_get_current_cert(ctx);
    int             depth = X509_STORE_CTX_get_error_depth(ctx);
    int             err   = X509_STORE_CTX_get_error(ctx);
    natsConnection  *nc   = NULL;

    // Retrieve the SSL object, then our connection...
    ssl = X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx());
    nc = (natsConnection*) SSL_get_ex_data(ssl, 0);

    // Should we skip serve certificate verification?
    if (nc->opts->sslCtx->skipVerify)
        return 1;

    if (!preverifyOk)
    {
        char certName[256]= {0};

        X509_NAME_oneline(X509_get_subject_name(cert), certName, sizeof(certName));

        if (err == X509_V_ERR_HOSTNAME_MISMATCH)
        {
            snprintf_truncate(nc->errStr, sizeof(nc->errStr), "%d:%s:expected=%s:cert=%s",
                              err, X509_verify_cert_error_string(err), nc->tlsName,
                              certName);
        }
        else
        {
            char issuer[256]  = {0};

            X509_NAME_oneline(X509_get_issuer_name(cert), issuer, sizeof(issuer));

            snprintf_truncate(nc->errStr, sizeof(nc->errStr), "%d:%s:depth=%d:cert=%s:issuer=%s",
                              err, X509_verify_cert_error_string(err), depth,
                              certName, issuer);
        }
    }

    return preverifyOk;
}
#endif

// makeTLSConn will wrap an existing Conn using TLS
static natsStatus
_makeTLSConn(natsConnection *nc)
{
#if defined(NATS_HAS_TLS)
    natsStatus  s       = NATS_OK;
    SSL         *ssl    = NULL;

    // Reset nc->errStr before initiating the handshake...
    nc->errStr[0] = '\0';

    natsMutex_Lock(nc->opts->sslCtx->lock);

    s = natsSock_SetBlocking(nc->sockCtx.fd, true);
    if (s == NATS_OK)
    {
        ssl = SSL_new(nc->opts->sslCtx->ctx);
        if (ssl == NULL)
        {
            s = nats_setError(NATS_SSL_ERROR,
                              "Error creating SSL object: %s",
                              NATS_SSL_ERR_REASON_STRING);
        }
        else
        {
            nats_sslRegisterThreadForCleanup();

            SSL_set_ex_data(ssl, 0, (void*) nc);
        }
    }
    if (s == NATS_OK)
    {
        SSL_set_connect_state(ssl);

        if (SSL_set_fd(ssl, (int) nc->sockCtx.fd) != 1)
        {
            s = nats_setError(NATS_SSL_ERROR,
                              "Error connecting the SSL object to a file descriptor : %s",
                              NATS_SSL_ERR_REASON_STRING);
        }
    }
    if (s == NATS_OK)
    {
#if defined(NATS_HAS_TLS)
        if (nc->opts->sslCtx->skipVerify)
        {
            SSL_set_verify(ssl, SSL_VERIFY_NONE, NULL);
        }
        else
        {
            nc->tlsName = NULL;

            // If we don't force hostname verification, perform it only
            // if expectedHostname is set (to be backward compatible with
            // releases prior to 2.0.0)
            if (nc->opts->sslCtx->expectedHostname != NULL)
                nc->tlsName = nc->opts->sslCtx->expectedHostname;
#if defined(NATS_FORCE_HOST_VERIFICATION)
            else if (nc->cur->tlsName != NULL)
                nc->tlsName = nc->cur->tlsName;
            else
                nc->tlsName = nc->cur->url->host;
#endif
            if (nc->tlsName != NULL)
            {
#if defined(NATS_USE_OPENSSL_1_1)
                SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                if (!SSL_set1_host(ssl, nc->tlsName))
#else
                X509_VERIFY_PARAM *param = SSL_get0_param(ssl);
                X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                if (!X509_VERIFY_PARAM_set1_host(param, nc->tlsName, 0))
#endif
                    s = nats_setError(NATS_SSL_ERROR, "unable to set expected hostname '%s'", nc->tlsName);
            }
            if (s == NATS_OK)
                SSL_set_verify(ssl, SSL_VERIFY_PEER, _collectSSLErr);
        }
#endif
    }
    if ((s == NATS_OK) && (SSL_do_handshake(ssl) != 1))
    {
        s = nats_setError(NATS_SSL_ERROR,
                          "SSL handshake error: %s",
                          (nc->errStr[0] != '\0' ? nc->errStr : NATS_SSL_ERR_REASON_STRING));
    }
    // Make sure that if nc-errStr was set in _collectSSLErr but
    // the overall handshake is ok, then we clear the error
    if (s == NATS_OK)
    {
        nc->errStr[0] = '\0';
        s = natsSock_SetBlocking(nc->sockCtx.fd, false);
    }

    natsMutex_Unlock(nc->opts->sslCtx->lock);

    if (s != NATS_OK)
    {
        if (ssl != NULL)
            SSL_free(ssl);
    }
    else
    {
        nc->sockCtx.ssl = ssl;
    }

    return NATS_UPDATE_ERR_STACK(s);
#else
    return nats_setError(NATS_ILLEGAL_STATE, "%s", NO_SSL_ERR);
#endif
}

// This will check to see if the connection should be
// secure. This can be dictated from either end and should
// only be called after the INIT protocol has been received.
static natsStatus
_checkForSecure(natsConnection *nc)
{
    natsStatus  s = NATS_OK;

    // Check for mismatch in setups
    if (nc->opts->secure && !nc->info.tlsRequired)
        s = nats_setDefaultError(NATS_SECURE_CONNECTION_WANTED);
    else if (nc->info.tlsRequired && !nc->opts->secure)
    {
        // Switch to Secure since server needs TLS.
        s = natsOptions_SetSecure(nc->opts, true);
    }

    if ((s == NATS_OK) && nc->opts->secure)
        s = _makeTLSConn(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_processExpectedInfo(natsConnection *nc)
{
    natsControl     control;
    natsStatus      s;

    _initControlContent(&control);

    s = _readOp(nc, &control);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if ((s == NATS_OK)
        && ((control.op == NULL)
            || (strcmp(control.op, _INFO_OP_) != 0)))
    {
        s = nats_setError(NATS_PROTOCOL_ERROR,
                          "Unexpected protocol: got '%s' instead of '%s'",
                          (control.op == NULL ? "<null>" : control.op),
                          _INFO_OP_);
    }
    if (s == NATS_OK)
        s = _processInfo(nc, control.args, -1);
    if (s == NATS_OK)
        s = _checkForSecure(nc);

    _clearControlContent(&control);

    return NATS_UPDATE_ERR_STACK(s);
}

static char*
_escape(char *origin)
{
    char    escChar[] = {'\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\'};
    char    escRepl[] = {'a', 'b', 'f', 'n', 'r', 't', 'v', '\\'};
    int     l         = (int) strlen(origin);
    int     ec        = 0;
    char    *dest     = NULL;
    char    *ptr      = NULL;
    int i;
    int j;

    for (i=0; i<l; i++)
    {
        for (j=0; j<8; j++)
        {
            if (origin[i] == escChar[j])
            {
                ec++;
                break;
            }
        }
    }
    if (ec == 0)
        return origin;

    dest = NATS_MALLOC(l + ec + 1);
    if (dest == NULL)
        return NULL;

    ptr = dest;
    for (i=0; i<l; i++)
    {
        for (j=0; j<8 ;j++)
        {
            if (origin[i] == escChar[j])
            {
                *ptr++ = '\\';
                *ptr++ = escRepl[j];
                 break;
            }
        }
        if(j == 8 )
            *ptr++ = origin[i];
    }
    *ptr = '\0';
    return dest;
}

static natsStatus
_connectProto(natsConnection *nc, char **proto)
{
    natsStatus  s     = NATS_OK;
    natsOptions *opts = nc->opts;
    const char  *token= NULL;
    const char  *user = NULL;
    const char  *pwd  = NULL;
    const char  *name = NULL;
    char        *sig  = NULL;
    char        *ujwt = NULL;
    char        *nkey = NULL;
    int         res;
    unsigned char   *sigRaw    = NULL;
    int             sigRawLen  = 0;

    // Check if NoEcho is set and we have a server that supports it.
    if (opts->noEcho && (nc->info.proto < 1))
        return NATS_NO_SERVER_SUPPORT;

    if (nc->cur->url->username != NULL)
        user = nc->cur->url->username;
    if (nc->cur->url->password != NULL)
        pwd = nc->cur->url->password;
    if ((user != NULL) && (pwd == NULL))
    {
        token = user;
        user  = NULL;
    }
    if ((user == NULL) && (token == NULL))
    {
        // Take from options (possibly all NULL)
        user  = opts->user;
        pwd   = opts->password;
        token = opts->token;
        nkey  = opts->nkey;
    }

    if (opts->userJWTHandler != NULL)
    {
        char *errTxt = NULL;
        bool userCb  = opts->userJWTHandler != natsConn_userFromFile;

        // If callback is not the internal one, we need to release connection lock.
        if (userCb)
            natsConn_Unlock(nc);

        s = opts->userJWTHandler(&ujwt, &errTxt, (void*) opts->userJWTClosure);

        if (userCb)
        {
            natsConn_Lock(nc);
            if (natsConn_isClosed(nc) && (s == NATS_OK))
                s = NATS_CONNECTION_CLOSED;
        }
        if ((s != NATS_OK) && (errTxt != NULL))
        {
            s = nats_setError(s, "%s", errTxt);
            NATS_FREE(errTxt);
        }
        if ((s == NATS_OK) && !nats_IsStringEmpty(nkey))
            s = nats_setError(NATS_ILLEGAL_STATE, "%s", "user JWT callback and NKey cannot be both specified");

        if ((s == NATS_OK) && (ujwt != NULL))
        {
            char *tmp = _escape(ujwt);
            if (tmp == NULL)
            {
                s = nats_setDefaultError(NATS_NO_MEMORY);
            }
            else if (tmp != ujwt)
            {
                NATS_FREE(ujwt);
                ujwt = tmp;
            }
        }
    }

    if ((s == NATS_OK) && (!nats_IsStringEmpty(ujwt) || !nats_IsStringEmpty(nkey)))
    {
        char *errTxt = NULL;
        bool userCb  = opts->sigHandler != natsConn_signatureHandler;

        if (userCb)
            natsConn_Unlock(nc);

        s = opts->sigHandler(&errTxt, &sigRaw, &sigRawLen, nc->info.nonce, opts->sigClosure);

        if (userCb)
        {
            natsConn_Lock(nc);
            if (natsConn_isClosed(nc) && (s == NATS_OK))
                s = NATS_CONNECTION_CLOSED;
        }
        if ((s != NATS_OK) && (errTxt != NULL))
        {
            s = nats_setError(s, "%s", errTxt);
            NATS_FREE(errTxt);
        }
        if (s == NATS_OK)
            s = nats_Base64RawURL_EncodeString((const unsigned char*) sigRaw, sigRawLen, &sig);
    }

    if ((s == NATS_OK) && (opts->tokenCb != NULL))
    {
        if (token != NULL)
            s = nats_setError(NATS_ILLEGAL_STATE, "%s", "Token and token handler options cannot be both set");

        if (s == NATS_OK)
            token = opts->tokenCb(opts->tokenCbClosure);
    }

    if ((s == NATS_OK) && (opts->name != NULL))
        name = opts->name;

    if (s == NATS_OK)
    {
        // If our server does not support headers then we can't do them or no responders.
        const char *hdrs = nats_GetBoolStr(nc->info.headers);
        const char *noResponders = nats_GetBoolStr(nc->info.headers && !nc->opts->disableNoResponders);

        res = nats_asprintf(proto,
                            "CONNECT {\"verbose\":%s,\"pedantic\":%s,%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\"tls_required\":%s," \
                            "\"name\":\"%s\",\"lang\":\"%s\",\"version\":\"%s\",\"protocol\":%d,\"echo\":%s," \
                            "\"headers\":%s,\"no_responders\":%s}%s",
                            nats_GetBoolStr(opts->verbose),
                            nats_GetBoolStr(opts->pedantic),
                            (nkey != NULL ? "\"nkey\":\"" : ""),
                            (nkey != NULL ? nkey : ""),
                            (nkey != NULL ? "\"," : ""),
                            (ujwt != NULL ? "\"jwt\":\"" : ""),
                            (ujwt != NULL ? ujwt : ""),
                            (ujwt != NULL ? "\"," : ""),
                            (sig != NULL ? "\"sig\":\"" : ""),
                            (sig != NULL ? sig : ""),
                            (sig != NULL ? "\"," : ""),
                            (user != NULL ? "\"user\":\"" : ""),
                            (user != NULL ? user : ""),
                            (user != NULL ? "\"," : ""),
                            (pwd != NULL ? "\"pass\":\"" : ""),
                            (pwd != NULL ? pwd : ""),
                            (pwd != NULL ? "\"," : ""),
                            (token != NULL ? "\"auth_token\":\"" :""),
                            (token != NULL ? token : ""),
                            (token != NULL ? "\"," : ""),
                            nats_GetBoolStr(opts->secure),
                            (name != NULL ? name : ""),
                            CString, NATS_VERSION_STRING,
                            CLIENT_PROTO_INFO,
                            nats_GetBoolStr(!opts->noEcho),
                            hdrs,
                            noResponders,
                            _CRLF_);
        if (res < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);
    }

    NATS_FREE(ujwt);
    NATS_FREE(sigRaw);
    NATS_FREE(sig);

    return s;
}

static natsStatus
_sendUnsubProto(natsConnection *nc, int64_t subId, int max)
{
    natsStatus  s       = NATS_OK;
    char        *proto  = NULL;
    int         res     = 0;

    if (max > 0)
        res = nats_asprintf(&proto, _UNSUB_PROTO_, subId, max);
    else
        res = nats_asprintf(&proto, _UNSUB_NO_MAX_PROTO_, subId);

    if (res < 0)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    else
    {
        s = natsConn_bufferWriteString(nc, proto);
        NATS_FREE(proto);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_resendSubscriptions(natsConnection *nc)
{
    natsStatus          s    = NATS_OK;
    natsSubscription    *sub = NULL;
    natsHashIter        iter;
    char                *proto;
    int                 res;
    int                 adjustedMax;
    natsSubscription    **subs = NULL;
    int                 i = 0;
    int                 count = 0;

    // Since we are going to send protocols to the server, we don't want to
    // be holding the subsMu lock (which is used in processMsg). So copy
    // the subscriptions in a temporary array.
    natsMutex_Lock(nc->subsMu);
    if (natsHash_Count(nc->subs) > 0)
    {
        subs = NATS_CALLOC(natsHash_Count(nc->subs), sizeof(natsSubscription*));
        if (subs == NULL)
            s = NATS_NO_MEMORY;

        if (s == NATS_OK)
        {
            void *p = NULL;

            natsHashIter_Init(&iter, nc->subs);
            while (natsHashIter_Next(&iter, NULL, &p))
            {
                subs[count++] = (natsSubscription*) p;
            }
            natsHashIter_Done(&iter);
        }
    }
    natsMutex_Unlock(nc->subsMu);

    SET_WRITE_DEADLINE(nc);

    for (i=0; (s == NATS_OK) && (i<count); i++)
    {
        sub = subs[i];

        proto = NULL;

        adjustedMax = 0;
        natsSub_Lock(sub);
        if (natsSub_drainStarted(sub))
        {
            natsSub_Unlock(sub);
            continue;
        }
        if (sub->max > 0)
        {
            if (sub->delivered < sub->max)
                adjustedMax = (int)(sub->max - sub->delivered);

            // The adjusted max could be 0 here if the number of delivered
            // messages have reached the max, if so, unsubscribe.
            if (adjustedMax == 0)
            {
                natsSub_Unlock(sub);
                s = _sendUnsubProto(nc, sub->sid, 0);
                continue;
            }
        }

        res = nats_asprintf(&proto, _SUB_PROTO_,
                            sub->subject,
                            (sub->queue == NULL ? "" : sub->queue),
                            (int) sub->sid);
        if (res < 0)
            s = NATS_NO_MEMORY;

        if (s == NATS_OK)
        {
            s = natsConn_bufferWriteString(nc, proto);
            NATS_FREE(proto);
            proto = NULL;
        }

        if ((s == NATS_OK) && (adjustedMax > 0))
            s = _sendUnsubProto(nc, sub->sid, adjustedMax);

        // Hold the lock up to that point so we are sure not to resend
        // any SUB/UNSUB for a subscription that is in draining mode.
        natsSub_Unlock(sub);
    }

    NATS_FREE(subs);

    return s;
}

static natsStatus
_flushReconnectPendingItems(natsConnection *nc)
{
    natsStatus s = NATS_OK;

    if (nc->pending == NULL)
        return NATS_OK;

    if (natsBuf_Len(nc->pending) > 0)
    {
        // Flush pending buffer
        s = natsConn_bufferWrite(nc, natsBuf_Data(nc->pending),
                                 natsBuf_Len(nc->pending));

        // Regardless of outcome, we must clear the pending buffer
        // here to avoid duplicates (if the flush were to fail
        // with some messages/partial messages being sent).
        natsBuf_Reset(nc->pending);
    }

    return s;
}

static void
_removePongFromList(natsConnection *nc, natsPong *pong)
{
    if (pong->prev != NULL)
        pong->prev->next = pong->next;

    if (pong->next != NULL)
        pong->next->prev = pong->prev;

    if (nc->pongs.head == pong)
        nc->pongs.head = pong->next;

    if (nc->pongs.tail == pong)
        nc->pongs.tail = pong->prev;

    pong->prev = pong->next = NULL;
}

// When the connection is closed, or is disconnected and we are about
// to reconnect, we need to unblock all pending natsConnection_Flush[Timeout]()
// calls: there is no chance that a PING sent to a server is going to be
// echoed by the new server.
static void
_clearPendingFlushRequests(natsConnection *nc)
{
    natsPong *pong = NULL;

    while ((pong = nc->pongs.head) != NULL)
    {
        // Pop from the queue
        _removePongFromList(nc, pong);

        // natsConnection_Flush[Timeout]() is waiting on a condition
        // variable and exit when this value is != 0. "Flush" will
        // return an error to the caller if the connection status
        // is not CONNECTED at that time.
        pong->id = -1;

        // There may be more than one user-thread making
        // natsConnection_Flush() calls.
        natsCondition_Broadcast(nc->pongs.cond);
    }

    nc->pongs.incoming      = 0;
    nc->pongs.outgoingPings = 0;
}

// Dispose of the respInfo object.
// The boolean `needsLock` indicates if connection lock is required or not.
void
natsConn_disposeRespInfo(natsConnection *nc, respInfo *resp, bool needsLock)
{
    if (resp == NULL)
        return;

    if (!resp->pooled)
    {
        natsCondition_Destroy(resp->cond);
        natsMutex_Destroy(resp->mu);
        NATS_FREE(resp);
    }
    else
    {
        if (needsLock)
            natsConn_Lock(nc);

        resp->closed = false;
        resp->closedSts = NATS_OK;
        resp->removed = false;
        resp->msg = NULL;

        nc->respPool[nc->respPoolIdx++] = resp;

        if (needsLock)
            natsConn_Unlock(nc);
    }
}

// Destroy the pool of respInfo objects.
void
natsConn_destroyRespPool(natsConnection *nc)
{
    int      i;
    respInfo *info;

    for (i=0; i<nc->respPoolSize; i++)
    {
        info = nc->respPool[i];
        info->pooled = false;
        natsConn_disposeRespInfo(nc, info, false);
    }
    NATS_FREE(nc->respPool);
}

// Creates a new respInfo object, binds it to the request's specific
// subject (that is set in respInbox). The respInfo object is returned.
// Connection's lock is held on entry.
natsStatus
natsConn_addRespInfo(respInfo **newResp, natsConnection *nc, char *respInbox, int respInboxSize)
{
    respInfo    *resp  = NULL;
    natsStatus  s      = NATS_OK;

    if (nc->respPoolIdx > 0)
    {
        resp = nc->respPool[--nc->respPoolIdx];
    }
    else
    {
        resp = (respInfo *) NATS_CALLOC(1, sizeof(respInfo));
        if (resp == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
        if (s == NATS_OK)
            s = natsMutex_Create(&(resp->mu));
        if (s == NATS_OK)
            s = natsCondition_Create(&(resp->cond));
        if (s == NATS_OK)
        {
            if (nc->respPoolSize < RESP_INFO_POOL_MAX_SIZE)
            {
                resp->pooled = true;
                nc->respPoolSize++;
            }
        }
    }

    if (s == NATS_OK)
    {
        nc->respId[nc->respIdPos] = '0' + nc->respIdVal;
        nc->respId[nc->respIdPos + 1] = '\0';

        // Build the response inbox
        memcpy(respInbox, nc->respSub, NATS_REQ_ID_OFFSET);
        respInbox[NATS_REQ_ID_OFFSET-1] = '.';
        memcpy(respInbox+NATS_REQ_ID_OFFSET, nc->respId, nc->respIdPos + 2); // copy the '\0' of respId

        nc->respIdVal++;
        if (nc->respIdVal == 10)
        {
            nc->respIdVal = 0;
            if (nc->respIdPos > 0)
            {
                bool shift = true;
                int  i, j;

                for (i=nc->respIdPos-1; i>=0; i--)
                {
                    if (nc->respId[i] != '9')
                    {
                        nc->respId[i]++;

                        for (j=i+1; j<=nc->respIdPos-1; j++)
                            nc->respId[j] = '0';

                        shift = false;
                        break;
                    }
                }
                if (shift)
                {
                    nc->respId[0] = '1';

                    for (i=1; i<=nc->respIdPos; i++)
                        nc->respId[i] = '0';

                    nc->respIdPos++;
                }
            }
            else
            {
                nc->respId[0] = '1';
                nc->respIdPos++;
            }
            if (nc->respIdPos == NATS_MAX_REQ_ID_LEN)
                nc->respIdPos = 0;
        }

        s = natsStrHash_Set(nc->respMap, respInbox+NATS_REQ_ID_OFFSET, true,
                            (void*) resp, NULL);
    }

    if (s == NATS_OK)
        *newResp = resp;
    else
        natsConn_disposeRespInfo(nc, resp, false);

    return NATS_UPDATE_ERR_STACK(s);
}

// Initialize some of the connection's fields used for request/reply mapping.
// Connection's lock is held on entry.
natsStatus
natsConn_initResp(natsConnection *nc, natsMsgHandler cb)
{
    natsStatus s = NATS_OK;
    char       ginbox[NATS_INBOX_PRE_LEN + NUID_BUFFER_LEN + 1 + 1 + 1]; // _INBOX.<nuid>.*

    nc->respPool = NATS_CALLOC(RESP_INFO_POOL_MAX_SIZE, sizeof(respInfo*));
    if (nc->respPool == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    if (s == NATS_OK)
        s = natsStrHash_Create(&nc->respMap, 4);
    if (s == NATS_OK)
        s = natsInbox_Create(&nc->respSub);
    if (s == NATS_OK)
    {
        snprintf(ginbox, sizeof(ginbox), "%s.*", nc->respSub);
        s = natsConn_subscribeNoPoolNoLock(&(nc->respMux), nc, ginbox, cb, (void*) nc);
    }
    if (s != NATS_OK)
    {
        natsInbox_Destroy(nc->respSub);
        nc->respSub = NULL;
        natsStrHash_Destroy(nc->respMap);
        nc->respMap = NULL;
        NATS_FREE(nc->respPool);
        nc->respPool = NULL;
    }

    return NATS_UPDATE_ERR_STACK(s);
}

// This will clear any pending Request calls.
// Lock is assumed to be held by the caller.
static void
_clearPendingRequestCalls(natsConnection *nc, natsStatus reason)
{
    natsStrHashIter iter;
    void            *p = NULL;

    if (nc->respMap == NULL)
        return;

    natsStrHashIter_Init(&iter, nc->respMap);
    while (natsStrHashIter_Next(&iter, NULL, &p))
    {
        respInfo *val = (respInfo*) p;
        natsMutex_Lock(val->mu);
        val->closed = true;
        val->closedSts = reason;
        val->removed = true;
        natsCondition_Signal(val->cond);
        natsMutex_Unlock(val->mu);
        natsStrHashIter_RemoveCurrent(&iter);
    }
    natsStrHashIter_Done(&iter);
}

static void
_clearSSL(natsConnection *nc)
{
    if (nc->sockCtx.ssl == NULL)
        return;

    SSL_free(nc->sockCtx.ssl);
    nc->sockCtx.ssl = NULL;
}

// Try to reconnect using the option parameters.
// This function assumes we are allowed to reconnect.
static void
_doReconnect(void *arg)
{
    natsStatus                      s           = NATS_OK;
    natsConnection                  *nc         = (natsConnection*) arg;
    natsSrvPool                     *pool       = NULL;
    int64_t                         sleepTime   = 0;
    struct threadsToJoin            ttj;
    natsThread                      *rt         = NULL;
    int                             wlf         = 0;
    bool                            doSleep     = false;
    int64_t                         jitter      = 0;
    int                             i           = 0;
    natsCustomReconnectDelayHandler crd         = NULL;
    void                            *crdClosure = NULL;

    natsConn_Lock(nc);

    _initThreadsToJoin(&ttj, nc, false);

    natsConn_Unlock(nc);

    _joinThreads(&ttj);

    natsConn_Lock(nc);

    // Clear any error.
    nc->err         = NATS_OK;
    nc->errStr[0]   = '\0';

    pool = nc->srvPool;

    // Perform appropriate callback if needed for a disconnect.
    // (do not do this if we are here on initial connect failure)
    if (!nc->initc && (nc->opts->disconnectedCb != NULL))
        natsAsyncCb_PostConnHandler(nc, ASYNC_DISCONNECTED);

    crd = nc->opts->customReconnectDelayCB;
    if (crd == NULL)
    {
        jitter = nc->opts->reconnectJitter;
        // TODO: since we sleep only after the whole list has been tried, we can't
        // rely on individual *natsSrv to know if it is a TLS or non-TLS url.
        // We have to pick which type of jitter to use, for now, we use these hints:
        jitter = nc->opts->reconnectJitter;
        if (nc->opts->secure || (nc->opts->sslCtx != NULL))
			jitter = nc->opts->reconnectJitterTLS;
    }
    else
        crdClosure = nc->opts->customReconnectDelayCBClosure;

    // Note that the pool's size may decrement after the call to
    // natsSrvPool_GetNextServer.
    for (i=0; (s == NATS_OK) && (natsSrvPool_GetSize(pool) > 0); )
    {
        nc->cur = natsSrvPool_GetNextServer(pool, nc->opts, nc->cur);
        if (nc->cur == NULL)
        {
            nc->err = NATS_NO_SERVER;
            break;
        }

        doSleep = (i+1 >= natsSrvPool_GetSize(pool));

        if (doSleep)
        {
            i = 0;
            if (crd != NULL)
            {
                wlf++;
                natsConn_Unlock(nc);
                sleepTime = crd(nc, wlf, crdClosure);
                natsConn_Lock(nc);
                if (natsConn_isClosed(nc))
                    break;
            }
            else
            {
                sleepTime = nc->opts->reconnectWait;
                if (jitter > 0)
                    sleepTime += rand() % jitter;
            }
            natsCondition_TimedWait(nc->reconnectCond, nc->mu, sleepTime);
        }
        else
        {
            i++;
            natsConn_Unlock(nc);
            natsThread_Yield();
            natsConn_Lock(nc);
        }

        // Check if we have been closed first.
        if (natsConn_isClosed(nc))
            break;

        // Mark that we tried a reconnect
        nc->cur->reconnects += 1;

        // Try to create a new connection
        s = _createConn(nc);
        if (s != NATS_OK)
        {
            // Reset error here. We will return NATS_NO_SERVERS at the end of
            // this loop if appropriate.
            nc->err = NATS_OK;

            // Reset status
            s = NATS_OK;

            // Not yet connected, retry...
            // Continue to hold the lock
            continue;
        }

        // We are reconnected
        nc->stats.reconnects += 1;

        // Process Connect logic
        s = _processConnInit(nc);
        // Check if connection has been closed (it could happen due to
        // user callback that may be invoked as part of the connect)
        // or if the reconnect process should be aborted.
        if (natsConn_isClosed(nc) || nc->ar)
        {
            if (s == NATS_OK)
                s = nats_setError(NATS_CONNECTION_CLOSED, "%s", "connection has been closed/destroyed while reconnecting");
            break;
        }

        // We have a valid FD. We are now going to send data directly
        // to the newly connected server, so we need to disable the
        // use of 'pending' for the moment.
        nc->usePending = false;

        // Send existing subscription state
        if (s == NATS_OK)
            s = _resendSubscriptions(nc);

        // Now send off and clear pending buffer
        if (s == NATS_OK)
            s = _flushReconnectPendingItems(nc);

        if (s != NATS_OK)
        {
            // In case we were at the last iteration, this is the error
            // we will report.
            nc->err = s;

            // Reset status
            s = NATS_OK;

            // Close the socket since we were connected, but a problem occurred.
            // (not doing this would cause an FD leak)
            natsSock_Close(nc->sockCtx.fd);
            nc->sockCtx.fd = NATS_SOCK_INVALID;

            // We need to re-activate the use of pending since we
            // may go back to sleep and release the lock
            nc->usePending = true;
            natsBuf_Reset(nc->bw);

            // We need to cleanup some things if the connection was SSL.
            _clearSSL(nc);

            nc->status = NATS_CONN_STATUS_RECONNECTING;
            continue;
        }

        // This is where we are truly connected.
        nc->status = NATS_CONN_STATUS_CONNECTED;

        // No more failure allowed past this point.

        // Clear connection's last error
        nc->err       = NATS_OK;
        nc->errStr[0] = '\0';

        // Clear the possible current lastErr
        nc->cur->lastAuthErrCode = 0;

        // Clear out server stats for the server we connected to..
        nc->cur->didConnect = true;
        nc->cur->reconnects = 0;

        // At this point we know that we don't need the pending buffer
        // anymore. Destroy now.
        natsBuf_Destroy(nc->pending);
        nc->pending     = NULL;
        nc->usePending  = false;

        // Normally only set in _connect() but we need in case we allow
        // reconnect logic on initial connect failure.
        if (nc->initc)
        {
            // This was the initial connect. Set this to false.
            nc->initc = false;
            // Invoke the callback.
            if (nc->opts->connectedCb != NULL)
                natsAsyncCb_PostConnHandler(nc, ASYNC_CONNECTED);
        }
        else
        {
            // Call reconnectedCB if appropriate. Since we are in a separate
            // thread, we could invoke the callback directly, however, we
            // still post it so all callbacks from a connection are serialized.
            if (nc->opts->reconnectedCb != NULL)
                natsAsyncCb_PostConnHandler(nc, ASYNC_RECONNECTED);
        }

        nc->inReconnect--;
        if (nc->reconnectThread != NULL)
        {
            rt = nc->reconnectThread;
            nc->reconnectThread = NULL;
        }

        // Release lock here, we will return below.
        natsConn_Unlock(nc);

        // Make sure we flush everything
        (void) natsConnection_Flush(nc);

        // Release to compensate for the retain in processOpError.
        natsConn_release(nc);

        if (rt != NULL)
        {
            natsThread_Detach(rt);
            natsThread_Destroy(rt);
        }

        return;
    }

    // Call into close.. We have no servers left..
    if (nc->err == NATS_OK)
        nc->err = NATS_NO_SERVER;

    nc->inReconnect--;
    nc->rle = true;
    natsConn_Unlock(nc);

    _close(nc, NATS_CONN_STATUS_CLOSED, false, true);

    // Release to compensate for the retain in processOpError.
    natsConn_release(nc);
}

// If the connection has the option `sendAsap`, flushes the buffer
// directly, otherwise, notifies the flusher thread that there is
// pending data to send to the server.
natsStatus
natsConn_flushOrKickFlusher(natsConnection *nc)
{
    natsStatus s = NATS_OK;

    if (nc->opts->sendAsap)
    {
        s = natsConn_bufferFlush(nc);
    }
    else if (!(nc->flusherSignaled) && (nc->bw != NULL))
    {
        nc->flusherSignaled = true;
        natsCondition_Signal(nc->flusherCond);
    }
    return s;
}

// reads a protocol one byte at a time.
static natsStatus
_readProto(natsConnection *nc, natsBuffer **proto)
{
	natsStatus	s 			= NATS_OK;
	char		protoEnd	= '\n';
	natsBuffer	*buf		= NULL;
	char		oneChar[1]  = { '\0' };

	s = natsBuf_Create(&buf, 10);
	if (s != NATS_OK)
		return s;

	for (;;)
	{
		s = natsSock_Read(&(nc->sockCtx), oneChar, 1, NULL);
		if (s == NATS_CONNECTION_CLOSED)
		    break;
		s = natsBuf_AppendByte(buf, oneChar[0]);
		if (s != NATS_OK)
		{
			natsBuf_Destroy(buf);
			return s;
		}
		if (oneChar[0] == protoEnd)
		    break;
	}
	s = natsBuf_AppendByte(buf, '\0');
	if (s != NATS_OK)
	{
	    natsBuf_Destroy(buf);
	    return s;
	}
	*proto = buf;
	return NATS_OK;
}

static natsStatus
_sendConnect(natsConnection *nc)
{
    natsStatus  s       = NATS_OK;
    char        *cProto = NULL;
    natsBuffer	*proto  = NULL;
    bool        rup     = (nc->pending != NULL);

    // Create the CONNECT protocol
    s = _connectProto(nc, &cProto);

    // Because we now possibly release the connection lock in _connectProto()
    // (if there is user callbacks for jwt/signing keys), we can't have the
    // set/reset of usePending be in doReconnect(). There are then windows in
    // which a user Publish() could sneak in a try to send to socket. So limit
    // the disable/re-enable of that boolean to around this buffer write/flush
    // calls.
    if (rup)
        nc->usePending = false;

    // Add it to the buffer
    if (s == NATS_OK)
        s = natsConn_bufferWriteString(nc, cProto);

    // Add the PING protocol to the buffer
    if (s == NATS_OK)
        s = natsConn_bufferWrite(nc, _PING_OP_, _PING_OP_LEN_);
    if (s == NATS_OK)
        s = natsConn_bufferWrite(nc, _CRLF_, _CRLF_LEN_);

    // Flush the buffer
    if (s == NATS_OK)
        s = natsConn_bufferFlush(nc);

    // Reset here..
    if (rup)
        nc->usePending = true;

    // Now read the response from the server.
    if (s == NATS_OK)
        s = _readProto(nc, &proto);

    // If Verbose is set, we expect +OK first.
    if ((s == NATS_OK) && nc->opts->verbose)
    {
        // Check protocol is as expected
        if (strncmp(natsBuf_Data(proto), _OK_OP_, _OK_OP_LEN_) != 0)
        {
            s = nats_setError(NATS_PROTOCOL_ERROR,
                              "Expected '%s', got '%s'",
                              _OK_OP_, natsBuf_Data(proto));
        }
        natsBuf_Destroy(proto);
        proto = NULL;

        // Read the rest now...
        if (s == NATS_OK)
            s = _readProto(nc, &proto);
    }

    // We except the PONG protocol
    if ((s == NATS_OK) && (strncmp(natsBuf_Data(proto), _PONG_OP_, _PONG_OP_LEN_) != 0))
    {
        // But it could be something else, like -ERR

        if (strncmp(natsBuf_Data(proto), _ERR_OP_, _ERR_OP_LEN_) == 0)
        {
            char    buffer[256];
            int     authErrCode = 0;

            buffer[0] = '\0';
            snprintf_truncate(buffer, sizeof(buffer), "%s", natsBuf_Data(proto));

            // Remove -ERR, trim spaces and quotes.
            nats_NormalizeErr(buffer);

            // Look for auth errors.
            if ((authErrCode = _checkAuthError(buffer)) != 0)
            {
                // This sets nc->err to NATS_CONNECTION_AUTH_FAILED
                // copy content of buffer into nc->errStr.
                _processAuthError(nc, authErrCode, buffer);
                s = nc->err;
            }
            else
                s = NATS_ERR;

            // Update stack
            s = nats_setError(s, "%s", buffer);
        }
        else
        {
            s = nats_setError(NATS_PROTOCOL_ERROR,
                              "Expected '%s', got '%s'",
                              _PONG_OP_, natsBuf_Data(proto));
        }
    }
    // Destroy proto (ok if proto is NULL).
    natsBuf_Destroy(proto);

    if (s == NATS_OK)
        nc->status = NATS_CONN_STATUS_CONNECTED;

    NATS_FREE(cProto);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_processConnInit(natsConnection *nc)
{
    natsStatus s = NATS_OK;

    nc->status = NATS_CONN_STATUS_CONNECTING;

    // Process the INFO protocol that we should be receiving
    s = _processExpectedInfo(nc);

    // Send the CONNECT and PING protocol, and wait for the PONG.
    if (s == NATS_OK)
        s = _sendConnect(nc);

    // Clear our deadline, regardless of error
    natsSock_ClearDeadline(&nc->sockCtx);

    // If there is no write deadline option, switch to blocking socket here...
   if ((s == NATS_OK) && (nc->opts->writeDeadline <= 0))
       s = natsSock_SetBlocking(nc->sockCtx.fd, true);

    // Start the readLoop and flusher threads
    if (s == NATS_OK)
        s = _spinUpSocketWatchers(nc);

    if ((s == NATS_OK) && (nc->opts->evLoop != NULL))
    {
        s = natsSock_SetBlocking(nc->sockCtx.fd, false);

        // If we are reconnecting, buffer will have already been allocated
        if ((s == NATS_OK) && (nc->el.buffer == NULL))
        {
            nc->el.buffer = (char*) malloc(nc->opts->ioBufSize);
            if (nc->el.buffer == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        if (s == NATS_OK)
        {
            // Set this first in case the event loop triggers the first READ
            // event just after this call returns.
            nc->sockCtx.useEventLoop = true;

            s = nc->opts->evCbs.attach(&(nc->el.data),
                                       nc->opts->evLoop,
                                       nc,
                                       (int) nc->sockCtx.fd);
            if (s == NATS_OK)
            {
                nc->el.attached = true;
            }
            else
            {
                nc->sockCtx.useEventLoop = false;

                nats_setError(s,
                              "Error attaching to the event loop: %d - %s",
                              s, natsStatus_GetText(s));
            }
        }
    }

    return NATS_UPDATE_ERR_STACK(s);
}

// Main connect function. Will connect to the server
static natsStatus
_connect(natsConnection *nc)
{
    natsStatus  s     = NATS_OK;
    natsStatus  retSts= NATS_OK;
    natsSrvPool *pool = NULL;
    int         i = 0;
    int         l = 0;
    int         max = 0;
    int64_t     wtime = 0;
    bool        retry = false;

    natsConn_Lock(nc);
    nc->initc = true;

    pool = nc->srvPool;

    if ((nc->opts->retryOnFailedConnect) && (nc->opts->connectedCb == NULL))
    {
        retry = true;
        max   = nc->opts->maxReconnect;
        wtime = nc->opts->reconnectWait;
    }

    for (;;)
    {
        // The pool may change inside the loop iteration due to INFO protocol.
        for (i = 0; i < natsSrvPool_GetSize(pool); i++)
        {
            nc->cur = natsSrvPool_GetSrv(pool,i);

            s = _createConn(nc);
            if (s == NATS_OK)
            {
                s = _processConnInit(nc);

                if (s == NATS_OK)
                {
                    nc->cur->lastAuthErrCode = 0;
                    natsSrvPool_SetSrvDidConnect(pool, i, true);
                    natsSrvPool_SetSrvReconnects(pool, i, 0);
                    retSts = NATS_OK;
                    retry = false;
                    break;
                }
                else
                {
                    retSts = s;

                    natsConn_Unlock(nc);

                    _close(nc, NATS_CONN_STATUS_DISCONNECTED, false, false);

                    natsConn_Lock(nc);

                    nc->cur = NULL;
                }
            }
            else
            {
                if (natsConn_isClosed(nc))
                {
                    if (s == NATS_OK)
                        s = NATS_CONNECTION_CLOSED;
                    break;
                }

                if (s == NATS_IO_ERROR)
                    retSts = NATS_OK;
            }
        }
        if (!retry)
            break;

        l++;
        if ((max > 0) && (l > max))
            break;

        if (wtime > 0)
            nats_Sleep(wtime);
    }

    // If not connected and retry asynchronously on failed connect
    if ((nc->status != NATS_CONN_STATUS_CONNECTED)
            && nc->opts->retryOnFailedConnect
            && (nc->opts->connectedCb != NULL))
    {
        natsConn_Unlock(nc);

        if (_processOpError(nc, retSts, true))
        {
            nats_clearLastError();
            return NATS_NOT_YET_CONNECTED;
        }

        natsConn_Lock(nc);
    }

    if ((retSts == NATS_OK) && (nc->status != NATS_CONN_STATUS_CONNECTED))
        s = nats_setDefaultError(NATS_NO_SERVER);

    nc->initc = false;
    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_evStopPolling(natsConnection *nc)
{
    natsStatus s;

    nc->sockCtx.useEventLoop = false;
    nc->el.writeAdded = false;
    s = nc->opts->evCbs.read(nc->el.data, NATS_EVENT_ACTION_REMOVE);
    if (s == NATS_OK)
        s = nc->opts->evCbs.write(nc->el.data, NATS_EVENT_ACTION_REMOVE);

    return s;
}

// _processOpError handles errors from reading or parsing the protocol.
// The lock should not be held entering this function.
static bool
_processOpError(natsConnection *nc, natsStatus s, bool initialConnect)
{
    natsConn_Lock(nc);

    if (!initialConnect)
    {
        if (_isConnecting(nc) || natsConn_isClosed(nc) || (nc->inReconnect > 0))
        {
            natsConn_Unlock(nc);

            return false;
        }
    }

    // Do reconnect only if allowed and we were actually connected
    // or if we are retrying on initial failed connect.
    if (initialConnect || (nc->opts->allowReconnect && (nc->status == NATS_CONN_STATUS_CONNECTED)))
    {
        natsStatus ls = NATS_OK;

        // Set our new status
        nc->status = NATS_CONN_STATUS_RECONNECTING;

        if (nc->ptmr != NULL)
            natsTimer_Stop(nc->ptmr);

        if (nc->sockCtx.fdActive)
        {
            SET_WRITE_DEADLINE(nc);
            natsConn_bufferFlush(nc);

            natsSock_Shutdown(nc->sockCtx.fd);
            nc->sockCtx.fdActive = false;
        }

        // If we use an external event loop, we need to stop polling
        // on the socket since we are going to reconnect.
        if (nc->el.attached)
        {
            ls = _evStopPolling(nc);
            natsSock_Close(nc->sockCtx.fd);
            nc->sockCtx.fd = NATS_SOCK_INVALID;

            // We need to cleanup some things if the connection was SSL.
            _clearSSL(nc);
        }

        // Fail pending flush requests.
        if (ls == NATS_OK)
            _clearPendingFlushRequests(nc);
        // If option set, also fail pending requests.
        if ((ls == NATS_OK) && nc->opts->failRequestsOnDisconnect)
            _clearPendingRequestCalls(nc, NATS_CONNECTION_DISCONNECTED);

        // Create the pending buffer to hold all write requests while we try
        // to reconnect.
        if (ls == NATS_OK)
            ls = natsBuf_Create(&(nc->pending), nc->opts->reconnectBufSize);
        if (ls == NATS_OK)
        {
            nc->usePending = true;

            // Start the reconnect thread
            ls = natsThread_Create(&(nc->reconnectThread),
                                  _doReconnect, (void*) nc);
        }
        if (ls == NATS_OK)
        {
            // We created the reconnect thread successfully, so retain
            // the connection.
            _retain(nc);
            nc->inReconnect++;
            natsConn_Unlock(nc);

            return true;
        }
    }

    // reconnect not allowed or we failed to setup the reconnect code.

    nc->status = NATS_CONN_STATUS_DISCONNECTED;
    nc->err = s;

    natsConn_Unlock(nc);

    _close(nc, NATS_CONN_STATUS_CLOSED, false, true);

    return false;
}

static void
_readLoop(void  *arg)
{
    natsStatus  s = NATS_OK;
    char        *buffer;
    int         n;
    int         bufSize;

    natsConnection *nc = (natsConnection*) arg;

    natsConn_Lock(nc);

    bufSize = nc->opts->ioBufSize;
    buffer = NATS_MALLOC(bufSize);
    if (buffer == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);

    if (nc->sockCtx.ssl != NULL)
        nats_sslRegisterThreadForCleanup();

    natsDeadline_Clear(&(nc->sockCtx.readDeadline));

    if (nc->ps == NULL)
        s = natsParser_Create(&(nc->ps));

    while ((s == NATS_OK)
           && !natsConn_isClosed(nc)
           && !natsConn_isReconnecting(nc))
    {
        natsConn_Unlock(nc);

        n = 0;

        s = natsSock_Read(&(nc->sockCtx), buffer, bufSize, &n);
        if ((s == NATS_IO_ERROR) && (NATS_SOCK_GET_ERROR == NATS_SOCK_WOULD_BLOCK))
            s = NATS_OK;
        if ((s == NATS_OK) && (n > 0))
            s = natsParser_Parse(nc, buffer, n);

        if (s != NATS_OK)
            _processOpError(nc, s, false);

        natsConn_Lock(nc);
    }

    NATS_FREE(buffer);

    natsSock_Close(nc->sockCtx.fd);
    nc->sockCtx.fd       = NATS_SOCK_INVALID;
    nc->sockCtx.fdActive = false;

    // We need to cleanup some things if the connection was SSL.
    _clearSSL(nc);

    natsParser_Destroy(nc->ps);
    nc->ps = NULL;

    // This unlocks and releases the connection to compensate for the retain
    // when this thread was created.
    natsConn_unlockAndRelease(nc);
}

static void
_flusher(void *arg)
{
    natsConnection  *nc  = (natsConnection*) arg;
    natsStatus      s;

    while (true)
    {
        natsConn_Lock(nc);

        while (!(nc->flusherSignaled) && !(nc->flusherStop))
            natsCondition_Wait(nc->flusherCond, nc->mu);

        if (nc->flusherStop)
        {
            natsConn_Unlock(nc);
            break;
        }

        //TODO: If we process the request right away, performance
        //      will suffer when sending quickly very small messages.
        //      The buffer is going to be always flushed, which
        //      defeats the purpose of a write buffer.
        //      We need to revisit this.

        // Give a chance to accumulate more requests...
        natsCondition_TimedWait(nc->flusherCond, nc->mu, 1);

        nc->flusherSignaled = false;

        if (!_isConnected(nc) || natsConn_isClosed(nc) || natsConn_isReconnecting(nc))
        {
            natsConn_Unlock(nc);
            break;
        }

        if (nc->sockCtx.fdActive && (natsBuf_Len(nc->bw) > 0))
        {
            SET_WRITE_DEADLINE(nc);
            s = natsConn_bufferFlush(nc);
            if ((s != NATS_OK) && (nc->err == NATS_OK))
                nc->err = s;
        }

        natsConn_Unlock(nc);
    }

    // Release the connection to compensate for the retain when this thread
    // was created.
    natsConn_release(nc);
}

static void
_sendPing(natsConnection *nc, natsPong *pong)
{
    natsStatus  s     = NATS_OK;

    SET_WRITE_DEADLINE(nc);
    s = natsConn_bufferWrite(nc, _PING_PROTO_, _PING_PROTO_LEN_);
    if (s == NATS_OK)
    {
        // Flush the buffer in place.
        s = natsConn_bufferFlush(nc);
    }
    if (s == NATS_OK)
    {
        // Now that we know the PING was sent properly, update
        // the number of PING sent.
        nc->pongs.outgoingPings++;

        if (pong != NULL)
        {
            pong->id = nc->pongs.outgoingPings;

            // Add this pong to the list.
            pong->next = NULL;
            pong->prev = nc->pongs.tail;

            if (nc->pongs.tail != NULL)
                nc->pongs.tail->next = pong;

            nc->pongs.tail = pong;

            if (nc->pongs.head == NULL)
                nc->pongs.head = pong;
        }
    }
}

static void
_processPingTimer(natsTimer *timer, void *arg)
{
    natsConnection  *nc = (natsConnection*) arg;

    natsConn_Lock(nc);

    if (nc->status != NATS_CONN_STATUS_CONNECTED)
    {
        natsConn_Unlock(nc);
        return;
    }

    // If we have more PINGs out than PONGs in, consider
    // the connection stale.
    if (++(nc->pout) > nc->opts->maxPingsOut)
    {
        natsConn_Unlock(nc);
        _processOpError(nc, NATS_STALE_CONNECTION, false);
        return;
    }

    _sendPing(nc, NULL);

    natsConn_Unlock(nc);
}

static void
_pingStopppedCb(natsTimer *timer, void *closure)
{
    natsConnection *nc = (natsConnection*) closure;

    natsConn_release(nc);
}

static natsStatus
_spinUpSocketWatchers(natsConnection *nc)
{
    natsStatus  s = NATS_OK;

    nc->pout        = 0;
    nc->flusherStop = false;

    if (nc->opts->evLoop == NULL)
    {
        // Let's not rely on the created threads acquiring lock that would make it
        // safe to retain only on success.
        _retain(nc);

        s = natsThread_Create(&(nc->readLoopThread), _readLoop, (void*) nc);
        if (s != NATS_OK)
            _release(nc);
    }

    // Don't start flusher thread if connection was created with SendAsap option.
    if ((s == NATS_OK) && !(nc->opts->sendAsap))
    {
        _retain(nc);

        s = natsThread_Create(&(nc->flusherThread), _flusher, (void*) nc);
        if (s != NATS_OK)
            _release(nc);
    }

    if ((s == NATS_OK) && (nc->opts->pingInterval > 0))
    {
        _retain(nc);

        if (nc->ptmr == NULL)
        {
            s = natsTimer_Create(&(nc->ptmr),
                                 _processPingTimer,
                                 _pingStopppedCb,
                                 nc->opts->pingInterval,
                                 (void*) nc);
            if (s != NATS_OK)
                _release(nc);
        }
        else
        {
            natsTimer_Reset(nc->ptmr, nc->opts->pingInterval);
        }
    }

    return s;
}

// Remove all subscriptions. This will kick out the delivery threads,
// and unblock NextMsg() calls.
static void
_removeAllSubscriptions(natsConnection *nc)
{
    natsHashIter     iter;
    void             *p = NULL;

    natsMutex_Lock(nc->subsMu);
    natsHashIter_Init(&iter, nc->subs);
    while (natsHashIter_Next(&iter, NULL, &p))
    {
        natsSubscription *sub = (natsSubscription*) p;

        (void) natsHashIter_RemoveCurrent(&iter);

        natsSub_close(sub, true);

        natsSub_release(sub);
    }
    natsHashIter_Done(&iter);
    natsMutex_Unlock(nc->subsMu);
}

// Low level close call that will do correct cleanup and set
// desired status. Also controls whether user defined callbacks
// will be triggered. The lock should not be held entering this
// function. This function will handle the locking manually.
static void
_close(natsConnection *nc, natsConnStatus status, bool fromPublicClose, bool doCBs)
{
    struct threadsToJoin    ttj;
    bool                    sockWasActive = false;
    bool                    detach = false;
    natsSubscription        *sub = NULL;

    natsConn_lockAndRetain(nc);

    // If invoked from the public Close() call, attempt to flush
    // to ensure that server has received all pending data.
    // Note that _flushTimeout will release the lock and wait
    // for PONG so this is why we do this early in that function.
    if (fromPublicClose
            && (nc->status == NATS_CONN_STATUS_CONNECTED)
            && nc->sockCtx.fdActive
            && (natsBuf_Len(nc->bw) > 0))
    {
        _flushTimeout(nc, 500);
    }

    if (natsConn_isClosed(nc))
    {
        nc->status = status;

        natsConn_unlockAndRelease(nc);
        return;
    }

    nc->status = NATS_CONN_STATUS_CLOSED;

    _initThreadsToJoin(&ttj, nc, true);

    // Kick out all calls to natsConnection_Flush[Timeout]().
    _clearPendingFlushRequests(nc);

    // Kick out any queued and blocking requests.
    _clearPendingRequestCalls(nc, NATS_CONNECTION_CLOSED);

    if (nc->ptmr != NULL)
        natsTimer_Stop(nc->ptmr);

    // Unblock reconnect thread block'ed in sleep of reconnectWait interval
    natsCondition_Broadcast(nc->reconnectCond);

    // Remove all subscriptions. This will kick out the delivery threads,
    // and unblock NextMsg() calls.
    _removeAllSubscriptions(nc);

    // Go ahead and make sure we have flushed the outbound buffer.
    if (nc->sockCtx.fdActive)
    {
        // If there is no readLoop (or using external event loop), then it is
        // our responsibility to close the socket. Otherwise, _readLoop is the
        // one doing it.
        if (ttj.readLoop == NULL)
        {
            // If event loop attached, stop polling...
            if (nc->el.attached)
                _evStopPolling(nc);

            natsSock_Close(nc->sockCtx.fd);
            nc->sockCtx.fd = NATS_SOCK_INVALID;

            // We need to cleanup some things if the connection was SSL.
            _clearSSL(nc);
        }
        else
        {
            // Shutdown the socket to stop any read/write operations.
            // The socket will be closed by the _readLoop thread.
            natsSock_Shutdown(nc->sockCtx.fd);
        }
        nc->sockCtx.fdActive = false;
        sockWasActive = true;
    }

    // Perform appropriate callback if needed for a disconnect.
    // Do not invoke if we were disconnected and failed to reconnect (since
    // it has already been invoked in doReconnect).
    if (doCBs && !nc->rle && (nc->opts->disconnectedCb != NULL) && sockWasActive)
        natsAsyncCb_PostConnHandler(nc, ASYNC_DISCONNECTED);

    sub = nc->respMux;
    nc->respMux = NULL;

    natsConn_Unlock(nc);

    if (sub != NULL)
        natsSub_release(sub);

    _joinThreads(&ttj);

    natsConn_Lock(nc);

    // Perform appropriate callback if needed for a connection closed.
    if (doCBs && (nc->opts->closedCb != NULL))
        natsAsyncCb_PostConnHandler(nc, ASYNC_CLOSED);

    nc->status = status;

    if (nc->el.attached)
    {
        nc->el.attached = false;
        detach = true;
        _retain(nc);
    }

    natsConn_unlockAndRelease(nc);

    if (detach)
    {
        nc->opts->evCbs.detach(nc->el.data);
        natsConn_release(nc);
    }
}

static natsStatus
_createMsg(natsMsg **newMsg, natsConnection *nc, char *buf, int bufLen, int hdrLen)
{
    natsStatus  s        = NATS_OK;
    int         subjLen  = 0;
    char        *reply   = NULL;
    int         replyLen = 0;

    subjLen = natsBuf_Len(nc->ps->ma.subject);

    if (nc->ps->ma.reply != NULL)
    {
        reply    = natsBuf_Data(nc->ps->ma.reply);
        replyLen = natsBuf_Len(nc->ps->ma.reply);
    }

    s = natsMsg_create(newMsg,
                       (const char*) natsBuf_Data(nc->ps->ma.subject), subjLen,
                       (const char*) reply, replyLen,
                       (const char*) buf, bufLen, hdrLen);
    return s;
}

natsStatus
natsConn_processMsg(natsConnection *nc, char *buf, int bufLen)
{
    natsStatus       s    = NATS_OK;
    natsSubscription *sub = NULL;
    natsMsg          *msg = NULL;
    natsMsgDlvWorker *ldw = NULL;
    bool             sc   = false;
    int              dl   = 0;

    natsMutex_Lock(nc->subsMu);

    nc->stats.inMsgs  += 1;
    nc->stats.inBytes += (uint64_t) bufLen;

    sub = natsHash_Get(nc->subs, nc->ps->ma.sid);

    natsMutex_Unlock(nc->subsMu);

    if (sub == NULL)
        return NATS_OK;

    // Do this outside of sub's lock, even if we end-up having to destroy
    // it because we have reached the maxPendingMsgs count. This reduces
    // lock contention.
    s = _createMsg(&msg, nc, buf, bufLen, nc->ps->ma.hdr);
    if (s != NATS_OK)
        return s;
    // bufLen is the overall buffer len. In presence of headers, we need
    // to capture the real message payload data length, which has been
    // computed as the bufLen - header size.
    dl = msg->dataLen;

    if ((ldw = sub->libDlvWorker) != NULL)
        natsMutex_Lock(ldw->lock);
    else
        natsSub_Lock(sub);

    if (sub->closed || sub->drainSkip)
    {
        if (ldw != NULL)
            natsMutex_Unlock(ldw->lock);
        else
            natsSub_Unlock(sub);

        natsMsg_Destroy(msg);
        return NATS_OK;
    }

    sub->msgList.msgs++;
    sub->msgList.bytes += dl;

    if (((sub->msgsLimit > 0) && (sub->msgList.msgs > sub->msgsLimit))
        || ((sub->bytesLimit > 0) && (sub->msgList.bytes > sub->bytesLimit)))
    {
        natsMsg_Destroy(msg);

        sub->dropped++;

        sc = !sub->slowConsumer;
        sub->slowConsumer = true;

        // Undo stats from above.
        sub->msgList.msgs--;
        sub->msgList.bytes -= dl;
    }
    else
    {
        natsMsgList *list = NULL;

        if (sub->msgList.msgs > sub->msgsMax)
            sub->msgsMax = sub->msgList.msgs;

        if (sub->msgList.bytes > sub->bytesMax)
            sub->bytesMax = sub->msgList.bytes;

        sub->slowConsumer = false;

        if (ldw != NULL)
        {
            msg->sub = sub;
            list = &ldw->msgList;
        }
        else
        {
            list = &sub->msgList;
        }

        if (list->head == NULL)
            list->head = msg;

        if (list->tail != NULL)
            list->tail->next = msg;

        list->tail = msg;

        if (ldw != NULL)
        {
            if (ldw->inWait)
                natsCondition_Broadcast(ldw->cond);
        }
        else
        {
            if (sub->inWait > 0)
                natsCondition_Broadcast(sub->cond);
        }
    }

    if (ldw != NULL)
        natsMutex_Unlock(ldw->lock);
    else
        natsSub_Unlock(sub);

    if (sc)
    {
        natsConn_Lock(nc);

        nc->err = NATS_SLOW_CONSUMER;

        if (nc->opts->asyncErrCb != NULL)
            natsAsyncCb_PostErrHandler(nc, sub, NATS_SLOW_CONSUMER);

        natsConn_Unlock(nc);
    }

    return s;
}

void
natsConn_processOK(natsConnection *nc)
{
    // Do nothing for now.
}

// _processPermissionViolation is called when the server signals a subject
// permissions violation on either publish or subscribe.
static void
_processPermissionViolation(natsConnection *nc, char *error)
{
    natsConn_Lock(nc);
    nc->err = NATS_NOT_PERMITTED;
    snprintf(nc->errStr, sizeof(nc->errStr), "%s", error);
    if (nc->opts->asyncErrCb != NULL)
        natsAsyncCb_PostErrHandler(nc, NULL, NATS_NOT_PERMITTED);
    natsConn_Unlock(nc);
}

// _processAuthError does common processing of auth errors.
// We want to do retries unless we get the same error again.
// This allows us for instance to swap credentials and have
// the app reconnect, but if nothing is changing we should bail.
static bool
_processAuthError(natsConnection *nc, int errCode, char *error)
{
    nc->err = NATS_CONNECTION_AUTH_FAILED;
    snprintf(nc->errStr, sizeof(nc->errStr), "%s", error);

    if (!nc->initc && nc->opts->asyncErrCb != NULL)
        natsAsyncCb_PostErrHandler(nc, NULL, NATS_CONNECTION_AUTH_FAILED);

    if (nc->cur->lastAuthErrCode == errCode)
        nc->ar = true;
    else
        nc->cur->lastAuthErrCode = errCode;

    return nc->ar;
}

// Checks if the error is an authentication error and if so returns
// the error code for the string, 0 otherwise.
static int
_checkAuthError(char *error)
{
    if (nats_strcasestr(error, AUTHORIZATION_ERR) != NULL)
        return ERR_CODE_AUTH_VIOLATION;
    else if (nats_strcasestr(error, AUTHENTICATION_EXPIRED_ERR) != NULL)
        return ERR_CODE_AUTH_EXPIRED;
    return 0;
}

void
natsConn_processErr(natsConnection *nc, char *buf, int bufLen)
{
    char error[256];
    bool close       = false;
    int  authErrCode = 0;

    // Copy the error in this local buffer.
    snprintf(error, sizeof(error), "%.*s", bufLen, buf);

    // Trim spaces and remove quotes.
    nats_NormalizeErr(error);

    if (strcasecmp(error, STALE_CONNECTION) == 0)
    {
        _processOpError(nc, NATS_STALE_CONNECTION, false);
    }
    else if (nats_strcasestr(error, PERMISSIONS_ERR) != NULL)
    {
        _processPermissionViolation(nc, error);
    }
    else if ((authErrCode = _checkAuthError(error)) != 0)
    {
        natsConn_Lock(nc);
        close = _processAuthError(nc, authErrCode, error);
        natsConn_Unlock(nc);
    }
    else
    {
        close = true;
        natsConn_Lock(nc);
        nc->err = NATS_ERR;
        snprintf(nc->errStr, sizeof(nc->errStr), "%s", error);
        natsConn_Unlock(nc);
    }
    if (close)
        _close(nc, NATS_CONN_STATUS_CLOSED, false, true);
}

void
natsConn_processPing(natsConnection *nc)
{
    natsConn_Lock(nc);

    SET_WRITE_DEADLINE(nc);
    if (natsConn_bufferWrite(nc, _PONG_PROTO_, _PONG_PROTO_LEN_) == NATS_OK)
        natsConn_flushOrKickFlusher(nc);

    natsConn_Unlock(nc);
}

void
natsConn_processPong(natsConnection *nc)
{
    natsPong *pong = NULL;

    natsConn_Lock(nc);

    nc->pongs.incoming++;

    // Check if the first pong's id in the list matches the incoming Id.
    if (((pong = nc->pongs.head) != NULL)
        && (pong->id == nc->pongs.incoming))
    {
        // Remove the pong from the list
        _removePongFromList(nc, pong);

        // Release the Flush[Timeout] call
        pong->id = 0;

        // There may be more than one thread waiting on this
        // condition variable, so we use broadcast instead of
        // signal.
        natsCondition_Broadcast(nc->pongs.cond);
    }

    nc->pout = 0;

    natsConn_Unlock(nc);
}

natsStatus
natsConn_addSubcription(natsConnection *nc, natsSubscription *sub)
{
    natsStatus          s       = NATS_OK;
    void                *oldSub = NULL;

    s = natsHash_Set(nc->subs, sub->sid, (void*) sub, &oldSub);
    if (s == NATS_OK)
    {
        assert(oldSub == NULL);
        natsSub_retain(sub);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

void
natsConn_removeSubscription(natsConnection *nc, natsSubscription *removedSub)
{
    natsSubscription *sub = NULL;

    natsMutex_Lock(nc->subsMu);

    sub = natsHash_Remove(nc->subs, removedSub->sid);

    // Note that the sub may have already been removed, so 'sub == NULL'
    // is not an error.
    if (sub != NULL)
        natsSub_close(sub, false);

    natsMutex_Unlock(nc->subsMu);

    // If we really removed the subscription, then release it.
    if (sub != NULL)
        natsSub_release(sub);
}

static bool
_checkSubjOrQueue(const char *name, bool checkToken)
{
    int     i       = 0;
    int     len     = 0;
    int     lastDot = -1;
    char    c;

    len = (int) strlen(name);
    for (i=0; i<len ; i++)
    {
        c = name[i];
        if ((c == ' ') || (c == '\t') || (c == '\r') || (c == '\n'))
            return true;

        if (checkToken && (c == '.'))
        {
            if ((i == len-1) || (i == lastDot+1))
                return true;

            lastDot = i;
        }
    }
    return false;
}

static bool
_badSubject(const char *subj)
{
    return _checkSubjOrQueue(subj, true);
}

static bool
_badQueue(const char *queue)
{
    return _checkSubjOrQueue(queue, false);
}

// subscribe is the internal subscribe function that indicates interest in a subject.
natsStatus
natsConn_subscribeImpl(natsSubscription **newSub,
                       natsConnection *nc, bool lock, const char *subj, const char *queue,
                       int64_t timeout, natsMsgHandler cb, void *cbClosure,
                       bool preventUseOfLibDlvPool)
{
    natsStatus          s    = NATS_OK;
    natsSubscription    *sub = NULL;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if ((subj == NULL) || (strlen(subj) == 0) || _badSubject(subj))
        return nats_setDefaultError(NATS_INVALID_SUBJECT);

    if ((queue != NULL) && ((strlen(subj) == 0) || _badQueue(queue)))
        return nats_setDefaultError(NATS_INVALID_QUEUE_NAME);

    if (lock)
        natsConn_Lock(nc);

    if (natsConn_isClosed(nc))
    {
        if (lock)
            natsConn_Unlock(nc);

        return nats_setDefaultError(NATS_CONNECTION_CLOSED);
    }

    if (natsConn_isDraining(nc))
    {
        if (lock)
            natsConn_Unlock(nc);

        return nats_setDefaultError(NATS_DRAINING);
    }

    s = natsSub_create(&sub, nc, subj, queue, timeout, cb, cbClosure, preventUseOfLibDlvPool);
    if (s == NATS_OK)
    {
        natsMutex_Lock(nc->subsMu);
        sub->sid = ++(nc->ssid);
        s = natsConn_addSubcription(nc, sub);
        natsMutex_Unlock(nc->subsMu);
    }

    if (s == NATS_OK)
    {
        // We will send these for all subs when we reconnect
        // so that we can suppress here.
        if (!natsConn_isReconnecting(nc))
        {
            char    *proto = NULL;
            int     res    = 0;

            res = nats_asprintf(&proto, _SUB_PROTO_,
                                subj,
                                (queue == NULL ? "" : queue),
                                (int) sub->sid);
            if (res < 0)
                s = nats_setDefaultError(NATS_NO_MEMORY);

            if (s == NATS_OK)
            {
                SET_WRITE_DEADLINE(nc);
                s = natsConn_bufferWriteString(nc, proto);
                if (s == NATS_OK)
                    s = natsConn_flushOrKickFlusher(nc);

                // We should not return a failure if we get an issue
                // with the buffer write (except if it is no memory).
                // For IO errors (if we just got disconnected), the
                // reconnect logic will resend the sub protocol.

                if (s != NATS_NO_MEMORY)
                    s = NATS_OK;
            }

            NATS_FREE(proto);
        }
    }

    if (s == NATS_OK)
    {
        *newSub = sub;
    }
    else if (sub != NULL)
    {
        // A delivery thread may have been started, but the subscription not
        // added to the connection's subscription map. So this is necessary
        // for the delivery thread to unroll.
        natsSub_close(sub, false);

        natsConn_removeSubscription(nc, sub);

        natsSub_release(sub);
    }

    if (lock)
        natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

// Will queue an UNSUB protocol, making sure it is not flushed in place.
// The connection lock is held on entry.
natsStatus
natsConn_enqueueUnsubProto(natsConnection *nc, int64_t sid)
{
    natsStatus  s       = NATS_OK;
    char        *proto  = NULL;
    int         res     = 0;

    res = nats_asprintf(&proto, _UNSUB_NO_MAX_PROTO_, sid);
    if (res < 0)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    else
    {
        nc->dontSendInPlace = true;
        natsConn_bufferWrite(nc, (const char*) proto, (int) strlen(proto));
        nc->dontSendInPlace = false;
        NATS_FREE(proto);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

// Performs the low level unsubscribe to the server.
natsStatus
natsConn_unsubscribe(natsConnection *nc, natsSubscription *sub, int max, bool drainMode, int64_t timeout)
{
    natsStatus      s = NATS_OK;

    natsConn_Lock(nc);

    if (natsConn_isClosed(nc))
    {
        natsConn_Unlock(nc);
        return nats_setDefaultError(NATS_CONNECTION_CLOSED);
    }

    natsMutex_Lock(nc->subsMu);
    sub = natsHash_Get(nc->subs, sub->sid);
    natsMutex_Unlock(nc->subsMu);
    if ((sub == NULL) || !natsSubscription_IsValid(sub))
    {
        // Already unsubscribed
        natsConn_Unlock(nc);
        return nats_setDefaultError(NATS_INVALID_SUBSCRIPTION);
    }

    if (max > 0)
        natsSub_setMax(sub, max);
    else if (!drainMode)
        natsConn_removeSubscription(nc, sub);

    if (!drainMode && !natsConn_isReconnecting(nc))
    {
        SET_WRITE_DEADLINE(nc);
        // We will send these for all subs when we reconnect
        // so that we can suppress here.
        s = _sendUnsubProto(nc, sub->sid, max);
        if (s == NATS_OK)
            s = natsConn_flushOrKickFlusher(nc);

        // We should not return a failure if we get an issue
        // with the buffer write (except if it is no memory).
        // For IO errors (if we just got disconnected), the
        // reconnect logic will resend the unsub protocol.
        if ((s != NATS_OK) && (s != NATS_NO_MEMORY))
        {
            nats_clearLastError();
            s = NATS_OK;
        }
    }
    else if (drainMode)
    {
        if (natsConn_isDraining(nc))
             s = nats_setError(NATS_DRAINING, "%s", "Illegal to drain a subscription while its connection is draining");
        else
            s = natsSub_startDrain(sub, timeout);
    }

    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_setupServerPool(natsConnection *nc)
{
    natsStatus  s;

    s = natsSrvPool_Create(&(nc->srvPool), nc->opts);
    if (s == NATS_OK)
        nc->cur = natsSrvPool_GetSrv(nc->srvPool, 0);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConn_create(natsConnection **newConn, natsOptions *options)
{
    natsStatus      s   = NATS_OK;
    natsConnection  *nc = NULL;

    s = nats_Open(-1);
    if (s == NATS_OK)
    {
        nc = NATS_CALLOC(1, sizeof(natsConnection));
        if (nc == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
    }
    if (s != NATS_OK)
    {
        // options have been cloned or created for the connection,
        // which was supposed to take ownership, so destroy it now.
        natsOptions_Destroy(options);
        return NATS_UPDATE_ERR_STACK(s);
    }

    natsLib_Retain();

    nc->refs        = 1;
    nc->sockCtx.fd  = NATS_SOCK_INVALID;
    nc->opts        = options;

    nc->errStr[0] = '\0';

    s = natsMutex_Create(&(nc->mu));
    if (s == NATS_OK)
        s = natsMutex_Create(&(nc->subsMu));
    if (s == NATS_OK)
        s = _setupServerPool(nc);
    if (s == NATS_OK)
        s = natsHash_Create(&(nc->subs), 8);
    if (s == NATS_OK)
        s = natsSock_Init(&nc->sockCtx);
    if (s == NATS_OK)
    {
        s = natsBuf_Create(&(nc->scratch), DEFAULT_SCRATCH_SIZE);
        if (s == NATS_OK)
            s = natsBuf_Append(nc->scratch, _HPUB_P_, _HPUB_P_LEN_);
    }
    if (s == NATS_OK)
        s = natsCondition_Create(&(nc->flusherCond));
    if (s == NATS_OK)
        s = natsCondition_Create(&(nc->pongs.cond));
    if (s == NATS_OK)
        s = natsCondition_Create(&(nc->reconnectCond));

    if (s == NATS_OK)
        *newConn = nc;
    else
        natsConn_release(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_Connect(natsConnection **newConn, natsOptions *options)
{
    natsStatus      s       = NATS_OK;
    natsConnection  *nc     = NULL;
    natsOptions     *opts   = NULL;

    if (options == NULL)
    {
        s = natsConnection_ConnectTo(newConn, NATS_DEFAULT_URL);
        return NATS_UPDATE_ERR_STACK(s);
    }

    opts = natsOptions_clone(options);
    if (opts == NULL)
        s = NATS_NO_MEMORY;

    if (s == NATS_OK)
        s = natsConn_create(&nc, opts);
    if (s == NATS_OK)
        s = _connect(nc);

    if ((s == NATS_OK) || (s == NATS_NOT_YET_CONNECTED))
        *newConn = nc;
    else
        natsConn_release(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_processUrlString(natsOptions *opts, const char *urls)
{
    int         count        = 0;
    natsStatus  s            = NATS_OK;
    char        **serverUrls = NULL;
    char        *urlsCopy    = NULL;
    char        *commaPos    = NULL;
    char        *ptr         = NULL;

    if (urls != NULL)
    {
        ptr = (char*) urls;
        while ((ptr = strchr(ptr, ',')) != NULL)
        {
            ptr++;
            count++;
        }
    }
    if (count == 0)
        return natsOptions_SetURL(opts, urls);

    serverUrls = (char**) NATS_CALLOC(count + 1, sizeof(char*));
    if (serverUrls == NULL)
        s = NATS_NO_MEMORY;
    if (s == NATS_OK)
    {
        urlsCopy = NATS_STRDUP(urls);
        if (urlsCopy == NULL)
        {
            NATS_FREE(serverUrls);
            return NATS_NO_MEMORY;
        }
    }

    count = 0;
    ptr = urlsCopy;

    do
    {
        serverUrls[count++] = ptr;

        commaPos = strchr(ptr, ',');
        if (commaPos != NULL)
        {
            ptr = (char*)(commaPos + 1);
            *(commaPos) = '\0';
        }

    } while (commaPos != NULL);

    if (s == NATS_OK)
        s = natsOptions_SetServers(opts, (const char**) serverUrls, count);

    NATS_FREE(urlsCopy);
    NATS_FREE(serverUrls);

    return s;
}

natsStatus
natsConnection_ConnectTo(natsConnection **newConn, const char *url)
{
    natsStatus      s       = NATS_OK;
    natsConnection  *nc     = NULL;
    natsOptions     *opts   = NULL;

    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
    {
        s = _processUrlString(opts, url);
        // We still own the options at this point (until the call to natsConn_create())
        // so if there was an error, we need to destroy the options now.
        if (s != NATS_OK)
            natsOptions_Destroy(opts);
    }
    if (s == NATS_OK)
        s = natsConn_create(&nc, opts);
    if (s == NATS_OK)
        s = _connect(nc);

    if (s == NATS_OK)
        *newConn = nc;
    else
        natsConn_release(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

// Test if connection  has been closed.
bool
natsConnection_IsClosed(natsConnection *nc)
{
    bool closed;

    if (nc == NULL)
        return true;

    natsConn_Lock(nc);

    closed = natsConn_isClosed(nc);

    natsConn_Unlock(nc);

    return closed;
}

// Test if connection is reconnecting.
bool
natsConnection_IsReconnecting(natsConnection *nc)
{
    bool reconnecting;

    if (nc == NULL)
        return false;

    natsConn_Lock(nc);

    reconnecting = natsConn_isReconnecting(nc);

    natsConn_Unlock(nc);

    return reconnecting;
}

bool
natsConnection_IsDraining(natsConnection *nc)
{
    bool draining;

    if (nc == NULL)
        return false;

    natsConn_Lock(nc);

    draining = natsConn_isDraining(nc);

    natsConn_Unlock(nc);

    return draining;
}

// Returns the current state of the connection.
natsConnStatus
natsConnection_Status(natsConnection *nc)
{
    natsConnStatus cs;

    if (nc == NULL)
        return NATS_CONN_STATUS_CLOSED;

    natsConn_Lock(nc);

    cs = nc->status;

    natsConn_Unlock(nc);

    return cs;
}

static void
_destroyPong(natsConnection *nc, natsPong *pong)
{
    // If this pong is the cached one, do not free
    if (pong == &(nc->pongs.cached))
        memset(pong, 0, sizeof(natsPong));
    else
        NATS_FREE(pong);
}

// Low-level flush. On entry, connection has been verified to no be closed
// and lock is held.
static natsStatus
_flushTimeout(natsConnection *nc, int64_t timeout)
{
    natsStatus  s       = NATS_OK;
    int64_t     target  = 0;
    natsPong    *pong   = NULL;

    // Use the cached PONG instead of creating one if the list
    // is empty
    if (nc->pongs.head == NULL)
        pong = &(nc->pongs.cached);
    else
        pong = (natsPong*) NATS_CALLOC(1, sizeof(natsPong));

    if (pong == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);

    if (s == NATS_OK)
    {
        // Send the ping (and add the pong to the list)
        _sendPing(nc, pong);

        target = nats_setTargetTime(timeout);

        // When the corresponding PONG is received, the PONG processing code
        // will set pong->id to 0 and do a broadcast. This will allow this
        // code to break out of the 'while' loop.
        while ((s != NATS_TIMEOUT)
               && !natsConn_isClosed(nc)
               && (pong->id > 0))
        {
            s = natsCondition_AbsoluteTimedWait(nc->pongs.cond, nc->mu, target);
        }

        if ((s == NATS_OK) && (nc->status == NATS_CONN_STATUS_CLOSED))
        {
            // The connection has been closed while we were waiting
            s = nats_setDefaultError(NATS_CONNECTION_CLOSED);
        }
        else if ((s == NATS_OK) && (pong->id == -1))
        {
            // The connection was disconnected and the library is in the
            // process of trying to reconnect
            s = nats_setDefaultError(NATS_CONNECTION_DISCONNECTED);
        }

        if (s != NATS_OK)
        {
            // If we are here, it is possible that we timed-out, or some other
            // error occurred. Make sure the request is no longer in the list.
            _removePongFromList(nc, pong);

            // Set the error. If we don't do that, and flush is called in a loop,
            // the stack would be growing with Flush/FlushTimeout.
            s = nats_setDefaultError(s);
        }

        // We are done with the pong
        _destroyPong(nc, pong);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_FlushTimeout(natsConnection *nc, int64_t timeout)
{
    natsStatus  s       = NATS_OK;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (timeout <= 0)
        return nats_setDefaultError(NATS_INVALID_TIMEOUT);

    natsConn_lockAndRetain(nc);

    if (natsConn_isClosed(nc))
        s = nats_setDefaultError(NATS_CONNECTION_CLOSED);

    if (s == NATS_OK)
        s = _flushTimeout(nc, timeout);

    natsConn_unlockAndRelease(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_Flush(natsConnection *nc)
{
    natsStatus s = natsConnection_FlushTimeout(nc, DEFAULT_FLUSH_TIMEOUT);
    return NATS_UPDATE_ERR_STACK(s);
}

static void
_pushDrainErr(natsConnection *nc, natsStatus s, const char *errTxt)
{
    natsConn_Lock(nc);
    nc->err = s;
    snprintf(nc->errStr, sizeof(nc->errStr), "Drain error: %s: %d (%s)", errTxt, s, natsStatus_GetText(s));
    if (nc->opts->asyncErrCb != NULL)
        natsAsyncCb_PostErrHandler(nc, NULL, s);
    natsConn_Unlock(nc);
}

// Callback invoked for each of the registered subscription.
typedef natsStatus (*subIterFunc)(natsStatus callerSts, natsConnection *nc, natsSubscription *sub);

// Iterates through all registered subscriptions and execute the callback `f`.
// If the callback returns an error, the iteration continues and the first
// non NATS_OK error is returned.
//
// Connection lock is held on entry.
static natsStatus
_iterateSubsAndInvokeFunc(natsStatus callerSts, natsConnection *nc, subIterFunc f)
{
    natsStatus       s    = NATS_OK;
    natsStatus       ls   = NATS_OK;
    natsSubscription *sub = NULL;
    void             *p   = NULL;
    natsHashIter     iter;

    natsMutex_Lock(nc->subsMu);
    if (natsHash_Count(nc->subs) == 0)
    {
        natsMutex_Unlock(nc->subsMu);
        return NATS_OK;
    }
    natsHashIter_Init(&iter, nc->subs);
    while (natsHashIter_Next(&iter, NULL, &p))
    {
        sub = (natsSubscription*) p;
        ls = (f)(callerSts, nc, sub);
        s = (s == NATS_OK ? ls : s);
    }
    natsHashIter_Done(&iter);
    natsMutex_Unlock(nc->subsMu);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_enqueUnsubProto(natsStatus callerSts, natsConnection *nc, natsSubscription *sub)
{
    natsStatus s;

    natsSub_Lock(sub);
    s = natsConn_enqueueUnsubProto(nc, sub->sid);
    natsSub_Unlock(sub);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_initSubDrain(natsStatus callerSts, natsConnection *nc, natsSubscription *sub)
{
    natsSub_initDrain(sub);
    return NATS_OK;
}

static natsStatus
_startSubDrain(natsStatus callerSts, natsConnection *nc, natsSubscription *sub)
{
    if (callerSts != NATS_OK)
        natsSub_setDrainSkip(sub, callerSts);

    natsSub_drain(sub);
    return NATS_OK;
}

static natsStatus
_setSubDrainStatus(natsStatus callerSts, natsConnection *nc, natsSubscription *sub)
{
    if (callerSts != NATS_OK)
        natsSub_updateDrainStatus(sub, callerSts);
    return NATS_OK;
}

static void
_flushAndDrain(void *closure)
{
    natsConnection  *nc      = (natsConnection*) closure;
    natsThread      *t       = NULL;
    int64_t         timeout  = 0;
    int64_t         deadline = 0;
    bool            doSubs   = false;
    bool            subsDone = false;
    bool            timedOut = false;
    bool            closed   = false;
    natsStatus      s        = NATS_OK;
    int64_t         start;

    natsConn_Lock(nc);
    t       = nc->drainThread;
    timeout = nc->drainTimeout;
    closed  = natsConn_isClosed(nc);
    natsMutex_Lock(nc->subsMu);
    doSubs = (natsHash_Count(nc->subs) > 0 ? true : false);
    natsMutex_Unlock(nc->subsMu);
    natsConn_Unlock(nc);

    if (timeout < 0)
        timeout = 0;
    else
        deadline = nats_setTargetTime(timeout);

    start = nats_Now();
    if (!closed && doSubs)
    {
        if (timeout == 0)
            s = natsConnection_Flush(nc);
        else
            s = natsConnection_FlushTimeout(nc, timeout);

        if (s != NATS_OK)
            _pushDrainErr(nc, s, "unable to flush all subscriptions UNSUB protocols");

        // Start the draining of all registered subscriptions.
        // Update the drain status with possibly failed flush.
        natsConn_Lock(nc);
        _iterateSubsAndInvokeFunc(s, nc, _startSubDrain);
        natsConn_Unlock(nc);

        // Reset status now.
        s = NATS_OK;

        // Now wait for the number of subscriptions to go down to 0, or deadline is reached.
        while ((timeout == 0) || (deadline - nats_Now() > 0))
        {
            natsConn_Lock(nc);
            if (!(closed = natsConn_isClosed(nc)))
            {
                natsMutex_Lock(nc->subsMu);
                subsDone = (natsHash_Count(nc->subs) == 0 ? true : false);
                natsMutex_Unlock(nc->subsMu);
            }
            natsConn_Unlock(nc);
            if (closed || subsDone)
                break;
            nats_Sleep(100);
        }
        // If the connection has been closed, then the subscriptions will have
        // be removed from the map. So only try to update the subs' drain status
        // if we are here due to a NATS_TIMEOUT, not a NATS_CONNECTION_CLOSED.
        if (!closed && !subsDone)
        {
            _iterateSubsAndInvokeFunc(NATS_TIMEOUT, nc, _setSubDrainStatus);
            _pushDrainErr(nc, NATS_TIMEOUT, "timeout waiting for subscriptions to drain");
            timedOut = true;
        }
    }

    // Now switch to draining PUBS, unless already closed.
    natsConn_Lock(nc);
    if (!(closed = natsConn_isClosed(nc)))
        nc->status = NATS_CONN_STATUS_DRAINING_PUBS;
    natsConn_Unlock(nc);

    // Attempt to flush, unless we have already timed out, or connection is closed.
    if (!closed && !timedOut)
    {
        // Reset status
        s = NATS_OK;

        // We drain the publish calls
        if (timeout == 0)
            s = natsConnection_Flush(nc);
        else
        {
            // If there is a timeout, see how long do we have left.
            int64_t elapsed = nats_Now() - start;

            if (elapsed < timeout)
                s = natsConnection_FlushTimeout(nc, timeout-elapsed);
        }
        if (s != NATS_OK)
            _pushDrainErr(nc, s, "unable to flush publish calls");
    }

    // Finally, close the connection.
    if (!closed)
        natsConnection_Close(nc);

    natsThread_Detach(t);
    natsThread_Destroy(t);
    natsConn_release(nc);
}

static natsStatus
_drain(natsConnection *nc, int64_t timeout)
{
    natsStatus s = NATS_OK;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);
    if (natsConn_isClosed(nc))
        s = nats_setDefaultError(NATS_CONNECTION_CLOSED);
    else if (nc->stanOwned)
        s = nats_setError(NATS_ILLEGAL_STATE, "%s", "Illegal to call Drain for connection owned by a streaming connection");
    else if (_isConnecting(nc) || natsConn_isReconnecting(nc))
        s = nats_setError(NATS_ILLEGAL_STATE, "%s", "Illegal to call Drain while the connection is reconnecting");
    else if (!natsConn_isDraining(nc))
    {
        // Enqueue UNSUB protocol for all current subscriptions.
        s = _iterateSubsAndInvokeFunc(NATS_OK, nc, _enqueUnsubProto);
        if (s == NATS_OK)
        {
            nc->drainTimeout = timeout;
            s = natsThread_Create(&nc->drainThread, _flushAndDrain, (void*) nc);
            if (s == NATS_OK)
            {
                // Prevent new subscriptions to be added.
                nc->status = NATS_CONN_STATUS_DRAINING_SUBS;

                // Switch drain state to "started" for all subs. This does not fail.
                _iterateSubsAndInvokeFunc(NATS_OK, nc, _initSubDrain);

                _retain(nc);
            }
        }
    }
    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_Drain(natsConnection *nc)
{
    natsStatus s = _drain(nc, DEFAULT_DRAIN_TIMEOUT);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_DrainTimeout(natsConnection *nc, int64_t timeout)
{
    natsStatus s = _drain(nc, timeout);
    return NATS_UPDATE_ERR_STACK(s);
}

int
natsConnection_Buffered(natsConnection *nc)
{
    int buffered = -1;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    if ((nc->status != NATS_CONN_STATUS_CLOSED) && (nc->bw != NULL))
        buffered = natsBuf_Len(nc->bw);

    natsConn_Unlock(nc);

    return buffered;
}

int64_t
natsConnection_GetMaxPayload(natsConnection *nc)
{
    int64_t mp = 0;

    if (nc == NULL)
        return 0;

    natsConn_Lock(nc);

    mp = nc->info.maxPayload;

    natsConn_Unlock(nc);

    return mp;
}

natsStatus
natsConnection_GetStats(natsConnection *nc, natsStatistics *stats)
{
    natsStatus  s = NATS_OK;

    if ((nc == NULL) || (stats == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    // Stats are updated either under connection's mu or subsMu mutexes.
    // Lock both to safely get them.
    natsConn_Lock(nc);
    natsMutex_Lock(nc->subsMu);

    memcpy(stats, &(nc->stats), sizeof(natsStatistics));

    natsMutex_Unlock(nc->subsMu);
    natsConn_Unlock(nc);

    return s;
}

natsStatus
natsConnection_GetConnectedUrl(natsConnection *nc, char *buffer, size_t bufferSize)
{
    natsStatus  s = NATS_OK;

    if ((nc == NULL) || (buffer == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    buffer[0] = '\0';

    if ((nc->status == NATS_CONN_STATUS_CONNECTED) && (nc->cur->url->fullUrl != NULL))
    {
        if (strlen(nc->cur->url->fullUrl) >= bufferSize)
            s = nats_setDefaultError(NATS_INSUFFICIENT_BUFFER);

        if (s == NATS_OK)
            snprintf(buffer, bufferSize, "%s", nc->cur->url->fullUrl);
    }

    natsConn_Unlock(nc);

    return s;
}

natsStatus
natsConnection_GetConnectedServerId(natsConnection *nc, char *buffer, size_t bufferSize)
{
    natsStatus  s = NATS_OK;

    if ((nc == NULL) || (buffer == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    buffer[0] = '\0';

    if ((nc->status == NATS_CONN_STATUS_CONNECTED) && (nc->info.id != NULL))
    {
        if (strlen(nc->info.id) >= bufferSize)
            s = nats_setDefaultError(NATS_INSUFFICIENT_BUFFER);

        if (s == NATS_OK)
            snprintf(buffer, bufferSize, "%s", nc->info.id);
    }

    natsConn_Unlock(nc);

    return s;
}

natsStatus
natsConnection_GetServers(natsConnection *nc, char ***servers, int *count)
{
    natsStatus  s       = NATS_OK;

    if ((nc == NULL) || (servers == NULL) || (count == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    s = natsSrvPool_GetServers(nc->srvPool, false, servers, count);

    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_GetDiscoveredServers(natsConnection *nc, char ***servers, int *count)
{
    natsStatus  s       = NATS_OK;

    if ((nc == NULL) || (servers == NULL) || (count == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    s = natsSrvPool_GetServers(nc->srvPool, true, servers, count);

    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_GetLastError(natsConnection *nc, const char **lastError)
{
    natsStatus  s;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);

    s = nc->err;
    if (s == NATS_OK)
        nc->errStr[0] = '\0';
    else if (nc->errStr[0] == '\0')
        snprintf(nc->errStr, sizeof(nc->errStr), "%s", natsStatus_GetText(s));

    *lastError = nc->errStr;

    natsConn_Unlock(nc);

    return s;
}

void
natsConn_close(natsConnection *nc)
{
    if (nc == NULL)
        return;

    nats_doNotUpdateErrStack(true);

    _close(nc, NATS_CONN_STATUS_CLOSED, true, true);

    nats_doNotUpdateErrStack(false);
}

void
natsConnection_Close(natsConnection *nc)
{
    bool stanOwned;

    if (nc == NULL)
        return;

    natsConn_Lock(nc);
    stanOwned = nc->stanOwned;
    natsConn_Unlock(nc);

    if (!stanOwned)
        natsConn_close(nc);
}

void
natsConn_destroy(natsConnection *nc, bool fromPublicDestroy)
{
    if (nc == NULL)
        return;

    nats_doNotUpdateErrStack(true);

    _close(nc, NATS_CONN_STATUS_CLOSED, fromPublicDestroy, true);

    nats_doNotUpdateErrStack(false);

    natsConn_release(nc);
}

void
natsConnection_Destroy(natsConnection *nc)
{
    bool stanOwned;

    if (nc == NULL)
        return;

    natsConn_Lock(nc);
    stanOwned = nc->stanOwned;
    natsConn_Unlock(nc);

    if (!stanOwned)
        natsConn_destroy(nc, true);
}

void
natsConnection_ProcessReadEvent(natsConnection *nc)
{
    natsStatus      s = NATS_OK;
    int             n = 0;
    char            *buffer;
    int             size;

    natsConn_Lock(nc);

    if (!(nc->el.attached) || (nc->sockCtx.fd == NATS_SOCK_INVALID))
    {
        natsConn_Unlock(nc);
        return;
    }

    if (nc->ps == NULL)
    {
        s = natsParser_Create(&(nc->ps));
        if (s != NATS_OK)
        {
            (void) NATS_UPDATE_ERR_STACK(s);
            natsConn_Unlock(nc);
            return;
        }
    }

    _retain(nc);

    buffer = nc->el.buffer;
    size   = nc->opts->ioBufSize;

    natsConn_Unlock(nc);

    // Do not try to read again here on success. If more than one connection
    // is attached to the same loop, and there is a constant stream of data
    // coming for the first connection, this would starve the second connection.
    // So return and we will be called back later by the event loop.
    s = natsSock_Read(&(nc->sockCtx), buffer, size, &n);
    if (s == NATS_OK)
        s = natsParser_Parse(nc, buffer, n);

    if (s != NATS_OK)
        _processOpError(nc, s, false);

    natsConn_release(nc);
}

void
natsConnection_ProcessWriteEvent(natsConnection *nc)
{
    natsStatus  s = NATS_OK;
    int         n = 0;
    char        *buf;
    int         len;

    natsConn_Lock(nc);

    if (!(nc->el.attached) || (nc->sockCtx.fd == NATS_SOCK_INVALID))
    {
        natsConn_Unlock(nc);
        return;
    }

    buf = natsBuf_Data(nc->bw);
    len = natsBuf_Len(nc->bw);

    s = natsSock_Write(&(nc->sockCtx), buf, len, &n);
    if (s == NATS_OK)
    {
        if (n == len)
        {
            // We sent all the data, reset buffer and remove WRITE event.
            natsBuf_Reset(nc->bw);

            s = nc->opts->evCbs.write(nc->el.data, NATS_EVENT_ACTION_REMOVE);
            if (s == NATS_OK)
                nc->el.writeAdded = false;
            else
                nats_setError(s, "Error processing write request: %d - %s",
                              s, natsStatus_GetText(s));
        }
        else
        {
            // We sent some part of the buffer. Move the remaining at the beginning.
            natsBuf_Consume(nc->bw, n);
        }
    }

    natsConn_Unlock(nc);

    if (s != NATS_OK)
        _processOpError(nc, s, false);

    (void) NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_GetClientID(natsConnection *nc, uint64_t *cid)
{
    natsStatus s = NATS_OK;

    if ((nc == NULL) || (cid == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);
    if (natsConn_isClosed(nc))
    {
        s = NATS_CONNECTION_CLOSED;
    }
    else
    {
        *cid = nc->info.CID;
        if (*cid == 0)
            s = NATS_NO_SERVER_SUPPORT;
    }
    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_getJwtOrSeed(char **val, const char *fn, bool seed, int item)
{
    natsStatus  s       = NATS_OK;
    natsBuffer  *buf    = NULL;

    s = nats_ReadFile(&buf, 1024, fn);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    s = nats_GetJWTOrSeed(val, (const char*) natsBuf_Data(buf), item);
    if (s == NATS_NOT_FOUND)
    {
        s = NATS_OK;
        if (!seed)
        {
            *val = NATS_STRDUP(natsBuf_Data(buf));
            if (*val == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        else
        {
            // Look for "SU..."
            char *nt  = NULL;
            char *pch = nats_strtok(natsBuf_Data(buf), "\n", &nt);

            while (pch != NULL)
            {
                char *ptr = pch;

                while (((*ptr == ' ') || (*ptr == '\t')) && (*ptr != '\0'))
                    ptr++;

                if ((*ptr != '\0') && (*ptr == 'S') && (*(ptr+1) == 'U'))
                {
                    *val = NATS_STRDUP(ptr);
                    if (*val == NULL)
                        s = nats_setDefaultError(NATS_NO_MEMORY);
                    break;
                }

                pch = nats_strtok(NULL, "\n", &nt);
            }
            if ((s == NATS_OK) && (*val == NULL))
                s = nats_setError(NATS_ERR, "no nkey user seed found in '%s'", fn);
        }
    }
    if (buf != NULL)
    {
        memset(natsBuf_Data(buf), 0, natsBuf_Capacity(buf));
        natsBuf_Destroy(buf);
        buf = NULL;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConn_userFromFile(char **userJWT, char **customErrTxt, void *closure)
{
    natsStatus  s   = NATS_OK;
    userCreds   *uc = (userCreds*) closure;

    s = _getJwtOrSeed(userJWT, uc->userOrChainedFile, false, 0);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_sign(userCreds *uc, const unsigned char *input, int inputLen, unsigned char *sig)
{
    natsStatus      s            = NATS_OK;
    char            *encodedSeed = NULL;

    if (uc->seedFile != NULL)
        s = _getJwtOrSeed(&encodedSeed, uc->seedFile, true, 0);
    else
        s = _getJwtOrSeed(&encodedSeed, uc->userOrChainedFile, true, 1);

    if (s == NATS_OK)
        s = natsKeys_Sign((const char*) encodedSeed, input, inputLen, sig);

    if (encodedSeed != NULL)
    {
        natsCrypto_Clear((void*) encodedSeed, (int) strlen(encodedSeed));
        NATS_FREE(encodedSeed);
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConn_signatureHandler(char **customErrTxt, unsigned char **psig, int *sigLen, const char *nonce, void *closure)
{
    natsStatus      s    = NATS_OK;
    userCreds       *uc  = (userCreds*) closure;
    char            *sig = NULL;

    *psig = NULL;
    if (sigLen != NULL)
        *sigLen = 0;

    sig = NATS_MALLOC(NATS_CRYPTO_SIGN_BYTES);
    if (sig == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = _sign(uc, (const unsigned char*) nonce, 0, (unsigned char*) sig);
    if (s == NATS_OK)
    {
        *psig = (unsigned char*) sig;
        if (sigLen != NULL)
            *sigLen = NATS_CRYPTO_SIGN_BYTES;
    }
    else
    {
        NATS_FREE(sig);
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_Sign(natsConnection *nc, const unsigned char *payload, int payloadLen, unsigned char sig[64])
{
    natsStatus  s   = NATS_OK;
    userCreds   *uc = NULL;

    if ((nc == NULL) || (payloadLen < 0) || (sig == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);
    // We can't sign if that is not set...
    uc = nc->opts->userCreds;
    if (uc == NULL)
        s = nats_setError(NATS_ERR, "%s", "unable to sign since no user credentials have been set");
    else
        s = _sign(uc, payload, payloadLen, sig);
    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsConnection_GetClientIP(natsConnection *nc, char **ip)
{
    natsStatus s = NATS_OK;

    if ((nc == NULL) || (ip == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    *ip = NULL;

    natsConn_Lock(nc);
    if (natsConn_isClosed(nc))
        s = nats_setDefaultError(NATS_CONNECTION_CLOSED);
    else if (nc->info.clientIP == NULL)
        s = nats_setDefaultError(NATS_NO_SERVER_SUPPORT);
    else if ((*ip = NATS_STRDUP(nc->info.clientIP)) == NULL)
        s = nats_setDefaultError(NATS_NO_MEMORY);
    natsConn_Unlock(nc);

    return s;
}

natsStatus
natsConnection_GetRTT(natsConnection *nc, int64_t *rtt)
{
    natsStatus  s = NATS_OK;
    int64_t     start;

    if ((nc == NULL) || (rtt == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    *rtt = 0;

    natsConn_Lock(nc);
    if (natsConn_isClosed(nc))
        s = nats_setDefaultError(NATS_CONNECTION_CLOSED);
    else if (natsConn_isReconnecting(nc))
        s = nats_setDefaultError(NATS_CONNECTION_DISCONNECTED);
    else
    {
        start = nats_NowInNanoSeconds();
        s = _flushTimeout(nc, DEFAULT_FLUSH_TIMEOUT);
        if (s == NATS_OK)
            *rtt = nats_NowInNanoSeconds()-start;
    }
    natsConn_Unlock(nc);

    return s;
}

natsStatus
natsConnection_HasHeaderSupport(natsConnection *nc)
{
    bool headers = false;

    if (nc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsConn_Lock(nc);
    headers = nc->info.headers;
    natsConn_Unlock(nc);

    if (headers)
        return NATS_OK;

    return NATS_NO_SERVER_SUPPORT;
}

natsStatus
natsConnection_GetLocalIPAndPort(natsConnection *nc, char **ip, int *port)
{
    natsStatus s = NATS_OK;

    if ((nc == NULL) || (ip == NULL) || (port == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    *ip = NULL;
    *port = 0;

    natsConn_Lock(nc);
    if (natsConn_isClosed(nc))
        s = nats_setDefaultError(NATS_CONNECTION_CLOSED);
    else if (!nc->sockCtx.fdActive)
        s = nats_setDefaultError(NATS_CONNECTION_DISCONNECTED);
    else
        s = natsSock_GetLocalIPAndPort(&(nc->sockCtx), ip, port);
    natsConn_Unlock(nc);

    return NATS_UPDATE_ERR_STACK(s);
}
