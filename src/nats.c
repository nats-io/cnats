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
#if defined(NATS_HAS_STREAMING)
#include "stan/stanp.h"
#endif

#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <stdarg.h>

#include "mem.h"
#include "timer.h"
#include "util.h"
#include "asynccb.h"
#include "conn.h"
#include "sub.h"
#include "nkeys.h"
#include "crypto.h"

static const char *inboxPrefix = "_INBOX.";

#define WAIT_LIB_INITIALIZED \
        natsMutex_Lock(gLib.lock); \
        while (!(gLib.initialized) && !(gLib.initAborted)) \
            natsCondition_Wait(gLib.cond, gLib.lock); \
        natsMutex_Unlock(gLib.lock)

typedef struct natsTLError
{
    natsStatus  sts;
    char        text[256];
    const char  *func[MAX_FRAMES];
    int         framesCount;
    int         skipUpdate;

} natsTLError;

typedef struct __natsLibTimers
{
    natsMutex       *lock;
    natsCondition   *cond;
    natsThread      *thread;
    natsTimer       *timers;
    int             count;
    bool            changed;
    bool            shutdown;

} natsLibTimers;

typedef struct __natsLibAsyncCbs
{
    natsMutex       *lock;
    natsCondition   *cond;
    natsThread      *thread;
    natsAsyncCbInfo *head;
    natsAsyncCbInfo *tail;
    bool            shutdown;

} natsLibAsyncCbs;

typedef struct __natsGCList
{
    natsMutex       *lock;
    natsCondition   *cond;
    natsThread      *thread;
    natsGCItem      *head;
    bool            shutdown;
    bool            inWait;

} natsGCList;

typedef struct __natsLibDlvWorkers
{
    natsMutex           *lock;
    int                 idx;
    int                 size;
    int                 maxSize;
    natsMsgDlvWorker    **workers;

} natsLibDlvWorkers;

typedef struct __natsLib
{
    // Leave these fields before 'refs'
    natsMutex       *lock;
    volatile bool   wasOpenedOnce;
    bool            sslInitialized;
    natsThreadLocal errTLKey;
    natsThreadLocal sslTLKey;
    natsThreadLocal natsThreadKey;
    bool            initialized;
    bool            closed;
    natsCondition   *closeCompleteCond;
    bool            *closeCompleteBool;
    bool            closeCompleteSignal;
    bool            finalCleanup;
    // Do not move 'refs' without checking _freeLib()
    int             refs;

    bool            initializing;
    bool            initAborted;
    bool            libHandlingMsgDeliveryByDefault;
    int64_t         libDefaultWriteDeadline;

    natsLibTimers       timers;
    natsLibAsyncCbs     asyncCbs;
    natsLibDlvWorkers   dlvWorkers;

    natsCondition   *cond;

    natsGCList      gc;

} natsLib;

int64_t gLockSpinCount = 2000;
int     jsonMaxNumSize = 0;

static natsInitOnceType gInitOnce = NATS_ONCE_STATIC_INIT;
static natsLib          gLib;

static void
_destroyErrTL(void *localStorage)
{
    natsTLError *err = (natsTLError*) localStorage;

    NATS_FREE(err);
}

static void
_cleanupThreadSSL(void *localStorage)
{
#if defined(NATS_HAS_TLS) && !defined(NATS_USE_OPENSSL_1_1)
    ERR_remove_thread_state(0);
#endif
}

static void
_finalCleanup(void)
{
    if (gLib.sslInitialized)
    {
#if defined(NATS_HAS_TLS)
        ERR_free_strings();
        EVP_cleanup();
        CRYPTO_cleanup_all_ex_data();
#if !defined(NATS_USE_OPENSSL_1_1)
        ERR_remove_thread_state(0);
#endif
        sk_SSL_COMP_free(SSL_COMP_get_compression_methods());
#endif
        natsThreadLocal_DestroyKey(gLib.sslTLKey);
    }

    natsThreadLocal_DestroyKey(gLib.errTLKey);
    natsThreadLocal_DestroyKey(gLib.natsThreadKey);
    natsMutex_Destroy(gLib.lock);
    gLib.lock = NULL;
}

void
nats_setNATSThreadKey(void)
{
    natsThreadLocal_Set(gLib.natsThreadKey, (const void*)1);
}

void
nats_ReleaseThreadMemory(void)
{
    void *tl = NULL;

    if (!(gLib.wasOpenedOnce))
        return;

    tl = natsThreadLocal_Get(gLib.errTLKey);
    if (tl != NULL)
    {
        _destroyErrTL(tl);
        natsThreadLocal_SetEx(gLib.errTLKey, NULL, false);
    }

    tl = NULL;

    natsMutex_Lock(gLib.lock);
    if (gLib.sslInitialized)
    {
        tl = natsThreadLocal_Get(gLib.sslTLKey);
        if (tl != NULL)
        {
            _cleanupThreadSSL(tl);
            natsThreadLocal_SetEx(gLib.sslTLKey, NULL, false);
        }
    }
    natsMutex_Unlock(gLib.lock);
}

#if defined(_WIN32) && _WIN32
#ifndef NATS_STATIC
BOOL WINAPI DllMain(HINSTANCE hinstDLL, // DLL module handle
     DWORD fdwReason,                   // reason called
     LPVOID lpvReserved)                // reserved
{
    switch (fdwReason)
    {
        // For applications linking dynamically NATS library,
        // release thread-local memory for user-created threads.
        // For portable applications, the user should manually call
        // nats_ReleaseThreadMemory() before the thread returns so
        // that no memory is leaked regardless if they link statically
        // or dynamically. It is safe to call nats_ReleaseThreadMemory()
        // twice for the same threads.
        case DLL_THREAD_DETACH:
        {
            nats_ReleaseThreadMemory();
            break;
        }
        default:
            break;
    }

    return TRUE;
    UNREFERENCED_PARAMETER(hinstDLL);
    UNREFERENCED_PARAMETER(lpvReserved);
}
#endif
#endif

static void
natsLib_Destructor(void)
{
    int refs = 0;

    if (!(gLib.wasOpenedOnce))
        return;

    // Destroy thread locals for the current thread.
    nats_ReleaseThreadMemory();

    // Do the final cleanup if possible
    natsMutex_Lock(gLib.lock);
    refs = gLib.refs;
    if (refs > 0)
    {
        // If some thread is still around when the process exits and has a
        // reference to the library, then don't do the final cleanup now.
        // If the process has not fully exited when the lib's last reference
        // is decremented, the final cleanup will be executed from that thread.
        gLib.finalCleanup = true;
    }
    natsMutex_Unlock(gLib.lock);

    if (refs != 0)
        return;

    _finalCleanup();
}

static void
_freeTimers(void)
{
    natsLibTimers *timers = &(gLib.timers);

    natsThread_Destroy(timers->thread);
    natsCondition_Destroy(timers->cond);
    natsMutex_Destroy(timers->lock);
}

static void
_freeAsyncCbs(void)
{
    natsLibAsyncCbs *cbs = &(gLib.asyncCbs);

    natsThread_Destroy(cbs->thread);
    natsCondition_Destroy(cbs->cond);
    natsMutex_Destroy(cbs->lock);
}

static void
_freeGC(void)
{
    natsGCList *gc = &(gLib.gc);

    natsThread_Destroy(gc->thread);
    natsCondition_Destroy(gc->cond);
    natsMutex_Destroy(gc->lock);
}

static void
_freeDlvWorker(natsMsgDlvWorker *worker)
{
    natsThread_Destroy(worker->thread);
    natsCondition_Destroy(worker->cond);
    natsMutex_Destroy(worker->lock);
    NATS_FREE(worker);
}

static void
_freeDlvWorkers(void)
{
    int i;
    natsLibDlvWorkers *workers = &(gLib.dlvWorkers);

    for (i=0; i<workers->size; i++)
        _freeDlvWorker(workers->workers[i]);

    NATS_FREE(workers->workers);
    natsMutex_Destroy(workers->lock);
    workers->idx     = 0;
    workers->size    = 0;
    workers->workers = NULL;
}

static void
_freeLib(void)
{
    const unsigned int offset = (unsigned int) offsetof(natsLib, refs);
    bool               callFinalCleanup = false;

    _freeTimers();
    _freeAsyncCbs();
    _freeGC();
    _freeDlvWorkers();
    natsNUID_free();

    natsCondition_Destroy(gLib.cond);

    memset((void*) (offset + (char*)&gLib), 0, sizeof(natsLib) - offset);

    natsMutex_Lock(gLib.lock);
    callFinalCleanup = gLib.finalCleanup;
    if (gLib.closeCompleteCond != NULL)
    {
        if (gLib.closeCompleteSignal)
        {
            *gLib.closeCompleteBool = true;
            natsCondition_Signal(gLib.closeCompleteCond);
        }
        gLib.closeCompleteCond   = NULL;
        gLib.closeCompleteBool   = NULL;
        gLib.closeCompleteSignal = false;
    }
    gLib.closed      = false;
    gLib.initialized = false;
    gLib.finalCleanup= false;
    natsMutex_Unlock(gLib.lock);

    if (callFinalCleanup)
        _finalCleanup();
}

void
natsLib_Retain(void)
{
    natsMutex_Lock(gLib.lock);

    gLib.refs++;

    natsMutex_Unlock(gLib.lock);
}

void
natsLib_Release(void)
{
    int refs = 0;

    natsMutex_Lock(gLib.lock);

    refs = --(gLib.refs);

    natsMutex_Unlock(gLib.lock);

    if (refs == 0)
        _freeLib();
}

static void
_doInitOnce(void)
{
    natsStatus s;

    memset(&gLib, 0, sizeof(natsLib));

    s = natsMutex_Create(&(gLib.lock));
    if (s == NATS_OK)
        s = natsThreadLocal_CreateKey(&(gLib.errTLKey), _destroyErrTL);
    if (s == NATS_OK)
        s = natsThreadLocal_CreateKey(&(gLib.natsThreadKey), NULL);
    if (s != NATS_OK)
    {
        fprintf(stderr, "FATAL ERROR: Unable to initialize library!\n");
        fflush(stderr);
        abort();
    }

    if (jsonMaxNumSize == 0)
    {
        int szInt    = (int) sizeof(int64_t);
        int szUInt   = (int) sizeof(uint64_t);
        int szDbl    = (int) sizeof(long double);

        jsonMaxNumSize = (szDbl > szInt ? szDbl : szInt);
        jsonMaxNumSize = (jsonMaxNumSize > szUInt ? jsonMaxNumSize : szUInt);
    }

    natsSys_Init();

    // Setup a hook for when the process exits.
    atexit(natsLib_Destructor);
}

static void
_insertTimer(natsTimer *t)
{
    natsTimer *cur  = gLib.timers.timers;
    natsTimer *prev = NULL;

    while ((cur != NULL) && (cur->absoluteTime <= t->absoluteTime))
    {
        prev = cur;
        cur  = cur->next;
    }

    if (cur != NULL)
    {
        t->prev = prev;
        t->next = cur;
        cur->prev = t;

        if (prev != NULL)
            prev->next = t;
    }
    else if (prev != NULL)
    {
        prev->next = t;
        t->prev = prev;
        t->next = NULL;
    }

    if (prev == NULL)
        gLib.timers.timers = t;
}

// Locks must be held before entering this function
static void
_removeTimer(natsLibTimers *timers, natsTimer *t)
{
    // Switch flag
    t->stopped = true;

    // It the timer was in the callback, it has already been removed from the
    // list, so skip that.
    if (!(t->inCallback))
    {
        if (t->prev != NULL)
            t->prev->next = t->next;
        if (t->next != NULL)
            t->next->prev = t->prev;

        if (t == gLib.timers.timers)
            gLib.timers.timers = t->next;

        t->prev = NULL;
        t->next = NULL;
    }

    // Decrease the global count of timers
    timers->count--;
}

int64_t
nats_setTargetTime(int64_t timeout)
{
    int64_t target = nats_Now() + timeout;
    if (target < 0)
        target = 0x7FFFFFFFFFFFFFFF;
    return target;
}

void
nats_resetTimer(natsTimer *t, int64_t newInterval)
{
    natsLibTimers *timers = &(gLib.timers);

    natsMutex_Lock(timers->lock);
    natsMutex_Lock(t->mu);

    // If timer is active, we need first to remove it. This call does the
    // right thing if the timer is in the callback.
    if (!(t->stopped))
        _removeTimer(timers, t);

    // Bump the timer's global count (it as decreased in the _removeTimers call
    timers->count++;

    // Switch stopped flag
    t->stopped = false;

    // Set the new interval (may be same than it was before, but that's ok)
    t->interval = newInterval;

    // If the timer is in the callback, the insertion and setting of the
    // absolute time will be done by the timer thread when returning from
    // the timer's callback.
    if (!(t->inCallback))
    {
        t->absoluteTime = nats_setTargetTime(t->interval);
        _insertTimer(t);
    }

    natsMutex_Unlock(t->mu);

    if (!(timers->changed))
        natsCondition_Signal(timers->cond);

    timers->changed = true;

    natsMutex_Unlock(timers->lock);
}

void
nats_stopTimer(natsTimer *t)
{
    natsLibTimers   *timers = &(gLib.timers);
    bool            doCb    = false;

    natsMutex_Lock(timers->lock);
    natsMutex_Lock(t->mu);

    // If the timer was already stopped, nothing to do.
    if (t->stopped)
    {
        natsMutex_Unlock(t->mu);
        natsMutex_Unlock(timers->lock);

        return;
    }

    _removeTimer(timers, t);

    doCb = (!(t->inCallback) && (t->stopCb != NULL));

    natsMutex_Unlock(t->mu);

    if (!(timers->changed))
        natsCondition_Signal(timers->cond);

    timers->changed = true;

    natsMutex_Unlock(timers->lock);

    if (doCb)
        (*(t->stopCb))(t, t->closure);
}

int
nats_getTimersCount(void)
{
    int         count = 0;

    natsMutex_Lock(gLib.timers.lock);

    count = gLib.timers.count;

    natsMutex_Unlock(gLib.timers.lock);

    return count;
}

int
nats_getTimersCountInList(void)
{
    int         count = 0;
    natsTimer   *t;

    natsMutex_Lock(gLib.timers.lock);

    t = gLib.timers.timers;
    while (t != NULL)
    {
        count++;
        t = t->next;
    }

    natsMutex_Unlock(gLib.timers.lock);

    return count;
}


static void
_timerThread(void *arg)
{
    natsLibTimers   *timers = &(gLib.timers);
    natsTimer       *t      = NULL;
    natsStatus      s       = NATS_OK;
    bool            doStopCb;
    int64_t         target;

    WAIT_LIB_INITIALIZED;

    natsMutex_Lock(timers->lock);

    while (!(timers->shutdown))
    {
        // Take the first timer that needs to fire.
        t = timers->timers;

        if (t == NULL)
        {
            // No timer, fire in an hour...
            target = nats_setTargetTime(3600 * 1000);
        }
        else
        {
            target = t->absoluteTime;
        }

        timers->changed = false;

        s = NATS_OK;

        while (!(timers->shutdown)
               && (s != NATS_TIMEOUT)
               && !(timers->changed))
        {
            s = natsCondition_AbsoluteTimedWait(timers->cond, timers->lock,
                                                target);
        }

        if (timers->shutdown)
            break;

        if ((t == NULL) || timers->changed)
            continue;

        natsMutex_Lock(t->mu);

        // Remove timer from the list:
        timers->timers = t->next;
        if (t->next != NULL)
            t->next->prev = NULL;

        t->prev = NULL;
        t->next = NULL;

        t->inCallback = true;

        // Retain the timer, since we are going to release the locks for the
        // callback. The user may "destroy" the timer from there, so we need
        // to be protected with reference counting.
        t->refs++;

        natsMutex_Unlock(t->mu);
        natsMutex_Unlock(timers->lock);

        (*(t->cb))(t, t->closure);

        natsMutex_Lock(timers->lock);
        natsMutex_Lock(t->mu);

        t->inCallback = false;

        // Timer may have been stopped from within the callback, or during
        // the window the locks were released.
        doStopCb = (t->stopped && (t->stopCb != NULL));

        // If not stopped, we need to put it back in our list
        if (!doStopCb)
        {
            // Reset our view of what is the time this timer should fire
            // because:
            // 1- the callback may have taken longer than it should
            // 2- the user may have called Reset() with a new interval
            t->absoluteTime = nats_setTargetTime(t->interval);
            _insertTimer(t);
        }

        natsMutex_Unlock(t->mu);
        natsMutex_Unlock(timers->lock);

        if (doStopCb)
            (*(t->stopCb))(t, t->closure);

        // Compensate for the retain that we made before invoking the timer's
        // callback
        natsTimer_Release(t);

        natsMutex_Lock(timers->lock);
    }

    // Process the timers that were left in the list (not stopped) when the
    // library is shutdown.
    while ((t = timers->timers) != NULL)
    {
        natsMutex_Lock(t->mu);

        // Check if we should invoke the callback. Note that although we are
        // releasing the locks below, a timer present in the list here is
        // guaranteed not to have been stopped (because it would not be in
        // the list otherwise, since there is no chance that it is in the
        // timer's callback). So just check if there is a stopCb to invoke.
        doStopCb = (t->stopCb != NULL);

        // Remove the timer from the list.
        _removeTimer(timers, t);

        natsMutex_Unlock(t->mu);
        natsMutex_Unlock(timers->lock);

        // Invoke the callback now.
        if (doStopCb)
            (*(t->stopCb))(t, t->closure);

        // No release of the timer here. The user is still responsible to call
        // natsTimer_Destroy().

        natsMutex_Lock(timers->lock);
    }

    natsMutex_Unlock(timers->lock);

    natsLib_Release();
}

static void
_asyncCbsThread(void *arg)
{
    natsLibAsyncCbs *asyncCbs = &(gLib.asyncCbs);
    natsAsyncCbInfo *cb       = NULL;
    natsConnection  *nc       = NULL;
#if defined(NATS_HAS_STREAMING)
    stanConnection  *sc       = NULL;
#endif

    WAIT_LIB_INITIALIZED;

    natsMutex_Lock(asyncCbs->lock);

    // We want all callbacks to be invoked even on shutdown
    while (true)
    {
        while (((cb = asyncCbs->head) == NULL) && !asyncCbs->shutdown)
            natsCondition_Wait(asyncCbs->cond, asyncCbs->lock);

        if ((cb == NULL) && asyncCbs->shutdown)
            break;

        asyncCbs->head = cb->next;

        if (asyncCbs->tail == cb)
            asyncCbs->tail = NULL;

        cb->next = NULL;

        natsMutex_Unlock(asyncCbs->lock);

        nc = cb->nc;
#if defined(NATS_HAS_STREAMING)
        sc = cb->sc;
#endif

        switch (cb->type)
        {
            case ASYNC_CLOSED:
               (*(nc->opts->closedCb))(nc, nc->opts->closedCbClosure);
               break;
            case ASYNC_DISCONNECTED:
                (*(nc->opts->disconnectedCb))(nc, nc->opts->disconnectedCbClosure);
                break;
            case ASYNC_RECONNECTED:
                (*(nc->opts->reconnectedCb))(nc, nc->opts->reconnectedCbClosure);
                break;
            case ASYNC_CONNECTED:
                (*(nc->opts->connectedCb))(nc, nc->opts->connectedCbClosure);
                break;
            case ASYNC_DISCOVERED_SERVERS:
                (*(nc->opts->discoveredServersCb))(nc, nc->opts->discoveredServersClosure);
                break;
            case ASYNC_LAME_DUCK_MODE:
                (*(nc->opts->lameDuckCb))(nc, nc->opts->lameDuckClosure);
                break;
            case ASYNC_ERROR:
                (*(nc->opts->asyncErrCb))(nc, cb->sub, cb->err, nc->opts->asyncErrCbClosure);
                break;
#if defined(NATS_HAS_STREAMING)
            case ASYNC_STAN_CONN_LOST:
                (*(sc->opts->connectionLostCB))(sc, sc->connLostErrTxt, sc->opts->connectionLostCBClosure);
                break;
#endif
            default:
                break;
        }

        natsAsyncCb_Destroy(cb);

        natsMutex_Lock(asyncCbs->lock);
    }

    natsMutex_Unlock(asyncCbs->lock);

    natsLib_Release();
}

natsStatus
nats_postAsyncCbInfo(natsAsyncCbInfo *info)
{
    natsMutex_Lock(gLib.asyncCbs.lock);

    if (gLib.asyncCbs.shutdown)
    {
        natsMutex_Unlock(gLib.asyncCbs.lock);
        return NATS_NOT_INITIALIZED;
    }

    info->next = NULL;

    if (gLib.asyncCbs.head == NULL)
        gLib.asyncCbs.head = info;

    if (gLib.asyncCbs.tail != NULL)
        gLib.asyncCbs.tail->next = info;

    gLib.asyncCbs.tail = info;

    natsCondition_Signal(gLib.asyncCbs.cond);

    natsMutex_Unlock(gLib.asyncCbs.lock);

    return NATS_OK;
}

static void
_garbageCollector(void *closure)
{
    natsGCList *gc = &(gLib.gc);
    natsGCItem *item;
    natsGCItem *list;

    WAIT_LIB_INITIALIZED;

    natsMutex_Lock(gc->lock);

    // Process all elements in the list, even on shutdown
    while (true)
    {
        // Go into wait until we are notified to shutdown
        // or there is something to garbage collect
        gc->inWait = true;

        while (!(gc->shutdown) && (gc->head == NULL))
        {
            natsCondition_Wait(gc->cond, gc->lock);
        }

        // Out of the wait. Setting this boolean avoids unnecessary
        // signaling when an item is added to the collector.
        gc->inWait = false;

        // Do not break out on shutdown here, we want to clear the list,
        // even on exit so that valgrind and the like are happy.

        // Under the lock, we will switch to a local list and reset the
        // GC's list (so that others can add to the list without contention
        // (at least from the GC itself).
        do
        {
            list = gc->head;
            gc->head = NULL;

            natsMutex_Unlock(gc->lock);

            // Now that we are outside of the lock, we can empty the list.
            while ((item = list) != NULL)
            {
                // Pops item from the beginning of the list.
                list = item->next;
                item->next = NULL;

                // Invoke the freeCb associated with this object
                (*(item->freeCb))((void*) item);
            }

            natsMutex_Lock(gc->lock);
        }
        while (gc->head != NULL);

        // If we were ask to shutdown and since the list is now empty, exit
        if (gc->shutdown)
            break;
    }

    natsMutex_Unlock(gc->lock);

    natsLib_Release();
}

bool
natsGC_collect(natsGCItem *item)
{
    natsGCList  *gc;
    bool        signal;

    // If the object was not setup for garbage collection, return false
    // so the caller frees the object.
    if (item->freeCb == NULL)
        return false;

    gc = &(gLib.gc);

    natsMutex_Lock(gc->lock);

    // We will signal only if the GC is in the condition wait.
    signal = gc->inWait;

    // Add to the front of the list.
    item->next = gc->head;

    // Update head.
    gc->head = item;

    if (signal)
        natsCondition_Signal(gc->cond);

    natsMutex_Unlock(gc->lock);

    return true;
}

static void
_libTearDown(void)
{
    int i;

    for (i=0; i<gLib.dlvWorkers.size; i++)
    {
        natsMsgDlvWorker *worker = gLib.dlvWorkers.workers[i];
        if (worker->thread != NULL)
            natsThread_Join(worker->thread);
    }

    if (gLib.timers.thread != NULL)
        natsThread_Join(gLib.timers.thread);

    if (gLib.asyncCbs.thread != NULL)
        natsThread_Join(gLib.asyncCbs.thread);

    if (gLib.gc.thread != NULL)
        natsThread_Join(gLib.gc.thread);

    natsLib_Release();
}

natsStatus
nats_Open(int64_t lockSpinCount)
{
    natsStatus s = NATS_OK;

    if (!nats_InitOnce(&gInitOnce, _doInitOnce))
        return NATS_FAILED_TO_INITIALIZE;

    natsMutex_Lock(gLib.lock);

    if (gLib.closed || gLib.initialized || gLib.initializing)
    {
        if (gLib.closed)
            s = NATS_FAILED_TO_INITIALIZE;
        else if (gLib.initializing)
            s = NATS_ILLEGAL_STATE;

        natsMutex_Unlock(gLib.lock);
        return s;
    }

    gLib.initializing = true;
    gLib.initAborted = false;

#if !defined(_WIN32)
    signal(SIGPIPE, SIG_IGN);
#endif

    srand((unsigned int) nats_NowInNanoSeconds());

    gLib.refs = 1;

    // If the caller specifies negative value, then we use the default
    if (lockSpinCount >= 0)
        gLockSpinCount = lockSpinCount;

    nats_Base32_Init();

    s = natsCondition_Create(&(gLib.cond));

    if (s == NATS_OK)
        s = natsCrypto_Init();

    if (s == NATS_OK)
        s = natsMutex_Create(&(gLib.timers.lock));
    if (s == NATS_OK)
        s = natsCondition_Create(&(gLib.timers.cond));
    if (s == NATS_OK)
    {
        s = natsThread_Create(&(gLib.timers.thread), _timerThread, NULL);
        if (s == NATS_OK)
            gLib.refs++;
    }

    if (s == NATS_OK)
        s = natsMutex_Create(&(gLib.asyncCbs.lock));
    if (s == NATS_OK)
        s = natsCondition_Create(&(gLib.asyncCbs.cond));
    if (s == NATS_OK)
    {
        s = natsThread_Create(&(gLib.asyncCbs.thread), _asyncCbsThread, NULL);
        if (s == NATS_OK)
            gLib.refs++;
    }
    if (s == NATS_OK)
        s = natsMutex_Create(&(gLib.gc.lock));
    if (s == NATS_OK)
        s = natsCondition_Create(&(gLib.gc.cond));
    if (s == NATS_OK)
    {
        s = natsThread_Create(&(gLib.gc.thread), _garbageCollector, NULL);
        if (s == NATS_OK)
            gLib.refs++;
    }
    if (s == NATS_OK)
        s = natsNUID_init();

    if (s == NATS_OK)
        s = natsMutex_Create(&(gLib.dlvWorkers.lock));
    if (s == NATS_OK)
    {
        char *defaultWriteDeadlineStr = getenv("NATS_DEFAULT_LIB_WRITE_DEADLINE");

        if (defaultWriteDeadlineStr != NULL)
            gLib.libDefaultWriteDeadline = (int64_t) atol(defaultWriteDeadlineStr);

        gLib.libHandlingMsgDeliveryByDefault = (getenv("NATS_DEFAULT_TO_LIB_MSG_DELIVERY") != NULL ? true : false);
        gLib.dlvWorkers.maxSize = 2;
        gLib.dlvWorkers.workers = NATS_CALLOC(gLib.dlvWorkers.maxSize, sizeof(natsMsgDlvWorker*));
        if (gLib.dlvWorkers.workers == NULL)
            s = NATS_NO_MEMORY;
    }

    if (s == NATS_OK)
        gLib.initialized = true;

    // In case of success or error, broadcast so that lib's threads
    // can proceed.
    if (gLib.cond != NULL)
    {
        if (s != NATS_OK)
        {
            gLib.initAborted = true;
            gLib.timers.shutdown = true;
            gLib.asyncCbs.shutdown = true;
            gLib.gc.shutdown = true;
        }
        natsCondition_Broadcast(gLib.cond);
    }

    gLib.initializing  = false;
    gLib.wasOpenedOnce = true;

    natsMutex_Unlock(gLib.lock);

    if (s != NATS_OK)
        _libTearDown();

    return s;
}

natsStatus
natsInbox_Create(natsInbox **newInbox)
{
    natsStatus  s;
    char        *inbox = NULL;
    char        tmpInbox[NATS_INBOX_PRE_LEN + NUID_BUFFER_LEN + 1];

    s = nats_Open(-1);
    if (s != NATS_OK)
        return s;

    sprintf(tmpInbox, "%s", inboxPrefix);
    s = natsNUID_Next(tmpInbox + NATS_INBOX_PRE_LEN, NUID_BUFFER_LEN + 1);
    if (s == NATS_OK)
    {
        inbox = NATS_STRDUP(tmpInbox);
        if (inbox == NULL)
            s = NATS_NO_MEMORY;
    }
    if (s == NATS_OK)
        *newInbox = inbox;

    return s;
}

natsStatus
natsInbox_init(char *inbox, int inboxLen)
{
    natsStatus s;

    s = nats_Open(-1);
    if (s != NATS_OK)
        return s;

    if (inboxLen < (NATS_INBOX_PRE_LEN + NUID_BUFFER_LEN + 1))
        return NATS_INSUFFICIENT_BUFFER;

    sprintf(inbox, "%s", inboxPrefix);
    s = natsNUID_Next(inbox + NATS_INBOX_PRE_LEN, NUID_BUFFER_LEN + 1);

    return s;
}

void
natsInbox_Destroy(natsInbox *inbox)
{
    if (inbox == NULL)
        return;

    NATS_FREE(inbox);
}


static natsStatus
_close(bool wait, int64_t timeout)
{
    natsStatus      s        = NATS_OK;
    natsCondition   *cond    = NULL;
    bool            complete = false;
    int             i;

    // This is to protect against a call to nats_Close() while there
    // was no prior call to nats_Open(), either directly or indirectly.
    if (!nats_InitOnce(&gInitOnce, _doInitOnce))
        return NATS_ERR;

    natsMutex_Lock(gLib.lock);

    if (gLib.closed || !gLib.initialized)
    {
        bool closed = gLib.closed;

        natsMutex_Unlock(gLib.lock);

        if (closed)
            return NATS_ILLEGAL_STATE;
        return NATS_NOT_INITIALIZED;
    }
    if (wait)
    {
        if (natsThreadLocal_Get(gLib.natsThreadKey) != NULL)
            s = NATS_ILLEGAL_STATE;
        if (s == NATS_OK)
            s = natsCondition_Create(&cond);
        if (s != NATS_OK)
        {
            natsMutex_Unlock(gLib.lock);
            return s;
        }
        gLib.closeCompleteCond   = cond;
        gLib.closeCompleteBool   = &complete;
        gLib.closeCompleteSignal = true;
    }

    gLib.closed = true;

    natsMutex_Lock(gLib.timers.lock);
    gLib.timers.shutdown = true;
    natsCondition_Signal(gLib.timers.cond);
    natsMutex_Unlock(gLib.timers.lock);

    natsMutex_Lock(gLib.asyncCbs.lock);
    gLib.asyncCbs.shutdown = true;
    natsCondition_Signal(gLib.asyncCbs.cond);
    natsMutex_Unlock(gLib.asyncCbs.lock);

    natsMutex_Lock(gLib.gc.lock);
    gLib.gc.shutdown = true;
    natsCondition_Signal(gLib.gc.cond);
    natsMutex_Unlock(gLib.gc.lock);

    natsMutex_Lock(gLib.dlvWorkers.lock);
    for (i=0; i<gLib.dlvWorkers.size; i++)
    {
        natsMsgDlvWorker *worker = gLib.dlvWorkers.workers[i];
        natsMutex_Lock(worker->lock);
        worker->shutdown = true;
        natsCondition_Signal(worker->cond);
        natsMutex_Unlock(worker->lock);
    }
    natsMutex_Unlock(gLib.dlvWorkers.lock);

    natsMutex_Unlock(gLib.lock);

    nats_ReleaseThreadMemory();
    _libTearDown();

    if (wait)
    {
        natsMutex_Lock(gLib.lock);
        while ((s != NATS_TIMEOUT) && !complete)
        {
            if (timeout <= 0)
                natsCondition_Wait(cond, gLib.lock);
            else
                s = natsCondition_TimedWait(cond, gLib.lock, timeout);
        }
        if (s != NATS_OK)
            gLib.closeCompleteSignal = false;
        natsMutex_Unlock(gLib.lock);

        natsCondition_Destroy(cond);
    }

    return s;
}

void
nats_Close(void)
{
    _close(false, true);
}

natsStatus
nats_CloseAndWait(int64_t timeout)
{
    return _close(true, timeout);
}

const char*
nats_GetVersion(void)
{
    return LIB_NATS_VERSION_STRING;
}

uint32_t
nats_GetVersionNumber(void)
{
    return LIB_NATS_VERSION_NUMBER;
}

static void
_versionGetString(char *buffer, size_t bufLen, uint32_t verNumber)
{
    snprintf(buffer, bufLen, "%d.%d.%d",
             ((verNumber >> 16) & 0xF),
             ((verNumber >> 8) & 0xF),
             (verNumber & 0xF));
}

bool
nats_CheckCompatibilityImpl(uint32_t headerReqVerNumber, uint32_t headerVerNumber,
                            const char *headerVerString)
{
    if ((headerVerNumber < LIB_NATS_VERSION_REQUIRED_NUMBER)
        || (headerReqVerNumber > LIB_NATS_VERSION_NUMBER))
    {
        char reqVerString[10];
        char libReqVerString[10];

        _versionGetString(reqVerString, sizeof(reqVerString), headerReqVerNumber);
        _versionGetString(libReqVerString, sizeof(libReqVerString), NATS_VERSION_REQUIRED_NUMBER);

        printf("Incompatible versions:\n" \
               "Header : %s (requires %s)\n" \
               "Library: %s (requires %s)\n",
               headerVerString, reqVerString,
               NATS_VERSION_STRING, libReqVerString);
        exit(1);
    }

    return true;
}

static natsTLError*
_getTLError(void)
{
    natsTLError *errTL   = NULL;
    bool        needFree = false;

    // The library should already be initialized, but let's protect against
    // situations where foo() invokes bar(), which invokes baz(), which
    // invokes nats_Open(). If that last call fails, when we un-wind down
    // to foo(), it may be difficult to know that nats_Open() failed and
    // that we should not try to invoke natsLib_setError. So we check again
    // here that the library has been initialized properly, and if not, we
    // simply don't set the error.
    if (nats_Open(-1) != NATS_OK)
        return NULL;

    errTL = natsThreadLocal_Get(gLib.errTLKey);
    if (errTL == NULL)
    {
        errTL = (natsTLError*) NATS_CALLOC(1, sizeof(natsTLError));
        if (errTL != NULL)
            errTL->framesCount = -1;
        needFree = (errTL != NULL);

    }

    if ((errTL != NULL)
        && (natsThreadLocal_SetEx(gLib.errTLKey,
                                  (const void*) errTL, false) != NATS_OK))
    {
        if (needFree)
            NATS_FREE(errTL);

        errTL = NULL;
    }

    return errTL;
}

static char*
_getErrorShortFileName(const char* fileName)
{
    char *file = strstr(fileName, "src");

    if (file != NULL)
        file = (file + 4);
    else
        file = (char*) fileName;

    return file;
}

static void
_updateStack(natsTLError *errTL, const char *funcName, natsStatus errSts,
             bool calledFromSetError)
{
    int idx;

    idx = errTL->framesCount;
    if ((idx >= 0)
        && (idx < MAX_FRAMES)
        && (strcmp(errTL->func[idx], funcName) == 0))
    {
        return;
    }

    // In case no error was already set...
    if ((errTL->framesCount == -1) && !calledFromSetError)
        errTL->sts = errSts;

    idx = ++(errTL->framesCount);

    if (idx >= MAX_FRAMES)
        return;

    errTL->func[idx] = funcName;
}

#if !defined(_WIN32)
__attribute__ ((format (printf, 5, 6)))
#endif
natsStatus
nats_setErrorReal(const char *fileName, const char *funcName, int line, natsStatus errSts, const char *errTxtFmt, ...)
{
    natsTLError *errTL  = _getTLError();
    char        tmp[256];
    va_list     ap;
    int         n;

    if ((errTL == NULL) || errTL->skipUpdate)
        return errSts;

    errTL->sts = errSts;
    errTL->framesCount = -1;

    tmp[0] = '\0';

    va_start(ap, errTxtFmt);
    nats_vsnprintf(tmp, sizeof(tmp), errTxtFmt, ap);
    va_end(ap);

    if (strlen(tmp) > 0)
    {
        n = snprintf(errTL->text, sizeof(errTL->text), "(%s:%d): %s",
                     _getErrorShortFileName(fileName), line, tmp);
        if ((n < 0) || (n >= (int) sizeof(errTL->text)))
        {
            int pos = ((int) strlen(errTL->text)) - 1;
            int i;

            for (i=0; i<3; i++)
                errTL->text[pos--] = '.';
        }
    }

    _updateStack(errTL, funcName, errSts, true);

    return errSts;
}

#if !defined(_WIN32)
__attribute__ ((format (printf, 4, 5)))
#endif
void
nats_updateErrTxt(const char *fileName, const char *funcName, int line, const char *errTxtFmt, ...)
{
    natsTLError *errTL  = _getTLError();
    char        tmp[256];
    va_list     ap;
    int         n;

    if ((errTL == NULL) || errTL->skipUpdate)
        return;

    tmp[0] = '\0';

    va_start(ap, errTxtFmt);
    nats_vsnprintf(tmp, sizeof(tmp), errTxtFmt, ap);
    va_end(ap);

    if (strlen(tmp) > 0)
    {
        n = snprintf(errTL->text, sizeof(errTL->text), "(%s:%d): %s",
                     _getErrorShortFileName(fileName), line, tmp);
        if ((n < 0) || (n >= (int) sizeof(errTL->text)))
        {
            int pos = ((int) strlen(errTL->text)) - 1;
            int i;

            for (i=0; i<3; i++)
                errTL->text[pos--] = '.';
        }
    }
}

natsStatus
nats_updateErrStack(natsStatus err, const char *func)
{
    natsTLError *errTL = _getTLError();

    if ((errTL == NULL) || errTL->skipUpdate)
        return err;

    _updateStack(errTL, func, err, false);

    return err;
}

void
nats_clearLastError(void)
{
    natsTLError *errTL  = _getTLError();

    if ((errTL == NULL) || errTL->skipUpdate)
        return;

    errTL->sts         = NATS_OK;
    errTL->text[0]     = '\0';
    errTL->framesCount = -1;
}

void
nats_doNotUpdateErrStack(bool skipStackUpdate)
{
    natsTLError *errTL  = _getTLError();

    if (errTL == NULL)
        return;

    if (skipStackUpdate)
    {
        errTL->skipUpdate++;
    }
    else
    {
        errTL->skipUpdate--;
        assert(errTL->skipUpdate >= 0);
    }
}

const char*
nats_GetLastError(natsStatus *status)
{
    natsStatus  s;
    natsTLError *errTL  = NULL;

    if (status != NULL)
        *status = NATS_OK;

    // Ensure the library is loaded
    s = nats_Open(-1);
    if (s != NATS_OK)
        return NULL;

    errTL = natsThreadLocal_Get(gLib.errTLKey);
    if ((errTL == NULL) || (errTL->sts == NATS_OK))
        return NULL;

    if (status != NULL)
        *status = errTL->sts;

    return errTL->text;
}

natsStatus
nats_GetLastErrorStack(char *buffer, size_t bufLen)
{
    natsTLError *errTL  = NULL;
    int         offset  = 0;
    int         i, max, n, len;

    if ((buffer == NULL) || (bufLen == 0))
        return NATS_INVALID_ARG;

    buffer[0] = '\0';
    len = (int) bufLen;

    // Ensure the library is loaded
    if (nats_Open(-1) != NATS_OK)
        return NATS_FAILED_TO_INITIALIZE;

    errTL = natsThreadLocal_Get(gLib.errTLKey);
    if ((errTL == NULL) || (errTL->sts == NATS_OK) || (errTL->framesCount == -1))
        return NATS_OK;

    max = errTL->framesCount;
    if (max >= MAX_FRAMES)
        max = MAX_FRAMES - 1;

    for (i=0; (i<=max) && (len > 0); i++)
    {
        n = snprintf(buffer + offset, len, "%s%s",
                     errTL->func[i],
                     (i < max ? "\n" : ""));
        // On Windows, n will be < 0 if len is not big enough.
        if (n < 0)
        {
            len = 0;
        }
        else
        {
            offset += n;
            len    -= n;
        }
    }

    if ((max != errTL->framesCount) && (len > 0))
    {
        n = snprintf(buffer + offset, len, "\n%d more...",
                     errTL->framesCount - max);
        // On Windows, n will be < 0 if len is not big enough.
        if (n < 0)
            len = 0;
        else
            len -= n;
    }

    if (len <= 0)
        return NATS_INSUFFICIENT_BUFFER;

    return NATS_OK;
}

void
nats_PrintLastErrorStack(FILE *file)
{
    natsTLError *errTL  = NULL;
    int i, max;

    // Ensure the library is loaded
    if (nats_Open(-1) != NATS_OK)
        return;

    errTL = natsThreadLocal_Get(gLib.errTLKey);
    if ((errTL == NULL) || (errTL->sts == NATS_OK) || (errTL->framesCount == -1))
        return;

    fprintf(file, "Error: %d - %s",
            errTL->sts, natsStatus_GetText(errTL->sts));
    if (errTL->text[0] != '\0')
        fprintf(file, " - %s", errTL->text);
    fprintf(file, "\n");
    fprintf(file, "Stack: (library version: %s)\n", nats_GetVersion());

    max = errTL->framesCount;
    if (max >= MAX_FRAMES)
        max = MAX_FRAMES - 1;

    for (i=0; i<=max; i++)
        fprintf(file, "  %02d - %s\n", (i+1), errTL->func[i]);

    if (max != errTL->framesCount)
        fprintf(file, " %d more...\n", errTL->framesCount - max);

    fflush(file);
}

void
nats_sslRegisterThreadForCleanup(void)
{
#if defined(NATS_HAS_TLS)
    // Set anything. The goal is that at thread exit, the thread local key
    // will have something non NULL associated, which will trigger the
    // destructor that we have registered.
    (void) natsThreadLocal_Set(gLib.sslTLKey, (void*) 1);
#endif
}

natsStatus
nats_sslInit(void)
{
    natsStatus s = NATS_OK;

    // Ensure the library is loaded
    s = nats_Open(-1);
    if (s != NATS_OK)
        return s;

    natsMutex_Lock(gLib.lock);

    if (!(gLib.sslInitialized))
    {
        // Regardless of success, mark as initialized so that we
        // can do cleanup on exit.
        gLib.sslInitialized = true;

#if defined(NATS_HAS_TLS)
        // Initialize SSL.
        SSL_library_init();
        SSL_load_error_strings();

#endif
        s = natsThreadLocal_CreateKey(&(gLib.sslTLKey), _cleanupThreadSSL);
    }

    natsMutex_Unlock(gLib.lock);

    return NATS_UPDATE_ERR_STACK(s);
}

static void
_deliverMsgs(void *arg)
{
    natsMsgDlvWorker    *dlv = (natsMsgDlvWorker*) arg;
    natsConnection      *nc;
    natsSubscription    *sub;
    natsMsgHandler      mcb;
    void                *mcbClosure;
    uint64_t            delivered;
    uint64_t            max;
    natsMsg             *msg;
    bool                timerNeedReset = false;

    natsMutex_Lock(dlv->lock);

    while (true)
    {
        while (((msg = dlv->msgList.head) == NULL) && !dlv->shutdown)
        {
            dlv->inWait = true;
            natsCondition_Wait(dlv->cond, dlv->lock);
            dlv->inWait = false;
        }

        // Break out only when list is empty
        if ((msg == NULL) && dlv->shutdown)
        {
            break;
        }

        // Remove message from list now...
        dlv->msgList.head = msg->next;
        if (dlv->msgList.tail == msg)
            dlv->msgList.tail = NULL;
        msg->next = NULL;

        // Get subscription reference from message
        sub = msg->sub;

        // Capture these under lock
        nc = sub->conn;
        mcb = sub->msgCb;
        mcbClosure = sub->msgCbClosure;
        max = sub->max;

        // Is this a control message?
        if (msg->subject[0] == '\0')
        {
            bool closed   = sub->closed;
            bool timedOut = sub->timedOut;
            bool draining = sub->libDlvDraining;

            // Switch off this flag.
            if (draining)
                sub->libDlvDraining = false;

            // We need to release this lock...
            natsMutex_Unlock(dlv->lock);

            // Release the message
            natsMsg_Destroy(msg);

            if (draining)
            {
                // Subscription is draining, we are past the last message,
                // remove the subscription. This will schedule another
                // control message for the close.
                natsSub_setDrainCompleteState(sub);
                natsConn_removeSubscription(nc, sub);
            }
            else if (closed)
            {
                natsOnCompleteCB cb         = NULL;
                void             *closure   = NULL;

                // Call this in case the subscription was draining.
                natsSub_setDrainCompleteState(sub);

                // Check for completion callback
                natsSub_Lock(sub);
                cb      = sub->onCompleteCB;
                closure = sub->onCompleteCBClosure;
                natsSub_Unlock(sub);

                if (cb != NULL)
                    (*cb)(closure);

                // Subscription closed, just release
                natsSub_release(sub);
            }
            else if (timedOut)
            {
                // Invoke the callback with a NULL message.
                (*mcb)(nc, sub, NULL, mcbClosure);
            }

            // Grab the lock, we go back to beginning of loop.
            natsMutex_Lock(dlv->lock);

            if (!draining && !closed && timedOut)
            {
                // Reset the timedOut boolean to allow for the
                // subscription to timeout again, and reset the
                // timer to fire again starting from now.
                sub->timedOut = false;
                natsTimer_Reset(sub->timeoutTimer, sub->timeout);
            }

            // Go back to top of loop.
            continue;
        }

        // Update before checking closed state.
        sub->msgList.msgs--;
        sub->msgList.bytes -= msg->dataLen;

        // Need to check for closed subscription again here.
        // The subscription could have been unsubscribed from a callback
        // but there were already pending messages. The control message
        // is queued up. Until it is processed, we need to simply
        // discard the message and continue.
        if (sub->closed)
        {
            natsMsg_Destroy(msg);
            continue;
        }

        delivered = ++(sub->delivered);

        // Is this a subscription that can timeout?
        if (!sub->draining && (sub->timeout != 0))
        {
            // Prevent the timer to post a timeout control message
            sub->timeoutSuspended = true;

            // If we are dealing with the last pending message for this sub,
            // we will reset the timer after the user callback returns.
            if (sub->msgList.msgs == 0)
                timerNeedReset = true;
        }

        natsMutex_Unlock(dlv->lock);

        if ((max == 0) || (delivered <= max))
        {
           (*mcb)(nc, sub, msg, mcbClosure);
        }
        else
        {
            // We need to destroy the message since the user can't do it
            natsMsg_Destroy(msg);
        }

        // Don't do 'else' because we need to remove when we have hit
        // the max (after the callback returns).
        if ((max > 0) && (delivered >= max))
        {
            // Call this blindly, it will be a no-op if the subscription was not draining.
            natsSub_setDrainCompleteState(sub);
            // If we have hit the max for delivered msgs, remove sub.
            natsConn_removeSubscription(nc, sub);
        }

        natsMutex_Lock(dlv->lock);

        // Check if timer need to be reset for subscriptions that can timeout.
        if (!sub->closed && (sub->timeout != 0) && timerNeedReset)
        {
            timerNeedReset = false;

            // Do this only on timer reset instead of after each return
            // from callback. The reason is that if there are still pending
            // messages for this subscription (this is the case otherwise
            // timerNeedReset would be false), we should prevent
            // the subscription to timeout anyway.
            sub->timeoutSuspended = false;

            // Reset the timer to fire in `timeout` from now.
            natsTimer_Reset(sub->timeoutTimer, sub->timeout);
        }
    }

    natsMutex_Unlock(dlv->lock);

    natsLib_Release();
}

natsStatus
nats_SetMessageDeliveryPoolSize(int max)
{
    natsStatus          s = NATS_OK;
    natsLibDlvWorkers   *workers;

    // Ensure the library is loaded
    s = nats_Open(-1);
    if (s != NATS_OK)
        return s;

    workers = &gLib.dlvWorkers;

    natsMutex_Lock(workers->lock);

    if (max <= 0)
    {
        natsMutex_Unlock(workers->lock);
        return nats_setError(NATS_ERR, "%s", "Pool size cannot be negative or zero");
    }

    // Do not error on max < workers->maxSize in case we allow shrinking
    // the pool in the future.
    if (max > workers->maxSize)
    {
        natsMsgDlvWorker **newArray = NATS_CALLOC(max, sizeof(natsMsgDlvWorker*));
        if (newArray == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
        if (s == NATS_OK)
        {
            int i;
            for (i=0; i<workers->size; i++)
                newArray[i] = workers->workers[i];

            NATS_FREE(workers->workers);
            workers->workers = newArray;
            workers->maxSize = max;
        }
    }

    natsMutex_Unlock(workers->lock);

    return NATS_UPDATE_ERR_STACK(s);
}

// Post a control message to the worker's queue.
natsStatus
natsLib_msgDeliveryPostControlMsg(natsSubscription *sub)
{
    natsStatus          s;
    natsMsg             *controlMsg = NULL;
    natsMsgDlvWorker    *worker = (sub->libDlvWorker);

    // Create a "end" message and post it to the delivery worker
    s = natsMsg_create(&controlMsg, NULL, 0, NULL, 0, NULL, 0, -1);
    if (s == NATS_OK)
    {
        natsMsgList *l;

        natsMutex_Lock(worker->lock);

        controlMsg->sub = sub;

        l = &(worker->msgList);
        if (l->tail != NULL)
            l->tail->next = controlMsg;
        if (l->head == NULL)
            l->head = controlMsg;
        l->tail = controlMsg;

        if (worker->inWait)
            natsCondition_Signal(worker->cond);

        natsMutex_Unlock(worker->lock);
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsLib_msgDeliveryAssignWorker(natsSubscription *sub)
{
    natsStatus          s = NATS_OK;
    natsLibDlvWorkers   *workers = &(gLib.dlvWorkers);
    natsMsgDlvWorker    *worker = NULL;

    natsMutex_Lock(workers->lock);

    if (workers->maxSize == 0)
    {
        natsMutex_Unlock(workers->lock);
        return nats_setError(NATS_FAILED_TO_INITIALIZE, "%s", "Message delivery thread pool size is 0!");
    }

    worker = workers->workers[workers->idx];
    if (worker == NULL)
    {
        worker = NATS_CALLOC(1, sizeof(natsMsgDlvWorker));
        if (worker == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
        if (s == NATS_OK)
            s = natsMutex_Create(&worker->lock);
        if (s == NATS_OK)
            s = natsCondition_Create(&worker->cond);
        if (s == NATS_OK)
        {
            natsLib_Retain();
            s = natsThread_Create(&worker->thread, _deliverMsgs, (void*) worker);
            if (s != NATS_OK)
                natsLib_Release();
        }
        if (s == NATS_OK)
        {
            workers->workers[workers->idx] = worker;
            workers->size++;
        }
        else
        {
            _freeDlvWorker(worker);
        }
    }
    if (s == NATS_OK)
    {
        sub->libDlvWorker = worker;
        if (++(workers->idx) == workers->maxSize)
            workers->idx = 0;
    }

    natsMutex_Unlock(workers->lock);

    return NATS_UPDATE_ERR_STACK(s);
}

bool
natsLib_isLibHandlingMsgDeliveryByDefault()
{
    return gLib.libHandlingMsgDeliveryByDefault;
}

int64_t
natsLib_defaultWriteDeadline()
{
    return gLib.libDefaultWriteDeadline;
}

void
natsLib_getMsgDeliveryPoolInfo(int *maxSize, int *size, int *idx, natsMsgDlvWorker ***workersArray)
{
    natsLibDlvWorkers *workers = &gLib.dlvWorkers;

    natsMutex_Lock(workers->lock);
    *maxSize = workers->maxSize;
    *size = workers->size;
    *idx = workers->idx;
    *workersArray = workers->workers;
    natsMutex_Unlock(workers->lock);
}
