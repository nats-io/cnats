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

#include <string.h>

#include "status.h"
#include "mem.h"
#include "hash.h"

#define _freeEntry(e)   { NATS_FREE(e); (e) = NULL; }

#define _OFF32  (2166136261)
#define _YP32   (709607)

#define _BSZ (8)
#define _WSZ (4)

static int _DWSZ   = _WSZ << 1; // 8
static int _DDWSZ  = _WSZ << 2; // 16

static int _MAX_BKT_SIZE = (1 << 30) - 1;

natsStatus
natsHash_Create(natsHash **newHash, int initialSize)
{
    natsHash    *hash = NULL;

    if (initialSize <= 0)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if ((initialSize & (initialSize - 1)) != 0)
    {
        // Size of buckets must be power of 2
        initialSize--;
        initialSize |= initialSize >> 1;
        initialSize |= initialSize >> 2;
        initialSize |= initialSize >> 4;
        initialSize |= initialSize >> 8;
        initialSize |= initialSize >> 16;
        initialSize++;
    }

    hash = (natsHash*) NATS_CALLOC(1, sizeof(natsHash));
    if (hash == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    hash->mask      = (initialSize - 1);
    hash->numBkts   = initialSize;
    hash->canResize = true;
    hash->bkts      = (natsHashEntry**) NATS_CALLOC(initialSize, sizeof(natsHashEntry*));
    if (hash->bkts == NULL)
    {
        NATS_FREE(hash);
        return nats_setDefaultError(NATS_NO_MEMORY);
    }

    *newHash = hash;

    return NATS_OK;
}

static natsStatus
_resize(natsHash *hash, int newSize)
{
    natsHashEntry   **bkts    = NULL;
    int             newMask   = newSize - 1;
    natsHashEntry   *ne;
    natsHashEntry   *e;
    int             k;
    int             newIndex;

    bkts = (natsHashEntry**) NATS_CALLOC(newSize, sizeof(natsHashEntry*));
    if (bkts == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    for (k = 0; k < hash->numBkts; k++)
    {
        e = hash->bkts[k];
        while (e != NULL)
        {
            ne = e;
            e  = e->next;

            newIndex = ne->key & newMask;
            ne->next = bkts[newIndex];
            bkts[newIndex] = ne;
        }
    }

    NATS_FREE(hash->bkts);
    hash->bkts = bkts;
    hash->mask = newMask;
    hash->numBkts = newSize;

    return NATS_OK;
}

static natsStatus
_grow(natsHash *hash)
{
    // Can't grow beyond max signed int for now
    if (hash->numBkts >= _MAX_BKT_SIZE)
        return nats_setDefaultError(NATS_NO_MEMORY);

    return _resize(hash, 2 * (hash->numBkts));
}

static void
_shrink(natsHash *hash)
{
    if (hash->numBkts <= _BSZ)
        return;

    // Ignore memory issue when resizing, since if we fail to allocate
    // the original hash is still intact.
    (void) _resize(hash, hash->numBkts / 2);
}

static natsHashEntry*
_createEntry(int64_t key, void *data)
{
    natsHashEntry *e = (natsHashEntry*) NATS_MALLOC(sizeof(natsHashEntry));

    if (e == NULL)
        return NULL;

    e->key  = key;
    e->data = data;
    e->next = NULL;

    return e;
}

natsStatus
natsHash_Set(natsHash *hash, int64_t key, void *data, void **oldData)
{
    natsStatus      s         = NATS_OK;
    int             index     = (int) (key & hash->mask);
    natsHashEntry   *newEntry = NULL;
    natsHashEntry   *e;

    if (oldData != NULL)
        *oldData = NULL;

    e = (natsHashEntry*) hash->bkts[index];
    while (e != NULL)
    {
        if (e->key == key)
        {
            // Success, replace data field
            if (oldData != NULL)
                *oldData = e->data;
            e->data  = data;
            return NATS_OK;
        }

        e = e->next;
    }

    // We have a new entry here
    newEntry = _createEntry(key, data);
    if (newEntry == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    newEntry->next = hash->bkts[index];
    hash->bkts[index] = newEntry;
    hash->used++;

    // Check for resizing
    if (hash->canResize && (hash->used > hash->numBkts))
        s = _grow(hash);

    return NATS_UPDATE_ERR_STACK(s);
}

void*
natsHash_Get(natsHash *hash, int64_t key)
{
    natsHashEntry *e;

    e = hash->bkts[key & hash->mask];
    while (e != NULL)
    {
        if (e->key == key)
            return e->data;

        e = e->next;
    }

    return NULL;
}

static void
_maybeShrink(natsHash *hash)
{
    if (hash->canResize
        && (hash->numBkts > _BSZ)
        && (hash->used < hash->numBkts / 4))
    {
        _shrink(hash);
    }
}

void*
natsHash_Remove(natsHash *hash, int64_t key)
{
    natsHashEntry *entryRemoved = NULL;
    void          *dataRemoved  = NULL;
    natsHashEntry **e;

    e = (natsHashEntry**) &(hash->bkts[key & hash->mask]);
    while (*e != NULL)
    {
        if ((*e)->key == key)
        {
            // Success
            entryRemoved = *e;
            dataRemoved  = entryRemoved->data;

            *e = entryRemoved->next;
            _freeEntry(entryRemoved);

            hash->used--;

            // Check for resizing
            _maybeShrink(hash);

            break;
        }

        e = (natsHashEntry**) &((*e)->next);
    }

    return dataRemoved;
}

natsStatus
natsHash_RemoveSingle(natsHash *hash, int64_t *key, void **data)
{
    natsHashEntry   *e = NULL;
    int             i;

    if (hash->used != 1)
        return nats_setDefaultError(NATS_ERR);

    for (i=0; i<hash->numBkts; i++)
    {
        e = hash->bkts[i];
        if (e != NULL)
        {
            if (key != NULL)
                *key = e->key;
            if (data != NULL)
                *data = e->data;
            _freeEntry(e);

            hash->used--;
            hash->bkts[i] = NULL;

            // Check for resizing
            _maybeShrink(hash);

            break;
        }
    }
    return NATS_OK;
}

void
natsHash_Destroy(natsHash *hash)
{
    natsHashEntry   *e, *ne;
    int             i;

    if (hash == NULL)
        return;

    for (i = 0; i < hash->numBkts; i++)
    {
        e = hash->bkts[i];
        while (e != NULL)
        {
            ne = e->next;

            _freeEntry(e);

            e = ne;
        }
    }

    NATS_FREE(hash->bkts);
    NATS_FREE(hash);
}

void
natsHashIter_Init(natsHashIter *iter, natsHash *hash)
{
    memset(iter, 0, sizeof(natsHashIter));

    hash->canResize = false;
    iter->hash      = hash;
    iter->current   = hash->bkts[0];
    iter->next      = iter->current;
}

bool
natsHashIter_Next(natsHashIter *iter, int64_t *key, void **value)
{
    if ((iter->started) && (iter->next == NULL))
        return false;

    if (!(iter->started) && (iter->current == NULL))
    {
        while ((iter->next == NULL)
               && (iter->currBkt < (iter->hash->numBkts - 1)))
        {
            iter->next = iter->hash->bkts[++(iter->currBkt)];
        }

        if (iter->next == NULL)
        {
            iter->started = true;
            return false;
        }
    }

    iter->started = true;

    iter->current = iter->next;
    if (iter->current != NULL)
    {
        if (key != NULL)
            *key = iter->current->key;
        if (value != NULL)
            *value = iter->current->data;

        iter->next = iter->current->next;
    }

    while ((iter->next == NULL)
           && (iter->currBkt < (iter->hash->numBkts - 1)))
    {
        iter->next = iter->hash->bkts[++(iter->currBkt)];
    }

    return true;
}

natsStatus
natsHashIter_RemoveCurrent(natsHashIter *iter)
{
    int64_t key;

    if (iter->current == NULL)
        return nats_setDefaultError(NATS_NOT_FOUND);

    key = iter->current->key;
    iter->current = iter->next;

    (void) natsHash_Remove(iter->hash, key);

    return NATS_OK;
}

void
natsHashIter_Done(natsHashIter *iter)
{
    iter->hash->canResize = true;
}


// Jesteress derivative of FNV1A from [http://www.sanmayce.com/Fastest_Hash/]
uint32_t
natsStrHash_Hash(const char *data, int dataLen)
{
    int      i      = 0;
    int      dlen   = dataLen;
    uint32_t h32    = (uint32_t)_OFF32;
    uint64_t k1, k2;
    uint32_t k3;

    for (; dlen >= _DDWSZ; dlen -= _DDWSZ)
    {
        memcpy(&k1, &(data[i]), sizeof(k1));
        memcpy(&k2, &(data[i + 4]), sizeof(k2));
        h32 = (uint32_t) ((((uint64_t) h32) ^ ((k1<<5 | k1>>27) ^ k2)) * _YP32);
        i += _DDWSZ;
    }

    // Cases: 0,1,2,3,4,5,6,7
    if ((dlen & _DWSZ) != 0)
    {
        memcpy(&k1, &(data[i]), sizeof(k1));
        h32 = (uint32_t) ((((uint64_t) h32) ^ k1) * _YP32);
        i += _DWSZ;
    }
    if ((dlen & _WSZ) != 0)
    {
        memcpy(&k3, &(data[i]), sizeof(k3));
        h32 = (uint32_t) ((((uint64_t) h32) ^ (uint64_t) k3) * _YP32);
        i += _WSZ;
    }
    if ((dlen & 1) != 0)
    {
        h32 = (h32 ^ (uint32_t)(data[i])) * _YP32;
    }

    return h32 ^ (h32 >> 16);
}

natsStatus
natsStrHash_Create(natsStrHash **newHash, int initialSize)
{
    natsStrHash *hash = NULL;

    if (initialSize <= 0)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if ((initialSize & (initialSize - 1)) != 0)
    {
        // Size of buckets must be power of 2
        initialSize--;
        initialSize |= initialSize >> 1;
        initialSize |= initialSize >> 2;
        initialSize |= initialSize >> 4;
        initialSize |= initialSize >> 8;
        initialSize |= initialSize >> 16;
        initialSize++;
    }

    hash = (natsStrHash*) NATS_CALLOC(1, sizeof(natsStrHash));
    if (hash == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    hash->mask      = (initialSize - 1);
    hash->numBkts   = initialSize;
    hash->canResize = true;
    hash->bkts      = (natsStrHashEntry**) NATS_CALLOC(initialSize, sizeof(natsStrHashEntry*));
    if (hash->bkts == NULL)
    {
        NATS_FREE(hash);
        return nats_setDefaultError(NATS_NO_MEMORY);
    }

    *newHash = hash;

    return NATS_OK;
}

static natsStatus
_resizeStr(natsStrHash *hash, int newSize)
{
    natsStrHashEntry    **bkts    = NULL;
    int                 newMask   = newSize - 1;
    natsStrHashEntry    *ne;
    natsStrHashEntry    *e;
    int                 k;
    int                 newIndex;

    bkts = (natsStrHashEntry**) NATS_CALLOC(newSize, sizeof(natsStrHashEntry*));
    if (bkts == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    for (k = 0; k < hash->numBkts; k++)
    {
        e = hash->bkts[k];
        while (e != NULL)
        {
            ne = e;
            e  = e->next;

            newIndex = ne->hk & newMask;
            ne->next = bkts[newIndex];
            bkts[newIndex] = ne;
        }
    }

    NATS_FREE(hash->bkts);
    hash->bkts = bkts;
    hash->mask = newMask;
    hash->numBkts = newSize;

    return NATS_OK;
}

static natsStatus
_growStr(natsStrHash *hash)
{
    // Can't grow beyond max signed int for now
    if (hash->numBkts >= _MAX_BKT_SIZE)
        return nats_setDefaultError(NATS_NO_MEMORY);

    return _resizeStr(hash, 2 * (hash->numBkts));
}

static void
_shrinkStr(natsStrHash *hash)
{
    if (hash->numBkts <= _BSZ)
        return;

    // Ignore memory issue when resizing, since if we fail to allocate
    // the original hash is still intact.
    (void) _resizeStr(hash, hash->numBkts / 2);
}


static natsStrHashEntry*
_createStrEntry(uint32_t hk, char *key, bool copyKey, bool freeKey, void *data)
{
    natsStrHashEntry *e = (natsStrHashEntry*) NATS_MALLOC(sizeof(natsStrHashEntry));

    if (e == NULL)
        return NULL;

    e->hk       = hk;
    e->key      = (copyKey ? NATS_STRDUP(key) : key);
    e->freeKey  = freeKey;
    e->data     = data;
    e->next     = NULL;

    if (e->key == NULL)
    {
        NATS_FREE(e);
        return NULL;
    }

    return e;
}

// Note that it would be invalid to call with copyKey:true and freeKey:false,
// since this would lead to a memory leak.
natsStatus
natsStrHash_SetEx(natsStrHash *hash, char *key, bool copyKey, bool freeKey,
                  void *data, void **oldData)
{
    natsStatus          s         = NATS_OK;
    uint32_t            hk        = 0;
    int                 index     = 0;
    natsStrHashEntry    *newEntry = NULL;
    natsStrHashEntry    *e;
    char                *oldKey;

    if (oldData != NULL)
        *oldData = NULL;

    hk    = natsStrHash_Hash(key, (int) strlen(key));
    index = hk & hash->mask;

    e = (natsStrHashEntry*) hash->bkts[index];
    while (e != NULL)
    {
        if ((e->hk == hk)
            && (strcmp(e->key, key) == 0))
        {
            // Success, replace data field
            if (oldData != NULL)
                *oldData = e->data;
            e->data  = data;

            // Need to care for situations where previous call
            // for same key hash was with different pointers and or
            // "config" values (copyKey/freeKey).

            // But if nothing has changed (same pointers and config) we
            // can bail early.
            if ((key == e->key) && (freeKey == e->freeKey))
                return NATS_OK;

            oldKey = e->key;
            // First try to dup the key if required.
            if (copyKey)
            {
                char *newKey = NATS_STRDUP(key);
                if (newKey == NULL)
                    return nats_setDefaultError(NATS_NO_MEMORY);

                e->key = newKey;
            }
            // If old config say that we had ownership, then free the
            // old key now.
            if (e->freeKey)
                NATS_FREE(oldKey);

            // Keep track of ownership of this key (copied or not).
            e->freeKey = freeKey;
            return NATS_OK;
        }

        e = e->next;
    }

    // We have a new entry here
    newEntry = _createStrEntry(hk, key, copyKey, freeKey, data);
    if (newEntry == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    newEntry->next = hash->bkts[index];
    hash->bkts[index] = newEntry;
    hash->used++;

    // Check for resizing
    if (hash->canResize && (hash->used > hash->numBkts))
        s = _growStr(hash);

    return NATS_UPDATE_ERR_STACK(s);
}

void*
natsStrHash_Get(natsStrHash *hash, char *key)
{
    natsStrHashEntry    *e;
    uint32_t            hk = natsStrHash_Hash(key, (int) strlen(key));

    e = hash->bkts[hk & hash->mask];
    while (e != NULL)
    {
        if ((e->hk == hk)
            && (strcmp(e->key, key) == 0))
        {
            return e->data;
        }

        e = e->next;
    }

    return NULL;
}

static void
_freeStrEntry(natsStrHashEntry *e)
{
    if (e->freeKey)
        NATS_FREE(e->key);

    NATS_FREE(e);
}

static void
_maybeShrinkStr(natsStrHash *hash)
{
    if (hash->canResize
        && (hash->numBkts > _BSZ)
        && (hash->used < hash->numBkts / 4))
    {
        _shrinkStr(hash);
    }
}

void*
natsStrHash_Remove(natsStrHash *hash, char *key)
{
    natsStrHashEntry    *entryRemoved = NULL;
    void                *dataRemoved  = NULL;
    natsStrHashEntry    **e;
    uint32_t            hk;

    hk = natsStrHash_Hash(key, (int) strlen(key));

    e = (natsStrHashEntry**) &(hash->bkts[hk & hash->mask]);
    while (*e != NULL)
    {
        if (((*e)->hk == hk)
            && (strcmp((*e)->key, key) == 0))
        {
            // Success
            entryRemoved = *e;
            dataRemoved  = entryRemoved->data;

            *e = entryRemoved->next;
            _freeStrEntry(entryRemoved);

            hash->used--;

            // Check for resizing
            _maybeShrinkStr(hash);

            break;
        }

        e = (natsStrHashEntry**) &((*e)->next);
    }

    return dataRemoved;
}

natsStatus
natsStrHash_RemoveSingle(natsStrHash *hash, char **key, void **data)
{
    natsStrHashEntry    *e = NULL;
    int                 i;

    if (hash->used != 1)
        return nats_setDefaultError(NATS_ERR);

    for (i=0; i<hash->numBkts; i++)
    {
        e = hash->bkts[i];
        if (e != NULL)
        {
            if (key != NULL)
            {
                char *retKey = e->key;

                if (e->freeKey)
                {
                    retKey = NATS_STRDUP(e->key);
                    if (retKey == NULL)
                        return nats_setDefaultError(NATS_NO_MEMORY);
                }
                *key = retKey;
            }
            if (data != NULL)
                *data = e->data;
            _freeStrEntry(e);

            hash->used--;
            hash->bkts[i] = NULL;

            // Check for resizing
            _maybeShrinkStr(hash);

            break;
        }
    }
    return NATS_OK;
}

void
natsStrHash_Destroy(natsStrHash *hash)
{
    natsStrHashEntry    *e, *ne;
    int                 i;

    if (hash == NULL)
        return;

    for (i = 0; i < hash->numBkts; i++)
    {
        e = hash->bkts[i];
        while (e != NULL)
        {
            ne = e->next;

            _freeStrEntry(e);

            e = ne;
        }
    }

    NATS_FREE(hash->bkts);
    NATS_FREE(hash);
}

void
natsStrHashIter_Init(natsStrHashIter *iter, natsStrHash *hash)
{
    memset(iter, 0, sizeof(natsStrHashIter));

    hash->canResize = false;
    iter->hash      = hash;
    iter->current   = hash->bkts[0];
    iter->next      = iter->current;
}

bool
natsStrHashIter_Next(natsStrHashIter *iter, char **key, void **value)
{
    if ((iter->started) && (iter->next == NULL))
        return false;

    if (!(iter->started) && (iter->current == NULL))
    {
        while ((iter->next == NULL)
               && (iter->currBkt < (iter->hash->numBkts - 1)))
        {
            iter->next = iter->hash->bkts[++(iter->currBkt)];
        }

        if (iter->next == NULL)
        {
            iter->started = true;
            return false;
        }
    }

    iter->started = true;

    iter->current = iter->next;
    if (iter->current != NULL)
    {
        if (key != NULL)
            *key = iter->current->key;
        if (value != NULL)
            *value = iter->current->data;

        iter->next = iter->current->next;
    }

    while ((iter->next == NULL)
           && (iter->currBkt < (iter->hash->numBkts - 1)))
    {
        iter->next = iter->hash->bkts[++(iter->currBkt)];
    }

    return true;
}

natsStatus
natsStrHashIter_RemoveCurrent(natsStrHashIter *iter)
{
    char *key;

    if (iter->current == NULL)
        return nats_setDefaultError(NATS_NOT_FOUND);

    key = iter->current->key;
    iter->current = iter->next;

    (void) natsStrHash_Remove(iter->hash, key);

    return NATS_OK;
}

void
natsStrHashIter_Done(natsStrHashIter *iter)
{
    natsHashIter_Done((natsHashIter*) iter);
}
