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


#ifndef HASH_H_
#define HASH_H_

struct __natsHashEntry;

typedef struct __natsHashEntry
{
    int64_t                 key;
    void                    *data;
    struct __natsHashEntry  *next;

} natsHashEntry;

typedef struct __natsHash
{
    natsHashEntry   **bkts;
    int             numBkts;
    int             mask;
    int             used;
    bool            canResize;

} natsHash;

typedef struct __natsHashIter
{
    natsHash        *hash;
    natsHashEntry   *current;
    natsHashEntry   *next;
    int             currBkt;
    bool            started;

} natsHashIter;

typedef struct __natsStrHashEntry
{
    uint32_t                    hk;
    char                        *key;
    bool                        freeKey;
    void                        *data;
    struct __natsStrHashEntry   *next;

} natsStrHashEntry;

typedef struct __natsStrHash
{
    natsStrHashEntry    **bkts;
    int                 numBkts;
    int                 mask;
    int                 used;
    bool                canResize;

} natsStrHash;

typedef struct __natsStrHashIter
{
    natsStrHash         *hash;
    natsStrHashEntry    *current;
    natsStrHashEntry    *next;
    int                 currBkt;
    bool                started;

} natsStrHashIter;

#define natsHash_Count(h)       ((h)->used)
#define natsStrHash_Count(h)    ((h)->used)

//
// Hash with in64_t as the key
//
natsStatus
natsHash_Create(natsHash **newHash, int initialSize);

natsStatus
natsHash_Set(natsHash *hash, int64_t key, void *data, void **oldData);

void*
natsHash_Get(natsHash *hash, int64_t key);

void*
natsHash_Remove(natsHash *hash, int64_t key);

natsStatus
natsHash_RemoveSingle(natsHash *hash, int64_t *key, void **data);

void
natsHash_Destroy(natsHash *hash);

//
// Iterator for Hash int64_t
//
void
natsHashIter_Init(natsHashIter *iter, natsHash *hash);

bool
natsHashIter_Next(natsHashIter *iter, int64_t *key, void **value);

natsStatus
natsHashIter_RemoveCurrent(natsHashIter *iter);

void
natsHashIter_Done(natsHashIter *iter);

//
// Hash with char* as the key
//
natsStatus
natsStrHash_Create(natsStrHash **newHash, int initialSize);

uint32_t
natsStrHash_Hash(const char *data, int dataLen);

#define natsStrHash_Set(h, k, c, d, o) natsStrHash_SetEx((h), (k), (c), ((c) ? true : false), (d), (o))

natsStatus
natsStrHash_SetEx(natsStrHash *hash, char *key, bool copyKey, bool freeKey,
                  void *data, void **oldData);

void*
natsStrHash_Get(natsStrHash *hash, char *key);

void*
natsStrHash_Remove(natsStrHash *hash, char *key);

natsStatus
natsStrHash_RemoveSingle(natsStrHash *hash, char **key, void **data);

void
natsStrHash_Destroy(natsStrHash *hash);

//
// Iterator for Hash char*
//
void
natsStrHashIter_Init(natsStrHashIter *iter, natsStrHash *hash);

bool
natsStrHashIter_Next(natsStrHashIter *iter, char **key, void **value);

natsStatus
natsStrHashIter_RemoveCurrent(natsStrHashIter *iter);

void
natsStrHashIter_Done(natsStrHashIter *iter);


#endif /* HASH_H_ */
