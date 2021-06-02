// Copyright 2016-2021 The NATS Authors
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

// From https://en.wikipedia.org/wiki/Multiply-with-carry

// CMWC working parts
#define CMWC_CYCLE 4096         // as Marsaglia recommends
#define CMWC_C_MAX 809430660    // as Marsaglia recommends
static uint32_t Q[CMWC_CYCLE];
static uint32_t carry = 362436;     // must be limited with CMWC_C_MAX (we will reinit it with seed)

// Make 32 bit random number (some systems use 16 bit RAND_MAX)
static uint32_t
_rand32(void)
{
    uint32_t result = 0;
    result = rand();
    result <<= 16;
    result |= rand();
    return result;
}

// Init all engine parts with seed
static void
_initCMWC(unsigned int seed)
{
    int i;

    for (i = 0; i < CMWC_CYCLE; i++)
        Q[i] = _rand32();

    do
    {
        carry = _rand32();
    }
    while (carry >= CMWC_C_MAX);
}

// CMWC engine
static uint32_t
_randCMWC(void)
{
    static uint32_t i = CMWC_CYCLE - 1;
    uint64_t t = 0;
    uint64_t a = 18782;         // as Marsaglia recommends
    uint32_t r = 0xfffffffe;    // as Marsaglia recommends
    uint32_t x = 0;

    i = (i + 1) & (CMWC_CYCLE - 1);
    t = a * Q[i] + carry;
    carry = t >> 32;
    x = (uint32_t) (t + carry);
    if (x < carry)
    {
        x++;
        carry++;
    }

    return (Q[i] = r - x);
}

static int64_t
_rand64(int64_t maxValue)
{
    int64_t v;

    v  = ((int64_t) _randCMWC() << 32);
    v |= (int64_t) _randCMWC();

    if (v < 0)
        v = v * -1;

    v = v % maxValue;

    return v;
}

int64_t
nats_Rand64(void)
{
    return _rand64(0x7FFFFFFFFFFFFFFF);
}

// A unique identifier generator that is high performance, very fast, and entropy pool friendly.
//
// NUID needs to be very fast to generate and truly unique, all while being entropy pool friendly.
// We will use 12 bytes of crypto generated data (entropy draining), and 10 bytes of sequential data
// that is started at a pseudo random number and increments with a pseudo-random increment.
// Total is 22 bytes of base 36 ascii text :)

#define NUID_PRE_LEN (12)
#define NUID_SEQ_LEN (10)

static const char       *digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int        base    = 36;
static const int64_t    maxPre  = 4738381338321616896L;
static const int64_t    maxSeq  = 3656158440062976L;
static const int64_t    minInc  = 33L;
static const int64_t    maxInc  = 333L;
static const int        totalLen= NUID_PRE_LEN + NUID_SEQ_LEN;

typedef struct natsNUID
{
    char    pre[NUID_PRE_LEN];
    int64_t seq;
    int64_t inc;

} natsNUID;

typedef struct natsLockedNUID
{
    natsMutex   *mu;
    natsNUID    nuid;

} natsLockedNUID;

// Global NUID
static natsLockedNUID   globalNUID;

static natsStatus
_nextLong(int64_t *next, bool useCrypto, int64_t maxValue)
{
    natsStatus s = NATS_OK;

    if (maxValue <= 0)
        return nats_setError(NATS_INVALID_ARG,
                             "Invalid argument for nextLong: %" PRId64 "",
                             maxValue);
#if defined(NATS_HAS_TLS)
    if (useCrypto)
    {
        int64_t r = 0;

        RAND_bytes((unsigned char*) &r, sizeof(int64_t));
        if (r < 0)
            r = r * -1;

        *next = r;
    }
    else
#endif
        *next = _rand64(maxValue);

    return s;
}

// Resets the sequential portion of the NUID.
static natsStatus
_resetSequential(natsNUID *nuid)
{
    natsStatus s;

    s = _nextLong(&(nuid->seq), false, maxSeq);
    if (s == NATS_OK)
        s = _nextLong(&(nuid->inc), false, maxInc - minInc);
    if (s == NATS_OK)
        nuid->inc += minInc;

    return NATS_UPDATE_ERR_STACK(s);
}

// Generate a new prefix from crypto rand.
// This will drain entropy and will be called automatically when we exhaust the sequential
static natsStatus
_randomizePrefix(natsNUID *nuid)
{
    natsStatus  s;
    int64_t     r = 0;

    s = _nextLong(&r, true, maxPre);
    if (s == NATS_OK)
    {
        int64_t l;
        int     i = NUID_PRE_LEN;
        for (l = r; i > 0; l /= base)
        {
            i--;
            nuid->pre[i] = digits[(int) (l % base)];
        }
    }

    return NATS_UPDATE_ERR_STACK(s);
}

void
natsNUID_free(void)
{
    natsMutex_Destroy(globalNUID.mu);
    globalNUID.mu = NULL;
}

// Seed sequential random with math/random and current time and generate crypto prefix.
natsStatus
natsNUID_init(void)
{
    natsStatus      s;
    unsigned int    seed = (unsigned int) nats_NowInNanoSeconds();

    memset(&globalNUID, 0, sizeof(natsNUID));

    srand(seed);
    _initCMWC(seed);

    s = natsMutex_Create(&(globalNUID.mu));
    if (s == NATS_OK)
        s = _resetSequential(&(globalNUID.nuid));
    if (s == NATS_OK)
        s = _randomizePrefix(&(globalNUID.nuid));

    if (s != NATS_OK)
        natsNUID_free();

    return NATS_UPDATE_ERR_STACK(s);
}

// Generate the next NUID string.
static natsStatus
_nextNUID(natsNUID *nuid, char *buffer, int bufferLen)
{
    natsStatus s = NATS_OK;

    // Check bufferLen is big enough
    if (bufferLen <= totalLen)
        return nats_setError(NATS_INSUFFICIENT_BUFFER, "Buffer should be at least %d bytes, it is only %d bytes", totalLen, bufferLen);

    // Increment and capture.
    nuid->seq += nuid->inc;
    if (nuid->seq >= maxSeq)
    {
        s = _randomizePrefix(nuid);
        if (s == NATS_OK)
            s = _resetSequential(nuid);
    }

    if (s == NATS_OK)
    {
        int64_t l;
        int     i;

        // Copy prefix
        memcpy(buffer, nuid->pre, NUID_PRE_LEN);

        // copy in the seq in base36.
        l = nuid->seq;
        for (i = totalLen; i > NUID_PRE_LEN; l /= base)
        {
            i--;
            buffer[i] = digits[(int) (l % base)];
        }

        buffer[totalLen] = '\0';
    }

    return NATS_UPDATE_ERR_STACK(s);
}

// Generate the next NUID string from the global locked NUID instance.
natsStatus
natsNUID_Next(char *buffer, int bufferLen)
{
    natsStatus s;

    natsMutex_Lock(globalNUID.mu);

    s = _nextNUID(&(globalNUID.nuid), buffer, bufferLen);

    natsMutex_Unlock(globalNUID.mu);

    return NATS_UPDATE_ERR_STACK(s);
}
