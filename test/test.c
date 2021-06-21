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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#ifdef _WIN32
#else
#include <dirent.h>
#include <sys/stat.h>
#endif

#include "buf.h"
#include "timer.h"
#include "url.h"
#include "opts.h"
#include "../src/util.h"
#include "hash.h"
#include "conn.h"
#include "sub.h"
#include "msg.h"
#include "stats.h"
#include "comsock.h"
#include "crypto.h"
#include "nkeys.h"
#include "parser.h"
#include "js.h"
#if defined(NATS_HAS_STREAMING)
#include "stan/conn.h"
#include "stan/pub.h"
#include "stan/sub.h"
#include "stan/copts.h"
#include "stan/sopts.h"
#endif

static int  tests  = 0;
static bool failed = false;

static bool keepServerOutput    = false;
static bool valgrind            = false;
static bool runOnTravis         = false;

static const char *natsServerExe = "nats-server";
static const char *serverVersion = NULL;

static const char *natsStreamingServerExe = "nats-streaming-server";

static natsMutex *slMu  = NULL;
static natsHash  *slMap = NULL;

#define test(s)         { printf("#%02d ", ++tests); printf("%s", (s)); fflush(stdout); }
#ifdef _WIN32
#define NATS_INVALID_PID    (NULL)
#define testCond(c)         if(c) { printf("PASSED\n"); fflush(stdout); } else { printf("FAILED\n"); nats_PrintLastErrorStack(stdout); fflush(stdout); failed=true; return; }
#define testCondNoReturn(c) if(c) { printf("PASSED\n"); fflush(stdout); } else { printf("FAILED\n"); nats_PrintLastErrorStack(stdout); fflush(stdout); failed=true; }
#define LOGFILE_NAME    "wserver.log"
#else
#define NATS_INVALID_PID    (-1)
#define testCond(c)         if(c) { printf("\033[0;32mPASSED\033[0;0m\n"); fflush(stdout); } else { printf("\033[0;31mFAILED\033[0;0m\n"); nats_PrintLastErrorStack(stdout); fflush(stdout); failed=true; return; }
#define testCondNoReturn(c) if(c) { printf("\033[0;32mPASSED\033[0;0m\n"); fflush(stdout); } else { printf("\033[0;31mFAILED\033[0;0m\n"); nats_PrintLastErrorStack(stdout); fflush(stdout); failed=true; }
#define LOGFILE_NAME    "server.log"
#endif
#define FAIL(m)         { printf("@@ %s @@\n", (m)); failed=true; return; }

#define CHECK_SERVER_STARTED(p) if ((p) == NATS_INVALID_PID) FAIL("Unable to start or verify that the server was started!")

static const char *testServers[] = {"nats://127.0.0.1:1222",
                                    "nats://127.0.0.1:1223",
                                    "nats://127.0.0.1:1224",
                                    "nats://127.0.0.1:1225",
                                    "nats://127.0.0.1:1226",
                                    "nats://127.0.0.1:1227",
                                    "nats://127.0.0.1:1228"};

#if defined(NATS_HAS_STREAMING)
static const char *clusterName = "test-cluster";
static const char *clientName  = "client";
#endif

// Forward declaration
static void _startMockupServerThread(void *closure);

typedef natsStatus (*testCheckInfoCB)(char *buffer);

struct threadArg
{
    natsMutex       *m;
    natsThread      *t;
    natsCondition   *c;
    natsCondition   *b;
    int             control;
    bool            current;
    int             sum;
    int             timerFired;
    int             timerStopped;
    natsStrHash     *inboxes;
    natsStatus      status;
    const char*     string;
    bool            connected;
    bool            disconnected;
    int64_t         disconnectedAt[4];
    int64_t         disconnects;
    bool            closed;
    bool            reconnected;
    int64_t         reconnectedAt[4];
    int             reconnects;
    bool            msgReceived;
    bool            done;
    int             results[10];
    const char      *tokens[3];
    int             tokenCallCount;
    testCheckInfoCB checkInfoCB;
    natsSock        sock;

    natsSubscription *sub;
    natsOptions      *opts;
    natsConnection   *nc;

#if defined(NATS_HAS_STREAMING)
    stanConnection   *sc;
    int              redelivered;
    const char*      channel;
    stanMsg          *sMsg;
#endif

    int              attached;
    int              detached;
    bool             evStop;
    bool             doRead;
    bool             doWrite;

};

static bool
serverVersionAtLeast(int major, int minor, int update)
{
    int     ma       = 0;
    int     mi       = 0;
    int     up       = 0;
    char    *version = NULL;

    if (serverVersion == NULL)
        return false;

    version = strstr(serverVersion, "version ");
    if (version != NULL)
    {
        version += 8;
    }
    else
    {
        version = strstr(serverVersion, " v");
        if (version == NULL)
            return false;

        version += 2;
    }

    sscanf(version, "%d.%d.%d", &ma, &mi, &up);
    if ((ma > major) || ((ma == major) && (mi > minor)) || ((ma == major) && (mi == minor) && (up >= update)))
        return true;

    return false;
}

static natsStatus
_createDefaultThreadArgsForCbTests(
    struct threadArg    *arg)
{
    natsStatus s;

    memset(arg, 0, sizeof(struct threadArg));

    s = natsMutex_Create(&(arg->m));
    if (s == NATS_OK)
        s = natsCondition_Create(&(arg->c));

    return s;
}

void
_destroyDefaultThreadArgs(struct threadArg *args)
{
    if (valgrind)
        nats_Sleep(100);

    natsMutex_Destroy(args->m);
    natsCondition_Destroy(args->c);
}

static void
test_natsNowAndSleep(void)
{
    int64_t start;
    int64_t end;

    test("Check now and sleep: ")
    start = nats_Now();
    nats_Sleep(1000);
    end = nats_Now();
    testCond(((end - start) >= 990) && ((end - start) <= 1010));
}

static void
test_natsAllocSprintf(void)
{
    char smallStr[20];
    char mediumStr[256]; // This is the size of the temp buffer in nats_asprintf
    char largeStr[1024];
    char *ptr = NULL;
    int ret;

    memset(smallStr, 'A', sizeof(smallStr) - 1);
    smallStr[sizeof(smallStr) - 1] = '\0';

    memset(mediumStr, 'B', sizeof(mediumStr) - 1);
    mediumStr[sizeof(mediumStr) - 1] = '\0';

    memset(largeStr, 'C', sizeof(largeStr) - 1);
    largeStr[sizeof(largeStr) - 1] = '\0';

    test("Check alloc sprintf with small string: ");
    ret = nats_asprintf(&ptr, "%s", smallStr);
    testCond((ret >= 0)
             && (strcmp(ptr, smallStr) == 0));

    free(ptr);
    ptr = NULL;

    test("Check alloc sprintf with medium string: ");
    ret = nats_asprintf(&ptr, "%s", mediumStr);
    testCond((ret >= 0)
             && (strcmp(ptr, mediumStr) == 0));

    free(ptr);
    ptr = NULL;

    test("Check alloc sprintf with large string: ");
    ret = nats_asprintf(&ptr, "%s", largeStr);
    testCond((ret >= 0)
             && (strcmp(ptr, largeStr) == 0));

    free(ptr);
    ptr = NULL;
}

static void
test_natsStrCaseStr(void)
{
    const char *s1 = "Hello World!";
    const char *s2 = "wo";
    const char *res = NULL;

    test("StrStr case insensitive (equal): ");
    res = nats_strcasestr(s1, s1);
    testCond((res != NULL)
             && (strcmp(res, s1) == 0)
             && (res == s1));

    test("StrStr case insensitive (match): ");
    res = nats_strcasestr(s1, s2);
    testCond((res != NULL)
             && (strcmp(res, "World!") == 0)
             && (res == (s1 + 6)));

    test("StrStr case insensitive (no match): ");
    res = nats_strcasestr(s1, "xx");
    testCond(res == NULL);
}

static void
test_natsSnprintf(void)
{
#if _WIN32
    // This test is specific to older version of Windows
    // that did not provide snprintf...
    char buf[5];

    test("snprintf over limit: ");
    snprintf(buf, sizeof(buf), "%s", "abcdefghijklmnopqrstuvwxyz");
    testCond(strcmp(buf, "abcd") == 0);
#else
    test("Skip when not running on Windows: ");
    testCond(true);
#endif
}

static void test_natsBuffer(void)
{
    natsStatus  s;
    char        backend[10];
    natsBuffer  *buf = NULL;
    natsBuffer  stackBuf;
    int         oldCapacity = 0;

    printf("== Buffer without data ==\n");

    test("Create buffer owning its data: ");
    s = natsBuf_Create(&buf, 1);
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Capacity(buf) == 1));

    test("Append less than capacity does not expand buffer: ");
    s = natsBuf_Append(buf, "a", 1);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 1)
            && (natsBuf_Capacity(buf) == 1)
            && (natsBuf_Available(buf) == 0));

    test("Appending one more (AppendByte) increases capacity: ");
    oldCapacity = natsBuf_Capacity(buf);
    s = natsBuf_AppendByte(buf, 'b');
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 2)
             && (natsBuf_Capacity(buf) > oldCapacity)
             && (natsBuf_Available(buf) > 0));

    test("Checking content: ");
    testCond((s == NATS_OK)
             && (natsBuf_Data(buf) != NULL)
             && (strncmp(natsBuf_Data(buf), "ab", 2) == 0));

    natsBuf_Destroy(buf);
    buf = NULL;

    oldCapacity = 0;
    test("Appending one more byte increases capacity: ");
    s = natsBuf_Create(&buf, 1);
    IFOK(s, natsBuf_Append(buf, "a", 1));
    if (s == NATS_OK)
    {
        oldCapacity = natsBuf_Capacity(buf);

        // Add one more!
        s = natsBuf_Append(buf, "b", 1);
    }
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 2)
             && (natsBuf_Capacity(buf) > oldCapacity)
             && (natsBuf_Available(buf) > 0));

    natsBuf_Destroy(buf);
    buf = NULL;

    printf("\n== Buffer with data ==\n");

    memset(backend, 0, sizeof(backend));

    test("Create buffer with backend: ");
    s = natsBuf_CreateWithBackend(&buf, backend, 0, 5);
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Capacity(buf) == 5));

    test("Check that changes are reflected in backend")
    s = natsBuf_Append(buf, "abcd", 4);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 4)
            && (natsBuf_Capacity(buf) == 5)
            && (natsBuf_Available(buf) > 0)
            && (strcmp(backend, "abcd") == 0));

    test("Changing backend is reflected in buffer: ");
    testCond((s == NATS_OK)
             && (backend[1] = 'x')
             && (natsBuf_Data(buf)[1] == 'x'));

    test("Append less than capacity does not expand buffer: ");
    s = natsBuf_AppendByte(buf, 'e');
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 5)
            && (natsBuf_Capacity(buf) == 5)
            && (natsBuf_Available(buf) == 0));

    test("Check natsBuf_Expand returns error for invalid arguments: ");
    {
        natsStatus ls;

        ls = natsBuf_Expand(buf, -10);
        if (ls != NATS_OK)
            ls = natsBuf_Expand(buf, 0);
        if (ls != NATS_OK)
            ls = natsBuf_Expand(buf, natsBuf_Capacity(buf));
        testCond(ls != NATS_OK);
    }

    test("Adding more causes expand: ");
    oldCapacity = natsBuf_Capacity(buf);
    s = natsBuf_Append(buf, "fghij", 5);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 10)
            && (natsBuf_Capacity(buf) > oldCapacity));

    test("Check that the backend did not change");
    testCond((s == NATS_OK)
             && (strcmp(backend, "axcde") == 0));

    test("Checking content: ");
    testCond((s == NATS_OK)
             && (natsBuf_Data(buf) != NULL)
             && (strncmp(natsBuf_Data(buf), "axcdefghij", 10) == 0));

    test("Destroying buffer does not affect backend: ");
    natsBuf_Destroy(buf);
    buf = NULL;
    testCond(strcmp(backend, "axcde") == 0);

    printf("\n== Buffer Init without data ==\n");

    test("Create buffer owning its data: ");
    s = natsBuf_Init(&stackBuf, 10);
    testCond((s == NATS_OK)
             && (buf = &stackBuf)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Capacity(buf) == 10));

    test("Append less than capacity does not expand buffer: ");
    s = natsBuf_Append(buf, "abcdefghij", 10);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 10)
            && (natsBuf_Capacity(buf) == 10)
            && (natsBuf_Available(buf) == 0));

    test("Appending one more increases capacity: ");
    oldCapacity = natsBuf_Capacity(buf);
    s = natsBuf_AppendByte(buf, 'k');
    testCond((s == NATS_OK)
              && (natsBuf_Len(buf) == 11)
              && (natsBuf_Capacity(buf) > oldCapacity)
              && (natsBuf_Available(buf) > 0));

    test("Checking content: ");
    testCond((s == NATS_OK)
             && (natsBuf_Data(buf) != NULL)
             && (strncmp(natsBuf_Data(buf), "abcdefghijk", 11) == 0));

    test("Destroying buffer: ");
    natsBuf_Destroy(buf);
    testCond((natsBuf_Data(buf) == NULL)
                && (natsBuf_Len(buf) == 0)
                && (natsBuf_Capacity(buf) == 0)
                && (natsBuf_Available(buf) == 0));
    buf = NULL;

    printf("\n== Buffer Init with data ==\n");

    memset(backend, 0, sizeof(backend));

    test("Create buffer with backend: ");
    s = natsBuf_InitWithBackend(&stackBuf, backend, 0, 5);
    testCond((s == NATS_OK)
             && (buf = &stackBuf)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Capacity(buf) == 5));

    test("Check that changes are reflected in backend: ")
    s = natsBuf_Append(buf, "abcd", 4);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 4)
            && (natsBuf_Capacity(buf) == 5)
            && (natsBuf_Available(buf) > 0)
            && (strcmp(backend, "abcd") == 0));

    test("Changing backend is reflected in buffer: ");
    testCond((s == NATS_OK)
             && (backend[1] = 'x')
             && (natsBuf_Data(buf)[1] == 'x'));

    test("Append less than capacity does not expand buffer: ");
    s = natsBuf_AppendByte(buf, 'e');
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 5)
            && (natsBuf_Capacity(buf) == 5)
            && (natsBuf_Available(buf) == 0));

    test("Adding more causes expand: ");
    s = natsBuf_Append(buf, "fghij", 5);
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 10)
            && (natsBuf_Capacity(buf) >= 10));

    test("Check that the backend did not change");
    testCond((s == NATS_OK)
             && (strcmp(backend, "axcde") == 0));

    test("Checking content: ");
    testCond((s == NATS_OK)
             && (natsBuf_Data(buf) != NULL)
             && (strncmp(natsBuf_Data(buf), "axcdefghij", 10) == 0));

    test("Destroying buffer does not affect backend: ");
    natsBuf_Destroy(buf);
    testCond(strcmp(backend, "axcde") == 0);

    test("Destroyed buffer state is clean: ");
    testCond((s == NATS_OK)
             && (natsBuf_Data(buf) == NULL)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Capacity(buf) == 0)
             && (natsBuf_Available(buf) == 0));

    buf = NULL;

    test("Check maximum size: ");
    s = natsBuf_Create(&buf, 5);
    IFOK(s, natsBuf_Append(buf, "abcd", 4));
    IFOK(s, natsBuf_Append(buf, "fake size that goes over int max size", 0x7FFFFFFC));
    testCond(s == NATS_NO_MEMORY);

    test("Check maximum size (append byte): ");
    buf->len = 0x7FFFFFFE;
    s = natsBuf_Append(buf, "e", 1);
    testCond(s == NATS_NO_MEMORY);

    natsBuf_Destroy(buf);
    buf = NULL;

    test("Consume half: ");
    s = natsBuf_Create(&buf, 10);
    IFOK(s, natsBuf_Append(buf, "abcdefghij", 10));
    if (s == NATS_OK)
        natsBuf_Consume(buf, 5);
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 5)
             && (strncmp(natsBuf_Data(buf), "fghij", 5) == 0)
             && (natsBuf_Available(buf) == 5)
             && (*(buf->pos) == 'f'));

    test("Consume rest: ");
    natsBuf_Consume(buf, 5);
    testCond((s == NATS_OK)
             && (natsBuf_Len(buf) == 0)
             && (natsBuf_Available(buf) == 10)
             && (*(buf->pos) == 'f'));

    natsBuf_Destroy(buf);
    buf = NULL;

    test("MoveTo (forward): ");
    s = natsBuf_Create(&buf, 100);
    if (s == NATS_OK)
    {
        memcpy(natsBuf_Data(buf), "this is a test", 14);
        natsBuf_MoveTo(buf, 14);
        memcpy(natsBuf_Data(buf)+14, " of move by", 11);
        natsBuf_MoveTo(buf, 14+11);
    }
    IFOK(s, natsBuf_AppendByte(buf, '\0'));
    testCond((s == NATS_OK)
                && (natsBuf_Len(buf) == 26)
                && (strcmp(natsBuf_Data(buf), "this is a test of move by") == 0));

    test("MoveTo (backward): ");
    natsBuf_MoveTo(buf, 14);
    s = natsBuf_AppendByte(buf, '\0');
    testCond((s == NATS_OK)
            && (natsBuf_Len(buf) == 15)
            && (strcmp(natsBuf_Data(buf), "this is a test") == 0));

    natsBuf_Destroy(buf);
    buf = NULL;
}

static void
test_natsParseInt64(void)
{
    int64_t n;

    test("Parse with non numeric: ");
    n = nats_ParseInt64("a", 1);
    testCond(n == -1);

    test("Parse with NULL buffer: ");
    n = nats_ParseInt64(NULL, 0);
    testCond(n == -1);

    test("Parse with 0 buffer size: ");
    n = nats_ParseInt64("whatever", 0);
    testCond(n == -1);

    test("Parse with '0': ");
    n = nats_ParseInt64("0", 1);
    testCond(n == 0);

    test("Parse with '1': ");
    n = nats_ParseInt64("1", 1);
    testCond(n == 1);

    test("Parse with '12': ");
    n = nats_ParseInt64("12", 2);
    testCond(n == 12);

    test("Parse with '-12': ");
    n = nats_ParseInt64("-12", 3);
    testCond(n == -1);

    test("Parse with trailing spaces: ");
    n = nats_ParseInt64("12 ", 3);
    testCond(n == -1);

    test("Parse with leading spaces: ");
    n = nats_ParseInt64(" 12", 3);
    testCond(n == -1);

    test("Parse with 'INT64_MAX': ");
    n = nats_ParseInt64("9223372036854775807", 19);
    testCond(n == INT64_MAX);

    test("Parse with overflow(1): ");
    n = nats_ParseInt64("9223372036854775809", 19);
    testCond(n == -1);

    test("Parse with overflow(2): ");
    n = nats_ParseInt64("92233720368547758099223372036854775809", 38);
    testCond(n == -1);

    test("Parse with '12345': ");
    n = nats_ParseInt64("12345", 5);
    testCond(n == 12345);

    test("Parse with '123.45': ");
    n = nats_ParseInt64("123.45", 6);
    testCond(n == -1);
}

static void
test_natsParseControl(void)
{
    natsStatus  s;
    natsControl c;

    c.op   = NULL;
    c.args = NULL;

    test("Test with NULL line: ");
    s = nats_ParseControl(&c, NULL);
    testCond(s == NATS_PROTOCOL_ERROR);

    test("Test line with single op: ");
    s = nats_ParseControl(&c, "op");
    testCond((s == NATS_OK)
             && (c.op != NULL)
             && (strcmp(c.op, "op") == 0)
             && (c.args == NULL));

    free(c.op);
    free(c.args);
    c.op = NULL;
    c.args = NULL;

    test("Test line with trailing spaces: ");
    s = nats_ParseControl(&c, "op   ");
    testCond((s == NATS_OK)
             && (c.op != NULL)
             && (strcmp(c.op, "op") == 0)
             && (c.args == NULL));

    free(c.op);
    free(c.args);
    c.op = NULL;
    c.args = NULL;

    test("Test line with op and args: ");
    s = nats_ParseControl(&c, "op    args");
    testCond((s == NATS_OK)
             && (c.op != NULL)
             && (strcmp(c.op, "op") == 0)
             && (c.args != NULL)
             && (strcmp(c.args, "args") == 0));

    free(c.op);
    free(c.args);
    c.op = NULL;
    c.args = NULL;

    test("Test line with op and args and trailing spaces: ");
    s = nats_ParseControl(&c, "op   args  ");
    testCond((s == NATS_OK)
             && (c.op != NULL)
             && (strcmp(c.op, "op") == 0)
             && (c.args != NULL)
             && (strcmp(c.args, "args") == 0));

    free(c.op);
    free(c.args);
    c.op = NULL;
    c.args = NULL;

    test("Test line with op and args args: ");
    s = nats_ParseControl(&c, "op   args  args   ");
    testCond((s == NATS_OK)
             && (c.op != NULL)
             && (strcmp(c.op, "op") == 0)
             && (c.args != NULL)
             && (strcmp(c.args, "args  args") == 0));

    free(c.op);
    free(c.args);
    c.op = NULL;
    c.args = NULL;
}

static void
test_natsNormalizeErr(void)
{
    char error[1024];
    char expected[256];

    test("Check typical -ERR: ");

    snprintf(expected, sizeof(expected), "%s", "Simple Error");
    snprintf(error, sizeof(error), "-ERR '%s'", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR without quotes: ");
    snprintf(expected, sizeof(expected), "%s", "Error Without Quotes");
    snprintf(error, sizeof(error), "-ERR %s", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR with spaces: ");
    snprintf(expected, sizeof(expected), "%s", "Error With Surrounding Spaces");
    snprintf(error, sizeof(error), "-ERR    '%s'    ", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR with spaces and without quotes: ");
    snprintf(expected, sizeof(expected), "%s", "Error With Surrounding Spaces And Without Quotes");
    snprintf(error, sizeof(error), "-ERR     %s     ", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR with quote on the left: ");
    snprintf(expected, sizeof(expected), "%s", "Error With Quote On Left");
    snprintf(error, sizeof(error), "-ERR '%s", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR with quote on right: ");
    snprintf(expected, sizeof(expected), "%s", "Error With Quote On Right");
    snprintf(error, sizeof(error), "-ERR %s'", expected);
    nats_NormalizeErr(error);
    testCond(strcmp(error, expected) == 0);

    test("Check -ERR with spaces and single quote: ");
    snprintf(error, sizeof(error), "%s", "-ERR      '      ");
    nats_NormalizeErr(error);
    testCond(error[0] == '\0');
}

static void
test_natsMutex(void)
{
    natsStatus  s;
    natsMutex   *m = NULL;
    bool        locked = false;

    test("Create mutex: ");
    s = natsMutex_Create(&m);
    testCond(s == NATS_OK);

    test("Lock: ");
    natsMutex_Lock(m);
    testCond(1);

    test("Recursive locking: ");
    locked = natsMutex_TryLock(m);
    testCond(locked);

    test("Release recursive lock: ");
    natsMutex_Unlock(m);
    testCond(1);

    test("Unlock: ");
    natsMutex_Unlock(m);
    testCond(1);

    test("Destroy: ");
    natsMutex_Destroy(m);
    testCond(1);
}

static void
testThread(void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->control = 1;
    tArg->current = natsThread_IsCurrent(tArg->t);

    natsMutex_Unlock(tArg->m);
}

static void
sumThread(void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->sum++;

    natsMutex_Unlock(tArg->m);
}

static int NUM_THREADS = 1000;

static void
test_natsThread(void)
{
    natsStatus          s  = NATS_OK;
    natsMutex           *m = NULL;
    natsThread          *t = NULL;
    bool                current = false;
    struct threadArg    tArgs;
    natsThread          **threads = NULL;
    int                 i,j;

    if (valgrind)
        NUM_THREADS = 100;

    test("Create threads array: ");
    threads = (natsThread**) calloc(NUM_THREADS, sizeof(natsThread*));
    if (threads == NULL)
        s = NATS_NO_MEMORY;
    IFOK(s, natsMutex_Create(&m));
    testCond(s == NATS_OK);

    natsMutex_Lock(m);

    tArgs.m         = m;
    tArgs.control   = 0;
    tArgs.current   = false;

    test("Create thread: ");
    s = natsThread_Create(&t, testThread, &tArgs);
    testCond(s == NATS_OK);

    tArgs.t = t;

    test("Check if thread current from other thread: ");
    current = natsThread_IsCurrent(t);
    testCond(!current);

    natsMutex_Unlock(m);

    test("Joining thread: ")
    natsThread_Join(t);
    testCond(1);

    natsMutex_Lock(m);

    test("Control updated: ");
    testCond(tArgs.control == 1);

    test("Check thread current works from current thread: ");
    testCond(tArgs.current);

    test("Destroy thread: ");
    natsThread_Destroy(t);
    testCond(1);

    tArgs.sum = 0;

    test("Creating multiple threads: ");
    for (i=0; (s == NATS_OK) && (i<NUM_THREADS); i++)
        s = natsThread_Create(&(threads[i]), sumThread, &tArgs);
    testCond(s == NATS_OK);

    if (s != NATS_OK)
        i--;

    natsMutex_Unlock(m);

    test("Waiting all done: ");
    for (j=0; j<i; j++)
    {
        natsThread_Join(threads[j]);
        natsThread_Destroy(threads[j]);
    }
    testCond(s == NATS_OK);

    test("Checking sum: ");
    testCond((s == NATS_OK) && (tArgs.sum == NUM_THREADS));

    natsMutex_Destroy(m);

    free(threads);
}

static void
testSignal(void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->control = 1;
    natsCondition_Signal(tArg->c);

    natsMutex_Unlock(tArg->m);
}

static void
testBroadcast(void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->sum++;
    natsCondition_Signal(tArg->c);

    while (tArg->control == 0)
        natsCondition_Wait(tArg->b, tArg->m);

    tArg->sum--;

    natsMutex_Unlock(tArg->m);
}

static void
_unblockLongWait(void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    nats_Sleep(500);
    natsMutex_Lock(args->m);
    natsCondition_Signal(args->c);
    natsMutex_Unlock(args->m);
}

static void
test_natsCondition(void)
{
    natsStatus          s;
    natsMutex           *m  = NULL;
    natsThread          *t1 = NULL;
    natsThread          *t2 = NULL;
    natsCondition       *c1 = NULL;
    natsCondition       *c2 = NULL;
    struct threadArg    tArgs;
    int64_t             before = 0;
    int64_t             diff   = 0;
    int64_t             target = 0;

    test("Create mutex: ");
    s = natsMutex_Create(&m);
    testCond(s == NATS_OK);

    test("Create condition variables: ");
    s = natsCondition_Create(&c1);
    IFOK(s, natsCondition_Create(&c2));
    testCond(s == NATS_OK);

    natsMutex_Lock(m);

    tArgs.m       = m;
    tArgs.c       = c1;
    tArgs.control = 0;

    test("Create thread: ");
    s = natsThread_Create(&t1, testSignal, &tArgs);
    testCond(s == NATS_OK);

    test("Wait for signal: ");
    while (tArgs.control != 1)
        natsCondition_Wait(c1, m);

    natsThread_Join(t1);
    natsThread_Destroy(t1);
    t1 = NULL;
    testCond(tArgs.control == 1);

    test("Wait timeout: ");
    before = nats_Now();
    s = natsCondition_TimedWait(c1, m, 1000);
    diff = (nats_Now() - before);
    testCond((s == NATS_TIMEOUT)
             && (diff >= 985) && (diff <= 1015));

    test("Wait timeout with 0: ");
    before = nats_Now();
    s = natsCondition_TimedWait(c1, m, 0);
    diff = (nats_Now() - before);
    testCond((s == NATS_TIMEOUT)
             && (diff >= 0) && (diff <= 10));

    test("Wait timeout with negative: ");
    before = nats_Now();
    s = natsCondition_TimedWait(c1, m, -10);
    diff = (nats_Now() - before);
    testCond((s == NATS_TIMEOUT)
             && (diff >= 0) && (diff <= 10));

    test("Wait absolute time: ");
    before = nats_Now();
    target = nats_setTargetTime(1000);
    s = natsCondition_AbsoluteTimedWait(c1, m, target);
    diff = (nats_Now() - before);
    testCond((s == NATS_TIMEOUT)
             && (diff >= 985) && (diff <= 1015));

    test("Wait absolute time in the past: ");
    before = nats_Now();
    target = nats_setTargetTime(-1000);
    s = natsCondition_AbsoluteTimedWait(c1, m, target);
    diff = (nats_Now() - before);
    testCond((s == NATS_TIMEOUT)
             && (diff >= 0) && (diff <= 10));

    test("Wait absolute time with very large value: ");
    tArgs.control = 0;
    s = natsThread_Create(&t1, _unblockLongWait, &tArgs);
    if (s == NATS_OK)
    {
        before = nats_Now();
        target = nats_setTargetTime(0x7FFFFFFFFFFFFFFF);
        s = natsCondition_AbsoluteTimedWait(c1, m, target);
        diff = (nats_Now() - before);
    }
    testCond((s == NATS_OK)
             && (diff >= 400) && (diff <= 600));

    natsThread_Join(t1);
    natsThread_Destroy(t1);
    t1 = NULL;

    test("Signal before wait: ");
    tArgs.control = 0;

    test("Create thread: ");
    s = natsThread_Create(&t1, testSignal, &tArgs);
    testCond(s == NATS_OK);

    while (tArgs.control == 0)
    {
        natsMutex_Unlock(m);
        nats_Sleep(1000);
        natsMutex_Lock(m);
    }

    s = natsCondition_TimedWait(c1, m, 1000);
    testCond(s == NATS_TIMEOUT);

    natsThread_Join(t1);
    natsThread_Destroy(t1);
    t1 = NULL;

    test("Broadcast: ");
    tArgs.control = 0;
    tArgs.sum     = 0;
    tArgs.b       = c2;

    s = natsThread_Create(&t1, testBroadcast, &tArgs);
    IFOK(s, natsThread_Create(&t2, testBroadcast, &tArgs));
    if (s != NATS_OK)
    {
        natsMutex_Unlock(m);
        FAIL("Unable to run test_natsCondition because got an error while creating thread!");
    }

    while (tArgs.sum != 2)
        natsCondition_Wait(c1, m);

    natsMutex_Unlock(m);

    nats_Sleep(1000);

    natsMutex_Lock(m);

    tArgs.control = 1;
    natsCondition_Broadcast(c2);

    natsMutex_Unlock(m);

    natsThread_Join(t1);
    natsThread_Destroy(t1);
    t1 = NULL;

    natsThread_Join(t2);
    natsThread_Destroy(t2);
    t2 = NULL;

    testCond(tArgs.sum == 0);

    test("Destroy condition: ");
    natsCondition_Destroy(c1);
    natsCondition_Destroy(c2);
    testCond(1);

    natsMutex_Destroy(m);
}

static void
testTimerCb(natsTimer *timer, void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->timerFired++;
    natsCondition_Signal(tArg->c);

    natsMutex_Unlock(tArg->m);

    if (tArg->control == 1)
        natsTimer_Reset(timer, 500);
    else if (tArg->control == 2)
        natsTimer_Stop(timer);
    else if (tArg->control == 3)
        nats_Sleep(500);

    natsMutex_Lock(tArg->m);

    natsCondition_Signal(tArg->c);

    natsMutex_Unlock(tArg->m);
}

static void
stopTimerCb(natsTimer *timer, void *arg)
{
    struct threadArg *tArg = (struct threadArg*) arg;

    natsMutex_Lock(tArg->m);

    tArg->timerStopped++;
    natsCondition_Signal(tArg->c);

    natsMutex_Unlock(tArg->m);
}

static void
_dummyTimerCB(natsTimer *timer, void *arg) {}

static void
_timerStopCB(natsTimer *timer, void *arg)
{
    natsTimer_Release(timer);
}

#define STOP_TIMER_AND_WAIT_STOPPED \
        natsTimer_Stop(t); \
        natsMutex_Lock(tArg.m); \
        while (tArg.timerStopped == 0) \
            natsCondition_Wait(tArg.c, tArg.m); \
        natsMutex_Unlock(tArg.m)

static void
test_natsTimer(void)
{
    natsStatus          s;
    natsTimer           *t = NULL;
    struct threadArg    tArg;
    int                 refs;

    test("Setup test: ");
    s = _createDefaultThreadArgsForCbTests(&tArg);
    testCond(s == NATS_OK);

    tArg.control      = 0;
    tArg.timerFired   = 0;
    tArg.timerStopped = 0;

    test("Create timer: ");
    s = natsTimer_Create(&t, testTimerCb, stopTimerCb, 400, &tArg);
    testCond(s == NATS_OK);

    test("Stop timer: ");
    tArg.control = 0;
    natsTimer_Stop(t);
    nats_Sleep(600);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 0)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    test("Firing of timer: ")
    tArg.control      = 0;
    tArg.timerStopped = 0;
    natsTimer_Reset(t, 200);
    nats_Sleep(1100);
    natsTimer_Stop(t);
    nats_Sleep(600);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired > 0)
             && (tArg.timerFired <= 5)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    test("Stop stopped timer: ");
    tArg.control      = 0;
    tArg.timerFired   = 0;
    tArg.timerStopped = 0;
    natsTimer_Reset(t, 100);
    nats_Sleep(300);
    natsTimer_Stop(t);
    nats_Sleep(100);
    natsTimer_Stop(t);
    nats_Sleep(100);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired > 0)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    tArg.control      = 1;
    tArg.timerFired   = 0;
    tArg.timerStopped = 0;
    test("Reset from callback: ");
    natsTimer_Reset(t, 250);
    nats_Sleep(900);
    natsTimer_Stop(t);
    nats_Sleep(600);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 2)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && nats_getTimersCount() == 0);
    natsMutex_Unlock(tArg.m);

    tArg.control      = 0;
    tArg.timerFired   = 0;
    tArg.timerStopped = 0;
    test("Multiple Reset: ");
    natsTimer_Reset(t, 1000);
    natsTimer_Reset(t, 800);
    natsTimer_Reset(t, 200);
    natsTimer_Reset(t, 500);
    nats_Sleep(600);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 1)
             && (tArg.timerStopped == 0)
             && (refs == 1)
             && nats_getTimersCount() == 1);
    natsMutex_Unlock(tArg.m);

    STOP_TIMER_AND_WAIT_STOPPED;

    tArg.control      = 3;
    tArg.timerFired   = 0;
    tArg.timerStopped = 0;
    test("Check refs while in callback: ");
    natsTimer_Reset(t, 1);

    // Wait that it is in callback
    natsMutex_Lock(tArg.m);
    while (tArg.timerFired != 1)
        natsCondition_Wait(tArg.c, tArg.m);
    natsMutex_Unlock(tArg.m);

    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    testCond((refs == 2)
             && nats_getTimersCountInList() == 0
             && nats_getTimersCount() == 1);

    STOP_TIMER_AND_WAIT_STOPPED;

    tArg.control        = 2;
    tArg.timerFired     = 0;
    tArg.timerStopped   = 0;
    test("Stop from callback: ");
    natsTimer_Reset(t, 250);
    nats_Sleep(500);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 1)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    tArg.control        = 3;
    tArg.timerFired     = 0;
    tArg.timerStopped   = 0;
    test("Slow callback: ");
    natsTimer_Reset(t, 100);
    nats_Sleep(800);
    natsTimer_Stop(t);
    nats_Sleep(500);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired <= 3)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    tArg.control        = 3;
    tArg.timerFired     = 0;
    tArg.timerStopped   = 0;
    test("Stopped while in callback: ");
    natsTimer_Reset(t, 100);
    nats_Sleep(200);
    natsTimer_Stop(t);
    nats_Sleep(700);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 1)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    tArg.control        = 4;
    tArg.timerFired     = 0;
    tArg.timerStopped   = 0;
    test("Use very large timeout: ");
    natsTimer_Reset(t, 0x7FFFFFFFFFFFFFFF);
    nats_Sleep(200);
    natsTimer_Stop(t);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    natsMutex_Lock(tArg.m);
    testCond((tArg.timerFired == 0)
             && (tArg.timerStopped == 1)
             && (refs == 1)
             && (nats_getTimersCount() == 0));
    natsMutex_Unlock(tArg.m);

    test("Destroy timer: ");
    natsMutex_Lock(t->mu);
    t->refs++;
    natsMutex_Unlock(t->mu);
    natsTimer_Destroy(t);
    natsMutex_Lock(t->mu);
    refs = t->refs;
    natsMutex_Unlock(t->mu);
    testCond(refs == 1);
    natsTimer_Release(t);

    _destroyDefaultThreadArgs(&tArg);

    // Create a timer that will not be stopped here to exercise
    // code that cleans up timers when library is unloaded.
    test("Create timer: ");
    s = natsTimer_Create(&t, _dummyTimerCB, _timerStopCB, 1000, NULL);
    testCond(s == NATS_OK);
}

static void
test_natsUrl(void)
{
    natsStatus  s;
    natsUrl     *u = NULL;

    test("NULL: ");
    s = natsUrl_Create(&u, NULL);
    testCond((s != NATS_OK) && (u == NULL));

    test("EMPTY: ");
    s = natsUrl_Create(&u, "");
    testCond((s != NATS_OK) && (u == NULL));

    nats_clearLastError();

    test("'tcp://localhost:4222':");
    s = natsUrl_Create(&u, "tcp://localhost:4222");
    testCond((s == NATS_OK)
            && (u != NULL)
            && (u->host != NULL)
            && (strcmp(u->host, "localhost") == 0)
            && (u->username == NULL)
            && (u->password == NULL)
            && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://localhost':");
    s = natsUrl_Create(&u, "tcp://localhost");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'localhost':");
    s = natsUrl_Create(&u, "localhost");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://[::1]:4222':");
    s = natsUrl_Create(&u, "tcp://[::1]:4222");
    testCond((s == NATS_OK)
            && (u != NULL)
            && (u->host != NULL)
            && (strcmp(u->host, "[::1]") == 0)
            && (u->username == NULL)
            && (u->password == NULL)
            && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://[::1]:':");
    s = natsUrl_Create(&u, "tcp://[::1]:");
    testCond((s == NATS_OK)
            && (u != NULL)
            && (u->host != NULL)
            && (strcmp(u->host, "[::1]") == 0)
            && (u->username == NULL)
            && (u->password == NULL)
            && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://[::1]':");
    s = natsUrl_Create(&u, "tcp://[::1]");
    testCond((s == NATS_OK)
            && (u != NULL)
            && (u->host != NULL)
            && (strcmp(u->host, "[::1]") == 0)
            && (u->username == NULL)
            && (u->password == NULL)
            && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://':");
    s = natsUrl_Create(&u, "tcp://");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://:':");
    s = natsUrl_Create(&u, "tcp://:");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://ivan:localhost:4222':");
    s = natsUrl_Create(&u, "tcp://ivan:localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "ivan:localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://ivan:pwd:localhost:4222':");
    s = natsUrl_Create(&u, "tcp://ivan:pwd:localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "ivan:pwd:localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://ivan@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://ivan@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "ivan") == 0)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://ivan:pwd@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://ivan:pwd@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "ivan") == 0)
              && (u->password != NULL)
              && (strcmp(u->password, "pwd") == 0)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://@@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://@@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "@") == 0)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://a:b:c@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://a:b:c@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "a") == 0)
              && (u->password != NULL)
              && (strcmp(u->password, "b:c") == 0)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://::a:b:c@localhost:4222':");
    s = natsUrl_Create(&u, "tcp://::a:b:c@localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password != NULL)
              && (strcmp(u->password, ":a:b:c") == 0)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://a:b@[::1]:4222':");
    s = natsUrl_Create(&u, "tcp://a:b@[::1]:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "[::1]") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "a") == 0)
              && (u->password != NULL)
              && (strcmp(u->password, "b") == 0)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://a@[::1]:4222':");
    s = natsUrl_Create(&u, "tcp://a@[::1]:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "[::1]") == 0)
              && (u->username != NULL)
              && (strcmp(u->username, "a") == 0)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("' tcp://localhost:4222':");
    s = natsUrl_Create(&u, " tcp://localhost:4222");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://localhost:4222 ':");
    s = natsUrl_Create(&u, "tcp://localhost:4222 ");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("' tcp://localhost:4222 ':");
    s = natsUrl_Create(&u, " tcp://localhost:4222 ");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'nats://localhost:4222/subject':");
    s = natsUrl_Create(&u, " nats://localhost:4222/subject");
    testCond((s == NATS_OK)
              && (u != NULL)
              && (u->host != NULL)
              && (strcmp(u->host, "localhost") == 0)
              && (u->username == NULL)
              && (u->password == NULL)
              && (u->port == 4222));
    natsUrl_Destroy(u);
    u = NULL;

    test("'tcp://localhost: 4222':");
    s = natsUrl_Create(&u, "tcp://localhost: 4222");
    testCond((s == NATS_INVALID_ARG)
              && (u == NULL)
              && (strstr(nats_GetLastError(NULL), "invalid port ' 4222'") != NULL));
    nats_clearLastError();

    test("'tcp://localhost:a4222':");
    s = natsUrl_Create(&u, "tcp://localhost:a4222");
    testCond((s == NATS_INVALID_ARG)
              && (u == NULL)
              && (strstr(nats_GetLastError(NULL), "invalid port 'a4222'") != NULL));
    nats_clearLastError();

    test("'tcp://localhost:2147483648':");
    s = natsUrl_Create(&u, "tcp://localhost:2147483648");
    testCond((s == NATS_INVALID_ARG)
              && (u == NULL)
              && (strstr(nats_GetLastError(NULL), "invalid port '2147483648'") != NULL));
}

static void
test_natsCreateStringFromBuffer(void)
{
    natsStatus  s = NATS_OK;
    natsBuffer  buf;
    char        *str = NULL;

    test("NULL buffer: ");
    s = nats_CreateStringFromBuffer(&str, NULL);
    testCond((s == NATS_OK)
             && (str == NULL))

    test("Init buffer: ");
    s = natsBuf_Init(&buf, 10);
    testCond(s == NATS_OK);

    test("Empty buffer: ");
    s = nats_CreateStringFromBuffer(&str, &buf);
    testCond((s == NATS_OK)
             && (str == NULL))

    test("Append to buf: ");
    s = natsBuf_Append(&buf, "123", 3);
    testCond(s == NATS_OK);

    test("Buffer containing '123': ");
    s = nats_CreateStringFromBuffer(&str, &buf);
    testCond((s == NATS_OK)
             && (str != NULL)
             && (strlen(str) == 3)
             && (strcmp(str, "123") == 0));

    test("Destroying the buffer does not affect the created string: ");
    natsBuf_Cleanup(&buf);
    testCond((str != NULL)
             && (strlen(str) == 3)
             && (strcmp(str, "123") == 0));

    free(str);
}

#define INBOX_THREADS_COUNT     (10)
#define INBOX_COUNT_PER_THREAD  (10000)
#define INBOX_TOTAL             (INBOX_THREADS_COUNT * INBOX_COUNT_PER_THREAD)

static void
_testInbox(void *closure)
{
    struct threadArg    *args = (struct threadArg*) closure;
    natsStatus          s     = NATS_OK;
    natsInbox           *inbox;
    void                *oldValue;

    for (int i=0; (s == NATS_OK) && (i<INBOX_COUNT_PER_THREAD); i++)
    {
        inbox    = NULL;
        oldValue = NULL;

        s = natsInbox_Create(&inbox);
        if (s == NATS_OK)
            s = natsStrHash_Set(args->inboxes, inbox, true, (void*) 1, (void**) &oldValue);
        if ((s == NATS_OK) && (oldValue != NULL))
        {
            printf("Duplicate inbox: %s\n", inbox);
            s = NATS_ERR;
        }

        natsInbox_Destroy(inbox);
    }

    args->status = s;
}

static void
test_natsInbox(void)
{
    natsStatus          s      = NATS_OK;
    natsThread          *threads[INBOX_THREADS_COUNT];
    struct threadArg    args[INBOX_THREADS_COUNT];
    int                 i, j;
    natsInbox           *key;
    void                *oldInbox;
    natsStrHash         *inboxes = NULL;
    natsStrHashIter     iter;

    test("Test inboxes are unique: ");
    for (i=0; i<INBOX_THREADS_COUNT; i++)
    {
        args[i].status = NATS_OK;
        args[i].inboxes    = NULL;
        threads[i]         = NULL;
    }

    s = natsStrHash_Create(&inboxes, 16);

    for (i=0; (s == NATS_OK) && (i<INBOX_THREADS_COUNT); i++)
    {
        s = natsStrHash_Create(&(args[i].inboxes), 16);
        if (s == NATS_OK)
            s = natsThread_Create(&(threads[i]), _testInbox, &(args[i]));
    }

    for (i=0; (i<INBOX_THREADS_COUNT); i++)
    {
        natsThread_Join(threads[i]);

        if (s == NATS_OK)
            s = args[i].status;
        if (s == NATS_OK)
        {
            j = 0;

            natsStrHashIter_Init(&iter, args[i].inboxes);
            while ((s == NATS_OK)
                    && natsStrHashIter_Next(&iter, &key, NULL))
            {
                j++;

                s = natsStrHash_Set(inboxes, key, true, (void*) 1, &oldInbox);

                natsStrHashIter_RemoveCurrent(&iter);
            }

            if (j != INBOX_COUNT_PER_THREAD)
                s = NATS_ERR;

            natsStrHashIter_Done(&iter);
        }

        natsThread_Destroy(threads[i]);
    }
    testCond(s == NATS_OK);

    for (i=0; i<INBOX_THREADS_COUNT; i++)
        natsStrHash_Destroy(args[i].inboxes);

    natsStrHash_Destroy(inboxes);
}

static int HASH_ITER = 10000000;

static void
test_natsHashing(void)
{
    const char *keys[] = {"foo",
                          "bar",
                          "apcera.continuum.router.foo.bar",
                          "apcera.continuum.router.foo.bar.baz"};
    const char *longKey = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()";

    uint32_t results[] = {1058908168, 1061739001, 4242539713, 3332038527};
    uint32_t r = 0;
    uint32_t lr = 0;
    natsStatus s = NATS_OK;
    int64_t start, end;
    int sizeLongKey = (int) strlen(longKey);

    if (valgrind)
        HASH_ITER = 10000;

    test("Test hashing algo: ");
    for (int i=0; i<(int)(sizeof(keys)/sizeof(char*)); i++)
    {
        r = natsStrHash_Hash(keys[i], (int) strlen(keys[i]));
        if (r != results[i])
        {
            printf("Expected: %u got: %u\n", results[i], r);
            s = NATS_ERR;
            break;
        }
    }
    testCond(s == NATS_OK);

    test("Hashing performance: ");
    s = NATS_OK;
    start = nats_Now();
    for (int i=0; i<HASH_ITER; i++)
    {
        r = natsStrHash_Hash(longKey, sizeLongKey);
        if ((i > 0) && (r != lr))
        {
            s = NATS_ERR;
            break;
        }
        lr = r;
    }
    end = nats_Now();
    testCond((s == NATS_OK) && ((end - start) < 1000));
}

static void
test_natsHash(void)
{
    natsStatus  s;
    natsHash    *hash = NULL;
    const char  *t1 = "this is a test";
    const char  *t2 = "this is another test";
    void        *oldval = NULL;
    int         lastNumBkts = 0;
    int         i;
    int64_t     key;
    int         values[40];
    natsHashIter iter;

    for (int i=0; i<40; i++)
        values[i] = (i+1);

    test("Create hash with invalid 0 size: ");
    s = natsHash_Create(&hash, 0);
    testCond((s != NATS_OK) && (hash == NULL));

    test("Create hash with invalid negative size: ");
    s = natsHash_Create(&hash, -2);
    testCond((s != NATS_OK) && (hash == NULL));

    nats_clearLastError();

    test("Create hash ok: ");
    s = natsHash_Create(&hash, 7);
    testCond((s == NATS_OK) && (hash != NULL) && (hash->used == 0)
             && (hash->numBkts == 8));

    test("Set: ");
    s = natsHash_Set(hash, 1234, (void*) t1, &oldval);
    testCond((s == NATS_OK) && (oldval == NULL) && (hash->used == 1));

    test("Set, get old value: ");
    s = natsHash_Set(hash, 1234, (void*) t2, &oldval);
    testCond((s == NATS_OK) && (oldval == t1) && (hash->used == 1))

    test("Get, not found: ");
    oldval = NULL;
    oldval = natsHash_Get(hash, 3456);
    testCond(oldval == NULL);

    test("Get, found: ");
    oldval = NULL;
    oldval = natsHash_Get(hash, 1234);
    testCond(oldval == t2);

    test("Remove, not found: ");
    oldval = NULL;
    oldval = natsHash_Remove(hash, 3456);
    testCond(oldval == NULL);

    test("Remove, found: ");
    oldval = NULL;
    oldval = natsHash_Remove(hash, 1234);
    testCond((oldval == t2) && (hash->used == 0));

    test("Test collision: ");
    oldval = NULL;
    s = natsHash_Set(hash, 2, (void*) t1, &oldval);
    if ((s == NATS_OK) && (oldval == NULL))
        s = natsHash_Set(hash, 10, (void*) t2, &oldval);
    testCond((s == NATS_OK)
             && (oldval == NULL)
             && (hash->used == 2)
             && (hash->bkts[2] != NULL)
             && (hash->bkts[2]->key == 10)
             && (hash->bkts[2]->next != NULL)
             && (hash->bkts[2]->next->key == 2));

    test("Remove from collisions (front to back): ");
    oldval = NULL;
    oldval = natsHash_Remove(hash, 10);
    if (oldval != t2)
        s = NATS_ERR;
    if (s == NATS_OK)
    {
        oldval = natsHash_Remove(hash, 2);
        if (oldval != t1)
            s = NATS_ERR;
    }
    testCond((s == NATS_OK) && (hash->used == 0));

    test("Remove from collisions (back to front): ");
    oldval = NULL;
    s = natsHash_Set(hash, 2, (void*) t1, &oldval);
    if ((s == NATS_OK) && (oldval == NULL))
        s = natsHash_Set(hash, 10, (void*) t2, &oldval);
    if (s == NATS_OK)
    {
        oldval = natsHash_Remove(hash, 2);
        if (oldval != t1)
            s = NATS_ERR;
    }
    if (s == NATS_OK)
    {
        oldval = natsHash_Remove(hash, 10);
        if (oldval != t2)
            s = NATS_ERR;
    }
    testCond((s == NATS_OK) && (hash->used == 0));

    test("Grow: ");
    for (int i=0; i<40; i++)
    {
        s = natsHash_Set(hash, (i+1), &(values[i]), &oldval);
        if (oldval != NULL)
            s = NATS_ERR;
        if (s != NATS_OK)
            break;
    }
    if (s == NATS_OK)
    {
        for (int i=0; i<40; i++)
        {
            oldval = natsHash_Get(hash, (i+1));
            if ((oldval == NULL)
                || ((*(int*)oldval) != values[i]))
            {
                s = NATS_ERR;
                break;
            }
        }
    }
    testCond((s == NATS_OK)
             && (hash->used == 40)
             && (hash->numBkts > 8));
    lastNumBkts = hash->numBkts;

    test("Shrink: ");
    for (int i=0; i<31; i++)
    {
        oldval = natsHash_Remove(hash, (i+1));
        if ((oldval == NULL)
            || ((*(int*)oldval) != values[i]))
        {
            s = NATS_ERR;
            break;
        }
    }
    testCond((s == NATS_OK)
             && (hash->used == 9)
             && (hash->numBkts < lastNumBkts));

    test("Iterator: ");
    natsHashIter_Init(&iter, hash);
    i = 0;
    while (natsHashIter_Next(&iter, &key, &oldval))
    {
        i++;
        if (((key < 31) || (key > 40))
            || (oldval == NULL)
            || ((*(int*)oldval) != values[key-1]))
        {
            s = NATS_ERR;
            break;
        }
    }
    natsHashIter_Done(&iter);
    testCond((s == NATS_OK) && (i == natsHash_Count(hash)));

    test("Iterator, remove current: ");
    natsHashIter_Init(&iter, hash);
    while (natsHashIter_Next(&iter, &key, NULL))
    {
        s = natsHashIter_RemoveCurrent(&iter);
        if (s != NATS_OK)
            break;
    }
    testCond((s == NATS_OK)
             && (natsHash_Count(hash) == 0)
             && (hash->canResize == false)
             && (hash->numBkts > 8));

    natsHashIter_Done(&iter);

    test("Grow again: ");
    oldval = NULL;
    for (int i=0; i<40; i++)
    {
        s = natsHash_Set(hash, (i+1), &(values[i]), &oldval);
        if (oldval != NULL)
            s = NATS_ERR;
        if (s != NATS_OK)
            break;
    }
    testCond((s == NATS_OK)
             && (hash->used == 40)
             && (hash->numBkts > 8));
    lastNumBkts = hash->numBkts;

    test("Iterator, remove current, hash does not shrink: ");
    natsHashIter_Init(&iter, hash);
    i = 0;
    while (natsHashIter_Next(&iter, &key, NULL))
    {
        s = natsHashIter_RemoveCurrent(&iter);
        if ((s != NATS_OK) || (++i == 31))
            break;
    }
    testCond((s == NATS_OK)
             && (natsHash_Count(hash) == 9)
             && (hash->canResize == false)
             && (hash->numBkts == lastNumBkts));

    natsHashIter_Done(&iter);

    test("After iterator done, shrink works: ");
    oldval = NULL;
    s = natsHash_Set(hash, 100, (void*) "last", &oldval);
    if ((s == NATS_OK) && (oldval == NULL))
    {
        oldval = natsHash_Remove(hash, 100);
        if ((oldval == NULL)
            || (strcmp((const char*) oldval, "last") != 0))
        {
            s = NATS_ERR;
        }
    }
    testCond((s == NATS_OK)
             && hash->canResize
             && (hash->numBkts != lastNumBkts));

    test("Destroy: ");
    natsHash_Destroy(hash);
    testCond(1);
}

static void
test_natsStrHash(void)
{
    natsStatus  s;
    natsStrHash *hash = NULL;
    const char  *t1 = "this is a test";
    const char  *t2 = "this is another test";
    void        *oldval = NULL;
    char        *myKey = NULL;
    int         lastNumBkts = 0;
    int         i;
    char        *key;
    int         values[40];
    char        k[64];
    uint32_t    hk;
    natsStrHashIter iter;

    for (int i=0; i<40; i++)
        values[i] = (i+1);

    test("Create hash with invalid 0 size: ");
    s = natsStrHash_Create(&hash, 0);
    testCond((s != NATS_OK) && (hash == NULL));

    test("Create hash with invalid negative size: ");
    s = natsStrHash_Create(&hash, -2);
    testCond((s != NATS_OK) && (hash == NULL));

    nats_clearLastError();

    test("Create hash ok: ");
    s = natsStrHash_Create(&hash, 7);
    testCond((s == NATS_OK) && (hash != NULL) && (hash->used == 0)
             && (hash->numBkts == 8));

    test("Set: ");
    s = natsStrHash_Set(hash, (char*) "1234", false, (void*) t1, &oldval);
    testCond((s == NATS_OK) && (oldval == NULL) && (hash->used == 1));

    test("Set, get old value: ");
    s = natsStrHash_Set(hash, (char*) "1234", false, (void*) t2, &oldval);
    testCond((s == NATS_OK) && (oldval == t1) && (hash->used == 1))

    test("Get, not found: ");
    oldval = NULL;
    oldval = natsStrHash_Get(hash, (char*) "3456");
    testCond(oldval == NULL);

    test("Get, found: ");
    oldval = NULL;
    oldval = natsStrHash_Get(hash, (char*) "1234");
    testCond(oldval == t2);

    test("Remove, not found: ");
    oldval = NULL;
    oldval = natsStrHash_Remove(hash, (char*) "3456");
    testCond(oldval == NULL);

    test("Remove, found: ");
    oldval = NULL;
    oldval = natsStrHash_Remove(hash, (char*) "1234");
    testCond((oldval == t2) && (hash->used == 0));

    test("Grow: ");
    for (int i=0; i<40; i++)
    {
        snprintf(k, sizeof(k), "%d", (i+1));
        s = natsStrHash_Set(hash, k, true, &(values[i]), &oldval);
        if (oldval != NULL)
            s = NATS_ERR;
        if (s != NATS_OK)
            break;
    }
    if (s == NATS_OK)
    {
        for (int i=0; i<40; i++)
        {
            snprintf(k, sizeof(k), "%d", (i+1));
            oldval = natsStrHash_Get(hash, k);
            if ((oldval == NULL)
                || ((*(int*)oldval) != values[i]))
            {
                s = NATS_ERR;
                break;
            }
        }
    }
    testCond((s == NATS_OK)
             && (hash->used == 40)
             && (hash->numBkts > 8));
    lastNumBkts = hash->numBkts;

    test("Shrink: ");
    for (int i=0; i<31; i++)
    {
        snprintf(k, sizeof(k), "%d", (i+1));
        oldval = natsStrHash_Remove(hash, k);
        if ((oldval == NULL)
            || ((*(int*)oldval) != values[i]))
        {
            s = NATS_ERR;
            break;
        }
    }
    testCond((s == NATS_OK)
             && (hash->used == 9)
             && (hash->numBkts < lastNumBkts));

    test("Iterator: ");
    natsStrHashIter_Init(&iter, hash);
    i = 0;
    while (natsStrHashIter_Next(&iter, &key, &oldval))
    {
        i++;
        if (((atoi(key) < 31) || (atoi(key) > 40))
            || (oldval == NULL)
            || ((*(int*)oldval) != values[atoi(key)-1]))
        {
            s = NATS_ERR;
            break;
        }
    }
    natsStrHashIter_Done(&iter);
    testCond((s == NATS_OK) && (i == natsStrHash_Count(hash)));

    test("Iterator, remove current: ");
    natsStrHashIter_Init(&iter, hash);
    while (natsStrHashIter_Next(&iter, &key, NULL))
    {
        s = natsStrHashIter_RemoveCurrent(&iter);
        if (s != NATS_OK)
            break;
    }
    testCond((s == NATS_OK)
             && (natsStrHash_Count(hash) == 0)
             && (hash->canResize == false)
             && (hash->numBkts > 8));

    natsStrHashIter_Done(&iter);

    test("Grow again: ");
    oldval = NULL;
    for (int i=0; i<40; i++)
    {
        snprintf(k, sizeof(k), "%d", (i+1));
        s = natsStrHash_Set(hash, k, true, &(values[i]), &oldval);
        if (oldval != NULL)
            s = NATS_ERR;
        if (s != NATS_OK)
            break;
    }
    testCond((s == NATS_OK)
             && (hash->used == 40)
             && (hash->numBkts > 8));
    lastNumBkts = hash->numBkts;

    test("Iterator, remove current, hash does not shrink: ");
    natsStrHashIter_Init(&iter, hash);
    i = 0;
    while (natsStrHashIter_Next(&iter, &key, NULL))
    {
        s = natsStrHashIter_RemoveCurrent(&iter);
        if ((s != NATS_OK) || (++i == 31))
            break;
    }
    testCond((s == NATS_OK)
             && (natsStrHash_Count(hash) == 9)
             && (hash->canResize == false)
             && (hash->numBkts == lastNumBkts));

    natsStrHashIter_Done(&iter);

    test("After iterator done, shrink works: ");
    oldval = NULL;
    s = natsStrHash_Set(hash, (char*) "100", true, (void*) "last", &oldval);
    if ((s == NATS_OK) && (oldval == NULL))
    {
        oldval = natsStrHash_Remove(hash, (char*) "100");
        if ((oldval == NULL)
            || (strcmp((const char*) oldval, "last") != 0))
        {
            s = NATS_ERR;
        }
    }
    testCond((s == NATS_OK)
             && hash->canResize
             && (hash->numBkts != lastNumBkts));

    test("Copy key: ");
    snprintf(k, sizeof(k), "%s", "keycopied");
    hk = natsStrHash_Hash(k, (int) strlen(k));
    s = natsStrHash_Set(hash, k, true, (void*) t1, &oldval);
    if (s == NATS_OK)
    {
        // Changing the key does not affect the hash
        snprintf(k, sizeof(k), "%s", "keychanged");
        if (natsStrHash_Get(hash, (char*) "keycopied") != t1)
            s = NATS_ERR;
    }
    testCond((s == NATS_OK)
              && (oldval == NULL)
              && (hash->bkts[hk & hash->mask]->hk == hk)
              && (hash->bkts[hk & hash->mask]->freeKey == true));

    test("Key referenced: ");
    snprintf(k, sizeof(k), "%s", "keyreferenced");
    hk = natsStrHash_Hash(k, (int) strlen(k));
    s = natsStrHash_Set(hash, k, false, (void*) t2, &oldval);
    if (s == NATS_OK)
    {
        // Changing the key affects the hash
        snprintf(k, sizeof(k), "%s", "keychanged");
        if (natsStrHash_Get(hash, (char*) "keyreferenced") == t2)
            s = NATS_ERR;
    }
    testCond((s == NATS_OK)
              && (oldval == NULL)
              && (hash->bkts[hk & hash->mask]->hk == hk)
              && (hash->bkts[hk & hash->mask]->freeKey == false)
              && (strcmp(hash->bkts[hk & hash->mask]->key, "keychanged") == 0));

    test("Key not copied, but asking to free when destroyed: ");
    myKey = strdup("mykey");
    hk = natsStrHash_Hash(myKey, (int) strlen(myKey));
    s = natsStrHash_SetEx(hash, myKey, false, true, (void*) t1, &oldval);
    testCond((s == NATS_OK)
              && (oldval == NULL)
              && (hash->bkts[hk & hash->mask]->hk == hk)
              && (hash->bkts[hk & hash->mask]->freeKey == true));

    test("Destroy: ");
    natsStrHash_Destroy(hash);
    testCond(1);
}

static const char*
_dummyTokenHandler(void *closure)
{
    return "token";
}

static void
_dummyErrHandler(natsConnection *nc, natsSubscription *sub, natsStatus err,
                 void *closure)
{
    // do nothing
}

static void
_dummyConnHandler(natsConnection *nc, void *closure)
{
    // do nothing
}

static natsStatus
_dummyUserJWTCb(char **userJWT, char **customErrTxt, void *closure)
{
    // do nothing
    return NATS_OK;
}

static natsStatus
_dummySigCb(char **customErrTxt, unsigned char **psig, int *sigLen, const char* nonce, void *closure)
{
    // do nothing
    return NATS_OK;
}

static void
test_natsOptions(void)
{
    natsStatus  s;
    natsOptions *opts = NULL;
    natsOptions *cloned = NULL;
    const char  *servers[] = {"1", "2", "3"};
    const char  *servers2[] = {"1", "2", "3", "4"};
    const char  *servers3[] = {" nats://localhost:4222", "nats://localhost:4223 ", " nats://localhost:4224 "};
    const char  *servers3t[] = {"nats://localhost:4222", "nats://localhost:4223", "nats://localhost:4224"};

    test("Create options: ");
    s = natsOptions_Create(&opts);
    testCond(s == NATS_OK);

    test("Test defaults: ");
    testCond((opts->allowReconnect == true)
             && (opts->maxReconnect == 60)
             && (opts->reconnectWait == 2 * 1000)
             && (opts->timeout == 2 * 1000)
             && (opts->pingInterval == 2 * 60 *1000)
             && (opts->maxPingsOut == 2)
             && (opts->ioBufSize == 32 * 1024)
             && (opts->maxPendingMsgs == 65536)
             && (opts->user == NULL)
             && (opts->password == NULL)
             && (opts->token == NULL)
             && (opts->tokenCb == NULL)
             && (opts->orderIP == 0)
             && (opts->writeDeadline == natsLib_defaultWriteDeadline())
             && !opts->noEcho
             && !opts->retryOnFailedConnect)

    test("Add URL: ");
    s = natsOptions_SetURL(opts, "test");
    testCond((s == NATS_OK)
              && (opts->url != NULL)
              && (strcmp(opts->url, "test") == 0));

    test("Replace URL: ");
    s = natsOptions_SetURL(opts, "test2");
    testCond((s == NATS_OK)
              && (opts->url != NULL)
              && (strcmp(opts->url, "test2") == 0));

    test("URL trimmed: ");
    s = natsOptions_SetURL(opts, "   nats://localhost:4222   ");
    testCond((s == NATS_OK)
              && (opts->url != NULL)
              && (strcmp(opts->url, "nats://localhost:4222") == 0));

    test("Remove URL: ");
    s = natsOptions_SetURL(opts, NULL);
    testCond((s == NATS_OK)
              && (opts->url == NULL));

    test("Set Servers (invalid args): ");
    s = natsOptions_SetServers(opts, servers, -2);
    if (s != NATS_OK)
        s = natsOptions_SetServers(opts, servers, 0);
    testCond(s != NATS_OK);

    test("Set Servers: ");
    s = natsOptions_SetServers(opts, servers, 3);
    testCond((s == NATS_OK)
             && (opts->servers != NULL)
             && (opts->serversCount == 3));

    test("Replace Servers: ");
    s = natsOptions_SetServers(opts, servers2, 4);
    if ((s == NATS_OK) && (opts->servers != NULL) && (opts->serversCount == 4))
    {
        for (int i=0; i<4; i++)
        {
            if (strcmp(opts->servers[i], servers2[i]) != 0)
            {
                s = NATS_ERR;
                break;
            }
        }
    }
    testCond(s == NATS_OK);

    test("Trimmed servers: ")
    s = natsOptions_SetServers(opts, servers3, 3);
    if ((s == NATS_OK) && (opts->servers != NULL) && (opts->serversCount == 3))
    {
        for (int i=0; i<3; i++)
        {
            if (strcmp(opts->servers[i], servers3t[i]) != 0)
            {
                s = NATS_ERR;
                break;
            }
        }
    }
    testCond(s == NATS_OK);

    test("Remove servers: ");
    s = natsOptions_SetServers(opts, NULL, 0);
    testCond((s == NATS_OK)
             && (opts->servers == NULL)
             && (opts->serversCount == 0));

    test("Set NoRandomize: ");
    s = natsOptions_SetNoRandomize(opts, true);
    testCond((s == NATS_OK) && (opts->noRandomize == true));

    test("Remove NoRandomize: ");
    s = natsOptions_SetNoRandomize(opts, false);
    testCond((s == NATS_OK) && (opts->noRandomize == false));

    test("Set Timeout (invalid args): ");
    s = natsOptions_SetTimeout(opts, -10);
    testCond(s != NATS_OK);

    test("Set Timeout to zero: ");
    s = natsOptions_SetTimeout(opts, 0);
    testCond((s == NATS_OK) && (opts->timeout == 0));

    test("Set Timeout: ");
    s = natsOptions_SetTimeout(opts, 2000);
    testCond((s == NATS_OK) && (opts->timeout == 2000));

    test("Set Name: ");
    s = natsOptions_SetName(opts, "test");
    testCond((s == NATS_OK) && (opts->name != NULL)
             && (strcmp(opts->name, "test") == 0));

    test("Remove Name: ");
    s = natsOptions_SetName(opts, NULL);
    testCond((s == NATS_OK) && (opts->name == NULL));

    test("Set Verbose: ");
    s = natsOptions_SetVerbose(opts, true);
    testCond((s == NATS_OK) && (opts->verbose == true));

    test("Remove Verbose: ");
    s = natsOptions_SetVerbose(opts, false);
    testCond((s == NATS_OK) && (opts->verbose == false));

    test("Set NoEcho: ");
    s = natsOptions_SetNoEcho(opts, true);
    testCond((s == NATS_OK) && (opts->noEcho == true));

    test("Remove NoEcho: ");
    s = natsOptions_SetNoEcho(opts, false);
    testCond((s == NATS_OK) && (opts->noEcho == false));

    test("Set RetryOnFailedConnect: ");
    s = natsOptions_SetRetryOnFailedConnect(opts, true, _dummyConnHandler, (void*)1);
    testCond((s == NATS_OK)
            && (opts->retryOnFailedConnect == true)
            && (opts->connectedCb == _dummyConnHandler)
            && (opts->connectedCbClosure == (void*) 1));

    test("Remove RetryOnFailedConnect: ");
    // If `retry` is false, connect CB and closure are ignored and should
    // be internally set to NULL.
    s = natsOptions_SetRetryOnFailedConnect(opts, false, _dummyConnHandler, (void*)1);
    testCond((s == NATS_OK)
            && (opts->retryOnFailedConnect == false)
            && (opts->connectedCb == NULL)
            && (opts->connectedCbClosure == NULL));

    test("Set Secure: ");
    s = natsOptions_SetSecure(opts, true);
#if defined(NATS_HAS_TLS)
    testCond((s == NATS_OK) && (opts->secure == true));
#else
    testCond((s == NATS_ILLEGAL_STATE) && (opts->secure == false));
#endif

    test("Remove Secure: ");
    s = natsOptions_SetSecure(opts, false);
#if defined(NATS_HAS_TLS)
    testCond((s == NATS_OK) && (opts->secure == false));
#else
    testCond((s == NATS_ILLEGAL_STATE) && (opts->secure == false));
#endif

    test("Set Pedantic: ");
    s = natsOptions_SetPedantic(opts, true);
    testCond((s == NATS_OK) && (opts->pedantic == true));

    test("Remove Pedantic: ");
    s = natsOptions_SetPedantic(opts, false);
    testCond((s == NATS_OK) && (opts->pedantic == false));

    test("Set Ping Interval (negative or 0 ok): ");
    s = natsOptions_SetPingInterval(opts, -1000);
    if ((s == NATS_OK) && (opts->pingInterval != -1000))
        s = NATS_ERR;
    IFOK(s, natsOptions_SetPingInterval(opts, 0));
    if ((s == NATS_OK) && (opts->pingInterval != 0))
        s = NATS_ERR;
    IFOK(s, natsOptions_SetPingInterval(opts, 1000));
    testCond((s == NATS_OK) && (opts->pingInterval == 1000));

    test("Set MaxPingsOut: ");
    s = natsOptions_SetMaxPingsOut(opts, -2);
    IFOK(s, natsOptions_SetMaxPingsOut(opts, 0));
    IFOK(s, natsOptions_SetMaxPingsOut(opts, 1));
    IFOK(s, natsOptions_SetMaxPingsOut(opts, 10));
    testCond((s == NATS_OK) && (opts->maxPingsOut == 10));

    test("Set IOBufSize: ");
    s = natsOptions_SetIOBufSize(opts, -1);
    if ((s != NATS_OK) && (opts->ioBufSize == NATS_OPTS_DEFAULT_IO_BUF_SIZE))
        s = natsOptions_SetIOBufSize(opts, 0);
    if ((s == NATS_OK) && (opts->ioBufSize == 0))
        s = natsOptions_SetIOBufSize(opts, 1024 * 1024);
    testCond((s == NATS_OK) && (opts->ioBufSize == 1024 * 1024));

    test("Set AllowReconnect: ");
    s = natsOptions_SetAllowReconnect(opts, true);
    testCond((s == NATS_OK) && (opts->allowReconnect == true));

    test("Remove AllowReconnect: ");
    s = natsOptions_SetAllowReconnect(opts, false);
    testCond((s == NATS_OK) && (opts->allowReconnect == false));

    test("Set MaxReconnect (negative ok): ");
    s = natsOptions_SetMaxReconnect(opts, -10);
    if ((s == NATS_OK) && (opts->maxReconnect != -10))
        s = NATS_ERR;
    IFOK(s, natsOptions_SetMaxReconnect(opts, 0));
    if ((s == NATS_OK) && (opts->maxReconnect != 0))
        s = NATS_ERR;
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    testCond((s == NATS_OK) && (opts->maxReconnect == 10));

    test("Set Reconnect Wait (invalid args: ");
    s = natsOptions_SetReconnectWait(opts, -1000);
    testCond(s != NATS_OK);

    test("Set Reconnect Wait: ");
    s = natsOptions_SetReconnectWait(opts, 1000);
    testCond((s == NATS_OK) && (opts->reconnectWait == 1000));

    test("Remove Reconnect Wait: ");
    s = natsOptions_SetReconnectWait(opts, 0);
    testCond((s == NATS_OK) && (opts->reconnectWait == 0));

    test("Set Max Pending Msgs (invalid args: ");
    s = natsOptions_SetMaxPendingMsgs(opts, -1000);
    if (s != NATS_OK)
        s = natsOptions_SetMaxPendingMsgs(opts, 0);
    testCond(s != NATS_OK);

    test("Set Max Pending Msgs : ");
    s = natsOptions_SetMaxPendingMsgs(opts, 10000);
    testCond((s == NATS_OK) && (opts->maxPendingMsgs == 10000));

    test("Set Error Handler: ");
    s = natsOptions_SetErrorHandler(opts, _dummyErrHandler, NULL);
    testCond((s == NATS_OK) && (opts->asyncErrCb == _dummyErrHandler));

    test("Remove Error Handler: ");
    s = natsOptions_SetErrorHandler(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->asyncErrCb == NULL));

    test("Set ClosedCB: ");
    s = natsOptions_SetClosedCB(opts, _dummyConnHandler, NULL);
    testCond((s == NATS_OK) && (opts->closedCb == _dummyConnHandler));

    test("Remove ClosedCB: ");
    s = natsOptions_SetClosedCB(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->closedCb == NULL));

    test("Set DisconnectedCB: ");
    s = natsOptions_SetDisconnectedCB(opts, _dummyConnHandler, NULL);
    testCond((s == NATS_OK) && (opts->disconnectedCb == _dummyConnHandler));

    test("Remove DisconnectedCB: ");
    s = natsOptions_SetDisconnectedCB(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->disconnectedCb == NULL));

    test("Set ReconnectedCB: ");
    s = natsOptions_SetReconnectedCB(opts, _dummyConnHandler, NULL);
    testCond((s == NATS_OK) && (opts->reconnectedCb == _dummyConnHandler));

    test("Remove ReconnectedCB: ");
    s = natsOptions_SetReconnectedCB(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->reconnectedCb == NULL));

    test("Set UserInfo: ");
    s = natsOptions_SetUserInfo(opts, "ivan", "pwd");
    testCond((s == NATS_OK)
                && (strcmp(opts->user, "ivan") == 0)
                && (strcmp(opts->password, "pwd") == 0));

    test("Remove UserInfo: ");
    s = natsOptions_SetUserInfo(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->user == NULL) && (opts->password == NULL));

    test("Set Token: ");
    s = natsOptions_SetToken(opts, "token");
    testCond((s == NATS_OK) && (strcmp(opts->token, "token") == 0));

    test("Remove Token: ");
    s = natsOptions_SetToken(opts, NULL);
    testCond((s == NATS_OK) && (opts->token == NULL));

    test("Set TokenHandler: ");
    s = natsOptions_SetTokenHandler(opts, _dummyTokenHandler, NULL);
    testCond((s == NATS_OK) && (opts->tokenCb == _dummyTokenHandler)
             && (strcmp(opts->tokenCb(NULL), "token") == 0));

    test("Remove TokenHandler: ");
    s = natsOptions_SetTokenHandler(opts, NULL, NULL);
    testCond((s == NATS_OK) && (opts->tokenCb == NULL));

    test("Set write deadline: ");
    s = natsOptions_SetWriteDeadline(opts, 1000);
    testCond((s == NATS_OK) && (opts->writeDeadline == 1000));

    test("Remove write deadline: ");
    s = natsOptions_SetWriteDeadline(opts, 0);
    testCond((s == NATS_OK) && (opts->writeDeadline == 0));

    test("IP order invalid values: ");
    s = natsOptions_IPResolutionOrder(opts, -1);
    if (s != NATS_OK)
        s = natsOptions_IPResolutionOrder(opts, 1);
    if (s != NATS_OK)
        s = natsOptions_IPResolutionOrder(opts, 466);
    if (s != NATS_OK)
        s = natsOptions_IPResolutionOrder(opts, 644);
    testCond(s != NATS_OK);

    test("IP order valid values: ");
    s = natsOptions_IPResolutionOrder(opts, 0);
    if ((s == NATS_OK) && (opts->orderIP == 0))
        s = natsOptions_IPResolutionOrder(opts, 4);
    if ((s == NATS_OK) && (opts->orderIP == 4))
        s = natsOptions_IPResolutionOrder(opts, 6);
    if ((s == NATS_OK) && (opts->orderIP == 6))
        s = natsOptions_IPResolutionOrder(opts, 46);
    if ((s == NATS_OK) && (opts->orderIP == 46))
        s = natsOptions_IPResolutionOrder(opts, 64);
    testCond((s == NATS_OK) && (opts->orderIP == 64));

    test("Set UseOldRequestStyle: ");
    s = natsOptions_UseOldRequestStyle(opts, true);
    testCond((s == NATS_OK) && (opts->useOldRequestStyle == true));

    test("Remove UseOldRequestStyle: ");
    s = natsOptions_UseOldRequestStyle(opts, false);
    testCond((s == NATS_OK) && (opts->useOldRequestStyle == false));

    test("Set SendAsap: ");
    s = natsOptions_SetSendAsap(opts, true);
    testCond((s == NATS_OK) && (opts->sendAsap == true));

    test("Remove SendAsap: ");
    s = natsOptions_SetSendAsap(opts, false);
    testCond((s == NATS_OK) && (opts->sendAsap == false));

    test("Set UserCreds: ");
    s = natsOptions_SetUserCredentialsCallbacks(opts, _dummyUserJWTCb, (void*) 1, _dummySigCb, (void*) 2);
    testCond((s == NATS_OK)
                && (opts->userJWTHandler == _dummyUserJWTCb)
                && (opts->userJWTClosure == (void*) 1)
                && (opts->sigHandler == _dummySigCb)
                && (opts->sigClosure == (void*) 2));

    test("Remove UserCreds: ");
    s = natsOptions_SetUserCredentialsCallbacks(opts, NULL, NULL, NULL, NULL);
    testCond((s == NATS_OK)
                && (opts->userJWTHandler == NULL)
                && (opts->userJWTClosure == NULL)
                && (opts->sigHandler == NULL)
                && (opts->sigClosure == NULL));

    test("Set UserCredsFromFile: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", "bar");
    testCond((s == NATS_OK)
                && (opts->userCreds != NULL)
                && (strcmp(opts->userCreds->userOrChainedFile, "foo") == 0)
                && (strcmp(opts->userCreds->seedFile, "bar") == 0)
                && (opts->userJWTHandler == natsConn_userFromFile)
                && (opts->userJWTClosure == (void*) opts->userCreds)
                && (opts->sigHandler == natsConn_signatureHandler)
                && (opts->sigClosure == (void*) opts->userCreds));

    test("Remove UserCredsFromFile: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, NULL, NULL);
    testCond((s == NATS_OK)
            && (opts->userCreds == NULL)
            && (opts->userJWTHandler == NULL)
            && (opts->userJWTClosure == NULL)
            && (opts->sigHandler == NULL)
            && (opts->sigClosure == NULL));

    test("Set NKey: ");
    s = natsOptions_SetNKey(opts, "pubkey", _dummySigCb, (void*) 1);
    testCond((s == NATS_OK)
                && (opts->nkey != NULL)
                && (strcmp(opts->nkey, "pubkey") == 0)
                && (opts->sigHandler == _dummySigCb)
                && (opts->sigClosure == (void*) 1));

    test("Remove NKey: ");
    s = natsOptions_SetNKey(opts, NULL, NULL, NULL);
    testCond((s == NATS_OK)
                && (opts->nkey == NULL)
                && (opts->sigHandler == NULL)
                && (opts->sigClosure == NULL));

    test("Set NKeyFromSeed: ");
    s = natsOptions_SetNKeyFromSeed(opts, "pubkey", "seed.file");
    testCond((s == NATS_OK)
                && (opts->nkey != NULL)
                && (strcmp(opts->nkey, "pubkey") == 0)
                && (opts->sigHandler == natsConn_signatureHandler)
                && (opts->sigClosure == (void*) opts->userCreds)
                && (opts->userCreds != NULL)
                && (opts->userCreds->seedFile != NULL)
                && (strcmp(opts->userCreds->seedFile, "seed.file") == 0));

    test("Remove NKeyFromSeed: ");
    s = natsOptions_SetNKeyFromSeed(opts, NULL, NULL);
    testCond((s == NATS_OK)
                && (opts->nkey == NULL)
                && (opts->sigHandler == NULL)
                && (opts->sigClosure == NULL)
                && (opts->userCreds == NULL));

    test("Disable no responders: ");
    s = natsOptions_DisableNoResponders(opts, true);
    testCond((s == NATS_OK) && opts->disableNoResponders);

    test("Enable no responders: ");
    s = natsOptions_DisableNoResponders(opts, false);
    testCond((s == NATS_OK) && !opts->disableNoResponders);

    // Prepare some values for the clone check
    s = natsOptions_SetURL(opts, "url");
    IFOK(s, natsOptions_SetServers(opts, servers, 3));
    IFOK(s, natsOptions_SetName(opts, "name"));
    IFOK(s, natsOptions_SetPingInterval(opts, 3000));
    IFOK(s, natsOptions_SetErrorHandler(opts, _dummyErrHandler, NULL));
    IFOK(s, natsOptions_SetUserInfo(opts, "ivan", "pwd"));
    IFOK(s, natsOptions_SetToken(opts, "token"));
    IFOK(s, natsOptions_IPResolutionOrder(opts, 46));
    IFOK(s, natsOptions_SetNoEcho(opts, true));
    IFOK(s, natsOptions_SetRetryOnFailedConnect(opts, true, _dummyConnHandler, NULL));
    if (s != NATS_OK)
        FAIL("Unable to test natsOptions_clone() because of failure while setting");

    test("Cloning: ");
    s = NATS_OK;
    cloned = natsOptions_clone(opts);
    if (cloned == NULL)
        s = NATS_NO_MEMORY;
    else if ((cloned->pingInterval != 3000)
             || (cloned->asyncErrCb != _dummyErrHandler)
             || (cloned->name == NULL)
             || (strcmp(cloned->name, "name") != 0)
             || (cloned->url == NULL)
             || (strcmp(cloned->url, "url") != 0)
             || (cloned->servers == NULL)
             || (cloned->serversCount != 3)
             || (strcmp(cloned->user, "ivan") != 0)
             || (strcmp(cloned->password, "pwd") != 0)
             || (strcmp(cloned->token, "token") != 0)
             || (cloned->orderIP != 46)
             || (!cloned->noEcho)
             || (!cloned->retryOnFailedConnect)
             || (cloned->connectedCb != _dummyConnHandler))
    {
        s = NATS_ERR;
    }
    if (s == NATS_OK)
    {
        for (int i=0; i<3; i++)
        {
            if (strcmp(cloned->servers[i], servers[i]) != 0)
            {
                s = NATS_ERR;
                break;
            }
        }
    }
    testCond(s == NATS_OK);

    test("Destroy original does not affect clone: ");
    natsOptions_Destroy(opts);
    testCond((cloned != NULL)
             && (cloned->url != NULL)
             && (strcmp(cloned->url, "url") == 0));

    natsOptions_Destroy(cloned);
}

static void
test_natsSock_ReadLine(void)
{
    char        buffer[20];
    natsStatus  s;
    natsSockCtx ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    snprintf(buffer, sizeof(buffer), "%s", "+OK\r\nPONG\r\nFOO\r\nxxx");
    buffer[3] = '\0';

    test("Read second line from buffer: ");
    s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
    testCond((s == NATS_OK) && (strcmp(buffer, "PONG") == 0));

    test("Read third line from buffer: ");
    s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
    testCond((s == NATS_OK) && (strcmp(buffer, "FOO") == 0));

    test("Next call should trigger recv, which is expected to fail: ");
    s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
    testCond(s != NATS_OK);
}

static void
test_natsJSON(void)
{
    natsStatus  s;
    nats_JSON   *json = NULL;
    char        buf[256];
    int         i;
    int         intVal = 0;
    int64_t     longVal = 0;
    char        *strVal = NULL;
    bool        boolVal = false;
    long double doubleVal = 0;
    char        **arrVal = NULL;
    bool        *arrBoolVal = NULL;
    long double *arrDoubleVal = NULL;
    int         *arrIntVal = NULL;
    int64_t     *arrLongVal = NULL;
    uint64_t    *arrULongVal = NULL;
    nats_JSON   **arrObjVal = NULL;
    nats_JSONArray **arrArrVal = NULL;
    int         arrCount = 0;
    uint64_t    ulongVal = 0;
    nats_JSON   *obj1 = NULL;
    nats_JSON   *obj2 = NULL;
    nats_JSON   *obj3 = NULL;
    int32_t     int32Val = 0;
    uint16_t    uint16Val = 0;
    const char  *wrong[] = {
            "{",
            "}",
            "{start quote missing\":0}",
            "{\"end quote missing: 0}",
            "{\"test\":start quote missing\"}",
            "{\"test\":\"end quote missing}",
            "{\"test\":1.2x}",
            "{\"test\":tRUE}",
            "{\"test\":true,}",
            "{\"test\":true}, xxx}",
            "{\"test\": \"abc\\error here\"}",
            "{\"test\": \"abc\\u123\"}",
            "{\"test\": \"abc\\u123g\"}",
            "{\"test\": \"abc\\u 23f\"}",
            "{\"test\": \"abc\\""",
            "{\"test\": \"abc\\u1234",
            "{\"test\": \"abc\\uabc",
            "{\"test\" \"separator missing\"}",
            "{\"test\":[1, \"abc\", true]}",
    };
    const char  *good[] = {
            "{}",
            " {}",
            " { }",
            " { } ",
            "{ \"test\":{}}",
            "{ \"test\":1.2}",
            "{ \"test\" :1.2}",
            "{ \"test\" : 1.2}",
            "{ \"test\" : 1.2 }",
            "{ \"test\" : 1.2,\"test2\":1}",
            "{ \"test\" : 1.2, \"test2\":1}",
            "{ \"test\":0}",
            "{ \"test\" :0}",
            "{ \"test\" : 0}",
            "{ \"test\" : 0 }",
            "{ \"test\" : 0,\"test2\":1}",
            "{ \"test\" : 0, \"test2\":1}",
            "{ \"test\":true}",
            "{ \"test\": true}",
            "{ \"test\": true }",
            "{ \"test\":true,\"test2\":1}",
            "{ \"test\": true,\"test2\":1}",
            "{ \"test\": true ,\"test2\":1}",
            "{ \"test\":false}",
            "{ \"test\": false}",
            "{ \"test\": false }",
            "{ \"test\":false,\"test2\":1}",
            "{ \"test\": false,\"test2\":1}",
            "{ \"test\": false ,\"test2\":1}",
            "{ \"test\":\"abc\"}",
            "{ \"test\": \"abc\"}",
            "{ \"test\": \"abc\" }",
            "{ \"test\":\"abc\",\"test2\":1}",
            "{ \"test\": \"abc\",\"test2\":1}",
            "{ \"test\": \"abc\" ,\"test2\":1}",
            "{ \"test\": \"a\\\"b\\\"c\" }",
            "{ \"test\": [\"a\", \"b\", \"c\"]}",
            "{ \"test\": [\"a\\\"b\\\"c\"]}",
            "{ \"test\": [\"abc,def\"]}",
            "{ \"test\": [{\"a\": 1}, {\"b\": \"c\"}]}",
            "{ \"test\": [[{\"a\": 1}], [{\"b\": \"c\"}]]}",
            "{ \"test\": {\"inner\":\"a\",\"inner2\":2,\"inner3\":false,\"inner4\":{\"inner_inner1\" : 1.234}}}",
            "{ \"test\": \"a\\\"b\\\"c\"}",
            "{ \"test\": \"\\\"\\\\/\b\f\n\r\t\\uabcd\"}",
            "{ \"test\": \"\\ua12f\"}",
            "{ \"test\": \"\\uA01F\"}",
    };
    nats_JSONField *f = NULL;

    for (i=0; i<(int)(sizeof(wrong)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "Negative test %d: ", (i+1));
        test(buf);
        s = nats_JSONParse(&json, wrong[i], -1);
        testCond((s != NATS_OK) && (json == NULL));
        json = NULL;
    }
    nats_clearLastError();

    for (i=0; i<(int)(sizeof(good)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "Positive test %d: ", (i+1));
        test(buf);
        s = nats_JSONParse(&json, good[i], -1);
        testCond((s == NATS_OK) && (json != NULL));
        nats_JSONDestroy(json);
        json = NULL;
    }
    nats_clearLastError();

    // Check values
    test("Empty string: ");
    s = nats_JSONParse(&json, "{}", -1);
    IFOK(s, nats_JSONGetInt(json, "test", &intVal));
    testCond((s == NATS_NOT_FOUND)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 0)
                && (intVal == 0));
    nats_JSONDestroy(json);
    json = NULL;

    test("Single field, string: ");
    s = nats_JSONParse(&json, "{\"test\":\"abc\"}", -1);
    IFOK(s, nats_JSONGetStr(json, "test", &strVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (strcmp(strVal, "abc") == 0));
    nats_JSONDestroy(json);
    json = NULL;
    free(strVal);
    strVal = NULL;

    test("Single field, string with escape chars: ");
    s = nats_JSONParse(&json, "{\"test\":\"escapes: \\\" \\\\ \\/ \\b \\n \\r \\t\"}", -1);
    IFOK(s, nats_JSONGetStr(json, "test", &strVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (strcmp(strVal, "escapes: \\\" \\\\ \\/ \\b \\n \\r \\t") == 0));
    nats_JSONDestroy(json);
    json = NULL;
    free(strVal);
    strVal = NULL;

    test("Single field, int: ");
    s = nats_JSONParse(&json, "{\"test\":1234}", -1);
    IFOK(s, nats_JSONGetInt(json, "test", &intVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (intVal == 1234));
    nats_JSONDestroy(json);
    json = NULL;
    intVal = 0;

    test("Single field, int32: ");
    s = nats_JSONParse(&json, "{\"test\":1234}", -1);
    IFOK(s, nats_JSONGetInt32(json, "test", &int32Val));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (int32Val == 1234));
    nats_JSONDestroy(json);
    json = NULL;
    int32Val = 0;

    test("Single field, uint16: ");
    s = nats_JSONParse(&json, "{\"test\":1234}", -1);
    IFOK(s, nats_JSONGetUInt16(json, "test", &uint16Val));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (uint16Val == 1234));
    nats_JSONDestroy(json);
    json = NULL;
    uint16Val = 0;

    test("Single field, long: ");
    s = nats_JSONParse(&json, "{\"test\":9223372036854775807}", -1);
    IFOK(s, nats_JSONGetLong(json, "test", &longVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (longVal == 9223372036854775807L));
    nats_JSONDestroy(json);
    json = NULL;
    longVal = 0;

    test("Single field, neg long: ");
    s = nats_JSONParse(&json, "{\"test\":-9223372036854775808}", -1);
    IFOK(s, nats_JSONGetLong(json, "test", &longVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (longVal == (int64_t) 0x8000000000000000));
    nats_JSONDestroy(json);
    json = NULL;
    longVal = 0;

    test("Single field, neg long as ulong: ");
    s = nats_JSONParse(&json, "{\"test\":-123456789}", -1);
    IFOK(s, nats_JSONGetULong(json, "test", &ulongVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (ulongVal == 0xFFFFFFFFF8A432EB));
    nats_JSONDestroy(json);
    json = NULL;
    ulongVal = 0;

    test("Single field, ulong: ");
    s = nats_JSONParse(&json, "{\"test\":18446744073709551615}", -1);
    IFOK(s, nats_JSONGetULong(json, "test", &ulongVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (ulongVal == 0xFFFFFFFFFFFFFFFF));
    nats_JSONDestroy(json);
    json = NULL;
    ulongVal = 0;

    test("Single field, ulong: ");
    s = nats_JSONParse(&json, "{\"test\":9007199254740993}", -1);
    IFOK(s, nats_JSONGetULong(json, "test", &ulongVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (ulongVal == 9007199254740993));
    nats_JSONDestroy(json);
    json = NULL;
    ulongVal = 0;

    test("Single field, double: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5e3}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 1234.5e+3));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double negative: ");
    s = nats_JSONParse(&json, "{\"test\":-1234}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) -1234));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp negative 1: ");
    s = nats_JSONParse(&json, "{\"test\":1234e-3}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test",&doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 1234.0/1000.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp negative 2: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5e-3}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345.0/10000.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp negative 3: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5e-1}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345.0/100.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp negative 4: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5e-0}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345.0/10.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 1: ");
    s = nats_JSONParse(&json, "{\"test\":1234e+3}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 1234.0*1000));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 2: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5e+3}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345.0*100.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 3: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5678e+2}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345678.0/100.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 4: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5678e+4}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345678.0/10000.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 5: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5678e+5}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345678.0*10.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 6: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5678e+0}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345678.0/10000.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, double exp positive 6: ");
    s = nats_JSONParse(&json, "{\"test\":1234.5678e1}", -1);
    IFOK(s, nats_JSONGetDouble(json, "test", &doubleVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (doubleVal == (long double) 12345678.0/1000.0));
    nats_JSONDestroy(json);
    json = NULL;
    doubleVal = 0;

    test("Single field, bool: ");
    s = nats_JSONParse(&json, "{\"test\":true}", -1);
    IFOK(s, nats_JSONGetBool(json, "test", &boolVal));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && boolVal);
    nats_JSONDestroy(json);
    json = NULL;
    boolVal = false;

    test("Single field, string array: ");
    s = nats_JSONParse(&json, "{\"test\":[\"a\",\"b\",\"c\",\"d\",\"e\"]}", -1);
    IFOK(s, nats_JSONGetArrayStr(json, "test", &arrVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 5)
                && (strcmp(arrVal[0], "a") == 0)
                && (strcmp(arrVal[1], "b") == 0)
                && (strcmp(arrVal[2], "c") == 0)
                && (strcmp(arrVal[3], "d") == 0)
                && (strcmp(arrVal[4], "e") == 0));
    nats_JSONDestroy(json);
    json = NULL;
    for (i=0; i<arrCount; i++)
        free(arrVal[i]);
    free(arrVal);
    arrVal = NULL;
    arrCount = 0;

    test("Single field, bool array: ");
    s = nats_JSONParse(&json, "{\"test\":[true, false, true]}", -1);
    IFOK(s, nats_JSONGetArrayBool(json, "test", &arrBoolVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && arrBoolVal[0]
                && !arrBoolVal[1]
                && arrBoolVal[2]);
    nats_JSONDestroy(json);
    json = NULL;
    free(arrBoolVal);
    arrBoolVal = NULL;
    arrCount = 0;

    test("Single field, double array: ");
    s = nats_JSONParse(&json, "{\"test\":[1.0, 2.0, 3.0]}", -1);
    IFOK(s, nats_JSONGetArrayDouble(json, "test", &arrDoubleVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && (arrDoubleVal[0] == 1.0)
                && (arrDoubleVal[1] == 2.0)
                && (arrDoubleVal[2] == 3.0));
    nats_JSONDestroy(json);
    json = NULL;
    free(arrDoubleVal);
    arrDoubleVal = NULL;
    arrCount = 0;

    test("Single field, int array: ");
    s = nats_JSONParse(&json, "{\"test\":[1, 2, 3]}", -1);
    IFOK(s, nats_JSONGetArrayInt(json, "test", &arrIntVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && (arrIntVal[0] == 1)
                && (arrIntVal[1] == 2)
                && (arrIntVal[2] == 3));
    nats_JSONDestroy(json);
    json = NULL;
    free(arrIntVal);
    arrIntVal = NULL;
    arrCount = 0;

    test("Single field, long array: ");
    s = nats_JSONParse(&json, "{\"test\":[1, 2, 3]}", -1);
    IFOK(s, nats_JSONGetArrayLong(json, "test", &arrLongVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && (arrLongVal[0] == 1)
                && (arrLongVal[1] == 2)
                && (arrLongVal[2] == 3));
    nats_JSONDestroy(json);
    json = NULL;
    free(arrLongVal);
    arrLongVal = NULL;
    arrCount = 0;

    test("Single field, ulong array: ");
    s = nats_JSONParse(&json, "{\"test\":[1, 2, 3]}", -1);
    IFOK(s, nats_JSONGetArrayULong(json, "test", &arrULongVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && (arrULongVal[0] == 1)
                && (arrULongVal[1] == 2)
                && (arrULongVal[2] == 3));
    nats_JSONDestroy(json);
    json = NULL;
    free(arrULongVal);
    arrULongVal = NULL;
    arrCount = 0;

    test("Single field, object array: ");
    s = nats_JSONParse(&json, "{\"test\":[{\"a\": 1},{\"b\": true}]}", -1);
    IFOK(s, nats_JSONGetArrayObject(json, "test", &arrObjVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 2)
                && (nats_JSONGetInt(arrObjVal[0], "a", &intVal) == NATS_OK)
                && (intVal == 1)
                && (nats_JSONGetBool(arrObjVal[1], "b", &boolVal) == NATS_OK)
                && boolVal);
    nats_JSONDestroy(json);
    json = NULL;
    free(arrObjVal);
    arrObjVal = NULL;
    arrCount = 0;
    intVal = 0;
    boolVal = false;

    test("Single field, array array: ");
    s = nats_JSONParse(&json, "{\"test\":[[\"a\", \"b\"],[1, 2, 3],[{\"c\": true}]]}", -1);
    IFOK(s, nats_JSONGetArrayArray(json, "test", &arrArrVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 3)
                && (nats_JSONArrayAsStrings(arrArrVal[0], &arrVal, &arrCount) == NATS_OK)
                && (arrCount == 2)
                && (strcmp(arrVal[0], "a") == 0)
                && (strcmp(arrVal[1], "b") == 0)
                && (nats_JSONArrayAsInts(arrArrVal[1], &arrIntVal, &arrCount) == NATS_OK)
                && (arrCount == 3)
                && (arrIntVal[0] == 1)
                && (arrIntVal[1] == 2)
                && (arrIntVal[2] == 3)
                && (nats_JSONArrayAsObjects(arrArrVal[2], &arrObjVal, &arrCount) == NATS_OK)
                && (arrCount == 1)
                && (nats_JSONGetBool(arrObjVal[0], "c", &boolVal) == NATS_OK)
                && boolVal);
    nats_JSONDestroy(json);
    json = NULL;
    for (i=0; i<2; i++)
        free(arrVal[i]);
    free(arrVal);
    arrVal = NULL;
    free(arrIntVal);
    arrIntVal = NULL;
    free(arrArrVal);
    arrArrVal = NULL;
    free(arrObjVal);
    arrObjVal = NULL;
    boolVal = false;
    arrCount = 0;

    test("Object: ");
    s = nats_JSONParse(&json, "{\"obj1\":{\"obj2\":{\"obj3\":{\"a\": 1},\"b\":true},\"c\":1.2},\"d\":3}", -1);
    IFOK(s, nats_JSONGetObject(json, "obj1", &obj1));
    IFOK(s, nats_JSONGetObject(obj1, "obj2", &obj2));
    IFOK(s, nats_JSONGetObject(obj2, "obj3", &obj3));
    IFOK(s, nats_JSONGetInt(obj3, "a", &intVal));
    IFOK(s, nats_JSONGetBool(obj2, "b", &boolVal));
    IFOK(s, nats_JSONGetDouble(obj1, "c", &doubleVal));
    IFOK(s, nats_JSONGetLong(json, "d", &longVal));
    testCond((s == NATS_OK)
                && (intVal == 1)
                && boolVal
                && (doubleVal == (long double) 12.0/10.0)
                && (longVal == 3));
    nats_JSONDestroy(json);
    intVal = 0;
    boolVal = false;
    doubleVal = 0.0;
    longVal = 0;

    test("All field types: ");
    s = nats_JSONParse(&json, "{\"bool\":true,\"str\":\"abc\",\"int\":123,\"long\":456,\"double\":123.5,\"array\":[\"a\"]}", -1);
    IFOK(s, nats_JSONGetBool(json, "bool", &boolVal));
    IFOK(s, nats_JSONGetStr(json, "str", &strVal));
    IFOK(s, nats_JSONGetInt(json, "int", &intVal));
    IFOK(s, nats_JSONGetLong(json, "long", &longVal));
    IFOK(s, nats_JSONGetDouble(json, "double", &doubleVal));
    IFOK(s, nats_JSONGetArrayStr(json, "array", &arrVal, &arrCount));
    testCond((s == NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 6)
                && boolVal
                && (strcmp(strVal, "abc") == 0)
                && (intVal == 123)
                && (longVal == 456)
                && (doubleVal == (long double) 1235.0/10.0)
                && (arrCount == 1)
                && (strcmp(arrVal[0], "a") == 0));
    test("Unknown field type: ");
    if (s == NATS_OK)
        s = nats_JSONGetField(json, "int", 255, &f);
    testCond(s != NATS_OK);
    nats_JSONDestroy(json);
    json = NULL;
    free(strVal);
    strVal = NULL;
    boolVal = false;
    intVal = 0;
    longVal = 0;
    doubleVal = 0;
    for (i=0; i<arrCount; i++)
        free(arrVal[i]);
    free(arrVal);
    arrVal = NULL;
    arrCount = 0;

    test("Ask for wrong type: ");
    s = nats_JSONParse(&json, "{\"test\":true}", -1);
    IFOK(s, nats_JSONGetInt(json, "test", &intVal));
    testCond((s != NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (intVal == 0));
    nats_JSONDestroy(json);
    json = NULL;

    test("Ask for wrong type (array): ");
    s = nats_JSONParse(&json, "{\"test\":[\"a\", \"b\"]}", -1);
    IFOK(s, nats_JSONGetArrayField(json, "test", TYPE_INT, &f));
    testCond((s != NATS_OK)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (arrCount == 0)
                && (arrVal == NULL));
    nats_JSONDestroy(json);
    json = NULL;

    test("Ask for unknown type: ");
    s = nats_JSONParse(&json, "{\"test\":true}", -1);
    IFOK(s, nats_JSONGetField(json, "test", 9999, &f));
    testCond((s == NATS_INVALID_ARG)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1));
    nats_JSONDestroy(json);
    json = NULL;

    test("Ask for unknown type (array): ");
    s = nats_JSONParse(&json, "{\"test\":true}", -1);
    IFOK(s, nats_JSONGetArrayField(json, "test", 9999, &f));
    testCond((s == NATS_INVALID_ARG)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1));
    nats_JSONDestroy(json);
    json = NULL;

    test("Check no change to vars for unknown fields: ");
    {
        const char *initStr = "test";
        const char *initStrArr[] = {"a", "b"};

        strVal = (char*) initStr;
        boolVal = true;
        intVal = 123;
        longVal = 456;
        doubleVal = 789;
        arrVal = (char**)initStrArr;
        arrCount = 2;
        s = nats_JSONParse(&json, "{\"test\":true}", -1);
        IFOK_INF(s, nats_JSONGetStr(json, "str", &strVal));
        IFOK_INF(s, nats_JSONGetInt(json, "int", &intVal));
        IFOK_INF(s, nats_JSONGetLong(json, "long", &longVal));
        IFOK_INF(s, nats_JSONGetBool(json, "bool", &boolVal));
        IFOK_INF(s, nats_JSONGetDouble(json, "bool", &doubleVal));
        IFOK_INF(s, nats_JSONGetArrayStr(json, "array", &arrVal, &arrCount));
        testCond((s == NATS_OK)
                    && (strcmp(strVal, initStr) == 0)
                    && boolVal
                    && (intVal == 123)
                    && (longVal == 456)
                    && (doubleVal == 789)
                    && (arrCount == 2)
                    && (strcmp(arrVal[0], "a") == 0)
                    && (strcmp(arrVal[1], "b") == 0));
        nats_JSONDestroy(json);
        json = NULL;
    }

    test("Wrong string type: ");
    strVal = NULL;
    s = nats_JSONParse(&json, "{\"test\":12345678901112}", -1);
    IFOK(s, nats_JSONGetStr(json, "test", &strVal));
    testCond((s == NATS_INVALID_ARG)
                && (json != NULL)
                && (json->fields != NULL)
                && (json->fields->used == 1)
                && (strVal == NULL));
    nats_JSONDestroy(json);
    json = NULL;

    test("NULL string with -1 len: ");
    s = nats_JSONParse(&json, NULL, -1);
    testCond((s == NATS_INVALID_ARG) && (json == NULL));
    nats_clearLastError();

    test("Field reused: ");
    s = nats_JSONParse(&json, "{\"field\":1,\"field\":2}", -1);
    IFOK(s, nats_JSONGetInt(json, "field", &intVal));
    testCond((s == NATS_OK) && (intVal == 2));
    nats_JSONDestroy(json);
    json = NULL;

    test("Nested arrays ok: ");
    jsonMaxNested = 10;
    s = nats_JSONParse(&json, "{\"test\":[[[1, 2]]]}", -1);
    testCond(s == NATS_OK);
    nats_JSONDestroy(json);
    json = NULL;

    test("Nested arrays not ok: ");
    jsonMaxNested = 10;
    s = nats_JSONParse(&json, "{\"test\":[[[[[[[[[[[[[1, 2]]]]]]]]]]]]]}", -1);
    testCond((s == NATS_ERR) && (json == NULL)
                && (strstr(nats_GetLastError(NULL), " nested arrays of 10") != NULL));
    nats_clearLastError();

    test("Nested objects ok: ");
    s = nats_JSONParse(&json, "{\"test\":{\"a\":{\"b\":{\"c\":1}}}}", -1);
    testCond(s == NATS_OK);
    nats_JSONDestroy(json);
    json = NULL;

    test("Nested arrays not ok: ");
    jsonMaxNested = 10;
    s = nats_JSONParse(&json, "{\"test\":{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h\":{\"i\":{\"j\":{\"k\":{\"l\":{\"m\":1}}}}}}}}}}}}}}", -1);
    testCond((s == NATS_ERR) && (json == NULL)
                && (strstr(nats_GetLastError(NULL), " nested objects of 10") != NULL));
    nats_clearLastError();
    jsonMaxNested = JSON_MAX_NEXTED;
}

static void
test_natsErrWithLongText(void)
{
    natsStatus  s;
    char        errTxt[300];
    const char  *output = NULL;
    int         i;

    nats_clearLastError();
    for (i=0; i<(int) sizeof(errTxt)-1; i++)
        errTxt[i] = 'A';
    errTxt[i-1] = '\0';

    test("nats_setError with long text: ");
    s = nats_setError(NATS_ERR, "This is the error: %s", errTxt);
    if (s == NATS_ERR)
        output = nats_GetLastError(&s);
    if (output != NULL)
    {
        int pos = ((int) strlen(output))-1;

        // End of text should contain `...` to indicate that it was truncated.
        for (i=0; i<3; i++)
        {
            if (output[pos--] != '.')
            {
                s = NATS_ILLEGAL_STATE;
                break;
            }
        }
    }
    else
    {
        s = NATS_ILLEGAL_STATE;
    }
    testCond(s == NATS_ERR);
    nats_clearLastError();
}

static void
test_natsErrStackMoreThanMaxFrames(void)
{
    int             i;
    const int       total = MAX_FRAMES+10;
    char            funcName[MAX_FRAMES+10][64];
    char            result[(MAX_FRAMES+10)*100];
    natsStatus      s = NATS_OK;

    test("Check natsUpdateErrStack called more than MAX_FRAMES: ");
    // When a stack trace is formed, it goes from the most recent
    // function called to the oldest. We are going to call more than
    // MAX_FRAMES with function names being numbers from total down
    // to 0. We expect not to crash and have at least from total to
    // total-MAX_FRAMES.
    for (i=total-1;i>=0;i--)
    {
        snprintf(funcName[i], sizeof(funcName[i]), "%d", (i+1));
        nats_updateErrStack(NATS_ERR, funcName[i]);
    }
    s = nats_GetLastErrorStack(result, sizeof(result));
    if (s == NATS_OK)
    {
        char *ptr = result;
        int   funcID;
        char  expected[64];

        snprintf(expected, sizeof(expected), "%d more...", total-MAX_FRAMES);

        for (i=total;i>total-MAX_FRAMES;i--)
        {
            if (sscanf(ptr, "%d", &funcID) != 1)
            {
                s = NATS_ERR;
                break;
            }
            if (funcID != i)
            {
                s = NATS_ERR;
                break;
            }
            if (funcID > 10)
                ptr += 3;
            else
                ptr +=2;
        }
        // The last should be something like: xx more...
        // where xx is total-MAX_FRAMES
        if ((s == NATS_OK) && (strcmp(ptr, expected) != 0))
            s = NATS_ERR;
    }
    testCond(s == NATS_OK);
}

static void
test_natsMsg(void)
{
    natsMsg     *msg = NULL;
    natsStatus  s    = NATS_OK;

    test("Check invalid subj (NULL): ");
    s = natsMsg_Create(&msg, NULL, "reply", "data", 4);
    testCond((msg == NULL) && (s == NATS_INVALID_ARG));

    test("Check invalid subj (empty): ");
    s = natsMsg_Create(&msg, "", "reply", "data", 4);
    testCond((msg == NULL) && (s == NATS_INVALID_ARG));

    test("Check invalid reply (empty): ");
    s = natsMsg_Create(&msg, "foo", "", "data", 4);
    testCond((msg == NULL) && (s == NATS_INVALID_ARG));

    test("GetSubject with NULL msg: ");
    testCond(natsMsg_GetSubject(NULL) == NULL);

    test("GetReply with NULL msg: ");
    testCond(natsMsg_GetReply(NULL) == NULL);

    test("GetData with NULL msg: ");
    testCond(natsMsg_GetData(NULL) == NULL);

    test("GetDataLength with NULL msg: ");
    testCond(natsMsg_GetDataLength(NULL) == 0);

    test("Create ok: ");
    s = natsMsg_Create(&msg, "foo", "reply", "data", 4);
    testCond((s == NATS_OK) && (msg != NULL));

    natsMsg_Destroy(msg);
}

static void
test_natsBase32Decode(void)
{
    natsStatus      s;
    const char      *src       = "KRUGS4ZANFZSA5DIMUQHEZLTOVWHIIDPMYQGCIDCMFZWKMZSEBSGKY3PMRUW4ZY";
    const char      *expected  = "This is the result of a base32 decoding";
    char            dst[256];
    int             dstLen = 0;

    test("Decode: ");
    s = nats_Base32_DecodeString((char*) src, dst, (int) sizeof(dst), &dstLen);
    testCond((s == NATS_OK)
                && (dstLen == (int) strlen(expected))
                && (memcmp((void*) expected, (void*) dst, dstLen) == 0));

    test("Dest too small: ");
    s = nats_Base32_DecodeString((char*) src, dst, 10, &dstLen);
    testCond((s == NATS_INSUFFICIENT_BUFFER) && (dstLen == 0));
    nats_clearLastError();

    test("Invalid string: ");
    s = nats_Base32_DecodeString((char*)"This is invalid content", dst, (int) sizeof(dst), &dstLen);
    testCond((s == NATS_ERR)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), "invalid") != NULL));
}

static void
test_natsBase64Encode(void)
{
    natsStatus  s;
    char        *enc = NULL;
    int         i;
    const char  *testStrings[] = {
            "this is testing base64 encoding",
            "dfslfdlkjsfdllkjfds dfsjlklkfsda dfsalkjklfdsalkj adfskjllkjfdaslkjfdslk",
            "This is another with numbers like 12345678.90 and special characters !@#$%^&*()-=+",
    };
    const char  *expectedResults[] = {
            "dGhpcyBpcyB0ZXN0aW5nIGJhc2U2NCBlbmNvZGluZw",
            "ZGZzbGZkbGtqc2ZkbGxramZkcyBkZnNqbGtsa2ZzZGEgZGZzYWxramtsZmRzYWxraiBhZGZza2psbGtqZmRhc2xramZkc2xr",
            "VGhpcyBpcyBhbm90aGVyIHdpdGggbnVtYmVycyBsaWtlIDEyMzQ1Njc4LjkwIGFuZCBzcGVjaWFsIGNoYXJhY3RlcnMgIUAjJCVeJiooKS09Kw",
    };

    test("Encode nil: ");
    s = nats_Base64RawURL_EncodeString(NULL, 0, &enc);
    testCond((s == NATS_OK) && (enc == NULL));

    test("Encode empty: ");
    s = nats_Base64RawURL_EncodeString((const unsigned char*) "", 0, &enc);
    testCond((s == NATS_OK) && (enc == NULL));

    test("Encode strings: ");
    for (i=0; i<(int)(sizeof(testStrings)/sizeof(char*)); i++)
    {
        s = nats_Base64RawURL_EncodeString((const unsigned char*) testStrings[i], (int)strlen(testStrings[i]), &enc);
        if ((s == NATS_OK) && ((enc == NULL) || (strcmp(enc, expectedResults[i]) != 0)))
            s = NATS_ERR;

        free(enc);
        enc = NULL;
        if (s != NATS_OK)
            break;
    }
    testCond(s == NATS_OK);

    test("Encode bytes: ");
    {
        const uint8_t   src[] = {1, 2, 0, 3, 4, 5, 0, 6, 7, 8, 0, 9, 0};
        int             len = 13;

        enc = NULL;

        s = nats_Base64RawURL_EncodeString((const unsigned char*) &src, len, &enc);
        if ((s == NATS_OK) && ((enc == NULL) || (strcmp(enc, "AQIAAwQFAAYHCAAJAA") != 0)))
            s = NATS_ERR;

        free(enc);
    }
    testCond(s == NATS_OK);
}

static void
test_natsCRC16(void)
{
    unsigned char   a[] = {153, 209, 36, 74, 103, 32, 65, 34, 111, 68, 104, 156, 50, 14, 164, 140, 144, 230};
    uint16_t        crc = 0;
    uint16_t        expected = 10272;

    test("Compute: ");
    crc = nats_CRC16_Compute(a, (int)sizeof(a));
    testCond(crc == expected);

    test("Verify: ");
    testCond(nats_CRC16_Validate(a, (int)sizeof(a), expected));

    test("Expect failure: ");
    a[3] = 63;
    testCond(!nats_CRC16_Validate(a, (int)sizeof(a), expected));
}

static void
test_natsKeys(void)
{
    natsStatus          s;
    unsigned char       sig[NATS_CRYPTO_SIGN_BYTES];
    const char          *nonceVal  = "nonce";
    const unsigned char *nonce     = (const unsigned char*) nonceVal;
    const unsigned char expected[] = {
            155, 157,   8, 183,  93, 154,  78,   7,
            219,  39,  11,  16, 134, 231,  46, 142,
            168,  87, 110, 202, 187, 180, 179,  62,
             49, 255, 225,  74,  48,  80, 176, 111,
            248, 162, 121, 188, 203, 101, 100, 195,
            162,  70, 213, 182, 220,  14,  71, 113,
             93, 239, 141, 131,  66, 190, 237, 127,
            104, 191, 138, 217, 227,   1,  92,  14,
    };

    test("Invalid key: ");
    s = natsKeys_Sign("ABC", nonce, 0, sig);
    testCond((s == NATS_ERR)
            && (nats_GetLastError(NULL) != NULL)
            && (strstr(nats_GetLastError(NULL), NKEYS_INVALID_ENCODED_KEY) != NULL));
    nats_clearLastError();

    // This is generated from XYTHISISNOTAVALIDSEED with correct checksum.
    // Expect to get invalid seed
    test("Invalid seed: ");
    s = natsKeys_Sign("LBMVISCJKNEVGTSPKRAVMQKMJFCFGRKFIQ52C", nonce, 0, sig);
    testCond((s == NATS_ERR)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), NKEYS_INVALID_SEED) != NULL));
    nats_clearLastError();

    test("Invalid prefix: ");
    s = natsKeys_Sign("SBAUEQ2EIVDEOSCJJJFUYTKOJ5IFCUSTKRKVMV2YLFNECQSDIRCUMR2IJFFEWTCNJZHVAUKSKNKFKVSXLBMVUQKCINCEKRSHJBEUUS2MJVHE6UCRKJJVIVKWK5MFSWV2QA", nonce, 0, sig);
    testCond((s == NATS_ERR)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), NKEYS_INVALID_PREFIX) != NULL));
    nats_clearLastError();

    // This is the valid seed: SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY
    // Make the checksum incorrect by changing last 2 bytes.
    test("Invalid checksum: ");
    s = natsKeys_Sign("SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4AA", nonce, 0, sig);
    testCond((s == NATS_ERR)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), NKEYS_INVALID_CHECKSUM) != NULL));
    nats_clearLastError();

    // Now use valid SEED
    test("Sign ok: ");
    s = natsKeys_Sign("SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY", nonce, 0, sig);
    testCond((s == NATS_OK)
                && (memcmp(sig, expected, sizeof(expected)) == 0));
}

static void
test_natsReadFile(void)
{
    natsStatus      s    = NATS_OK;
    natsBuffer      *buf = NULL;
    FILE            *f   = NULL;
    const char      *fn  = "test_readfile.txt";
    const char      *content = "This is some content.\nThere are 2 lines in this file.\n";

    test("Invalid arg 1: ");
    s = nats_ReadFile(&buf, 0, "file.txt");
    testCond((s == NATS_INVALID_ARG) && (buf == NULL));

    test("Invalid arg 2: ");
    s = nats_ReadFile(&buf, -1, "file.txt");
    testCond((s == NATS_INVALID_ARG) && (buf == NULL));

    test("Invalid arg 3: ");
    s = nats_ReadFile(&buf, 100, NULL);
    testCond((s == NATS_INVALID_ARG) && (buf == NULL));

    test("Invalid arg 4: ");
    s = nats_ReadFile(&buf, 100, "");
    testCond((s == NATS_INVALID_ARG) && (buf == NULL));

    test("File not found: ");
    s = nats_ReadFile(&buf, 100, "fileNotFound.txt");
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "fileNotFound.txt") != NULL)
                && (buf == NULL));
    nats_clearLastError();

    // Create temp file with some content...
    f = fopen(fn, "w");
    if (f == NULL)
    {
        FAIL("Unable to create test file");
    }
    else
    {
        int res = fputs(content, f);
        if (res < 0)
            FAIL("Unable to write content of test file");

        fclose(f);
        f = NULL;
    }

    test("Read with large buffer: ");
    s = nats_ReadFile(&buf, 1024, fn);
    testCond((s == NATS_OK)
                && (buf != NULL)
                && (natsBuf_Capacity(buf) == 1024)
                && (natsBuf_Len(buf) == (int) strlen(content)+1)
                && (strcmp(natsBuf_Data(buf), content) == 0));
    natsBuf_Destroy(buf);
    buf = NULL;

    test("Read with small buffer: ");
    // The content is 55 bytes. We start with buffer of 10 and x2
    // when expanding, so capacity should be 10, 20, 40, 80.
    s = nats_ReadFile(&buf, 10, fn);
    testCond((s == NATS_OK)
                && (buf != NULL)
                && (natsBuf_Capacity(buf) == 80)
                && (natsBuf_Len(buf) == (int) strlen(content)+1)
                && (strcmp(natsBuf_Data(buf), content) == 0));
    natsBuf_Destroy(buf);
    buf = NULL;

    test("Read with buffer of exact file content: ");
    // Use a buf capacity that matches exactly the content on file.
    // But since we need to add the terminating `\0`, then we will
    // need to expand.
    s = nats_ReadFile(&buf, (int)strlen(content), fn);
    testCond((s == NATS_OK)
                && (buf != NULL)
                && (natsBuf_Capacity(buf) == (int)strlen(content)*2)
                && (natsBuf_Len(buf) == (int) strlen(content)+1)
                && (strcmp(natsBuf_Data(buf), content) == 0));
    natsBuf_Destroy(buf);
    buf = NULL;

    s = nats_ReadFile(&buf, (int)strlen(content)+1, fn);
    testCond((s == NATS_OK)
                && (buf != NULL)
                && (natsBuf_Capacity(buf) == (int)strlen(content)+1)
                && (natsBuf_Len(buf) == (int) strlen(content)+1)
                && (strcmp(natsBuf_Data(buf), content) == 0));
    natsBuf_Destroy(buf);
    buf = NULL;

    remove(fn);
}

static void
test_natsGetJWTOrSeed(void)
{
    natsStatus  s;
    char        *val = NULL;
    char        buf[256];
    const char  *valids[] = {
        "--- START JWT ---\nsome value\n--- END JWT ---\n",
        "--- ---\nsome value\n--- ---\n",
        "------\nsome value\n------\n",
        "---\nabc\n--\n---START---\nsome value\n---END---\n----\ndef\n--- ---\n",
        "nothing first\nthen it starts\n  --- START ---\nsome value\n--- END ---\n---START---\nof something else\n---END---\n",
        "--- START ---\nsome value\n\n\n--- END ---\n",
    };
    const char *invalids[] = {
        "-- JWT -- START ----\nsome value\n--- END ---\n",
        "--- START --- \nsome value\n--- END ---\n",
        "--- START ---\nsome value\n-- END ---\n",
        "--- START ---\nsome value\n---- END --- \n",
    };
    int i;
    int iter;

    for (iter=0; iter<2; iter++)
    {
        for (i=0; i<(int)(sizeof(valids)/sizeof(char*));i++)
        {
            snprintf(buf, sizeof(buf), "%s %d: ", (iter==0 ? "JWT" : "Seed"), (i+1));
            test(buf);
            snprintf(buf, sizeof(buf), "%s%s", (iter == 0 ? "" : "------\njwt\n------\n"), valids[i]);
            s = nats_GetJWTOrSeed(&val, buf, iter);
            testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "some value") == 0));
            free(val);
            val = NULL;
        }

        for (i=0; i<(int)(sizeof(invalids)/sizeof(char*));i++)
        {
            snprintf(buf, sizeof(buf), "%s invalid %d: ", (iter == 0 ? "JWT" : "Seed"), (i+1));
            test(buf);
            snprintf(buf, sizeof(buf), "%s%s", (iter == 0 ? "" : "------\njwt\n------\n"), invalids[i]);
            s = nats_GetJWTOrSeed(&val, buf, iter);
            testCond((s == NATS_NOT_FOUND) && (val == NULL));
        }
    }
}

static void
test_natsHostIsIP(void)
{
    struct _testHost {
        const char *host;
        bool       isIP;
    };
    const struct _testHost hosts[] = {
        { "1.2.3.4", true },
        { "::1", true },
        { "localhost", false },
        { "www.host.name.com", false },
    };
    int i;

    for (i=0; i<(int)(sizeof(hosts)/sizeof(struct _testHost)); i++)
    {
        char buf[256];

        snprintf(buf, sizeof(buf), "Check '%s': ", hosts[i].host);
        test(buf);
        testCond(nats_HostIsIP(hosts[i].host) == hosts[i].isIP)
    }
}

static void
_testWaitReadyServer(void *closure)
{
    struct addrinfo     *servinfo = NULL;
    natsStatus          s         = NATS_OK;
    natsSock            sock      = NATS_SOCK_INVALID;
    natsSock            cliSock   = NATS_SOCK_INVALID;
    struct threadArg    *arg      = (struct threadArg*) closure;
    struct addrinfo     hints;
    int                 res;

    memset(&hints,0,sizeof(hints));

    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags    = AI_PASSIVE;

    if ((res = getaddrinfo("127.0.0.1", "1234", &hints, &servinfo)) != 0)
         s = NATS_SYS_ERROR;
    if (s == NATS_OK)
    {
        sock = socket(servinfo->ai_family, servinfo->ai_socktype,
                      servinfo->ai_protocol);
        if (sock == NATS_SOCK_INVALID)
            s = NATS_SYS_ERROR;

        IFOK(s, natsSock_SetCommonTcpOptions(sock));
        IFOK(s, natsSock_SetBlocking(sock, true));
    }
    if ((s == NATS_OK)
        && (bind(sock, servinfo->ai_addr, (natsSockLen) servinfo->ai_addrlen) == NATS_SOCK_ERROR))
    {
        s = NATS_SYS_ERROR;
    }

    if ((s == NATS_OK) && (listen(sock, 100) == 0))
    {
        cliSock = accept(sock, NULL, NULL);
        if ((cliSock != NATS_SOCK_INVALID)
                && (natsSock_SetCommonTcpOptions(cliSock) == NATS_OK))
        {
            nats_Sleep(500);

            send(cliSock, "*", 1, 0);

            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !arg->done)
                s = natsCondition_TimedWait(arg->c, arg->m, 10000);
            natsMutex_Unlock(arg->m);

            natsSock_Close(cliSock);
        }
    }
    natsSock_Close(sock);
    nats_FreeAddrInfo(servinfo);
}

static void
_testSockShutdownThread(void *closure)
{
    natsSockCtx *ctx = (natsSockCtx*) closure;

    nats_Sleep(500);
    natsSock_Shutdown(ctx->fd);
}

static void
test_natsWaitReady(void)
{
    natsStatus          s  = NATS_OK;
    natsThread          *t = NULL;
    natsThread          *t2 = NULL;
    natsSockCtx         ctx;
    int64_t             start, dur;
    char                buffer[1];
    int                 i;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    if (natsThread_Create(&t, _testWaitReadyServer, &arg) != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Connect: ");
    natsSock_Init(&ctx);
    ctx.orderIP = 4;
    natsSock_ClearDeadline(&ctx);
    for (i=0; i<20; i++)
    {
        s = natsSock_ConnectTcp(&ctx, "127.0.0.1", 1234);
        if (s == NATS_OK)
            break;
        nats_Sleep(100);
    }
    testCond(s == NATS_OK);

    test("Set non blocking: ");
    s = natsSock_SetCommonTcpOptions(ctx.fd);
    IFOK(s, natsSock_SetBlocking(ctx.fd, false));
    testCond(s == NATS_OK);

    // Ensure that we get a would_block on read..
    while (recv(ctx.fd, buffer, 1, 0) != -1) {}

    test("WaitReady no deadline: ");
    natsSock_ClearDeadline(&ctx);
    start = nats_Now();
    s = natsSock_WaitReady(WAIT_FOR_READ, &ctx);
    dur = nats_Now()-start;
    testCond((s == NATS_OK) && (dur >= 450) && (dur <= 600));

    // Ensure that we get a would_block on read..
    while (recv(ctx.fd, buffer, 1, 0) != -1) {}

    test("WaitReady deadline timeout: ");
    natsSock_InitDeadline(&ctx, 50);
    start = nats_Now();
    s = natsSock_WaitReady(WAIT_FOR_READ, &ctx);
    dur = nats_Now()-start;
    testCond((s == NATS_TIMEOUT) && (dur >= 40) && (dur <= 100));

    // Ensure that we get a would_block on read..
    while (recv(ctx.fd, buffer, 1, 0) != -1) {}

    test("WaitReady kicked out by shutdown: ");
    natsSock_ClearDeadline(&ctx);
    start = nats_Now();
    s = natsThread_Create(&t2, _testSockShutdownThread, &ctx);
    IFOK(s, natsSock_WaitReady(WAIT_FOR_READ, &ctx));
    dur = nats_Now()-start;
    testCond((s == NATS_OK) && (dur <= 3000));

    natsSock_Close(ctx.fd);

    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    if (t2 != NULL)
    {
        natsThread_Join(t2);
        natsThread_Destroy(t2);
    }

    _destroyDefaultThreadArgs(&arg);
}

static void
test_natsSign(void)
{
    unsigned char   *sig   = NULL;
    int             sigLen = 0;
    char            *sig64 = NULL;
    natsStatus      s;

    test("nats_Sign invalid param 1: ");
    s = nats_Sign(NULL, "nonce", &sig, &sigLen);
    testCond(s == NATS_INVALID_ARG);

    test("nats_Sign invalid param 2: ");
    s = nats_Sign("seed", NULL, &sig, &sigLen);
    testCond(s == NATS_INVALID_ARG);

    test("nats_Sign invalid param 3: ");
    s = nats_Sign("seed", "nonce", NULL, &sigLen);
    testCond(s == NATS_INVALID_ARG);

    test("nats_Sign invalid param 4: ");
    s = nats_Sign("seed", "nonce", &sig, NULL);
    testCond(s == NATS_INVALID_ARG);

    nats_clearLastError();

    test("Sign ok: ");
    s = nats_Sign(
        "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY",
        "nonce", &sig, &sigLen);
    IFOK(s, nats_Base64RawURL_EncodeString((const unsigned char*) sig, sigLen, &sig64));
    testCond((s == NATS_OK)
        && (sig != NULL)
        && (sig64 != NULL)
        && (sigLen == NATS_CRYPTO_SIGN_BYTES)
        && (memcmp((void*) sig64,
                   (void*) "AVfpO7Pw3rc56hoO1OJcFxXUCfBmO2qouchBchSlL45Fuur9zS15UzytEI1QC5wwVG7uiHIdqyfmOS6uPrwqCg",
                   NATS_CRYPTO_SIGN_BYTES) == 0));

    free(sig);
    free(sig64);
}

static void
_testHeader(const char *testName, char *buf, natsStatus expected, const char *errTxt,
            const char *key, const char *value)
{
    natsStatus  s    = NATS_OK;
    natsMsg     *msg = NULL;
    const char  *val = NULL;
    const char  *k   = (key == NULL ? "k" : key);

    test(testName);
    s = natsMsg_create(&msg, "foo", 3, NULL, 0, buf, (int)strlen(buf), (int) strlen(buf));
    IFOK(s, natsMsgHeader_Get(msg, k, &val));
    if (expected == NATS_OK)
    {
        testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, value) == 0));
    }
    else
    {
        const char *le = nats_GetLastError(&s);
        testCond((s == expected) && (strstr(le, errTxt) != NULL));
        nats_clearLastError();
    }

    natsMsg_Destroy(msg);
}

static void
_testStatus(const char *testName, char *buf, const char *expectedStatus, const char *expectedDescription)
{
    natsStatus  s     = NATS_OK;
    natsMsg     *msg  = NULL;
    const char  *sts  = NULL;
    const char  *desc = NULL;

    test(testName);
    s = natsMsg_create(&msg, "foo", 3, NULL, 0, buf, (int)strlen(buf), (int) strlen(buf));
    IFOK(s, natsMsgHeader_Get(msg, STATUS_HDR, &sts));
    IFOK(s, natsMsgHeader_Get(msg, DESCRIPTION_HDR, &desc));
    testCond((s == (expectedDescription == NULL ? NATS_NOT_FOUND : NATS_OK)
            && ((sts != NULL) && (strcmp(sts, expectedStatus) == 0))
            && (expectedDescription == NULL
                    ? desc == NULL
                    : ((desc != NULL) && (strcmp(desc, expectedDescription) == 0)))));

    natsMsg_Destroy(msg);
}

static void
test_natsMsgHeadersLift(void)
{
    char buf[512];

    snprintf(buf, sizeof(buf), "%sk:v\r\n\r\n", HDR_LINE);
    _testHeader("Valid simple header: ", buf, NATS_OK, "", "k", "v");

    snprintf(buf, sizeof(buf), "%sk e y:v\r\n\r\n", HDR_LINE);
    _testHeader("Key with spaces ok: ", buf, NATS_OK, "", "k e y", "v");

    snprintf(buf, sizeof(buf), "%sk e y  :v\r\n\r\n", HDR_LINE);
    _testHeader("Key with spaces (including traling) ok: ", buf, NATS_OK, "", "k e y  ", "v");

    snprintf(buf, sizeof(buf), "%sk:  v   \r\n\r\n", HDR_LINE);
    _testHeader("Trim spaces for value: ", buf, NATS_OK, "", "k", "v");

    snprintf(buf, sizeof(buf), "%sk: a\r\n   bc\r\n def\r\n\r\n", HDR_LINE);
    _testHeader("Multiline values: ", buf, NATS_OK, "", "k", "a bc def");

    snprintf(buf, sizeof(buf), "%s", "NATS\r\nk:v\r\n\r\n");
    _testHeader("NATS header missing: ", buf, NATS_PROTOCOL_ERROR, "header prefix missing", NULL, NULL);

    snprintf(buf, sizeof(buf), "%s", HDR_LINE);
    _testHeader("NATS header missing CRLF: ", buf, NATS_PROTOCOL_ERROR, "early termination of headers", NULL, NULL);

    snprintf(buf, sizeof(buf), "%sk:v\r\n\rbad\r\n", HDR_LINE);
    _testHeader("Invalid key start: ", buf, NATS_PROTOCOL_ERROR, "invalid start of a key", NULL, NULL);

    snprintf(buf, sizeof(buf), "%s k:v\r\n\r\n", HDR_LINE);
    _testHeader("Space in key name: ", buf, NATS_PROTOCOL_ERROR, "key cannot start with a space", NULL, NULL);

    snprintf(buf, sizeof(buf), "%sk\r\n\r\n", HDR_LINE);
    _testHeader("Column missing: ", buf, NATS_PROTOCOL_ERROR, "column delimiter not found", NULL, NULL);

    snprintf(buf, sizeof(buf), "%sk:\r\n\r\n", HDR_LINE);
    _testHeader("No value: ", buf, NATS_PROTOCOL_ERROR, "no value found for key", NULL, NULL);

    snprintf(buf, sizeof(buf), "%sk:       \r\n\r\n", HDR_LINE);
    _testHeader("No value (extra spaces): ", buf, NATS_PROTOCOL_ERROR, "no value found for key", NULL, NULL);

    // Check status description in header line prefix...

    snprintf(buf, sizeof(buf), "%s 503\r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status no description: ", buf, "503", NULL);

    snprintf(buf, sizeof(buf), "%s  503 \r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status no description (extra space): ", buf, "503", NULL);

    snprintf(buf, sizeof(buf), "%s  503  \r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status no description (extra spaces): ", buf, "503", NULL);

    snprintf(buf, sizeof(buf), "%s 503 No Responders\r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status with description: ", buf, "503", "No Responders");

    snprintf(buf, sizeof(buf), "%s 404 No Messages \r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status with description (extra space): ", buf, "404", "No Messages");

    snprintf(buf, sizeof(buf), "%s  404   No Messages   \r\n\r\n", HDR_LINE_PRE);
    _testStatus("Status with description (extra spaces): ", buf, "404", "No Messages");
}

static void
test_natsMsgHeaderAPIs(void)
{
    natsStatus  s        = NATS_OK;
    natsMsg     *msg     = NULL;
    const char  *val     = NULL;
    const char* *values  = NULL;
    const char* *keys    = NULL;
    int         count    = 0;
    const char  *longKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    test("Create message: ");
    s = natsMsg_Create(&msg, "foo", NULL, "body", 4);
    testCond(s == NATS_OK);

    test("Key cannot be NULL: ");
    s = natsMsgHeader_Set(msg, NULL, "value");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Key cannot be empty: ");
    s = natsMsgHeader_Set(msg, "", "value");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Set msg cannot be NULL: ");
    s = natsMsgHeader_Set(NULL, "key", "value");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Set value: ");
    s = natsMsgHeader_Set(msg, "my-key", "value1");
    testCond(s == NATS_OK);

    test("Get msg cannot be NULL: ");
    s = natsMsgHeader_Get(NULL, "my-key", &val);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Get must provide mem location: ");
    s = natsMsgHeader_Get(msg, "my-key", NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Get: ");
    s = natsMsgHeader_Get(msg, "my-key", &val);
    testCond((s == NATS_OK) &&
                (val != NULL) &&
                (strcmp(val, "value1") == 0));
    val = NULL;

    test("Get value with different case: ");
    s = natsMsgHeader_Get(msg, "my-Key", &val);
    testCond((s == NATS_NOT_FOUND) && (val == NULL));
    val = NULL;

    test("Key not found: ");
    s = natsMsgHeader_Get(msg, "unknown-key", &val);
    testCond((s == NATS_NOT_FOUND) && (val == NULL));
    val = NULL;

    test("Set value replace old: ");
    s = natsMsgHeader_Set(msg, "my-key", "value2");
    testCond(s == NATS_OK);

    test("Get value: ");
    s = natsMsgHeader_Get(msg, "my-key", &val);
    testCond((s == NATS_OK) &&
                (val != NULL) &&
                (strcmp(val, "value2") == 0));
    val = NULL;

    test("Set NULL value: ");
    s = natsMsgHeader_Set(msg, "my-key", NULL);
    testCond(s == NATS_OK);

    test("Get value: ");
    s = natsMsgHeader_Get(msg, "my-key", &val);
    testCond((s == NATS_OK) && (val == NULL));
    val = NULL;

    test("Set empty value: ");
    s = natsMsgHeader_Set(msg, "my-key", "");
    testCond(s == NATS_OK);

    test("Get value: ");
    s = natsMsgHeader_Get(msg, "my-key", &val);
    testCond((s == NATS_OK) && (val != NULL) && (val[0] == '\0'));
    val = NULL;

    test("Add msg cannot be NULL: ");
    s = natsMsgHeader_Add(NULL, "key", "value");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Add first: ");
    s = natsMsgHeader_Add(msg, "two-fields", "val1");
    testCond(s == NATS_OK);

    test("Add second: ");
    s = natsMsgHeader_Add(msg, "two-fields", "val2");
    testCond(s == NATS_OK);

    test("Get should return first: ");
    s = natsMsgHeader_Get(msg, "two-fields", &val);
    testCond((s == NATS_OK) &&
                (val != NULL) &&
                (strcmp(val, "val1") == 0));
    val = NULL;

    test("Values: ");
    s = natsMsgHeader_Values(msg, "two-fields", &values, &count);
    testCond((s == NATS_OK) && (values != NULL) && (count == 2) &&
                (strcmp(values[0], "val1") == 0) &&
                (strcmp(values[1], "val2") == 0));

    if (values != NULL)
        free((void*) values);
    values = NULL;
    count  = 0;

    test("Add after a Set: ");
    s = natsMsgHeader_Set(msg, "my-other-key", "val3");
    IFOK(s, natsMsgHeader_Add(msg, "my-other-key", "val4"));
    IFOK(s, natsMsgHeader_Values(msg, "my-other-key", &values, &count));
    testCond((s == NATS_OK) && (values != NULL) && (count == 2) &&
                (strcmp(values[0], "val3") == 0) &&
                (strcmp(values[1], "val4") == 0));

    if (values != NULL)
        free((void*) values);
    values = NULL;
    count  = 0;

    test("Keys msg cannot be NULL: ");
    s = natsMsgHeader_Keys(NULL, &keys, &count);
    testCond((s == NATS_INVALID_ARG) && (keys == NULL) && (count == 0));
    if (s == NATS_INVALID_ARG)
    {
        s = NATS_OK;
        nats_clearLastError();
    }

    test("Keys keys cannot be NULL: ");
    s = natsMsgHeader_Keys(msg, NULL, &count);
    testCond((s == NATS_INVALID_ARG) && (keys == NULL) && (count == 0));
    nats_clearLastError();

    test("Keys count cannot be NULL: ");
    s = natsMsgHeader_Keys(msg, &keys, NULL);
    testCond((s == NATS_INVALID_ARG) && (keys == NULL) && (count == 0));
    nats_clearLastError();

    test("Keys: ");
    s = natsMsgHeader_Keys(msg, &keys, &count);
    if ((s == NATS_OK) && ((keys == NULL) || (count != 3)))
    {
        s = NATS_ERR;
    }
    else
    {
        int i;
        bool ok1 = false;
        bool ok2 = false;
        bool ok3 = false;

        for (i=0; i<count; i++)
        {
            if (strcmp(keys[i], "my-key") == 0)
                ok1 = true;
            else if (strcmp(keys[i], "two-fields") == 0)
                ok2 = true;
            else if (strcmp(keys[i], "my-other-key") == 0)
                ok3 = true;
        }
        if (!ok1 || !ok2 || !ok3)
            s = NATS_ERR;
    }
    testCond(s == NATS_OK);
    if (keys != NULL)
        free((void*) keys);
    count = 0;

    test("Set with long key: ");
    s = natsMsgHeader_Set(msg, longKey, "val1");
    testCond(s == NATS_OK);

    test("Add with long key: ");
    s = natsMsgHeader_Add(msg, longKey, "val2");
    testCond(s == NATS_OK);

    test("Get with long key: ");
    s = natsMsgHeader_Get(msg, longKey, &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "val1") == 0));

    test("Values with long key: ");
    s = natsMsgHeader_Values(msg, longKey, &values, &count);
    testCond((s == NATS_OK) && (values != NULL) && (count == 2) &&
                (strcmp(values[0], "val1") == 0) &&
                (strcmp(values[1], "val2") == 0));
    free((void*) values);
    count = 0;

    test("Delete msg cannot be NULL: ");
    s = natsMsgHeader_Delete(NULL, "key");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Delete key cannot be NULL: ");
    s = natsMsgHeader_Delete(msg, NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Delete key cannot be empty: ");
    s = natsMsgHeader_Delete(msg, "");
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Delete: ");
    s = natsMsgHeader_Delete(msg, "my-other-key");
    testCond(s == NATS_OK);

    test("Should be gone: ");
    s = natsMsgHeader_Get(msg, "my-other-key", &val);
    testCond((s == NATS_NOT_FOUND) && (val == NULL));

    natsMsg_Destroy(msg);
}

static natsStatus
_checkStart(const char *url, int orderIP, int maxAttempts)
{
    natsStatus      s        = NATS_OK;
    natsUrl         *nUrl    = NULL;
    int             attempts = 0;
    natsSockCtx     ctx;

    natsSock_Init(&ctx);
    ctx.orderIP = orderIP;

    natsDeadline_Init(&(ctx.writeDeadline), 2000);

    s = natsUrl_Create(&nUrl, url);
    if (s == NATS_OK)
    {
        while (((s = natsSock_ConnectTcp(&ctx,
                                         nUrl->host, nUrl->port)) != NATS_OK)
               && (attempts++ < maxAttempts))
        {
            nats_Sleep(200);
        }

        natsUrl_Destroy(nUrl);

        if (s == NATS_OK)
            natsSock_Close(ctx.fd);
        else
            s = NATS_NO_SERVER;
    }

    nats_clearLastError();

    return s;
}

static natsStatus
_checkStreamingStart(const char *url, int maxAttempts)
{
    natsStatus      s     = NATS_NOT_PERMITTED;

#if defined(NATS_HAS_STREAMING)

    stanConnOptions *opts = NULL;
    stanConnection  *sc   = NULL;
    int             attempts = 0;

    s = stanConnOptions_Create(&opts);
    IFOK(s, stanConnOptions_SetURL(opts, url));
    IFOK(s, stanConnOptions_SetConnectionWait(opts, 250));
    if (s == NATS_OK)
    {
        while (((s = stanConnection_Connect(&sc, clusterName, "checkStart", opts)) != NATS_OK)
                && (attempts++ < maxAttempts))
        {
            nats_Sleep(200);
        }
    }

    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);

    if (s != NATS_OK)
        nats_clearLastError();
#else
#endif
    return s;
}

#ifdef _WIN32

typedef PROCESS_INFORMATION *natsPid;

static HANDLE logHandle = NULL;

static void
_stopServer(natsPid pid)
{
    if (pid == NATS_INVALID_PID)
        return;

    TerminateProcess(pid->hProcess, 0);
    WaitForSingleObject(pid->hProcess, INFINITE);

    CloseHandle(pid->hProcess);
    CloseHandle(pid->hThread);

    natsMutex_Lock(slMu);
    natsHash_Remove(slMap, (int64_t) pid);
    natsMutex_Unlock(slMu);

    free(pid);
}

static natsPid
_startServerImpl(const char *serverExe, const char *url, const char *cmdLineOpts, bool checkStart)
{
    SECURITY_ATTRIBUTES     sa;
    STARTUPINFO             si;
    HANDLE                  h;
    PROCESS_INFORMATION     *pid;
    DWORD                   flags = 0;
    BOOL                    createdOk = FALSE;
    BOOL                    hInheritance = FALSE;
    char                    *exeAndCmdLine = NULL;
    int                     ret;

    pid = calloc(1, sizeof(PROCESS_INFORMATION));
    if (pid == NULL)
        return NATS_INVALID_PID;

    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);

    ret = nats_asprintf(&exeAndCmdLine, "%s%s%s", serverExe,
                        (cmdLineOpts != NULL ? " " : ""),
                        (cmdLineOpts != NULL ? cmdLineOpts : ""));
    if (ret < 0)
    {
        printf("No memory allocating command line string!\n");
        free(pid);
        return NATS_INVALID_PID;
    }

    if (!keepServerOutput)
    {
        ZeroMemory(&sa, sizeof(sa));
        sa.nLength = sizeof(sa);
        sa.lpSecurityDescriptor = NULL;
        sa.bInheritHandle = TRUE;

        h = logHandle;
        if (h == NULL)
        {
            h = CreateFile(LOGFILE_NAME,
                           GENERIC_WRITE,
                           FILE_SHARE_WRITE | FILE_SHARE_READ,
                           &sa,
                           CREATE_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL,
                           NULL);
        }

        si.dwFlags   |= STARTF_USESTDHANDLES;
        si.hStdInput  = NULL;
        si.hStdError  = h;
        si.hStdOutput = h;

        hInheritance = TRUE;
        flags        = CREATE_NO_WINDOW;

        if (logHandle == NULL)
            logHandle = h;
    }

    // Start the child process.
    if (!CreateProcess(NULL,
                       (LPSTR) exeAndCmdLine,
                       NULL,         // Process handle not inheritable
                       NULL,         // Thread handle not inheritable
                       hInheritance, // Set handle inheritance
                       flags,        // Creation flags
                       NULL,         // Use parent's environment block
                       NULL,         // Use parent's starting directory
                       &si,          // Pointer to STARTUPINFO structure
                       pid))        // Pointer to PROCESS_INFORMATION structure
    {

        printf("Unable to start '%s': error (%d).\n",
               exeAndCmdLine, GetLastError());
        free(exeAndCmdLine);
        return NATS_INVALID_PID;
    }

    free(exeAndCmdLine);

    if (checkStart)
    {
        natsStatus s;

        if (strcmp(serverExe, natsServerExe) == 0)
            s = _checkStart(url, 46, 10);
        else
            s = _checkStreamingStart(url, 10);

        if (s != NATS_OK)
        {
            _stopServer(pid);
            return NATS_INVALID_PID;
        }
    }

    natsMutex_Lock(slMu);
    natsHash_Set(slMap, (int64_t) pid, NULL, NULL);
    natsMutex_Unlock(slMu);

    return (natsPid) pid;
}

#else

typedef pid_t               natsPid;

static void
_stopServer(natsPid pid)
{
    int status = 0;

    if (pid == NATS_INVALID_PID)
        return;

    if (kill(pid, SIGINT) < 0)
    {
        perror("kill with SIGINT");
        if (kill(pid, SIGKILL) < 0)
        {
            perror("kill with SIGKILL");
        }
    }

    waitpid(pid, &status, 0);

    natsMutex_Lock(slMu);
    natsHash_Remove(slMap, (int64_t) pid);
    natsMutex_Unlock(slMu);
}

static natsPid
_startServerImpl(const char *serverExe, const char *url, const char *cmdLineOpts, bool checkStart)
{
    natsPid pid = fork();
    if (pid == -1)
    {
        perror("fork");
        return NATS_INVALID_PID;
    }

    if (pid == 0)
    {
        char *exeAndCmdLine = NULL;
        char *argvPtrs[64];
        char *line = NULL;
        int index = 0;
        int ret = 0;
        bool overrideAddr = false;

        if ((cmdLineOpts == NULL) || (strstr(cmdLineOpts, "-a ") == NULL))
            overrideAddr = true;

        ret = nats_asprintf(&exeAndCmdLine, "%s%s%s%s%s", serverExe,
                                (cmdLineOpts != NULL ? " " : ""),
                                (cmdLineOpts != NULL ? cmdLineOpts : ""),
                                (overrideAddr ? " -a 127.0.0.1" : ""),
                                (keepServerOutput ? "" : " -l " LOGFILE_NAME));
        if (ret < 0)
        {
            perror("No memory allocating command line string!\n");
            exit(1);
        }

        memset(argvPtrs, 0, sizeof(argvPtrs));
        line = exeAndCmdLine;

        while (*line != '\0')
        {
            while ((*line == ' ') || (*line == '\t') || (*line == '\n'))
                *line++ = '\0';

            argvPtrs[index++] = line;
            while ((*line != '\0') && (*line != ' ')
                   && (*line != '\t') && (*line != '\n'))
            {
                line++;
            }
        }
        argvPtrs[index++] = NULL;

        // Child process. Replace with NATS server
        execvp(argvPtrs[0], argvPtrs);
        perror("Exec failed: ");
        exit(1);
    }
    else if (checkStart)
    {
        natsStatus s;

        if (strcmp(serverExe, natsServerExe) == 0)
            s = _checkStart(url, 46, 10);
        else
            s = _checkStreamingStart(url, 10);

        if (s != NATS_OK)
        {
            _stopServer(pid);
            return NATS_INVALID_PID;
        }
    }

    natsMutex_Lock(slMu);
    natsHash_Set(slMap, (int64_t) pid, NULL, NULL);
    natsMutex_Unlock(slMu);

    // parent, return the child's PID back.
    return pid;
}
#endif

static natsPid
_startServer(const char *url, const char *cmdLineOpts, bool checkStart)
{
    return _startServerImpl(natsServerExe, url, cmdLineOpts, checkStart);
}

static natsPid
_startStreamingServer(const char* url, const char *cmdLineOpts, bool checkStart)
{
    return _startServerImpl(natsStreamingServerExe, url, cmdLineOpts, checkStart);
}

static void
test_natsSock_IPOrder(void)
{
    natsStatus  s;
    natsPid     serverPid;

    test("Server listen to IPv4: ");
    serverPid = _startServer("", "-a 127.0.0.1 -p 4222", false);
    testCond(true);
    test("IPv4 only: ");
    s = _checkStart("nats://localhost:4222", 4, 5);
    testCond(s == NATS_OK);
    test("IPv4+v6: ");
    s = _checkStart("nats://localhost:4222", 46, 5);
    testCond(s == NATS_OK);
    test("IPv6+v4: ");
    s = _checkStart("nats://localhost:4222", 64, 5);
    testCond(s == NATS_OK);
    test("IP any: ");
    s = _checkStart("nats://localhost:4222", 0, 5);
    testCond(s == NATS_OK);
    // This one should fail.
    test("IPv6 only: ");
    s = _checkStart("nats://localhost:4222", 6, 5);
    testCond(s != NATS_OK);
    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    if (!runOnTravis)
    {
        test("Server listen to IPv6: ");
        serverPid = _startServer("", "-a :: -p 4222", false);
        testCond(true);
        test("IPv6 only: ");
        s = _checkStart("nats://localhost:4222", 6, 5);
        testCond(s == NATS_OK);
        test("IPv4+v6: ");
        s = _checkStart("nats://localhost:4222", 46, 5);
        testCond(s == NATS_OK);
        test("IPv6+v4: ");
        s = _checkStart("nats://localhost:4222", 64, 5);
        testCond(s == NATS_OK);
        test("IP any: ");
        s = _checkStart("nats://localhost:4222", 0, 5);
        testCond(s == NATS_OK);
        // This one should fail, but the server when listening
        // to [::] is actually accepting IPv4 connections,
        // so be tolerant of that.
        test("IPv4 only: ");
        s = _checkStart("nats://localhost:4222", 4, 5);
        if (s == NATS_OK)
            fprintf(stderr, ">>>> IPv4 to [::] should have failed, but server accepted it\n");
        else
            s = NATS_OK;
        testCond(s == NATS_OK);
        _stopServer(serverPid);
    }
}

static void
test_natsSock_ConnectTcp(void)
{
    natsPid     serverPid = NATS_INVALID_PID;

    test("Check connect tcp: ");
    serverPid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
    testCond(serverPid != NATS_INVALID_PID);
    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    test("Check connect tcp (force server to listen to IPv4): ");
    serverPid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
    testCond(serverPid != NATS_INVALID_PID);
    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;
}

static natsOptions*
_createReconnectOptions(void)
{
    natsStatus  s;
    natsOptions *opts = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:22222"));
    IFOK(s, natsOptions_SetAllowReconnect(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    if (s == NATS_OK)
#ifdef WIN32
        s = natsOptions_SetTimeout(opts, 500);
#else
        s = natsOptions_SetTimeout(opts, NATS_OPTS_DEFAULT_TIMEOUT);
#endif

    if (s != NATS_OK)
    {
        natsOptions_Destroy(opts);
        opts = NULL;
    }

    return opts;
}

static void
_reconnectedCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    int64_t             now  = nats_Now();

    natsMutex_Lock(arg->m);
    arg->reconnected = true;
    arg->reconnects++;
    if (arg->control == 9)
    {
        if (arg->reconnects <= 4)
            arg->reconnectedAt[arg->reconnects-1] = now;
    }
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
test_ReconnectServerStats(void)
{
    natsStatus      s;
    natsConnection  *nc       = NULL;
    natsOptions     *opts     = NULL;
    natsSrv         *srv      = NULL;
    natsPid         serverPid = NATS_INVALID_PID;
    natsStatistics  *stats    = NULL;
    uint64_t        reconnects= 0;
    struct threadArg args;

    test("Reconnect Server Stats: ");

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsOptions_SetDisconnectedCB(opts, _reconnectedCb, &args);
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Flush(nc));

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    if (s == NATS_OK)
    {
        serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
        CHECK_SERVER_STARTED(serverPid);

        natsMutex_Lock(args.m);
        while ((s != NATS_TIMEOUT) && !args.reconnected)
            s = natsCondition_TimedWait(args.c, args.m, 5000);
        natsMutex_Unlock(args.m);

        IFOK(s, natsConnection_FlushTimeout(nc, 5000));
    }

    if (s == NATS_OK)
    {
        srv = natsSrvPool_GetCurrentServer(nc->srvPool, nc->cur, NULL);
        if (srv == NULL)
            s = NATS_ILLEGAL_STATE;
    }

    testCond((s == NATS_OK) && (srv->reconnects == 0));

    test("Tracking Reconnects stats: ");
    s = natsStatistics_Create(&stats);
    IFOK(s, natsConnection_GetStats(nc, stats));
    IFOK(s, natsStatistics_GetCounts(stats, NULL, NULL, NULL, NULL, &reconnects));
    testCond((s == NATS_OK) && (reconnects == 1));

    natsStatistics_Destroy(stats);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);

    _destroyDefaultThreadArgs(&args);
}

static void
_disconnectedCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    int64_t             now  = nats_Now();

    natsMutex_Lock(arg->m);
    arg->disconnected = true;
    arg->disconnects++;
    if ((arg->control == 9) && (arg->disconnects > 1))
    {
        if (arg->disconnects <= 5)
            arg->disconnectedAt[arg->disconnects-2] = now;
    }
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
_recvTestString(natsConnection *nc, natsSubscription *sub, natsMsg *msg,
                void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    bool                doSignal = true;

    natsMutex_Lock(arg->m);

    switch (arg->control)
    {
        case 0:
        {
            if (strncmp(arg->string,
                        natsMsg_GetData(msg),
                        natsMsg_GetDataLength(msg)) != 0)
            {
                arg->status = NATS_ERR;
            }
            break;
        }
        case 1:
        {
            if (sub == NULL)
                arg->status = NATS_ERR;
            else if (strncmp(arg->string,
                    natsMsg_GetData(msg),
                    natsMsg_GetDataLength(msg)) != 0)
            {
                arg->status = NATS_ERR;
            }
            break;
        }
        case 2:
        {
            if (strcmp(arg->string, natsMsg_GetReply(msg)) != 0)
            {
                arg->status = NATS_ERR;
            }
            break;
        }
        case 3:
        case 9:
        {
            doSignal = false;
            arg->sum++;

            if ((arg->control != 9) && (arg->sum == 10))
            {
                arg->status = natsSubscription_Unsubscribe(sub);
                doSignal = true;
            }
            break;
        }
        case 4:
        {
            arg->status = natsConnection_PublishString(nc,
                                                       natsMsg_GetReply(msg),
                                                       arg->string);
            if (arg->status == NATS_OK)
                arg->status = natsConnection_Flush(nc);
            break;
        }
        case 5:
        {
            arg->status = natsConnection_Flush(nc);
            break;
        }
        case 6:
        {
            char seqnoStr[10];
            int seqno = 0;

            doSignal = false;

            snprintf(seqnoStr, sizeof(seqnoStr), "%.*s", natsMsg_GetDataLength(msg),
                     natsMsg_GetData(msg));

            seqno = atoi(seqnoStr);
            if (seqno >= 10)
                arg->status = NATS_ERR;
            else
                arg->results[seqno] = (arg->results[seqno] + 1);

            break;
        }
        case 7:
        {
            arg->msgReceived = true;
            natsCondition_Signal(arg->c);

            while (!arg->closed)
                natsCondition_Wait(arg->c, arg->m);

            break;
        }
        case 8:
        {
            arg->sum++;

            while (!arg->closed)
                natsCondition_Wait(arg->c, arg->m);

            break;
        }
        case 10:
        {
            arg->status = (natsMsg_IsNoResponders(msg) ? NATS_OK : NATS_ERR);
            break;
        }
    }

    natsMsg_Destroy(msg);

    if (doSignal)
    {
        arg->msgReceived = true;
        natsCondition_Signal(arg->c);
    }
    natsMutex_Unlock(arg->m);
}

static void
_closedCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->closed = true;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);
}

static natsStatus
_waitForConnClosed(struct threadArg *arg)
{
    natsStatus s = NATS_OK;

    natsMutex_Lock(arg->m);
    while ((s != NATS_TIMEOUT) && !arg->closed)
        s = natsCondition_TimedWait(arg->c, arg->m, 2000);
    arg->closed = false;
    natsMutex_Unlock(arg->m);

    return s;
}

static void
test_ParseStateReconnectFunctionality(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    test("Parse State Reconnect Functionality: ");

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.string = "bar";
        arg.status = NATS_OK;
    }
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg) != NATS_OK)
        || (natsOptions_SetClosedCB(opts, _closedCb, &arg) != NATS_OK))
    {
        FAIL("Unable to create reconnect options!");
    }

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, &arg));
    IFOK(s, natsConnection_Flush(nc));

    if (s == NATS_OK)
    {
        // Simulate partialState, this needs to be cleared
        natsConn_Lock(nc);
        nc->ps->state = OP_PON;
        natsConn_Unlock(nc);
    }

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.disconnected)
            s = natsCondition_TimedWait(arg.c, arg.m, 500);
        natsMutex_Unlock(arg.m);
    }

    IFOK(s, natsConnection_PublishString(nc, "foo", arg.string));

    if (s == NATS_OK)
    {
        serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
        CHECK_SERVER_STARTED(serverPid);
    }

    IFOK(s, natsConnection_FlushTimeout(nc, 5000));

    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.msgReceived)
            s = natsCondition_TimedWait(arg.c, arg.m, 1500);
        natsMutex_Unlock(arg.m);

        if (s == NATS_OK)
            s = arg.status;
    }

    testCond((s == NATS_OK) && (nc->stats.reconnects == 1));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _waitForConnClosed(&arg);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ServersRandomize(void)
{
    natsStatus      s;
    natsOptions     *opts   = NULL;
    natsConnection  *nc     = NULL;
    int serversCount;

    serversCount = sizeof(testServers) / sizeof(char *);

    test("Server Pool with Randomize: ");

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    if (s == NATS_OK)
    {
        int same = 0;
        int allSame = 0;

        for (int iter=0; (s == NATS_OK) && (iter<1000); iter++)
        {
            s = natsConn_create(&nc, natsOptions_clone(opts));
            if (s == NATS_OK)
            {
                // In theory, this could happen...
                for (int i=0; i<serversCount; i++)
                {
                    if (strcmp(testServers[i],
                               nc->srvPool->srvrs[i]->url->fullUrl) == 0)
                    {
                        same++;
                    }
                }
                if (same == serversCount)
                    allSame++;
            }
            natsConn_release(nc);
            nc = NULL;
        }

        if (allSame > 10)
            s = NATS_ERR;
    }
    testCond(s == NATS_OK);

    // Now test that we do not randomize if proper flag is set.
    test("Server Pool With NoRandomize: ")
    s = natsOptions_SetNoRandomize(opts, true);
    IFOK(s, natsConn_create(&nc, natsOptions_clone(opts)));
    if (s == NATS_OK)
    {
        for (int i=0; i<serversCount; i++)
            if (strcmp(testServers[i],
                       nc->srvPool->srvrs[i]->url->fullUrl) != 0)
            {
                s = NATS_ERR;
                break;
            }
    }
    testCond(s == NATS_OK);
    natsConn_release(nc);
    nc = NULL;

    // Although the original intent was that if Opts.Url is
    // set, Opts.Servers is not (and vice versa), the behavior
    // is that Opts.Url is always first, even when randomization
    // is enabled. So make sure that this is still the case.
    test("If Options.URL is set, it should be first: ")
    s = natsOptions_SetNoRandomize(opts, false);
    IFOK(s, natsOptions_SetURL(opts, NATS_DEFAULT_URL));
    IFOK(s, natsConn_create(&nc, natsOptions_clone(opts)));
    if (s == NATS_OK)
    {
        int same = 0;
        // In theory, this could happen...
        for (int i=0; i<serversCount; i++)
        {
            if (strcmp(testServers[i],
                       nc->srvPool->srvrs[i+1]->url->fullUrl) == 0)
            {
                same++;
            }
        }
        if (same == serversCount)
            s = NATS_ERR;
    }
    if ((s == NATS_OK)
            && strcmp(nc->srvPool->srvrs[0]->url->fullUrl,
                      NATS_DEFAULT_URL) != 0)
    {
        s = NATS_ERR;
    }
    testCond(s == NATS_OK);

    natsConn_release(nc);
    natsOptions_Destroy(opts);
}

static void
test_SelectNextServer(void)
{
    natsStatus      s;
    natsOptions     *opts   = NULL;
    natsConnection  *nc     = NULL;
    natsSrv         *srv    = NULL;
    int             serversCount;

    serversCount = sizeof(testServers) / sizeof(char *);

    test("Test default server pool selection: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsConn_create(&nc, natsOptions_clone(opts)));
    testCond((s == NATS_OK)
             && (nc->cur->url == nc->srvPool->srvrs[0]->url));

    test("Get next server: ");
    if (s == NATS_OK)
    {
        srv = natsSrvPool_GetNextServer(nc->srvPool, nc->opts, nc->cur);
        if (srv != NULL)
            nc->cur = srv;
    }
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->cur != NULL));

    test("Check list size: ");
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->srvPool != NULL)
             && (nc->srvPool->size == serversCount));

    test("Check selection: ");
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->cur->url != NULL)
             && (nc->cur->url->fullUrl != NULL)
             && (strcmp(nc->cur->url->fullUrl, testServers[1]) == 0));

    test("Check old was pushed to last position: ");
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->srvPool != NULL)
             && (nc->srvPool->srvrs != NULL)
             && (nc->srvPool->size > 0)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1] != NULL)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1]->url != NULL)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1]->url->fullUrl != NULL)
             && (strcmp(nc->srvPool->srvrs[nc->srvPool->size - 1]->url->fullUrl,
                        testServers[0]) == 0));

    test("Got correct server: ");
    testCond((s == NATS_OK)
             && (srv != NULL)
             && (nc != NULL)
             && (nc->srvPool != NULL)
             && (nc->srvPool->srvrs != NULL)
             && (nc->srvPool->size > 0)
             && (srv == (nc->srvPool->srvrs[0])));

    // Test that we do not keep servers where we have tried to reconnect past our limit.
    if (s == NATS_OK)
    {
        test("Get next server: ");
        if ((nc == NULL)
            || (nc->srvPool == NULL)
            || (nc->srvPool->srvrs == NULL)
            || (nc->srvPool->srvrs[0] == NULL))
        {
            s = NATS_ERR;
        }
        else
        {
            nc->srvPool->srvrs[0]->reconnects = nc->opts->maxReconnect;
        }
        if (s == NATS_OK)
        {
            srv = natsSrvPool_GetNextServer(nc->srvPool, nc->opts, nc->cur);
            if (srv != NULL)
                nc->cur = srv;
        }
        testCond((s == NATS_OK) && (nc->cur != NULL));
    }

    // Check that we are now looking at #3, and current is not in the list.
    test("Check list size: ");
    testCond((s == NATS_OK)
             && (nc->srvPool->size == (serversCount - 1)));

    test("Check selection: ");
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->cur != NULL)
             && (nc->cur->url != NULL)
             && (nc->cur->url->fullUrl != NULL)
             && (strcmp(nc->cur->url->fullUrl, testServers[2]) == 0));

    test("Check last server was discarded: ");
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (nc->srvPool != NULL)
             && (nc->srvPool->srvrs != NULL)
             && (nc->srvPool->size > 0)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1] != NULL)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1]->url != NULL)
             && (nc->srvPool->srvrs[nc->srvPool->size - 1]->url->fullUrl != NULL)
             && (strcmp(nc->srvPool->srvrs[nc->srvPool->size - 1]->url->fullUrl,
                        testServers[1]) != 0));

    natsConn_release(nc);
    natsOptions_Destroy(opts);
}

static void
parserNegTest(int lineNum)
{
    char txt[64];

    snprintf(txt, sizeof(txt), "Test line %d: ", lineNum);
    test(txt);
}

#define PARSER_START_TEST parserNegTest(__LINE__)

static void
test_ParserPing(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            ping[64];

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    IFOK(s, natsBuf_Create(&(nc->pending), 1000));
    if (s == NATS_OK)
        nc->usePending = true;
    if (s != NATS_OK)
        FAIL("Unable to setup test");


    PARSER_START_TEST;
    testCond(nc->ps->state == OP_START);

    snprintf(ping, sizeof(ping), "PING\r\n");

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_P));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping + 1, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PI));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping + 2, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PIN));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping + 3, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PING));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping + 4, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PING));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping + 5, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping, (int)strlen(ping));
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // Should tolerate spaces
    snprintf(ping, sizeof(ping), "%s", "PING  \r");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping, (int)strlen(ping));
    testCond((s == NATS_OK) && (nc->ps->state == OP_PING));

    nc->ps->state = OP_START;
    snprintf(ping, sizeof(ping), "%s", "PING  \r  \n");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, ping, (int)strlen(ping));
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    natsConnection_Destroy(nc);
}

static void
test_ParserErr(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            errProto[1024];
    char            expected[256];
    int             len;

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    IFOK(s, natsBuf_Create(&(nc->pending), 1000));
    if (s == NATS_OK)
    {
        nc->usePending = true;
        nc->status = NATS_CONN_STATUS_CLOSED;
    }
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    // This test focuses on the parser only, not how the error is
    // actually processed by the upper layer.

    PARSER_START_TEST;
    testCond(nc->ps->state == OP_START);

    snprintf(expected, sizeof(expected), "%s", "'Any kind of error'");
    snprintf(errProto, sizeof(errProto), "-ERR  %s\r\n", expected);
    len = (int) strlen(errProto);

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 1, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS_E));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 2, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS_ER));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 3, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS_ERR));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 4, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS_ERR_SPC));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 5, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_MINUS_ERR_SPC));

    // Check with split arg buffer
    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 6, 1);
    testCond((s == NATS_OK) && (nc->ps->state == MINUS_ERR_ARG));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 7, 3);
    testCond((s == NATS_OK) && (nc->ps->state == MINUS_ERR_ARG));

    // Verify content
    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + 10, len - 10 - 2);
    testCond((s == NATS_OK)
             && (nc->ps->state == MINUS_ERR_ARG)
             && (nc->ps->argBuf != NULL)
             && (strncmp(nc->ps->argBuf->data, expected, nc->ps->argBuf->len) == 0));

    // Finish parsing
    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto + len - 1, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // Check without split arg buffer
    snprintf(errProto, sizeof(errProto), "-ERR '%s'\r\n", "Any Error");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, errProto, (int)strlen(errProto));
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    natsConnection_Destroy(nc);
}

static void
test_ParserOK(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            okProto[256];

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    PARSER_START_TEST;
    testCond(nc->ps->state == OP_START);

    snprintf(okProto, sizeof(okProto), "+OKay\r\n");

    PARSER_START_TEST;
    s = natsParser_Parse(nc, okProto, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PLUS));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, okProto + 1, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PLUS_O));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, okProto + 2, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_PLUS_OK));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, okProto + 3, (int)strlen(okProto) - 3);
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    natsConnection_Destroy(nc);
}

static void
test_ParseINFO(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            infoProto[256];

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    PARSER_START_TEST;
    testCond(nc->ps->state == OP_START);

    snprintf(infoProto, sizeof(infoProto), "INFO \t{\"server_id\": \"abc\"}\r\n");

    PARSER_START_TEST;
    s = natsParser_Parse(nc, infoProto, 7);
    testCond((s == NATS_OK) && (nc->ps->state == INFO_ARG) && (infoProto[nc->ps->afterSpace] == '{'));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, infoProto +7, (int)strlen(infoProto) - 7);
    testCond((s == NATS_OK)
                && (nc->ps->state == OP_START)
                && (nc->info.id != NULL) && (strcmp(nc->info.id, "abc") == 0));

    natsConnection_Destroy(nc);
}

static void
test_ParserShouldFail(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            buf[64];

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    // Negative tests:

    PARSER_START_TEST;
    snprintf(buf, sizeof(buf), "%s", " PING");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "POO");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "Px");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "PIx");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "PINx");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);
    // Stop here because 'PING' protos are tolerant for anything between PING and \n

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "POx");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "PONx");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);
    // Stop here because 'PONG' protos are tolerant for anything between PONG and \n

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "ZOO");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "Mx\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSx\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSGx\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG  foo\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG \r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG foo 1\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG foo bar 1\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG foo bar 1 baz\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "MSG foo 1 bar baz\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "+x\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "+Ox\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "-x\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "-Ex\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "-ERx\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    PARSER_START_TEST;
    nc->ps->state = OP_START;
    snprintf(buf, sizeof(buf), "%s", "-ERRx\r\n");
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond(s != NATS_OK);

    natsConnection_Destroy(nc);
}

static void
test_ParserSplitMsg(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            buf[2*MAX_CONTROL_LINE_SIZE];
    uint64_t        expectedCount;
    uint64_t        expectedSize;
    int             msgSize, start, i;

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    expectedCount = 1;
    expectedSize = 3;

    snprintf(buf, sizeof(buf), "%s", "MSG a 1 3\r\nfoo\r\n");

    // parsing: 'MSG a'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 5);
    testCond((s == NATS_OK) && (nc->ps->argBuf != NULL));

    // parsing: ' 1 3\r\nf'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + 5, 7);
    testCond((s == NATS_OK)
             && (nc->ps->ma.size == 3)
             && (nc->ps->ma.sid == 1)
             && (nc->ps->ma.subject->len == 1)
             && (strncmp(nc->ps->ma.subject->data, "a", 1) == 0)
             && (nc->ps->msgBuf != NULL));

    // parsing: 'oo\r\n'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + 12, (int)strlen(buf) - 12);
    testCond((s == NATS_OK)
             && (nc->stats.inMsgs == expectedCount)
             && (nc->stats.inBytes == expectedSize)
             && (nc->ps->argBuf == NULL)
             && (nc->ps->msgBuf == NULL)
             && (nc->ps->state == OP_START));

    // parsing: 'MSG a 1 3\r\nfo'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 13);
    testCond((s == NATS_OK)
             && (nc->ps->ma.size == 3)
             && (nc->ps->ma.sid == 1)
             && (nc->ps->ma.subject->len == 1)
             && (strncmp(nc->ps->ma.subject->data, "a", 1) == 0)
             && (nc->ps->argBuf != NULL)
             && (nc->ps->msgBuf != NULL));

    expectedCount++;
    expectedSize += 3;

    // parsing: 'o\r\n'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + 13, (int)strlen(buf) - 13);
    testCond((s == NATS_OK)
             && (nc->stats.inMsgs == expectedCount)
             && (nc->stats.inBytes == expectedSize)
             && (nc->ps->argBuf == NULL)
             && (nc->ps->msgBuf == NULL)
             && (nc->ps->state == OP_START));

    snprintf(buf, sizeof(buf), "%s", "MSG a 1 6\r\nfoobar\r\n");

    // parsing: 'MSG a 1 6\r\nfo'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 13);
    testCond((s == NATS_OK)
             && (nc->ps->ma.size == 6)
             && (nc->ps->ma.sid == 1)
             && (nc->ps->ma.subject->len == 1)
             && (strncmp(nc->ps->ma.subject->data, "a", 1) == 0)
             && (nc->ps->argBuf != NULL)
             && (nc->ps->msgBuf != NULL));

    // parsing: 'ob'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + 13, 2);
    testCond(s == NATS_OK)

    expectedCount++;
    expectedSize += 6;

    // parsing: 'ar\r\n'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + 15, (int)strlen(buf) - 15);
    testCond((s == NATS_OK)
             && (nc->stats.inMsgs == expectedCount)
             && (nc->stats.inBytes == expectedSize)
             && (nc->ps->argBuf == NULL)
             && (nc->ps->msgBuf == NULL)
             && (nc->ps->state == OP_START));

    // Let's have a msg that is bigger than the parser's scratch size.
    // Since we prepopulate the msg with 'foo', adding 3 to the size.
    msgSize = sizeof(nc->ps->scratch) + 100 + 3;

    snprintf(buf, sizeof(buf), "MSG a 1 b %d\r\nfoo", msgSize);
    start = (int)strlen(buf);
    for (i=0; i<msgSize - 3; i++)
        buf[start + i] = 'a' + (i % 26);
    buf[start + i++] = '\r';
    buf[start + i++] = '\n';
    buf[start + i++] = '\0';

    // parsing: 'MSG a 1 b <size>\r\nfoo'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, start);
    testCond((s == NATS_OK)
             && (nc->ps->ma.size == msgSize)
             && (nc->ps->ma.sid == 1)
             && (nc->ps->ma.subject->len == 1)
             && (strncmp(nc->ps->ma.subject->data, "a", 1) == 0)
             && (nc->ps->ma.reply->len == 1)
             && (strncmp(nc->ps->ma.reply->data, "b", 1) == 0)
             && (nc->ps->argBuf != NULL)
             && (nc->ps->msgBuf != NULL));

    expectedCount++;
    expectedSize += (uint64_t)msgSize;

    // parsing: 'abcde...'
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + start, (int)strlen(buf) - start - 2);
    testCond((s == NATS_OK)
             && (nc->ps->argBuf != NULL)
             && (nc->ps->msgBuf != NULL)
             && (nc->ps->state == MSG_PAYLOAD));

    // Verify content
    PARSER_START_TEST;
    s = ((strncmp(nc->ps->msgBuf->data, "foo", 3) == 0) ? NATS_OK : NATS_ERR);
    if (s == NATS_OK)
    {
        int k;

        for (k=3; (s == NATS_OK) && (k<nc->ps->ma.size); k++)
            s = (nc->ps->msgBuf->data[k] == (char)('a' + ((k-3) % 26)) ? NATS_OK : NATS_ERR);
    }
    testCond(s == NATS_OK)

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf + (int)strlen(buf) - 2, 2);
    testCond((s == NATS_OK)
             && (nc->stats.inMsgs == expectedCount)
             && (nc->stats.inBytes == expectedSize)
             && (nc->ps->argBuf == NULL)
             && (nc->ps->msgBuf == NULL)
             && (nc->ps->state == OP_START));

    natsConnection_Destroy(nc);
}

#define RECREATE_PARSER \
    natsParser_Destroy(nc->ps); \
    s = natsParser_Create(&(nc->ps)); \
    if (s != NATS_OK) \
        FAIL("Unable to setup test"); \

static void
test_ProcessMsgArgs(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            buf[2048];
    const char*     le = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    snprintf(buf, sizeof(buf), "%s", "MSG a b c d e\r\n");

    test("Parsing MSG with too many arguments: ")
    // parsing: 'MSG a'
    natsParser_Parse(nc, buf, 5);
    // parse the rest..
    natsParser_Parse(nc, buf + 5, 10);
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "wrong number of arguments") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG foo 1\r\n");
    test("Parsing MSG with not enough arguments: ")
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "wrong number of arguments") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG foo abc 2\r\n");
    test("Parsing MSG with bad sid: ")
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Sid") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG foo 1 abc\r\n");
    test("Parsing MSG with bad size: ")
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Size") != NULL));

    snprintf(buf, sizeof(buf), "%s", "MSG foo 1 bar abc\r\n");
    test("Parsing MSG with bad size (with reply): ")
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Size") != NULL));

    // Test extra spaces first without reply

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG foo  1 2\r\n");
    test("Parsing MSG with extra space before sid: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "foo", 3) == 0)
                && (nc->ps->ma.sid == 1)
                && (nc->ps->ma.reply == NULL)
                && (nc->ps->ma.size == 2));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG bar 1  2\r\n");
    test("Parsing MSG with extra space before size: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "bar", 3) == 0)
                && (nc->ps->ma.sid == 1)
                && (nc->ps->ma.reply == NULL)
                && (nc->ps->ma.size == 2));

    // Test extra spaces first with reply

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG baz  3 bat 4\r\n");
    test("Parsing MSG with extra space before sid: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "baz", 3) == 0)
                && (nc->ps->ma.sid == 3)
                && (natsBuf_Len(nc->ps->ma.reply) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.reply), "bat", 3) == 0)
                && (nc->ps->ma.size == 4));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG boo 5  baa 6\r\n");
    test("Parsing MSG with extra space before reply: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "boo", 3) == 0)
                && (nc->ps->ma.sid == 5)
                && (natsBuf_Len(nc->ps->ma.reply) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.reply), "baa", 3) == 0)
                && (nc->ps->ma.size == 6));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG coo 7 caa  8\r\n");
    test("Parsing MSG with extra space before size: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "coo", 3) == 0)
                && (nc->ps->ma.sid == 7)
                && (natsBuf_Len(nc->ps->ma.reply) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.reply), "caa", 3) == 0)
                && (nc->ps->ma.size == 8));

    // Test with extra space everywhere

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "MSG  doo  8  daa   9  \r\n");
    test("Parsing MSG with extra space everywhere: ")
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "doo", 3) == 0)
                && (nc->ps->ma.sid == 8)
                && (natsBuf_Len(nc->ps->ma.reply) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.reply), "daa", 3) == 0)
                && (nc->ps->ma.size == 9));

    // Test HMSG

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 bar 2 3\r\n");
    test("Parsing HMSG: ");
    s = natsParser_Parse(nc, buf, (int) strlen(buf));
    testCond((s == NATS_OK)
                && (natsBuf_Len(nc->ps->ma.subject) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.subject), "foo", 3) == 0)
                && (nc->ps->ma.sid == 1)
                && (natsBuf_Len(nc->ps->ma.reply) == 3)
                && (strncmp(natsBuf_Data(nc->ps->ma.reply), "bar", 3) == 0)
                && (nc->ps->ma.hdr == 2)
                && (nc->ps->ma.size == 3));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 3\r\n");
    test("Parsing HMSG not enough args: ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "wrong number of arguments") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG a b c d e f\r\n");
    test("Parsing HMSG too many args: ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "wrong number of arguments") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo abc 2 4\r\n");
    test("Parsing HMSG with bad sid: ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Sid") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 baz 10\r\n");
    test("Parsing HMSG with bad header size: ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Header Size") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 bar baz 10\r\n");
    test("Parsing HMSG with bad header size (with reply): ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Header Size") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 10 4\r\n");
    test("Parsing HMSG with bad header size (out of range): ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Header Size") != NULL));

    RECREATE_PARSER;

    snprintf(buf, sizeof(buf), "%s", "HMSG foo 1 bar 10 4\r\n");
    test("Parsing HMSG with bad header size (out of range with reply): ");
    natsParser_Parse(nc, buf, (int) strlen(buf));
    s = natsConnection_GetLastError(nc, &le);
    testCond((s == NATS_PROTOCOL_ERROR)
                && (nc->ps->argBuf == NULL)
                && (nc->ps->msgBuf == NULL)
                && (nc->ps->ma.subject == NULL)
                && (nc->ps->ma.reply == NULL)
                && (le != NULL)
                && (strstr(le, "Bad or Missing Header Size") != NULL));

    natsConnection_Destroy(nc);
}

static natsStatus
_checkPool(natsConnection *nc, char **expectedURLs, int expectedURLsCount)
{
    int     i, j, attempts;
    natsSrv *srv;
    char    *url;
    char    buf[64];
    bool    ok;

    natsMutex_Lock(nc->mu);
    if (nc->srvPool->size != expectedURLsCount)
    {
        printf("Expected pool size to be %d, got %d\n", expectedURLsCount, nc->srvPool->size);
        natsMutex_Unlock(nc->mu);
        return NATS_ERR;
    }
    for (attempts=0; attempts<20; attempts++)
    {
        for (i=0; i<expectedURLsCount; i++)
        {
            url = expectedURLs[i];
            ok = false;
            for (j=0; j<nc->srvPool->size; j++)
            {
                srv = nc->srvPool->srvrs[j];
                snprintf(buf, sizeof(buf), "%s:%d", srv->url->host, srv->url->port);
                if (strcmp(buf, url))
                {
                    ok = true;
                    break;
                }
            }
            if (!ok)
            {
                natsMutex_Unlock(nc->mu);
                nats_Sleep(100);
                natsMutex_Lock(nc->mu);
                continue;
            }
        }
        natsMutex_Unlock(nc->mu);
        return NATS_OK;
    }
    natsMutex_Unlock(nc->mu);
    return NATS_ERR;
}

static natsStatus
checkNewURLsAddedRandomly(natsConnection *nc, char **urlsAfterPoolSetup, int initialPoolSize)
{
    natsStatus  s;
    int         i;
    char        **currentPool = NULL;
    int         currentPoolSize = 0;

    s = natsConnection_GetServers(nc, &currentPool, &currentPoolSize);
    if (s == NATS_OK)
    {
        // Reset status to error by default. If we find a new URL
        // before the end of the initial list, we consider success
        // and return.
        s = NATS_ERR;
        for (i= 0; i < initialPoolSize; i++)
        {
            // If one of the position in initial list is occupied
            // by a new URL, we are ok.
            if (strcmp(urlsAfterPoolSetup[i], currentPool[i]))
            {
                s = NATS_OK;
                break;
            }
        }
    }
    if (currentPool != NULL)
    {
        for (i=0; i<currentPoolSize; i++)
            free(currentPool[i]);
        free(currentPool);
    }
    return s;
}

static void
test_AsyncINFO(void)
{
    natsConnection  *nc = NULL;
    natsOptions     *opts = NULL;
    natsStatus      s;
    char            buf[2048];
    int             i;
    const char      *good[] = {"INFO {}\r\n", "INFO  {}\r\n", "INFO {} \r\n",
                               "INFO { \"server_id\": \"test\"  }   \r\n",
                               "INFO {\"connect_urls\":[]}\r\n"};
    const char      *wrong[] = {"IxNFO {}\r\n", "INxFO {}\r\n", "INFxO {}\r\n",
                                "INFOx {}\r\n", "INFO{}\r\n", "INFO {}"};
    const char      *lastErr = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    snprintf(buf, sizeof(buf), "%s", "INFO {\"test\":\"abcde\"x\r\n");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 9);
    testCond((s == NATS_OK)
                && (nc->ps->state == INFO_ARG)
                && (nc->ps->argBuf != NULL));

    PARSER_START_TEST;
    natsParser_Parse(nc, buf+9, 30);
    lastErr = nats_GetLastError(&s);
    testCond((s == NATS_ERR)
                && (lastErr != NULL)
                && (strstr(lastErr, "missing") != NULL));
    nats_clearLastError();

    snprintf(buf, sizeof(buf), "%s", "INFO {}\r\n");

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_I));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+1, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_IN));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+2, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_INF));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+3, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_INFO));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+4, 1);
    testCond((s == NATS_OK) && (nc->ps->state == OP_INFO_SPC));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+5, (int)strlen(buf)-5);
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // All at once
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // Server pool was setup in natsConn_create()

    // Partials requiring argBuf
    snprintf(buf, sizeof(buf), "INFO {\"server_id\":\"%s\", \"host\":\"%s\", \"port\": %d, " \
            "\"auth_required\":%s, \"tls_required\": %s, \"max_payload\":%d}\r\n",
            "test", "localhost", 4222, "true", "true", 2*1024*1024);

    PARSER_START_TEST;
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, 9);
    testCond((s == NATS_OK)
                && (nc->ps->state == INFO_ARG)
                && (nc->ps->argBuf != NULL));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+9, 2);
    testCond((s == NATS_OK)
                && (nc->ps->state == INFO_ARG)
                && (nc->ps->argBuf != NULL));

    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf+11, (int)strlen(buf)-11);
    testCond((s == NATS_OK)
                && (nc->ps->state == OP_START)
                && (nc->ps->argBuf == NULL));

    test("Check INFO is correct: ");
    testCond((s == NATS_OK)
                && (strcmp(nc->info.id, "test") == 0)
                && (strcmp(nc->info.host, "localhost") == 0)
                && (nc->info.port == 4222)
                && nc->info.authRequired
                && nc->info.tlsRequired
                && (nc->info.maxPayload == 2*1024*1024));

    // Destroy parser, it will be recreated in the loops.
    natsParser_Destroy(nc->ps);
    nc->ps = NULL;

    // Good INFOs
    for (i=0; i<(int) (sizeof(good)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "Test with good INFO proto number %d: ", (i+1));
        test(buf);
        s = natsParser_Create(&(nc->ps));
        IFOK(s, natsParser_Parse(nc, (char*) good[i], (int)strlen(good[i])));
        testCond((s == NATS_OK)
                    && (nc->ps->state == OP_START)
                    && (nc->ps->argBuf == NULL));
        natsParser_Destroy(nc->ps);
        nc->ps = NULL;
    }

    // Wrong INFOs
    for (i=0; i<(int) (sizeof(wrong)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "Test with wrong INFO proto number %d: ", (i+1));
        test(buf);
        s = natsParser_Create(&(nc->ps));
        IFOK(s, natsParser_Parse(nc, (char*) wrong[i], (int)strlen(wrong[i])));
        testCond(!((s == NATS_OK) && (nc->ps->state == OP_START)));
        natsParser_Destroy(nc->ps);
        nc->ps = NULL;
    }
    nats_clearLastError();

    // Now test the decoding of "connect_urls"

    // Destroy, we create a new one
    natsConnection_Destroy(nc);
    nc = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsConn_create(&nc, opts));
    IFOK(s, natsParser_Create(&(nc->ps)));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    snprintf(buf, sizeof(buf), "%s", "INFO {\"connect_urls\":[\"localhost:4222\",\"localhost:5222\"]}\r\n");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    if (s == NATS_OK)
    {
        // Pool now should contain localhost:4222 (the default URL) and localhost:5222
        const char *urls[] = {"localhost:4222", "localhost:5222"};

        s = _checkPool(nc, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
    }
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // Make sure that if client receives the same, it is not added again.
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    if (s == NATS_OK)
    {
        // Pool should still contain localhost:4222 (the default URL) and localhost:5222
        const char *urls[] = {"localhost:4222", "localhost:5222"};

        s = _checkPool(nc, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
    }
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    // Receive a new URL
    snprintf(buf, sizeof(buf), "%s", "INFO {\"connect_urls\":[\"localhost:4222\",\"localhost:5222\",\"localhost:6222\"]}\r\n");
    PARSER_START_TEST;
    s = natsParser_Parse(nc, buf, (int)strlen(buf));
    if (s == NATS_OK)
    {
        // Pool now should contain localhost:4222 (the default URL) localhost:5222 and localhost:6222
        const char *urls[] = {"localhost:4222", "localhost:5222", "localhost:6222"};

        s = _checkPool(nc, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
    }
    testCond((s == NATS_OK) && (nc->ps->state == OP_START));

    natsConnection_Destroy(nc);
    nc = NULL;

    // Check that new URLs are added at random places in the pool
    if (s == NATS_OK)
    {
        int  initialPoolSize = 0;
        char **urlsAfterPoolSetup = NULL;
        const char *newURLs = "\"impA:4222\", \"impB:4222\", \"impC:4222\", "\
            "\"impD:4222\", \"impE:4222\", \"impF:4222\", \"impG:4222\", "\
            "\"impH:4222\", \"impI:4222\", \"impJ:4222\"";

        s = natsOptions_Create(&opts);
        IFOK(s, natsOptions_SetNoRandomize(opts, false));
        IFOK(s, natsOptions_SetServers(opts, testServers, sizeof(testServers)/sizeof(char*)));
        IFOK(s, natsConn_create(&nc, opts));
        IFOK(s, natsParser_Create(&(nc->ps)));
        // Capture the pool sequence after randomization
        IFOK(s, natsConnection_GetServers(nc, &urlsAfterPoolSetup, &initialPoolSize));
        if (s != NATS_OK)
            FAIL("Unable to setup test");

        // Add new urls
        snprintf(buf, sizeof(buf), "INFO {\"connect_urls\":[%s]}\r\n", newURLs);
        test("New URLs are added randomly: ");
        s = natsParser_Parse(nc, buf, (int)strlen(buf));
        IFOK(s, checkNewURLsAddedRandomly(nc, urlsAfterPoolSetup, initialPoolSize));
        testCond((s == NATS_OK) && (nc->ps->state == OP_START));

        test("First URL should not have been changed: ")
        testCond((s == NATS_OK) && !strcmp(nc->srvPool->srvrs[0]->url->fullUrl, urlsAfterPoolSetup[0]));

        if (urlsAfterPoolSetup != NULL)
        {
            for (i=0; i<initialPoolSize; i++)
                free(urlsAfterPoolSetup[i]);
            free(urlsAfterPoolSetup);
        }

        natsConnection_Destroy(nc);
    }
}

static void
_parallelRequests(void *closure)
{
    natsConnection      *nc = (natsConnection*) closure;
    natsMsg             *msg;

    // Expecting this to timeout. This is to force parallel
    // requests.
    natsConnection_RequestString(&msg, nc, "foo", "test", 500);
}

static void
test_RequestPool(void)
{
    natsStatus          s;
    natsPid             pid = NATS_INVALID_PID;
    int                 i;
    natsConnection      *nc = NULL;
    natsSubscription    *sub = NULL;
    natsMsg             *msg = NULL;
    int                 numThreads = RESP_INFO_POOL_MAX_SIZE+5;
    natsThread          *threads[RESP_INFO_POOL_MAX_SIZE+5];

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    // For part of this test, we really need to have the requestor
    // timeout with the given timeout value (to increase number)
    // of parallel requests. So we create a sync subscription that
    // will not send a response back.
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    if (s != NATS_OK)
    {
        natsConnection_Destroy(nc);
        FAIL("Unable to setup test!");
    }

    // With current implementation, the pool should not
    // increase at all.
    test("Pool not growing: ");
    for (i=0; (i<RESP_INFO_POOL_MAX_SIZE); i++)
        natsConnection_RequestString(&msg, nc, "foo", "test", 1);
    natsMutex_Lock(nc->mu);
    testCond(nc->respPoolSize == 1);
    natsMutex_Unlock(nc->mu);

    test("Pool max size: ");
    for (i=0; i<numThreads; i++)
        threads[i] = NULL;

    s = NATS_OK;
    for (i=0; (s == NATS_OK) && (i<numThreads); i++)
        s = natsThread_Create(&threads[i], _parallelRequests, (void*) nc);

    for (i=0; i<numThreads; i++)
    {
        if (threads[i] != NULL)
        {
            natsThread_Join(threads[i]);
            natsThread_Destroy(threads[i]);
        }
    }
    natsMutex_Lock(nc->mu);
    testCond((s == NATS_OK) && (nc->respPoolSize == RESP_INFO_POOL_MAX_SIZE));
    natsMutex_Unlock(nc->mu);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    _stopServer(pid);
}

static void
test_NoFlusherIfSendAsap(void)
{
    natsStatus          s;
    natsPid             pid = NATS_INVALID_PID;
    natsConnection      *nc = NULL;
    natsOptions         *opts = NULL;
    natsSubscription    *sub = NULL;
    struct threadArg    arg;
    int                 i;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if ((opts == NULL)
            || (natsOptions_SetURL(opts, "nats://127.0.0.1:4222") != NATS_OK)
            || (natsOptions_SetSendAsap(opts, true) != NATS_OK)
            || (natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg)))
    {
        FAIL("Failed to setup test");
    }
    arg.string = "test";
    arg.control = 1;

    pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect/subscribe ok: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    for (i=0; i<2; i++)
    {
        test("Send ok: ");
        s = natsConnection_PublishString(nc, "foo", "test");
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.msgReceived)
            s = natsCondition_TimedWait(arg.c, arg.m, 1500);
        natsMutex_Unlock(arg.m);
        testCond(s == NATS_OK);

        test("Flusher does not exist: ");
        natsMutex_Lock(nc->mu);
        s = (nc->flusherThread == NULL ? NATS_OK : NATS_ERR);
        natsMutex_Unlock(nc->mu);
        testCond(s == NATS_OK);

        if (i == 0)
        {
            _stopServer(pid);
            pid = NATS_INVALID_PID;
            pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
            CHECK_SERVER_STARTED(pid);
        }
    }

    natsSubscription_Destroy(sub);
    natsConnection_Close(nc);
    _waitForConnClosed(&arg);
    natsConnection_Destroy(nc);

    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
test_HeadersAndSubPendingBytes(void)
{
    natsStatus          s;
    natsPid             pid   = NATS_INVALID_PID;
    natsConnection      *nc   = NULL;
    natsSubscription    *sub1 = NULL;
    natsSubscription    *sub2 = NULL;
    natsMsg             *msg  = NULL;
    natsMsg             *smsg = NULL;
    int                 msgs  = 0;
    int                 bytes = 0;
    struct threadArg    arg;
    int                 i;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");
    arg.string = "test";

    pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect/subscribe ok: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub1, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsSubscription_SetPendingLimits(sub1, 1000, 100));
    IFOK(s, natsConnection_SubscribeSync(&sub2, nc, "foo"));
    IFOK(s, natsSubscription_SetPendingLimits(sub2, 1000, 100));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Create message with header: ");
    s = natsMsg_Create(&msg, "foo", NULL, "hello", 5);
    // Set a header with "large" value (50 bytes)
    IFOK(s, natsMsgHeader_Set(msg, "Key", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    testCond(s == NATS_OK);

    for (i=0; i<10; i++) {
        test("Publish and receive message: ");
        s = natsConnection_PublishMsg(nc, msg);
        IFOK(s, natsSubscription_NextMsg(&smsg, sub2, 1000));
        if (s == NATS_OK)
        {
            natsMutex_Lock(arg.m);
            while ((s != NATS_TIMEOUT) && !arg.msgReceived)
                s = natsCondition_TimedWait(arg.c, arg.m, 1000);
            arg.msgReceived = false;
            natsMutex_Unlock(arg.m);
        }
        natsMsg_Destroy(smsg);
        smsg = NULL;
        testCond(s == NATS_OK);
    }

    test("Check sub1's pending: ");
    s = natsSubscription_GetPending(sub1, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    test("Check sub2's pending: ");
    s = natsSubscription_GetPending(sub1, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub1);
    natsSubscription_Destroy(sub2);
    natsConnection_Destroy(nc);
    _destroyDefaultThreadArgs(&arg);
    _stopServer(pid);
}

static void
_dummyMsgHandler(natsConnection *nc, natsSubscription *sub, natsMsg *msg,
                void *closure)
{
    // do nothing

    natsMsg_Destroy(msg);
}

static void
test_LibMsgDelivery(void)
{
    natsStatus          s;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;
    natsConnection      *nc       = NULL;
    natsSubscription    *s1       = NULL;
    natsSubscription    *s2       = NULL;
    natsSubscription    *s3       = NULL;
    natsSubscription    *s4       = NULL;
    natsSubscription    *s5       = NULL;
    natsMsgDlvWorker    *lmd1     = NULL;
    natsMsgDlvWorker    *lmd2     = NULL;
    natsMsgDlvWorker    *lmd3     = NULL;
    natsMsgDlvWorker    *lmd4     = NULL;
    natsMsgDlvWorker    *lmd5     = NULL;
    natsMsgDlvWorker    **pwks    = NULL;
    int                 psize     = 0;
    int                 pmaxSize  = 0;
    int                 pidx      = 0;

    // First, close the library and re-open, to reset things
    nats_Close();

    nats_Sleep(100);

    nats_Open(-1);

    // Check some pre-conditions that need to be met for the test to work.
    test("Check initial values: ")
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    testCond((pmaxSize == 2) && (psize == 0) && (pidx == 0));

    test("Check pool size not negative: ")
    s = nats_SetMessageDeliveryPoolSize(-1);
    testCond(s != NATS_OK);

    test("Check pool size not zero: ")
    s = nats_SetMessageDeliveryPoolSize(0);
    testCond(s != NATS_OK);

    // Reset stack since we know the above generated errors.
    nats_clearLastError();

    test("Check pool size decreased no error: ")
    s = nats_SetMessageDeliveryPoolSize(1);
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    testCond((s == NATS_OK) && (pmaxSize == 2) && (psize == 0));

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_UseGlobalMessageDelivery(opts, true));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Subscribe(&s1, nc, "foo", _dummyMsgHandler, NULL));
    if (s == NATS_OK)
    {
        natsMutex_Lock(s1->mu);
        lmd1 = s1->libDlvWorker;
        natsMutex_Unlock(s1->mu);
    }
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check 1st sub assigned 1st worker: ")
    testCond((s == NATS_OK) && (psize == 1) && (lmd1 != NULL)
             && (pidx == 1) && (pwks != NULL) && (lmd1 == pwks[0]));

    s = natsConnection_Subscribe(&s2, nc, "foo", _dummyMsgHandler, NULL);
    if (s == NATS_OK)
    {
        natsMutex_Lock(s2->mu);
        lmd2 = s2->libDlvWorker;
        natsMutex_Unlock(s2->mu);
    }
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check 2nd sub assigned 2nd worker: ")
    testCond((s == NATS_OK) && (psize == 2) && (lmd2 != lmd1)
             && (pidx == 0) && (pwks != NULL) && (lmd2 == pwks[1]));

    s = natsConnection_Subscribe(&s3, nc, "foo", _dummyMsgHandler, NULL);
    if (s == NATS_OK)
    {
        natsMutex_Lock(s3->mu);
        lmd3 = s3->libDlvWorker;
        natsMutex_Unlock(s3->mu);
    }
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check 3rd sub assigned 1st worker: ")
    testCond((s == NATS_OK) && (psize == 2) && (lmd3 == lmd1)
             && (pidx == 1) && (pwks != NULL) && (lmd3 == pwks[0]));

    // Bump the pool size to 4
    s = nats_SetMessageDeliveryPoolSize(4);
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check increase of pool size: ");
    testCond((s == NATS_OK) && (psize == 2) && (pidx == 1)
             && (pmaxSize == 4) && (pwks != NULL));

    s = natsConnection_Subscribe(&s4, nc, "foo", _dummyMsgHandler, NULL);
    if (s == NATS_OK)
    {
        natsMutex_Lock(s4->mu);
        lmd4 = s4->libDlvWorker;
        natsMutex_Unlock(s4->mu);
    }
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check 4th sub assigned 2nd worker: ")
    testCond((s == NATS_OK) && (psize == 2) && (lmd4 == lmd2)
             && (pidx == 2) && (pwks != NULL) && (lmd4 == pwks[1]));

    s = natsConnection_Subscribe(&s5, nc, "foo", _dummyMsgHandler, NULL);
    if (s == NATS_OK)
    {
        natsMutex_Lock(s5->mu);
        lmd5 = s5->libDlvWorker;
        natsMutex_Unlock(s5->mu);
    }
    natsLib_getMsgDeliveryPoolInfo(&pmaxSize, &psize, &pidx, &pwks);
    test("Check 5th sub assigned 3rd worker: ")
    testCond((s == NATS_OK) && (psize == 3) && (lmd5 != lmd2)
             && (pidx == 3) && (pwks != NULL) && (lmd5 == pwks[2]));

    natsSubscription_Destroy(s5);
    natsSubscription_Destroy(s4);
    natsSubscription_Destroy(s3);
    natsSubscription_Destroy(s2);
    natsSubscription_Destroy(s1);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _stopServer(serverPid);

    // Close the library and re-open, to reset things
    nats_Close();

    nats_Sleep(100);

    nats_Open(-1);
}

static void
test_DefaultConnection(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetTimeout(opts, 500));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Check connection fails without running server: ");
#ifndef _WIN32
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    if (s != NATS_OK)
#endif
        s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_NO_SERVER);

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test default connection: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
}

static void
test_SimplifiedURLs(void)
{
    natsStatus          s         = NATS_OK;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
#if defined(NATS_HAS_TLS)
    natsOptions         *opts     = NULL;
#endif
    const char          *urls[] = {
        "nats://127.0.0.1:4222",
        "nats://127.0.0.1:",
        "nats://127.0.0.1",
        "127.0.0.1:",
        "127.0.0.1"
    };
    int                 urlsCount = sizeof(urls) / sizeof(char *);
    int                 i;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test simplified URLs to non TLS server: ");
    for (i=0; ((s == NATS_OK) && (i<urlsCount)); i++)
    {
        s = natsConnection_ConnectTo(&nc, urls[i]);
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
    }
    testCond(s == NATS_OK);

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

#if defined(NATS_HAS_TLS)
    serverPid = _startServer("nats://127.0.0.1:4222", "-c tls_default_port.conf -DV", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SkipServerVerification(opts, true));

    test("Test simplified URLs to TLS server: ");
    for (i=0; ((s == NATS_OK) && (i<urlsCount)); i++)
    {
        s = natsOptions_SetURL(opts, urls[i]);
        IFOK(s, natsConnection_Connect(&nc, opts));
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
    }
    testCond(s == NATS_OK);

    natsOptions_Destroy(opts);
    _stopServer(serverPid);
#endif
}

static void
test_IPResolutionOrder(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://localhost:4222"));
    IFOK(s, natsOptions_SetTimeout(opts, 500));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Server listens to IPv4: ");
    serverPid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
    CHECK_SERVER_STARTED(serverPid);
    testCond(serverPid != NATS_INVALID_PID);

    test("Order: 4: ");
    s = natsOptions_IPResolutionOrder(opts, 4);
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsConnection_Destroy(nc);
        nc = NULL;
    }
    testCond(s == NATS_OK);

    test("Order: 46: ");
    s = natsOptions_IPResolutionOrder(opts, 46);
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsConnection_Destroy(nc);
        nc = NULL;
    }
    testCond(s == NATS_OK);


    test("Order: 64: ");
    s = natsOptions_IPResolutionOrder(opts, 64);
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsConnection_Destroy(nc);
        nc = NULL;
    }
    testCond(s == NATS_OK);

    test("Order: 0: ");
    s = natsOptions_IPResolutionOrder(opts, 0);
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsConnection_Destroy(nc);
        nc = NULL;
    }
    testCond(s == NATS_OK);

    test("Order: 6: ");
    s = natsOptions_IPResolutionOrder(opts, 6);
    if (s == NATS_OK)
    {
        s = natsConnection_Connect(&nc, opts);
        if (s == NATS_OK)
        {
            // Should not have connected
            natsConnection_Destroy(nc);
            nc = NULL;
            s = NATS_ERR;
        }
        else
        {
            s = NATS_OK;
        }
    }
    testCond(s == NATS_OK);

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    if (!runOnTravis)
    {
        test("Server listens to IPv6: ");
        serverPid = _startServer("nats://[::1]:4222", "-a :: -p 4222", true);
        CHECK_SERVER_STARTED(serverPid);
        testCond(serverPid != NATS_INVALID_PID);

        test("Order: 6: ");
        s = natsOptions_IPResolutionOrder(opts, 6);
        IFOK(s, natsConnection_Connect(&nc, opts));
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
        testCond(s == NATS_OK);

        test("Order: 46: ");
        s = natsOptions_IPResolutionOrder(opts, 46);
        IFOK(s, natsConnection_Connect(&nc, opts));
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
        testCond(s == NATS_OK);

        test("Order: 64: ");
        s = natsOptions_IPResolutionOrder(opts, 64);
        IFOK(s, natsConnection_Connect(&nc, opts));
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
        testCond(s == NATS_OK);

        test("Order: 0: ");
        s = natsOptions_IPResolutionOrder(opts, 0);
        IFOK(s, natsConnection_Connect(&nc, opts));
        if (s == NATS_OK)
        {
            natsConnection_Destroy(nc);
            nc = NULL;
        }
        testCond(s == NATS_OK);

        test("Order: 4: ");
        s = natsOptions_IPResolutionOrder(opts, 4);
        if (s == NATS_OK)
        {
            s = natsConnection_Connect(&nc, opts);
            // This should fail, but server listening to
            // [::] still accepts IPv4 connections, so be
            // tolerant of that.
            if (s == NATS_OK)
            {
                fprintf(stderr, ">>>> Server listening on [::] accepted an IPv4 connection");
                natsConnection_Destroy(nc);
                nc = NULL;
            }
            else
            {
                s = NATS_OK;
            }
        }
        testCond(s == NATS_OK);

        _stopServer(serverPid);
        serverPid = NATS_INVALID_PID;
    }

    natsOptions_Destroy(opts);
}

static void
test_UseDefaultURLIfNoServerSpecified(void)
{
    natsStatus          s;
    natsOptions         *opts     = NULL;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    s = natsOptions_Create(&opts);
    if (s != NATS_OK)
        FAIL("Unable to create options!");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check we can connect even if no server is specified: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnectToWithMultipleURLs(void)
{
    natsStatus      s;
    natsConnection  *nc       = NULL;
    natsPid         serverPid = NATS_INVALID_PID;
    char            buf[256];

    buf[0] = '\0';

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check multiple URLs work: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4444,nats://127.0.0.1:4443,nats://127.0.0.1:4222");
    IFOK(s, natsConnection_Flush(nc));
    IFOK(s, natsConnection_GetConnectedUrl(nc, buf, sizeof(buf)));
    testCond((s == NATS_OK)
             && (strcmp(buf, "nats://127.0.0.1:4222") == 0));
    natsConnection_Destroy(nc);

    test("Check multiple URLs work, even with spaces: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4444 , nats://127.0.0.1:4443  ,  nats://127.0.0.1:4222   ");
    IFOK(s, natsConnection_Flush(nc));
    IFOK(s, natsConnection_GetConnectedUrl(nc, buf, sizeof(buf)));
    testCond((s == NATS_OK)
             && (strcmp(buf, "nats://127.0.0.1:4222") == 0));
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnectionToWithNullURLs(void)
{
    natsStatus      s;
    natsConnection  *nc       = NULL;
    natsPid         serverPid = NATS_INVALID_PID;
    char            buf[256];

    test("Check NULL URLs: ");
    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NULL);
    IFOK(s, natsConnection_Flush(nc));
    IFOK(s, natsConnection_GetConnectedUrl(nc, buf, sizeof(buf)));

    testCond((s == NATS_OK) && (strcmp(buf, NATS_DEFAULT_URL) == 0));
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnectionWithNullOptions(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check connect with NULL options is allowed: ");
    s = natsConnection_Connect(&nc, NULL);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnectionStatus(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    test("Test connection status should be CONNECTED: ");
    testCond((s == NATS_OK)
             && (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    if (s == NATS_OK)
    {
        natsConnection_Close(nc);
        test("Test connection status should be CLOSED: ");
        testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CLOSED);
    }

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnClosedCB(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetURL(opts, NATS_DEFAULT_URL) != NATS_OK)
        || (natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg) != NATS_OK))
    {
        FAIL("Unable to setup test for ConnClosedCB!");
    }

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    if (s == NATS_OK)
        natsConnection_Close(nc);

    test("Test connection closed CB invoked: ");

    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK) && arg.closed);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_CloseDisconnectedCB(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetURL(opts, NATS_DEFAULT_URL) != NATS_OK)
        || (natsOptions_SetAllowReconnect(opts, false) != NATS_OK)
        || (natsOptions_SetDisconnectedCB(opts, _closedCb, (void*) &arg) != NATS_OK))
    {
        FAIL("Unable to setup test for ConnClosedCB!");
    }

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    if (s == NATS_OK)
        natsConnection_Close(nc);

    test("Test connection disconnected CB invoked: ");

    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK) && arg.closed);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ServerStopDisconnectedCB(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetURL(opts, NATS_DEFAULT_URL) != NATS_OK)
        || (natsOptions_SetAllowReconnect(opts, false) != NATS_OK)
        || (natsOptions_SetDisconnectedCB(opts, _closedCb, (void*) &arg) != NATS_OK))
    {
        FAIL("Unable to setup test for ConnClosedCB!");
    }

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);

    _stopServer(serverPid);

    test("Test connection disconnected CB invoked on server shutdown: ");

    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK) && arg.closed);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ClosedConnections(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *goodsub  = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&goodsub, nc, "foo"));
    if (s == NATS_OK)
        natsConnection_Close(nc);

    // Test all API endpoints do the right thing with a closed connection.

    test("Publish on closed should fail: ")
    s = natsConnection_Publish(nc, "foo", NULL, 0);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("PublishMsg on closed should fail: ")
    s = natsMsg_Create(&msg, "foo", NULL, NULL, 0);
    IFOK(s, natsConnection_PublishMsg(nc, msg));
    testCond(s == NATS_CONNECTION_CLOSED);

    natsMsg_Destroy(msg);
    msg = NULL;

    test("Flush on closed should fail: ")
    s = natsConnection_Flush(nc);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("Subscribe on closed should fail: ")
    s = natsConnection_Subscribe(&sub, nc, "foo", _dummyMsgHandler, NULL);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("SubscribeSync on closed should fail: ")
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    testCond(s == NATS_CONNECTION_CLOSED);

    test("QueueSubscribe on closed should fail: ")
    s = natsConnection_QueueSubscribe(&sub, nc, "foo", "bar", _dummyMsgHandler, NULL);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("QueueSubscribeSync on closed should fail: ")
    s = natsConnection_QueueSubscribeSync(&sub, nc, "foo", "bar");
    testCond(s == NATS_CONNECTION_CLOSED);

    test("Request on closed should fail: ")
    s = natsConnection_Request(&msg, nc, "foo", NULL, 0, 10);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("NextMsg on closed should fail: ")
    s = natsSubscription_NextMsg(&msg, goodsub, 10);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("Unsubscribe on closed should fail: ")
    s = natsSubscription_Unsubscribe(goodsub);
    testCond(s == NATS_CONNECTION_CLOSED);

    natsSubscription_Destroy(goodsub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnectVerboseOption(void)
{
    natsStatus          s         = NATS_OK;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
    {
        opts = _createReconnectOptions();
        if (opts == NULL)
            s = NATS_ERR;
    }
    IFOK(s, natsOptions_SetVerbose(opts, true));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check connect OK with Verbose option: ");

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Flush(nc));

    testCond(s == NATS_OK)

    _stopServer(serverPid);
    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check reconnect OK with Verbose option: ");

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.reconnected)
        s = natsCondition_TimedWait(args.c, args.m, 5000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_Flush(nc));

    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
}

static void
test_ReconnectThreadLeak(void)
{
    natsStatus          s;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsConnection      *nc       = NULL;
    int                 i;
    struct threadArg    arg;

    serverPid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = _createDefaultThreadArgsForCbTests(&arg);

    opts = _createReconnectOptions();
    if ((opts == NULL)
            || (natsOptions_SetURL(opts, "nats://127.0.0.1:4222") != NATS_OK)
            || (natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg) != NATS_OK)
            || (natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg) != NATS_OK)
            || (natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg) != NATS_OK))
    {
        FAIL("Unable to setup test");
    }

    test("Connect ok: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    for (i=0; i<10; i++)
    {
        natsMutex_Lock(nc->mu);
        natsSock_Shutdown(nc->sockCtx.fd);
        natsMutex_Unlock(nc->mu);

        test("Waiting for disconnect: ");
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.disconnected)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        arg.disconnected = false;
        natsMutex_Unlock(arg.m);
        testCond(s == NATS_OK);

        test("Waiting for reconnect: ");
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.reconnected)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        arg.reconnected = false;
        natsMutex_Unlock(arg.m);
        testCond(s == NATS_OK);
    }

    natsConnection_Close(nc);
    _waitForConnClosed(&arg);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ReconnectTotalTime(void)
{
    natsStatus  s;
    natsOptions *opts = NULL;

    test("Check reconnect time: ");
    s = natsOptions_Create(&opts);
    testCond((s == NATS_OK)
             && ((opts->maxReconnect * opts->reconnectWait) >= (2 * 60 * 1000)));

    natsOptions_Destroy(opts);
}

static void
test_ReconnectDisallowedFlags(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:22222"));
    IFOK(s, natsOptions_SetAllowReconnect(opts, false));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    test("Test connection closed CB invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ReconnectAllowedFlags(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Create options and connect: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:22222"));
    IFOK(s, natsOptions_SetAllowReconnect(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 2));
    IFOK(s, natsOptions_SetReconnectWait(opts, 1000));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    test("Test reconnecting in progress: ");

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 500);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_TIMEOUT)
             && !arg.disconnected
             && natsConnection_IsReconnecting(nc));

    natsConnection_Close(nc);
    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 500);
    natsMutex_Unlock(arg.m);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
_closeConn(void *arg)
{
    natsConnection *nc = (natsConnection*) arg;

    natsConnection_Close(nc);
}

static void
test_ConnCloseBreaksReconnectLoop(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsThread          *t        = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        opts = _createReconnectOptions();
        if (opts == NULL)
            s = NATS_NO_MEMORY;
    }
    IFOK(s, natsOptions_SetMaxReconnect(opts, 1000));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, &arg));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connection close breaks out reconnect loop: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Flush(nc));

    // Shutdown the server
    _stopServer(serverPid);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 3000);
    natsMutex_Unlock(arg.m);

    // Wait a bit before trying to close the connection to make sure
    // that the reconnect loop has started.
    nats_Sleep(1000);

    // Close the connection, this should break the reconnect loop.
    // Do this in a go routine since the issue was that Close()
    // would block until the reconnect loop is done.
    s = natsThread_Create(&t, _closeConn, (void*) nc);

    // Even on Windows (where a createConn takes more than a second)
    // we should be able to break the reconnect loop with the following
    // timeout.
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 3000);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK) && arg.closed);

    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_BasicReconnectFunctionality(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.string = "bar";
        arg.status = NATS_OK;
    }
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg) != NATS_OK)
        || (natsOptions_SetClosedCB(opts, _closedCb, &arg) != NATS_OK))
    {
        FAIL("Unable to create reconnect options!");
    }

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, &arg));
    IFOK(s, natsConnection_Flush(nc));

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    test("Disconnected CB invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 500);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    test("Publish message: ");
    s = natsConnection_PublishString(nc, "foo", arg.string);
    if (s == NATS_OK)
    {
        serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
        CHECK_SERVER_STARTED(serverPid);
    }
    IFOK(s, natsConnection_FlushTimeout(nc, 5000));
    testCond(s == NATS_OK);

    test("Check message received after reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 1500);
    natsMutex_Unlock(arg.m);

    if (s == NATS_OK)
        s = arg.status;
    testCond((s == NATS_OK) && (nc->stats.reconnects == 1));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _waitForConnClosed(&arg);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
_doneCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->done = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);

    natsMsg_Destroy(msg);
}

static void
test_ExtendedReconnectFunctionality(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsSubscription    *sub2     = NULL;
    natsSubscription    *sub3     = NULL;
    natsSubscription    *sub4     = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.string = "bar";
        arg.status = NATS_OK;
        arg.control=3;
    }
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg) != NATS_OK)
        || (natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg) != NATS_OK)
        || (natsOptions_SetClosedCB(opts, _closedCb, &arg) != NATS_OK))
    {
        FAIL("Unable to create reconnect options!");
    }

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Setup: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, &arg));
    IFOK(s, natsConnection_Subscribe(&sub2, nc, "foobar", _recvTestString, &arg));
    IFOK(s, natsConnection_PublishString(nc, "foo", arg.string));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    test("Disconnected CB invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    test("Some protos while disconnected: ");
    // Sub while disconnected
    s = natsConnection_Subscribe(&sub3, nc, "bar", _recvTestString, &arg);

    // Unsubscribe foo and bar while disconnected
    IFOK(s, natsSubscription_Unsubscribe(sub2));
    IFOK(s, natsConnection_PublishString(nc, "foo", arg.string));
    IFOK(s, natsConnection_PublishString(nc, "bar", arg.string));
    testCond(s == NATS_OK);

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    // Server is restarted, wait for reconnect
    test("Check reconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.reconnected);

    test("Publish more: ");
    s = natsConnection_PublishString(nc, "foobar", arg.string);
    IFOK(s, natsConnection_PublishString(nc, "foo", arg.string));
    IFOK(s, natsConnection_Subscribe(&sub4, nc, "done", _doneCb, &arg));
    IFOK(s, natsConnection_PublishString(nc, "done", "done"));
    testCond(s == NATS_OK);

    test("Done msg received: ")
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.done)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.done);

    nats_Sleep(50);

    test("All msgs were received: ");
    testCond(arg.sum == 4);

    natsSubscription_Destroy(sub);
    natsSubscription_Destroy(sub2);
    natsSubscription_Destroy(sub3);
    natsSubscription_Destroy(sub4);;
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _waitForConnClosed(&arg);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_QueueSubsOnReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub1     = NULL;
    natsSubscription    *sub2     = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.string = "bar";
        arg.status = NATS_OK;
        arg.control= 6;
    }
    if (s == NATS_OK)
        opts = _createReconnectOptions();

    if ((opts == NULL)
        || (natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg) != NATS_OK))
    {
        FAIL("Unable to create reconnect options!");
    }

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_QueueSubscribe(&sub1, nc, "foo.bar", "workers",
                                          _recvTestString, &arg));
    IFOK(s, natsConnection_QueueSubscribe(&sub2, nc, "foo.bar", "workers",
                                          _recvTestString, &arg));
    IFOK(s, natsConnection_Flush(nc));

    for (int i=0; (s == NATS_OK) && (i < 10); i++)
    {
        char seq[20];

        snprintf(seq, sizeof(seq), "%d", i);
        s = natsConnection_PublishString(nc, "foo.bar", seq);
    }

    IFOK(s, natsConnection_Flush(nc));

    nats_Sleep(50);

    natsMutex_Lock(arg.m);
    for (int i=0; (s == NATS_OK) && (i<10); i++)
    {
        if (arg.results[i] != 1)
            s = NATS_ERR;
    }
    IFOK(s, arg.status);

    memset(&arg.results, 0, sizeof(arg.results));
    natsMutex_Unlock(arg.m);

    test("Base results: ");
    testCond(s == NATS_OK);

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Reconnects: ")
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.reconnected);

    for (int i=0; (s == NATS_OK) && (i < 10); i++)
    {
        char seq[20];

        snprintf(seq, sizeof(seq), "%d", i);
        s = natsConnection_PublishString(nc, "foo.bar", seq);
    }

    IFOK(s, natsConnection_Flush(nc));

    nats_Sleep(50);

    natsMutex_Lock(arg.m);
    for (int i=0; (s == NATS_OK) && (i<10); i++)
    {
        if (arg.results[i] != 1)
            s = NATS_ERR;
    }
    IFOK(s, arg.status);

    memset(&arg.results, 0, sizeof(arg.results));
    natsMutex_Unlock(arg.m);

    test("Reconnect results: ");
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub1);
    natsSubscription_Destroy(sub2);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_IsClosed(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:22222");
    test("Check IsClosed is correct: ");
    testCond((s == NATS_OK) && !natsConnection_IsClosed(nc));

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    test("Check IsClosed after server shutdown: ");
    testCond((s == NATS_OK) && !natsConnection_IsClosed(nc));

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check IsClosed after server restart: ");
    testCond((s == NATS_OK) && !natsConnection_IsClosed(nc));

    natsConnection_Close(nc);
    test("Check IsClosed after connection closed: ");
    testCond((s == NATS_OK) && natsConnection_IsClosed(nc));

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_IsReconnectingAndStatus(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check reconnecting state: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:22222"));
    IFOK(s, natsOptions_SetAllowReconnect(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10000));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));

    // Connect, verify initial reconnecting state check, then stop the server
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_OK) && !natsConnection_IsReconnecting(nc));

    test("Check status: ");
    testCond((s == NATS_OK) && (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    // Wait until we get the disconnected callback
    test("Check we are disconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    test("Check IsReconnecting is correct: ");
    testCond(natsConnection_IsReconnecting(nc));

    test("Check Status is correct: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_RECONNECTING);

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    // Wait until we get the reconnect callback
    test("Check we are reconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.reconnected);

    test("Check IsReconnecting is correct: ");
    testCond(!natsConnection_IsReconnecting(nc));

    test("Check Status is correct: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED);

    // Close the connection, reconnecting should still be false
    natsConnection_Close(nc);

    test("Check IsReconnecting is correct: ");
    testCond(!natsConnection_IsReconnecting(nc));

    test("Check Status is correct: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CLOSED);

    natsMutex_Lock(arg.m);
    while (!arg.closed)
        natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ReconnectBufSize(void)
{
    natsStatus          s         = NATS_OK;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        opts = _createReconnectOptions();
        if (opts == NULL)
            s = NATS_ERR;
    }
    if (s == NATS_OK)
        s = natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg);

    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Option invalid settings. NULL options: ");
    s = natsOptions_SetReconnectBufSize(NULL, 1);
    testCond(s != NATS_OK);

    test("Option invalid settings. Negative value: ");
    s = natsOptions_SetReconnectBufSize(opts, -1);
    testCond(s != NATS_OK);

    test("Option valid settings. Zero: ");
    s = natsOptions_SetReconnectBufSize(opts, 0);
    testCond(s == NATS_OK);

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    // For this test, set to a low value.
    s = natsOptions_SetReconnectBufSize(opts, 32);
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Flush(nc));

    _stopServer(serverPid);

    // Wait until we get the disconnected callback
    test("Check we are disconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    // Publish 2 messages, they should be accepted
    test("Can publish while server is down: ");
    s = natsConnection_PublishString(nc, "foo", "abcd");
    IFOK(s, natsConnection_PublishString(nc, "foo", "abcd"));
    testCond(s == NATS_OK);

    // This publish should fail
    test("Exhausted buffer should return an error: ");
    s = natsConnection_PublishString(nc, "foo", "abcd");
    testCond(s == NATS_INSUFFICIENT_BUFFER);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
_startServerForRetryOnConnect(void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;
    natsPid           pid = NATS_INVALID_PID;

    // Delay start a bit...
    nats_Sleep(300);

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    natsMutex_Lock(arg->m);
    while (!arg->done)
        natsCondition_Wait(arg->c, arg->m);
    natsMutex_Unlock(arg->m);

    _stopServer(pid);
}

static void
_connectedCb(natsConnection *nc, void* closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->connected = true;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);
}

static int64_t
_testCustomReconnectDelayOnInitialConnect(natsConnection *nc, int attempts, void *closure)
{
    if (attempts == 10)
        natsConnection_Close(nc);
    return 50;
}

static void
test_RetryOnFailedConnect(void)
{
    natsStatus          s;
    natsConnection      *nc   = NULL;
    natsOptions         *opts = NULL;
    int64_t             start = 0;
    int64_t             end   = 0;
    natsThread          *t    = NULL;
    natsSubscription    *sub  = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetRetryOnFailedConnect(opts, true, NULL, NULL));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
#ifdef _WIN32
    // Windows takes the full timeout to report connect failure, so reduce
    // timeout here.
    IFOK(s, natsOptions_SetTimeout(opts, 100));
#endif
    if (s != NATS_OK)
    {
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    start = nats_Now();
    test("Connect failed: ");
    s = natsConnection_Connect(&nc, opts);
    end = nats_Now();
    testCond(s == NATS_NO_SERVER);

    test("Retried: ")
#ifdef _WIN32
    testCond((((end-start) >= 1000) && ((end-start) <= 2600)));
#else
    testCond((((end-start) >= 300) && ((end-start) <= 1500)));
#endif

    test("Connects ok: ");
    s = natsOptions_SetMaxReconnect(opts, 20);
    IFOK(s, natsThread_Create(&t, _startServerForRetryOnConnect, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    // close to avoid reconnect when shutting down server.
    natsConnection_Close(nc);

    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);
    t = NULL;

    natsConnection_Destroy(nc);
    nc = NULL;

    // Try with async connect
    test("Connect does not block: ");
    s = natsOptions_SetRetryOnFailedConnect(opts, true, _connectedCb, (void*)&arg);
    // Set disconnected/reconnected to make sure that these are not
    // invoked as part of async connect.
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_NOT_YET_CONNECTED) && (nc != NULL));
    nats_clearLastError();

    test("Subscription ok: ");
    arg.control = 99;
    s = natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*)&arg);
    testCond(s == NATS_OK);

    test("Publish ok: ");
    s = natsConnection_Publish(nc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    // Start server
    arg.done = false;
    s = natsThread_Create(&t, _startServerForRetryOnConnect, (void*) &arg);

    test("Connected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.connected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("No disconnected and reconnected callbacks: ");
    natsMutex_Lock(arg.m);
    s = ((arg.disconnected || arg.reconnected) ? NATS_ERR : NATS_OK);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Message received: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    // Close nc to avoid reconnect when shutting down server
    natsConnection_Close(nc);

    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Broadcast(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    nc = NULL;

    // Try the custom reconnect handler and close the connection after
    // certain number of attempts.
    test("Close in custom reconnect delay: ")
    s = natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg);
    IFOK(s, natsOptions_SetCustomReconnectDelay(opts, _testCustomReconnectDelayOnInitialConnect, NULL));
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_NOT_YET_CONNECTED)
        s = NATS_OK;

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);
}

static void
_closeConnWithDelay(void *arg)
{
    natsConnection *nc = (natsConnection*) arg;

    nats_Sleep(200);
    natsConnection_Close(nc);
}

static void
_connectToMockupServer(void *closure)
{
    struct threadArg    *arg = (struct threadArg *) closure;
    natsConnection      *nc = NULL;
    natsOptions         *opts = arg->opts;
    natsStatus          s = NATS_OK;
    int                 control;

    // Make sure that the server is ready to accept our connection.
    nats_Sleep(100);

    if (opts == NULL)
    {
        s = natsOptions_Create(&opts);
        IFOK(s, natsOptions_SetAllowReconnect(opts, false));
    }
    IFOK(s, natsConnection_Connect(&nc, opts));

    natsOptions_Destroy(opts);

    natsMutex_Lock(arg->m);
    control = arg->control;
    natsMutex_Unlock(arg->m);

    if (control == 2)
    {
        int64_t payload = 0;

        if (s == NATS_OK)
        {
            test("Check expected max payload: ")
            payload = natsConnection_GetMaxPayload(nc);
            if (payload != 10)
                s = NATS_ERR;
            testCondNoReturn(s == NATS_OK);
        }
        if (s == NATS_OK)
        {
            test("Expect getting an error when publish more than max payload: ");
            s = natsConnection_PublishString(nc, "hello", "Hello World!");
            testCondNoReturn(s != NATS_OK);

            // reset status
            s = NATS_OK;
        }
        if (s == NATS_OK)
        {
            test("Expect success if publishing less than max payload: ");
            s = natsConnection_PublishString(nc, "hello", "a");
            testCondNoReturn(s == NATS_OK);
        }

        natsMutex_Lock(arg->m);
        arg->closed = true;
        natsCondition_Signal(arg->c);
        natsMutex_Unlock(arg->m);
    }
    else if (control == 3)
    {
        natsThread *t = NULL;

        s = natsThread_Create(&t, _closeConnWithDelay, (void*) nc);
        if (s == NATS_OK)
        {
            s = natsConnection_Flush(nc);

            natsThread_Join(t);
            natsThread_Destroy(t);
        }
    }
    else if (control == 4)
    {
        s = natsConnection_Flush(nc);
    }
    else if ((control == 5) || (control == 6))
    {
        // Wait for disconnect Cb
        natsMutex_Lock(arg->m);
        while ((s != NATS_TIMEOUT) && !arg->disconnected)
            s = natsCondition_TimedWait(arg->c, arg->m, 5000);
        natsMutex_Unlock(arg->m);

        if ((s == NATS_OK) && (control == 5))
        {
            // Should reconnect
            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !arg->reconnected)
                s = natsCondition_TimedWait(arg->c, arg->m, 5000);
            natsMutex_Unlock(arg->m);

            natsConnection_Close(nc);
        }
        else if (s == NATS_OK)
        {
            // Wait that we are closed, then check nc's last error.
            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !arg->closed)
                s = natsCondition_TimedWait(arg->c, arg->m, 5000);
            natsMutex_Unlock(arg->m);
            if (s == NATS_OK)
            {
                const char* lastErr = NULL;

                s = natsConnection_GetLastError(nc, &lastErr);
                if (strcmp(lastErr, arg->string) != 0)
                    s = NATS_ILLEGAL_STATE;
            }
        }
    }
    else if (control == 7)
    {
        natsMutex_Lock(arg->m);
        while ((s != NATS_TIMEOUT) && !arg->done)
            s = natsCondition_TimedWait(arg->c, arg->m, 5000);
        natsMutex_Unlock(arg->m);
    }

    natsConnection_Destroy(nc);

    natsMutex_Lock(arg->m);
    arg->status = s;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static natsStatus
_startMockupServer(natsSock *serverSock, const char *host, const char *port)
{
    struct addrinfo     hints;
    struct addrinfo     *servinfo = NULL;
    int                 res;
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;

    memset(&hints,0,sizeof(hints));

    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags    = AI_PASSIVE;

    if ((res = getaddrinfo(host, port, &hints, &servinfo)) != 0)
    {
         hints.ai_family = AF_INET6;

         if ((res = getaddrinfo(host, port, &hints, &servinfo)) != 0)
             s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        sock = socket(servinfo->ai_family, servinfo->ai_socktype,
                      servinfo->ai_protocol);
        if (sock == NATS_SOCK_INVALID)
            s = NATS_SYS_ERROR;

        IFOK(s, natsSock_SetCommonTcpOptions(sock));
        IFOK(s, natsSock_SetBlocking(sock, true));
    }
    if ((s == NATS_OK)
        && (bind(sock, servinfo->ai_addr, (natsSockLen) servinfo->ai_addrlen) == NATS_SOCK_ERROR))
    {
        s = NATS_SYS_ERROR;
    }

    if ((s == NATS_OK) && (listen(sock, 100) == NATS_SOCK_ERROR))
        s = NATS_SYS_ERROR;

    if (s == NATS_OK)
        *serverSock = sock;
    else
        natsSock_Close(sock);

    nats_FreeAddrInfo(servinfo);

    return s;
}

static void
test_ErrOnConnectAndDeadlock(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    arg.control = 1;

    test("Verify that bad INFO does not cause deadlock in client: ");

    // We will hand run a fake server that will timeout and not return a proper
    // INFO proto. This is to test that we do not deadlock.

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }

    if (s == NATS_OK)
    {
        const char* badInfo = "INFOZ \r\n";

        // Send back a mal-formed INFO.
        s = natsSock_WriteFully(&ctx, badInfo, (int) strlen(badInfo));
    }

    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);

        while ((s != NATS_TIMEOUT) && (arg.status == NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 3000);

        natsMutex_Unlock(arg.m);
    }

    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    testCond((s == NATS_OK) && (arg.status != NATS_OK));

    _destroyDefaultThreadArgs(&arg);

    natsSock_Close(ctx.fd);
    natsSock_Close(sock);
}

static void
test_ErrOnMaxPayloadLimit(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    int                 expectedMaxPayLoad = 10;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    arg.control = 2;

    s = _startMockupServer(&sock, "localhost", "4222");

    if ((s == NATS_OK) && (listen(sock, 100) == NATS_SOCK_ERROR))
        s = NATS_SYS_ERROR;

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        snprintf(info, sizeof(info),
                 "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":%d}\r\n",
                 expectedMaxPayLoad);

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            char buffer[1024];

            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));
    }

    // Wait for the client to be about to close the connection.
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 3000);
    natsMutex_Unlock(arg.m);

    natsSock_Close(ctx.fd);
    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    test("Test completed ok: ");
    testCond(s == NATS_OK);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_Auth(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;

    test("Server with auth on, client without should fail: ");

    serverPid = _startServer("nats://127.0.0.1:8232", "--user ivan --pass foo -p 8232", false);
    CHECK_SERVER_STARTED(serverPid);

    nats_Sleep(1000);

    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:8232");
    testCond((s == NATS_CONNECTION_AUTH_FAILED)
             && (nats_strcasestr(nats_GetLastError(NULL), "Authorization Violation") != NULL));

    test("Server with auth on, client with proper auth should succeed: ");

    s = natsConnection_ConnectTo(&nc, "nats://ivan:foo@127.0.0.1:8232");
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    // Use Options
    test("Connect using SetUserInfo: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:8232"));
    IFOK(s, natsOptions_SetUserInfo(opts, "ivan", "foo"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);
    natsConnection_Destroy(nc);
    nc = NULL;

    // Verify that credentials in URL take precedence.
    test("URL takes precedence: ");
    s = natsOptions_SetURL(opts, "nats://ivan:foo@127.0.0.1:8232");
    IFOK(s, natsOptions_SetUserInfo(opts, "foo", "bar"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
}

static void
test_AuthFailNoDisconnectCB(void)
{
    natsStatus          s;
    natsOptions         *opts     = NULL;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:8232", "--user ivan --pass foo -p 8232", true);
    CHECK_SERVER_STARTED(serverPid);

    opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create options!");

    test("Connect should fail: ");
    s = natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s != NATS_OK);

    test("DisconnectCb should not be invoked on auth failure: ");
    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 1000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_TIMEOUT) && !arg.disconnected);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_AuthToken(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;

    serverPid = _startServer("nats://testSecret@127.0.0.1:8232", "-auth testSecret -p 8232", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Server with token authorization, client without should fail: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:8232");
    testCond(s != NATS_OK);

    test("Server with token authorization, client with proper auth should succeed: ");
    s = natsConnection_ConnectTo(&nc, "nats://testSecret@127.0.0.1:8232");
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    // Use Options
    test("Connect using SetUserInfo: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:8232"));
    IFOK(s, natsOptions_SetToken(opts, "testSecret"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);
    natsConnection_Destroy(nc);
    nc = NULL;

    // Verify that token in URL take precedence.
    test("URL takes precedence: ");
    s = natsOptions_SetURL(opts, "nats://testSecret@127.0.0.1:8232");
    IFOK(s, natsOptions_SetToken(opts, "badtoken"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
}

static const char*
_tokenHandler(void* closure)
{
    return (char*) closure;
}

static void
test_AuthTokenHandler(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;

    serverPid = _startServer("nats://testSecret@127.0.0.1:8232", "-auth testSecret -p 8232", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect using SetTokenHandler: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:8232"));
    IFOK(s, natsOptions_SetTokenHandler(opts, _tokenHandler, (char*) "testSecret"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);
    natsConnection_Destroy(nc);
    nc = NULL;

    test("cannot set a tokenHandler when token set: ");
    s = natsOptions_SetTokenHandler(opts, NULL, NULL);
    IFOK(s, natsOptions_SetToken(opts, "token"));
    IFOK(s, natsOptions_SetTokenHandler(opts, _tokenHandler, (char*) "testSecret"));
    testCond(s == NATS_ILLEGAL_STATE);

    test("cannot set a token when tokenHandler set: ");
    s = natsOptions_SetToken(opts, NULL);
    IFOK(s, natsOptions_SetTokenHandler(opts, _tokenHandler, (char*) "testSecret"));
    IFOK(s, natsOptions_SetToken(opts, "token"));
    testCond(s == NATS_ILLEGAL_STATE);

    test("token in URL not valid with tokenHandler: ");
    s = natsOptions_SetURL(opts, "nats://testSecret@127.0.0.1:8232");
    IFOK(s, natsOptions_SetTokenHandler(opts, _dummyTokenHandler, NULL));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_ILLEGAL_STATE);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
}

static void
_permsViolationHandler(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    struct threadArg *args      = (struct threadArg*) closure;
    const char       *lastError = NULL;

    // Technically, there is no guarantee that the connection's last error
    // be still the one that is given to this callback.
    if (err == NATS_NOT_PERMITTED)
    {
        bool ok = true;

        // So consider ok if currently not the same or if the same, the
        // error string is as expected.
        if (natsConnection_GetLastError(nc, &lastError) == NATS_NOT_PERMITTED)
            ok = (nats_strcasestr(lastError, args->string) != NULL ? true : false);

        if (ok)
        {
            natsMutex_Lock(args->m);
            args->done = true;
            natsCondition_Broadcast(args->c);
            natsMutex_Unlock(args->m);
        }
    }
}

static void
test_PermViolation(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsSubscription    *sub  = NULL;
    natsOptions         *opts = NULL;
    natsPid             pid   = NATS_INVALID_PID;
    int                 i;
    bool                cbCalled;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
    {
        args.string = PERMISSIONS_ERR;
        s = natsOptions_Create(&opts);
    }
    IFOK(s, natsOptions_SetURL(opts, "nats://ivan:pwd@127.0.0.1:8232"));
    IFOK(s, natsOptions_SetErrorHandler(opts, _permsViolationHandler, &args));
    if (s != NATS_OK)
        FAIL("Error setting up test");

    pid = _startServer("nats://127.0.0.1:8232", "-c permissions.conf -a 127.0.0.1 -p 8232", false);
    CHECK_SERVER_STARTED(pid);
    s = _checkStart("nats://ivan:pwd@127.0.0.1:8232", 4, 10);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Error starting server!");
    }
    test("Check connection created: ");
    s = natsConnection_Connect(&conn, opts);
    testCond(s == NATS_OK);

    for (i=0; i<2; i++)
    {
        cbCalled = false;

        test("Should get perm violation: ");
        if (i == 0)
            s = natsConnection_PublishString(conn, "bar", "fail");
        else
            s = natsConnection_Subscribe(&sub, conn, "foo", _dummyMsgHandler, NULL);

        if (s == NATS_OK)
        {
            natsMutex_Lock(args.m);
            while (!args.done && s == NATS_OK)
                s = natsCondition_TimedWait(args.c, args.m, 2000);
            cbCalled = args.done;
            args.done = false;
            natsMutex_Unlock(args.m);
        }
        testCond((s == NATS_OK) && cbCalled);
    }

    test("Connection not closed: ");
    testCond((s == NATS_OK) && !natsConnection_IsClosed(conn));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
_authViolationHandler(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    struct threadArg *args      = (struct threadArg*) closure;
    const char       *lastError = NULL;

    // Technically, there is no guarantee that the connection's last error
    // be still the one that is given to this callback.
    if (err == NATS_CONNECTION_AUTH_FAILED)
    {
        bool ok = true;

        // So consider ok if currently not the same or if the same, the
        // error string is as expected.
        if (natsConnection_GetLastError(nc, &lastError) == NATS_CONNECTION_AUTH_FAILED)
            ok = (nats_strcasestr(nc->errStr, AUTHORIZATION_ERR) != NULL ? true : false);
        if (ok)
        {
            natsMutex_Lock(args->m);
            args->results[0]++;
            args->done = true;
            natsCondition_Broadcast(args->c);
            natsMutex_Unlock(args->m);
        }
    }
}

static void
test_AuthViolation(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&(arg.opts)));
    IFOK(s, natsOptions_SetAllowReconnect(arg.opts, false));
    IFOK(s, natsOptions_SetErrorHandler(arg.opts, _authViolationHandler, &arg));
    IFOK(s, natsOptions_SetClosedCB(arg.opts, _closedCb, &arg));
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    arg.control = 7;
    arg.string  = AUTHORIZATION_ERR;

    test("Behavior of connection on Server Error: ")

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        strncpy(info,
                "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n",
                sizeof(info));

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            char buffer[1024];

            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx,
                                    _PONG_PROTO_, _PONG_PROTO_LEN_));

        if (s == NATS_OK)
        {
            // Wait a tiny, and simulate an error sent by the server
            nats_Sleep(50);

            snprintf(info, sizeof(info), "-ERR '%s'\r\n", arg.string);
            s = natsSock_WriteFully(&ctx, info, (int)strlen(info));
        }
    }
    if (s == NATS_OK)
    {
        // Wait for the client to process the async err
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done)
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);

        natsSock_Close(ctx.fd);
    }

    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    // Wait for closed CB
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.closed)
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        if ((s == NATS_OK) && arg.reconnects != 0)
            s = NATS_ERR;
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    _destroyDefaultThreadArgs(&arg);
}

static void
_startServerSendErrThread(void *closure)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    struct threadArg    *arg = (struct threadArg*) closure;
    natsSockCtx         ctx;
    char                buffer[1024];
    int                 connect = 0;

    memset(&ctx, 0, sizeof(natsSockCtx));

    _startMockupServer(&sock, "localhost", "4222");

    for (connect=1; connect<4; connect++)
    {
        if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
                || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
        {
            s = NATS_SYS_ERROR;
        }
        if (s == NATS_OK)
        {
            const char info[] = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";

            natsMutex_Lock(arg->m);
            arg->control++;
            natsMutex_Unlock(arg->m);

            // Send INFO.
            s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
            if (s == NATS_OK)
            {
                memset(buffer, 0, sizeof(buffer));

                // Read connect and ping commands sent from the client
                s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
                IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
            }
        }
        if ((s == NATS_OK) && connect == 1)
        {
            // Send PONG
            s = natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_);
            nats_Sleep(500);
            snprintf(buffer, sizeof(buffer), "-ERR '%s'\r\n", AUTHENTICATION_EXPIRED_ERR);
        }
        else if (s == NATS_OK)
        {
            snprintf(buffer, sizeof(buffer), "-ERR '%s'\r\n", AUTHORIZATION_ERR);
        }
        if (s == NATS_OK)
        {
            s = natsSock_WriteFully(&ctx, buffer, (int)strlen(buffer));
            nats_Sleep(200);
        }
        natsSock_Close(ctx.fd);
    }

    natsMutex_Lock(arg->m);
    while ((s != NATS_TIMEOUT) && !arg->disconnected)
        s = natsCondition_TimedWait(arg->c, arg->m, 5000);
    natsMutex_Unlock(arg->m);

    natsSock_Close(sock);
}

static void
_authExpiredHandler(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    struct threadArg *args      = (struct threadArg*) closure;
    const char       *lastError = NULL;

    // Technically, there is no guarantee that the connection's last error
    // be still the one that is given to this callback.
    if (err == NATS_CONNECTION_AUTH_FAILED)
    {
        bool ok = true;

        natsMutex_Lock(args->m);

        // So consider ok if currently not the same or if the same, the
        // error string is as expected.
        if (natsConnection_GetLastError(nc, &lastError) == NATS_CONNECTION_AUTH_FAILED)
        {
            if (args->control == 1)
                ok = (nats_strcasestr(nc->errStr, AUTHENTICATION_EXPIRED_ERR) != NULL ? true : false);
            else
                ok = (nats_strcasestr(nc->errStr, AUTHORIZATION_ERR) != NULL ? true : false);
        }
        if (ok)
        {
            args->results[0]++;
            args->done = true;
            natsCondition_Broadcast(args->c);
        }

        natsMutex_Unlock(args->m);
    }
}

static void
test_AuthenticationExpired(void)
{
    natsStatus          s    = NATS_OK;
    natsConnection      *nc  = NULL;
    natsOptions         *opts= NULL;
    natsThread          *t   = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;
    int                 i;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetReconnectWait(opts, 25));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetErrorHandler(opts, _authExpiredHandler, &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, &arg));
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    s = natsThread_Create(&t, _startServerSendErrThread, (void*) &arg);
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Should connect ok: ");
    for (i=0; i<10; i++)
    {
        s = natsConnection_Connect(&nc, opts);
        if (s == NATS_OK)
            break;
        nats_Sleep(100);
    }
    testCond(s == NATS_OK);

    test("Should have been closed: ");
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    // First we would have gotten the async authentication expired error,
    // then 2 consecutive auth violations that causes the lib to stop
    // trying to reconnect.
    test("Should have posted 3 errors: ");
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        s = (((arg.results[0] == 3) && arg.done) ? NATS_OK : NATS_ERR);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    natsMutex_Lock(arg.m);
    arg.disconnected = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);
}

static void
_startServerSendErr2Thread(void *closure)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    struct threadArg    *arg = (struct threadArg*) closure;
    natsSockCtx         ctx;
    char                buffer[1024];
    int                 connect = 0;
    bool                done = false;

    memset(&ctx, 0, sizeof(natsSockCtx));

    _startMockupServer(&sock, "localhost", "4222");

    for (connect=1; !done; connect++)
    {
        if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
                || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
        {
            s = NATS_SYS_ERROR;
        }
        if (s == NATS_OK)
        {
            const char info[] = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";

            natsMutex_Lock(arg->m);
            arg->control++;
            natsMutex_Unlock(arg->m);

            // Send INFO.
            s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
            if (s == NATS_OK)
            {
                memset(buffer, 0, sizeof(buffer));

                // Read connect and ping commands sent from the client
                s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
                IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
            }
            if (s == NATS_OK)
            {
                // Send PONG
                s = natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_);
            }
        }
        if ((s == NATS_OK) && connect == 1)
        {
            nats_Sleep(500);
            snprintf(buffer, sizeof(buffer), "-ERR '%s'\r\n", AUTHENTICATION_EXPIRED_ERR);
            s = natsSock_WriteFully(&ctx, buffer, (int)strlen(buffer));
            nats_Sleep(200);
        }
        else if (s == NATS_OK)
        {
            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !arg->disconnected)
                s = natsCondition_TimedWait(arg->c, arg->m, 5000);
            natsMutex_Unlock(arg->m);
            done = true;
        }
        natsSock_Close(ctx.fd);
    }
    natsSock_Close(sock);
}

static void
test_AuthenticationExpiredReconnect(void)
{
    natsStatus          s    = NATS_OK;
    natsConnection      *nc  = NULL;
    natsOptions         *opts= NULL;
    natsThread          *t   = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;
    int                 i;
    const char          *lastErr = NULL;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 2));
    IFOK(s, natsOptions_SetReconnectWait(opts, 25));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetErrorHandler(opts, _authExpiredHandler, &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, &arg));
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    s = natsThread_Create(&t, _startServerSendErr2Thread, (void*) &arg);
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Should connect ok: ");
    for (i=0; i<10; i++)
    {
        s = natsConnection_Connect(&nc, opts);
        if (s == NATS_OK)
            break;
        nats_Sleep(100);
    }
    testCond(s == NATS_OK);

    test("Should have posted 1 error: ");
    if (s == NATS_OK)
    {
        // Waiting for the err handler to fire
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done && arg.results[0] != 1)
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    test("Should have reconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    // Wait a tiny bit and make sure connection is still connected
    nats_Sleep(100);
    test("Still connected: ");
    testCond(!natsConnection_IsClosed(nc));

    // Check last error is cleared
    test("Check last error cleared: ");
    s = natsConnection_GetLastError(nc, &lastErr);
    testCond((s == NATS_OK) && (lastErr[0] == '\0'));

    test("Close: ");
    natsConnection_Destroy(nc);
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);
    nc = NULL;

    natsMutex_Lock(arg.m);
    arg.disconnected = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ConnectedServer(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    char                buffer[128];

    buffer[0] = '\0';

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Verify ConnectedUrl is correct: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer)));
    testCond((s == NATS_OK)
             && (buffer[0] != '\0')
             && (strcmp(buffer, NATS_DEFAULT_URL) == 0));

    buffer[0] = '\0';

    test("Verify ConnectedServerId is not null: ")
    s = natsConnection_GetConnectedServerId(nc, buffer, sizeof(buffer));
    testCond((s == NATS_OK)  && (buffer[0] != '\0'));

    buffer[0] = '\0';

    test("Verify ConnectedUrl is empty after disconnect: ")
    natsConnection_Close(nc);
    s = natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer));
    testCond((s == NATS_OK) && (buffer[0] == '\0'));

    buffer[0] = '\0';

    test("Verify ConnectedServerId is empty after disconnect: ")
    s = natsConnection_GetConnectedServerId(nc, buffer, sizeof(buffer));
    testCond((s == NATS_OK)  && (buffer[0] == '\0'));

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_MultipleClose(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsThread          *threads[10];
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test that multiple Close are fine: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    for (int i=0; (s == NATS_OK) && (i<10); i++)
        s = natsThread_Create(&(threads[i]), _closeConn, (void*) nc);

    for (int i=0; (s == NATS_OK) && (i<10); i++)
    {
        natsThread_Join(threads[i]);
        natsThread_Destroy(threads[i]);
    }
    testCond((s == NATS_OK)
             && (nc->status == NATS_CONN_STATUS_CLOSED)
             && (nc->refs == 1));

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SimplePublish(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test simple publish: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_PublishString(nc, "foo", "Hello world!"));
    IFOK(s, natsConnection_Publish(nc, "foo", (const void*) "Hello world!",
                                   (int) strlen("Hello world!")));
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SimplePublishNoData(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test simple publish with no data: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_PublishString(nc, "foo", NULL));
    IFOK(s, natsConnection_PublishString(nc, "foo", ""));
    IFOK(s, natsConnection_Publish(nc, "foo", NULL, 0));
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_PublishMsg(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.string = "hello!";
        arg.status = NATS_OK;
    }
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test simple publishMsg: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, &arg));
    IFOK(s, natsConnection_Flush(nc));
    if (s == NATS_OK)
    {
        const char  data[] = {104, 101, 108, 108, 111, 33};
        natsMsg     *msg   = NULL;

        s = natsMsg_Create(&msg, "foo", NULL, data, sizeof(data));
        IFOK(s, natsConnection_PublishMsg(nc, msg));

        natsMsg_Destroy(msg);
    }
    IFOK(s, natsConnection_Flush(nc));

    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.msgReceived)
            s = natsCondition_TimedWait(arg.c, arg.m, 1500);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_InvalidSubsArgs(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsSubscription    *sub = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    // ASYNC Subscription

    test("Test async subscriber, invalid connection: ")
    s = natsConnection_Subscribe(&sub, NULL, "foo", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber, invalid subject: ")
    s = natsConnection_Subscribe(&sub, nc, NULL, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber, invalid subject (empty string): ")
    s = natsConnection_Subscribe(&sub, nc, "", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber, invalid cb: ")
    s = natsConnection_Subscribe(&sub, nc, "foo", NULL, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid connection: ")
    s = natsConnection_QueueSubscribe(&sub, NULL, "foo", "group", _recvTestString, NULL);
    testCond(s != NATS_OK);

    // Async Subscription Timeout

    test("Test async subscriber timeout, invalid connection: ")
    s = natsConnection_SubscribeTimeout(&sub, NULL, "foo", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber timeout, invalid subject: ")
    s = natsConnection_SubscribeTimeout(&sub, nc, NULL, 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber timeout, invalid subject (empty string): ")
    s = natsConnection_SubscribeTimeout(&sub, nc, "", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber timeout, invalid cb: ")
    s = natsConnection_SubscribeTimeout(&sub, nc, "foo", 100, NULL, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber timeout, invalid timeout (<0): ")
    s = natsConnection_SubscribeTimeout(&sub, nc, "foo", -100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async subscriber timeout, invalid timeout (0): ")
    s = natsConnection_SubscribeTimeout(&sub, nc, "foo", 0, _recvTestString, NULL);
    testCond(s != NATS_OK);

    // ASYNC Queue Subscription

    test("Test async queue subscriber timeout, invalid connection: ")
    s = natsConnection_QueueSubscribe(&sub, NULL, "foo", "group", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid subject: ")
    s = natsConnection_QueueSubscribe(&sub, nc, NULL, "group", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid subject (empty string): ")
    s = natsConnection_QueueSubscribe(&sub, nc, "", "group", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid group name: ")
    s = natsConnection_QueueSubscribe(&sub, nc, "foo", NULL, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid group name (empty): ")
    s = natsConnection_QueueSubscribe(&sub, nc, "foo", "", _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber, invalid cb: ")
    s = natsConnection_QueueSubscribe(&sub, nc, "foo", "group", NULL, NULL);
    testCond(s != NATS_OK);

    // ASYNC Queue Subscription Timeout

    test("Test async queue subscriber timeout, invalid connection: ")
    s = natsConnection_QueueSubscribeTimeout(&sub, NULL, "foo", "group", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid subject: ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, NULL, "group", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid subject (empty string): ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "", "group", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid group name: ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", NULL, 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid group name (empty): ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", "", 100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid cb: ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", "group", 100, NULL, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid timeout (<0): ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", "group", -100, _recvTestString, NULL);
    testCond(s != NATS_OK);

    test("Test async queue subscriber timeout, invalid timeout (0): ")
    s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", "group", 0, _recvTestString, NULL);
    testCond(s != NATS_OK);

    // SYNC Subscription

    test("Test sync subscriber, invalid connection: ")
    s = natsConnection_SubscribeSync(&sub, NULL, "foo");
    testCond(s != NATS_OK);

    test("Test sync subscriber, invalid subject: ")
    s = natsConnection_SubscribeSync(&sub, nc, NULL);
    testCond(s != NATS_OK);

    test("Test sync subscriber, invalid subject (empty string): ")
    s = natsConnection_SubscribeSync(&sub, nc, "");
    testCond(s != NATS_OK);

    // SYNC Queue Subscription

    test("Test sync queue subscriber, invalid connection: ")
    s = natsConnection_QueueSubscribeSync(&sub, NULL, "foo", "group");
    testCond(s != NATS_OK);

    test("Test sync queue subscriber, invalid subject: ")
    s = natsConnection_QueueSubscribeSync(&sub, nc, NULL, "group");
    testCond(s != NATS_OK);

    test("Test sync queue subscriber, invalid subject (empty string): ")
    s = natsConnection_QueueSubscribeSync(&sub, nc, "", "group");
    testCond(s != NATS_OK);

    test("Test sync queue subscriber, invalid group name: ")
    s = natsConnection_QueueSubscribeSync(&sub, nc, "foo", NULL);
    testCond(s != NATS_OK);

    test("Test sync queue subscriber, invalid group name (empty): ")
    s = natsConnection_QueueSubscribeSync(&sub, nc, "foo", "");
    testCond(s != NATS_OK);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_AsyncSubscribe(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "Hello World";
    arg.status = NATS_OK;
    arg.control= 1;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test async subscriber: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString,
                                     (void*) &arg));
    IFOK(s, natsConnection_PublishString(nc, "foo", arg.string));

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 1500);
    natsMutex_Unlock(arg.m);

    IFOK(s, arg.status);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

typedef struct __asyncTimeoutInfo
{
    struct threadArg    *arg;
    int64_t             timeout;
    int64_t             timeAfterFirstMsg;
    int64_t             timeSecondMsg;
    int64_t             timeFirstTimeout;
    int64_t             timeSecondTimeout;

} _asyncTimeoutInfo;

static void
_asyncTimeoutCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
   _asyncTimeoutInfo *ai = (_asyncTimeoutInfo*) closure;

   natsMutex_Lock(ai->arg->m);
   if (msg != NULL)
   {
       ai->arg->sum++;
       switch (ai->arg->sum)
       {
           case 1:
           {
               // Release lock for sleep...
               natsMutex_Unlock(ai->arg->m);

               // Sleep for 1.5x the timeout value
               nats_Sleep(ai->timeout+ai->timeout/2);

               natsMutex_Lock(ai->arg->m);
               ai->timeAfterFirstMsg = nats_Now();
               break;
           }
           case 2: ai->timeSecondMsg = nats_Now(); break;
           case 3:
           {
               ai->arg->done = true;
               natsSubscription_Destroy(sub);
               natsCondition_Signal(ai->arg->c);
               break;
           }
           default:
           {
               ai->arg->status = NATS_ERR;
               break;
           }
       }
       natsMsg_Destroy(msg);
   }
   else
   {
       ai->arg->timerFired++;
       switch (ai->arg->timerFired)
       {
           case 1:
           {
               ai->timeFirstTimeout = nats_Now();
               // Notify the main thread to send the second message
               // after waiting 1/2 of the timeout period.
               natsCondition_Signal(ai->arg->c);
               break;
           }
           case 2:
           {
               ai->timeSecondTimeout = nats_Now();
               // Signal that we timed-out for the 2nd time.
               ai->arg->timerStopped = 1;
               natsCondition_Signal(ai->arg->c);
               break;
           }
           default:
           {
               ai->arg->status = NATS_ERR;
               break;
           }
       }
    }
    natsMutex_Unlock(ai->arg->m);
}

static void
test_AsyncSubscribeTimeout(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsOptions         *opts     = NULL;
    struct threadArg    arg;
    bool                useLibDlv = false;
    int                 i;
    char                testText[128];
    int64_t             timeout   = 100;
    _asyncTimeoutInfo   ai;

    for (i=0; i<4; i++)
    {
        memset(&ai, 0, sizeof(_asyncTimeoutInfo));
        memset(&arg, 0, sizeof(struct threadArg));

        s = natsOptions_Create(&opts);
        IFOK(s, natsOptions_UseGlobalMessageDelivery(opts, useLibDlv));
        IFOK(s, _createDefaultThreadArgsForCbTests(&arg));
        if ( s != NATS_OK)
            FAIL("Unable to setup test!");

        ai.arg = &arg;
        ai.timeout = timeout;
        arg.status = NATS_OK;

        serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
        CHECK_SERVER_STARTED(serverPid);

        snprintf(testText, sizeof(testText), "Test async %ssubscriber timeout%s: ",
                 ((i == 1 || i == 3) ? "queue " : ""),
                 (i > 1 ? " (lib msg delivery)" : ""));
        test(testText);
        s = natsConnection_Connect(&nc, opts);
        if (s == NATS_OK)
        {
            if (i == 0 || i == 2)
                s = natsConnection_SubscribeTimeout(&sub, nc, "foo", timeout,
                                                    _asyncTimeoutCb, (void*) &ai);
            else
                s = natsConnection_QueueSubscribeTimeout(&sub, nc, "foo", "group",
                                                         timeout, _asyncTimeoutCb, (void*) &ai);
        }
        IFOK(s, natsConnection_PublishString(nc, "foo", "msg1"));

        // Wait to be notified that sub timed-out 2 times
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.timerFired != 1))
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);

        // Wait half the timeout
        nats_Sleep(timeout/2);

        // Send the second message. This should reset the timeout timer.
        IFOK(s, natsConnection_PublishString(nc, "foo", "msg2"));
        IFOK(s, natsConnection_Flush(nc));

        // Wait for 2nd timeout
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.timerStopped == 0))
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);

        // Send two more messages, only one should be received since the
        // subscription will be unsubscribed/closed when receiving the
        // first.
        IFOK(s, natsConnection_PublishString(nc, "foo", "msg3"));
        IFOK(s, natsConnection_PublishString(nc, "foo", "msg4"));
        IFOK(s, natsConnection_Flush(nc));

        // Wait for end of test
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done)
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);

        // Wait more than the timeout time to see if extra timeout callbacks
        // incorrectly fire
        nats_Sleep(timeout+timeout/2);

        // Check for success
        natsMutex_Lock(arg.m);
        testCond((s == NATS_OK) && (arg.status == NATS_OK)
                    && (arg.sum == 3) && (arg.timerFired == 2)
                    && (ai.timeFirstTimeout >= ai.timeAfterFirstMsg + timeout - 50)
                    && (ai.timeFirstTimeout <= ai.timeAfterFirstMsg + timeout + 50)
                    && (ai.timeSecondTimeout >= ai.timeSecondMsg + timeout - 50)
                    && (ai.timeSecondTimeout <= ai.timeSecondMsg + timeout + 50))
        natsMutex_Unlock(arg.m);

        natsConnection_Destroy(nc);
        natsOptions_Destroy(opts);

        _destroyDefaultThreadArgs(&arg);

        _stopServer(serverPid);

        if (i >= 1)
            useLibDlv = true;
    }
}

static void
test_SyncSubscribe(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *string   = "Hello World";

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test sync subscriber: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsConnection_PublishString(nc, "foo", string));
    IFOK(s, natsSubscription_NextMsg(&msg, sub, 1000));
    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(string, natsMsg_GetData(msg), natsMsg_GetDataLength(msg)) == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_PubSubWithReply(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *string   = "Hello World";

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test PubSub with reply: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsConnection_PublishRequestString(nc, "foo", "bar", string));
    IFOK(s, natsSubscription_NextMsg(&msg, sub, 1000));
    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(string, natsMsg_GetData(msg), natsMsg_GetDataLength(msg)) == 0)
             && (strncmp(natsMsg_GetReply(msg), "bar", 3) == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_NoResponders(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *string   = "Hello World";
    struct threadArg    arg;

    if (!serverVersionAtLeast(2,2,0))
    {
        char txt[200];

        snprintf(txt, sizeof(txt), "Skipping since requires server version of at least 2.2.0, got %s: ", serverVersion);
        test(txt);
        testCond(true);
        return;
    }

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("No responders on NextMsg: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsConnection_PublishRequestString(nc, "bar", "foo", string));
    IFOK(s, natsSubscription_NextMsg(&msg, sub, 1000));
    testCond(NATS_NO_RESPONDERS);

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    sub = NULL;

    arg.status  = NATS_ERR;
    arg.control = 10;

    test("No responders in callback: ");
    s = natsConnection_Subscribe(&sub, nc, "bar", _recvTestString, (void*)&arg);
    IFOK(s, natsConnection_PublishRequestString(nc, "foo", "bar", string));
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.msgReceived)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        if (s == NATS_OK)
            s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;

    test("Disable no responders: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_DisableNoResponders(opts, true));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_RequestString(&msg, nc, "foo", string, 500));
    testCond((s == NATS_TIMEOUT) && (msg == NULL));

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

struct flushArg
{
    natsConnection      *nc;
    natsStatus          s;
    int                 count;
    int64_t             timeout;
    int64_t             initialSleep;
    int64_t             loopSleep;
};

static void
_doFlush(void *arg)
{
    struct flushArg     *p = (struct flushArg*) arg;
    int                 i;

    nats_Sleep(p->initialSleep);

    for (i = 0; (p->s == NATS_OK) && (i < p->count); i++)
    {
        p->s = natsConnection_FlushTimeout(p->nc, p->timeout);
        if ((p->s == NATS_OK) && (p->loopSleep > 0))
            nats_Sleep(p->loopSleep);
    }
}

static void
test_Flush(void)
{
    natsStatus          s;
    natsOptions         *opts     = NULL;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *string   = "Hello World";
    natsThread          *threads[3] = { NULL, NULL, NULL };
    struct flushArg     args[3];
    int64_t             start = 0;
    int64_t             elapsed = 0;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetPingInterval(opts, 100));

    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test Flush empties buffer: ")
    s = natsConnection_Connect(&nc, opts);
    for (int i=0; (s == NATS_OK) && (i < 1000); i++)
        s = natsConnection_PublishString(nc, "flush", string);
    IFOK(s, natsConnection_Flush(nc));
    testCond((s == NATS_OK)
             && natsConnection_Buffered(nc) == 0);

    test("Check parallel Flush: ")
    for (int i=0; (s == NATS_OK) && (i < 3); i++)
    {
        args[i].nc           = nc;
        args[i].s            = NATS_OK;
        args[i].timeout      = 5000;
#ifdef _WIN32
        args[i].count        = 100;
#else
        args[i].count        = 1000;
#endif
        args[i].initialSleep = 500;
        args[i].loopSleep    = 1;
        s = natsThread_Create(&(threads[i]), _doFlush, (void*) &(args[i]));
    }

    for (int i=0; (s == NATS_OK) && (i < 10000); i++)
        s = natsConnection_PublishString(nc, "flush", "Hello world");

    for (int i=0; (i < 3); i++)
    {
        if (threads[i] == NULL)
            continue;

        natsThread_Join(threads[i]);
        natsThread_Destroy(threads[i]);

        if (args[i].s != NATS_OK)
            s = args[i].s;
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    test("Check Flush while in doReconnect: ")
    s = natsOptions_SetReconnectWait(opts, 3000);
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        // Capture the moment we connected
        start = nats_Now();

        // Stop the server
        _stopServer(serverPid);
        serverPid = NATS_INVALID_PID;

        // We can restart right away, since the client library will wait 3 sec
        // before reconnecting.
        serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
        CHECK_SERVER_STARTED(serverPid);

        // Attempt to Flush. This should wait for the reconnect to occur, then
        // proceed.
        for (int i=0; (s == NATS_OK) && (i < 3); i++)
        {
            args[i].nc           = nc;
            args[i].s            = NATS_OK;
            args[i].timeout      = 5000;
            args[i].count        = 1;
            args[i].initialSleep = 1000;
            args[i].loopSleep    = 0;
            s = natsThread_Create(&(threads[i]), _doFlush, (void*) &(args[i]));
        }
    }
    for (int i=0; (i < 3); i++)
    {
        if (threads[i] == NULL)
            continue;

        natsThread_Join(threads[i]);
        natsThread_Destroy(threads[i]);

        if ((s == NATS_OK) && (args[i].s != NATS_OK))
        {
            s = args[i].s;
            printf("t=%d s=%d\n", i, s);
        }
    }
    if (s == NATS_OK)
        elapsed = (nats_Now() - start);

    testCond((s == NATS_OK) && (elapsed >= 2500) && (elapsed <= 3200));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ConnCloseDoesFlush(void)
{
    natsStatus          s    = NATS_OK;
    natsPid             pid  = NATS_INVALID_PID;
    natsConnection      *nc1 = NULL;
    natsConnection      *nc2 = NULL;
    natsSubscription    *sub = NULL;
    int                 tc   = 100000;
    int                 i, iter;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    if (valgrind)
        tc = 1000;

    test("Connection close flushes: ");
    for (iter=0; (s == NATS_OK) && (iter<10); iter++)
    {
        s = natsConnection_ConnectTo(&nc1, NATS_DEFAULT_URL);
        IFOK(s, natsConnection_SubscribeSync(&sub, nc1, "foo"));
        IFOK(s, natsSubscription_SetPendingLimits(sub, -1, -1));
        IFOK(s, natsConnection_Flush(nc1));

        IFOK(s, natsConnection_ConnectTo(&nc2, NATS_DEFAULT_URL));

        for (i=0; (s == NATS_OK) && (i<tc); i++)
            s = natsConnection_PublishString(nc2, "foo", "hello");
        if (s == NATS_OK)
            natsConnection_Close(nc2);

        for (i=0; (s == NATS_OK) && (i<tc); i++)
        {
            natsMsg *msg = NULL;
            s = natsSubscription_NextMsg(&msg, sub, 1000);
            natsMsg_Destroy(msg);
        }

        natsConnection_Destroy(nc2);
        nc2 = NULL;
        natsSubscription_Destroy(sub);
        sub = NULL;
        natsConnection_Destroy(nc1);
        nc1 = NULL;
    }
    testCond(s == NATS_OK);

    _stopServer(pid);
}

static void
test_QueueSubscriber(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *s1       = NULL;
    natsSubscription    *s2       = NULL;
    natsMsg             *msg      = NULL;
    uint64_t            r1        = 0;
    uint64_t            r2        = 0;
    float               v         = 1000.0 * 0.15;
    int64_t             d1;
    int64_t             d2;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *string   = "Hello World";

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test QueueSubscriber receive correct amount: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_QueueSubscribeSync(&s1, nc, "foo", "bar"));
    IFOK(s, natsConnection_QueueSubscribeSync(&s2, nc, "foo", "bar"));
    IFOK(s, natsConnection_PublishString(nc, "foo", string));
    IFOK(s, natsConnection_Flush(nc));
    IFOK(s, natsSubscription_QueuedMsgs(s1, &r1));
    IFOK(s, natsSubscription_QueuedMsgs(s2, &r2));
    testCond((s == NATS_OK)
             && (r1 + r2 == 1));

    (void)natsSubscription_NextMsg(&msg, s1, 0);
    natsMsg_Destroy(msg);
    msg = NULL;

    (void)natsSubscription_NextMsg(&msg, s2, 0);
    natsMsg_Destroy(msg);
    msg = NULL;

    test("Test correct amount when more messages are sent: ");
    for (int i=0; (s == NATS_OK) && (i<1000); i++)
        s = natsConnection_PublishString(nc, "foo", string);

    IFOK(s, natsConnection_Flush(nc));

    r1 = r2 = 0;

    IFOK(s, natsSubscription_QueuedMsgs(s1, &r1));
    IFOK(s, natsSubscription_QueuedMsgs(s2, &r2));

    testCond((s == NATS_OK) && (r1 + r2 == 1000));

    test("Variance acceptable: ");
    d1 = 500 - r1;
    if (d1 < 0)
        d1 *= -1;

    d2 = 500 - r1;
    if (d2 < 0)
        d2 *= -1;

    testCond((d1 <= v) && (d2 <= v));

    natsSubscription_Destroy(s1);
    natsSubscription_Destroy(s2);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ReplyArg(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "bar";
    arg.status = NATS_OK;
    arg.control= 2;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test for correct Reply arg in callback: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString,
                                     (void*) &arg));
    IFOK(s, natsConnection_PublishRequestString(nc, "foo", "bar", "hello"));

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 1500);
    natsMutex_Unlock(arg.m);

    IFOK(s, arg.status);

    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_SyncReplyArg(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test for correct Reply arg in msg: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsConnection_PublishRequestString(nc, "foo", "bar", "hello"));
    IFOK(s, natsSubscription_NextMsg(&msg, sub, 1000));

    testCond((s == NATS_OK)
             && (msg != NULL)
             && (natsMsg_GetReply(msg) != NULL)
             && (strcmp("bar", natsMsg_GetReply(msg)) == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_Unsubscribe(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "bar";
    arg.status = NATS_OK;
    arg.control= 3;
    arg.sum    = 0;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect and create sub: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Send messages and flush: ");
    for (int i=0; (s == NATS_OK) && (i<20); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Unsubscribe from callback: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    IFOK(s, arg.status);
    testCond(s == NATS_OK);

    test("No more callback: ");
    nats_Sleep(250);
    natsMutex_Lock(arg.m);
    testCond((s == NATS_OK) && (arg.sum == 10));
    natsMutex_Unlock(arg.m);

    natsSubscription_Destroy(sub);
    sub = NULL;

    test("Create new sub: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg);
    testCond(s == NATS_OK);

    test("Unsubscribe after connection close: ");
    natsConnection_Destroy(nc);
    s = natsSubscription_Unsubscribe(sub);
    testCond(s == NATS_CONNECTION_CLOSED);

    natsSubscription_Destroy(sub);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_DoubleUnsubscribe(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect and create subscription: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    testCond(s == NATS_OK);

    test("Unsubscribe: ");
    s = natsSubscription_Unsubscribe(sub);
    testCond(s == NATS_OK);

    test("Double Unsubscribe: ");
    s = natsSubscription_Unsubscribe(sub);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SubRemovedWhileProcessingMsg(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect and create sub: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    testCond(s == NATS_OK);

    // Lock the sub so that we can remove while the connection
    // readLoop is trying to push to the sub.
    natsSub_Lock(sub);

    test("Send message: ");
    s = natsConnection_PublishString(nc, "foo", "hello");
    testCond(s == NATS_OK);

    test("Close sub: ");
    natsSub_Unlock(sub);
    natsSub_close(sub, false);
    testCond(s == NATS_OK);

    test("Check msg not given: ");
    natsSub_Lock(sub);
    testCond(sub->msgList.msgs == 0);
    natsSub_Unlock(sub);

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;

    // Repeat with global message delivery option.
    test("Set global delivery option: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_UseGlobalMessageDelivery(opts, true));
    testCond(s == NATS_OK);

    test("Connect and create sub: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _dummyMsgHandler, NULL));
    testCond(s == NATS_OK);

    natsSub_Lock(sub);
    natsMutex_Lock(sub->libDlvWorker->lock);
    test("Send message: ");
    s = natsConnection_PublishString(nc, "foo", "hello");
    testCond(s == NATS_OK);

    test("Close sub: ");
    natsMutex_Unlock(sub->libDlvWorker->lock);
    natsSub_Unlock(sub);
    natsSub_close(sub, false);
    testCond(s == NATS_OK);

    test("Check msg not given: ");
    natsSub_Lock(sub);
    natsMutex_Lock(sub->libDlvWorker->lock);
    testCond(sub->msgList.msgs == 0);
    natsMutex_Unlock(sub->libDlvWorker->lock);
    natsSub_Unlock(sub);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
}

static void
test_RequestTimeout(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test Request should timeout: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_RequestString(&msg, nc, "foo", "bar", 500));
    testCond(serverVersionAtLeast(2, 2, 0) ? (s == NATS_NO_RESPONDERS) : (s == NATS_TIMEOUT));

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_Request(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsMsg             *req      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "I will help you";
    arg.status = NATS_OK;
    arg.control= 4;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect and subscribe: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Test Request: ")
    s = natsConnection_RequestString(&msg, nc, "foo", "help", 500);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(arg.string,
                         natsMsg_GetData(msg),
                         natsMsg_GetDataLength(msg)) == 0));

    natsMsg_Destroy(msg);
    msg = NULL;

    test("Create req message: ");
    s = natsMsg_Create(&req, "foo", NULL, "help", 4);
    testCond(s == NATS_OK);

    test("Test RequestMsg: ");
    s = natsConnection_RequestMsg(&msg, nc, req, 500);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(arg.string,
                         natsMsg_GetData(msg),
                         natsMsg_GetDataLength(msg)) == 0));

    natsMsg_Destroy(msg);
    natsMsg_Destroy(req);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_RequestNoBody(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "I will help you";
    arg.status = NATS_OK;
    arg.control= 4;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect and subscribe: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Test Request with no body content: ")
    s = natsConnection_RequestString(&msg, nc, "foo", NULL, 500);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT)
           && !arg.msgReceived)
    {
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    }
    natsMutex_Unlock(arg.m);

    IFOK(s, arg.status);

    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(arg.string,
                         natsMsg_GetData(msg),
                         natsMsg_GetDataLength(msg)) == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
_serverForMuxWithMappedSubject(void *closure)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    struct threadArg    *arg = (struct threadArg*) closure;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _startMockupServer(&sock, "127.0.0.1", "4222");
    natsMutex_Lock(arg->m);
    arg->status = s;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);

    if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        snprintf(info, sizeof(info), "%s", "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n");

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
    }
    if (s == NATS_OK)
    {
        char buffer[1024];

        memset(buffer, 0, sizeof(buffer));

        // Read connect and ping commands sent from the client
        s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
        IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));

        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));

        // Now wait for the SUB proto and the Request
        IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));

        // Send the reply on a different subject
        IFOK(s, natsSock_WriteFully(&ctx, "MSG bar 1 2\r\nok\r\n", 17));
        if (s == NATS_OK)
        {
            // Wait for client to tell us it is done
            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !(arg->done))
                s = natsCondition_TimedWait(arg->c, arg->m, 10000);
            natsMutex_Unlock(arg->m);
        }
        natsSock_Close(ctx.fd);
    }
    natsSock_Close(sock);
}

static void
test_RequestMuxWithMappedSubject(void)
{
    natsStatus          s;
    natsConnection      *nc  = NULL;
    natsMsg             *msg = NULL;
    natsThread          *t   = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    test("Start server: ");
    arg.status = NATS_ERR;
    s = natsThread_Create(&t, _serverForMuxWithMappedSubject, (void*) &arg);
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsConnection_RequestString(&msg, nc, "foo", "help", 1000);
    testCond(s == NATS_OK);

    natsMsg_Destroy(msg);
    natsConnection_Destroy(nc);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_OldRequest(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.string = "I will help you";
    arg.status = NATS_OK;
    arg.control= 4;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Setup: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_UseOldRequestStyle(opts, true));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Test Old Request Style: ")
    s = natsConnection_RequestString(&msg, nc, "foo", "help", 500);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT)
           && !arg.msgReceived)
    {
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    }
    natsMutex_Unlock(arg.m);

    IFOK(s, arg.status);

    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strncmp(arg.string,
                         natsMsg_GetData(msg),
                         natsMsg_GetDataLength(msg)) == 0));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
_sendRequest(void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    natsStatus          s;
    natsMsg             *msg = NULL;

    nats_Sleep(250);

    s = natsConnection_RequestString(&msg, arg->nc, "foo", "Help!", 2000);
    natsMutex_Lock(arg->m);
    if ((s == NATS_OK)
            && (msg != NULL)
            && strncmp(arg->string,
                       natsMsg_GetData(msg),
                       natsMsg_GetDataLength(msg)) == 0)
    {
        arg->sum++;
    }
    else
    {
        arg->status = NATS_ERR;
    }
    natsMutex_Unlock(arg->m);
    natsMsg_Destroy(msg);
}

static void
test_SimultaneousRequest(void)
{
    natsStatus          s;
     natsConnection      *nc       = NULL;
     natsSubscription    *sub      = NULL;
     natsThread          *threads[10];
     natsPid             serverPid = NATS_INVALID_PID;
     struct threadArg    arg;
     int                 i;

     s = _createDefaultThreadArgsForCbTests(&arg);
     if ( s != NATS_OK)
         FAIL("Unable to setup test!");

     arg.string = "ok";
     arg.status = NATS_OK;
     arg.control= 4;

     serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
     CHECK_SERVER_STARTED(serverPid);

     s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
     if (s == NATS_OK)
     {
         arg.nc = nc;
         s = natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg);
     }

     for (i=0; i<10; i++)
         threads[i] = NULL;

     test("Test simultaneous requests: ")
     for (i=0; (s == NATS_OK) && (i<10); i++)
         s = natsThread_Create(&(threads[i]), _sendRequest, (void*) &arg);

     for (i=0; i<10; i++)
     {
         if (threads[i] != NULL)
         {
             natsThread_Join(threads[i]);
             natsThread_Destroy(threads[i]);
         }
     }

     natsMutex_Lock(arg.m);
     if ((s != NATS_OK)
             || ((s = arg.status) != NATS_OK)
             || (arg.sum != 10))
     {
         s = NATS_ERR;
     }
     natsMutex_Unlock(arg.m);

     testCond(s == NATS_OK);

     natsSubscription_Destroy(sub);
     natsConnection_Destroy(nc);

     _destroyDefaultThreadArgs(&arg);

     _stopServer(serverPid);
}

static void
test_RequestClose(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsThread          *t        = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    // Because of no responders, we would get an immediate timeout.
    // So we need to create a sync subscriber that is simply not
    // going to send a reply back.

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    test("Test Request is kicked out with a connection close: ")
    IFOK(s, natsThread_Create(&t, _closeConnWithDelay, (void*) nc));
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsConnection_RequestString(&msg, nc, "foo", "help", 2000));

    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }
    testCond((s == NATS_CONNECTION_CLOSED) && (msg == NULL));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_FlushInCb(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 5;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Test Flush in callback: ")
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsConnection_PublishString(nc, "foo", "hello"));

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT)
           && !arg.msgReceived)
    {
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    }
    natsMutex_Unlock(arg.m);

    IFOK(s, arg.status);

    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ReleaseFlush(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    test("Check that Flush() release on connection close: ")

    arg.control = 3;

    // We will hand run a fake server that will to reply to the second PING

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
    {
        s = NATS_SYS_ERROR;
    }

    if (s == NATS_OK)
    {
        char buffer[1024];
        char info[1024];

        strncpy(info,
                "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n",
                sizeof(info));

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));

        // Get the PING
        IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
    }

    // Now wait for the client to close the connection...
    nats_Sleep(500);

    // Need to close those for the client side to unblock.
    natsSock_Close(ctx.fd);
    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    testCond((s == NATS_OK) && (arg.status != NATS_OK));

    _destroyDefaultThreadArgs(&arg);
}


static void
test_FlushErrOnDisconnect(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    test("Check that Flush() returns an error during a disconnect: ");

    arg.control = 4;

    // We will hand run a fake server that will to reply to the second PING

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        strncpy(info,
                "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n",
                sizeof(info));

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            char buffer[1024];

            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));
    }

    // Wait a bit and kill the server.
    nats_Sleep(500);
    natsSock_Close(ctx.fd);
    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    testCond(arg.status != NATS_OK);

    if (valgrind)
        nats_Sleep(900);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_Inbox(void)
{
    natsStatus  s;
    natsInbox   *inbox = NULL;

    test("Inbox starts with correct prefix: ");
    s = natsInbox_Create(&inbox);
    testCond((s == NATS_OK)
             && (inbox != NULL)
             && (strncmp(inbox, "_INBOX.", 7) == 0));

    natsInbox_Destroy(inbox);
}

static void
test_Stats(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsStatistics      *stats  = NULL;
    natsSubscription    *s1     = NULL;
    natsSubscription    *s2     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    const char          *data = "The quick brown fox jumped over the lazy dog";
    int                 iter  = 10;
    uint64_t            outMsgs = 0;
    uint64_t            outBytes = 0;
    uint64_t            inMsgs = 0;
    uint64_t            inBytes = 0;

    test("Check invalid arg: ");
    s = natsStatistics_GetCounts(NULL, NULL, NULL, NULL, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);

    for (int i=0; (s == NATS_OK) && (i<iter); i++)
        s = natsConnection_PublishString(nc, "foo", data);

    IFOK(s, natsStatistics_Create(&stats));
    IFOK(s, natsConnection_GetStats(nc, stats));
    IFOK(s, natsStatistics_GetCounts(stats, NULL, NULL, &outMsgs, &outBytes, NULL));

    test("Tracking OutMsgs properly: ");
    testCond((s == NATS_OK) && (outMsgs == (uint64_t) iter));

    test("Tracking OutBytes properly: ");
    testCond((s == NATS_OK) && (outBytes == (uint64_t) (iter * strlen(data))));

    // Test both sync and async versions of subscribe.
    s = natsConnection_Subscribe(&s1, nc, "foo", _dummyMsgHandler, NULL);
    IFOK(s, natsConnection_SubscribeSync(&s2, nc, "foo"));

    for (int i=0; (s == NATS_OK) && (i<iter); i++)
        s = natsConnection_PublishString(nc, "foo", data);

    IFOK(s, natsConnection_Flush(nc));

    IFOK(s, natsConnection_GetStats(nc, stats));
    IFOK(s, natsStatistics_GetCounts(stats, &inMsgs, &inBytes, NULL, NULL, NULL));

    test("Tracking inMsgs properly: ");
    testCond((s == NATS_OK) && (inMsgs == (uint64_t)(2 * iter)));

    test("Tracking inBytes properly: ");
    testCond((s == NATS_OK) && (inBytes == (uint64_t)(2 * (iter * strlen(data)))));

    natsStatistics_Destroy(stats);
    natsSubscription_Destroy(s1);
    natsSubscription_Destroy(s2);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_BadSubject(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Should get an error with empty subject: ");
    s = natsConnection_PublishString(nc, "", "hello");
    testCond(s != NATS_OK);

    test("Error should be NATS_INVALID_SUBJECT: ");
    testCond(s == NATS_INVALID_SUBJECT);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SubBadSubjectAndQueueName(void)
{
    natsStatus          s;
    natsConnection      *nc         = NULL;
    natsSubscription    *sub        = NULL;
    natsPid             pid         = NATS_INVALID_PID;
    const char          *badSubs[]  = {"foo bar", "foo..bar", ".foo", "bar.baz.", "baz\t.foo"};
    const char          *badQueues[]= {"foo group", "group\t1", "g1\r\n2"};
    const char          *goodSubs[] = {"foo.bar", "a.bcd", "abc.d"};
    char                buf[256];
    int                 i;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect ok: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    for (i=0; i<(int) (sizeof(badSubs)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "test subject '%s': ", badSubs[i]);
        test(buf)
        s = natsConnection_SubscribeSync(&sub, nc, badSubs[i]);
        testCond((s == NATS_INVALID_SUBJECT) && (sub == NULL));
        nats_clearLastError();
    }

    for (i=0; i<(int) (sizeof(goodSubs)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "test subject '%s': ", goodSubs[i]);
        test(buf)
        s = natsConnection_SubscribeSync(&sub, nc, goodSubs[i]);
        testCond(s == NATS_OK);
        natsSubscription_Destroy(sub);
        sub = NULL;
    }

    for (i=0; i<(int) (sizeof(badQueues)/sizeof(char*)); i++)
    {
        snprintf(buf, sizeof(buf), "test queue '%s': ", badQueues[i]);
        test(buf)
        s = natsConnection_QueueSubscribeSync(&sub, nc, "foo", badQueues[i]);
        testCond((s == NATS_INVALID_QUEUE_NAME) && (sub == NULL));
        nats_clearLastError();
    }

    natsConnection_Destroy(nc);

    _stopServer(pid);
}


static void
test_ClientAsyncAutoUnsub(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;
    int                 checks;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 9;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsSubscription_AutoUnsubscribe(sub, 10));

    for (int i=0; (s == NATS_OK) && (i<100); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");

    IFOK(s, natsConnection_Flush(nc));

    // Wait for the subscription to become invalid
    checks = 0;
    while (natsSubscription_IsValid(sub)
           && (checks++ < 10))
    {
        nats_Sleep(100);
    }
    test("IsValid should be false: ");
    testCond((sub != NULL) && !natsSubscription_IsValid(sub));

    test("Received no more than max: ");
    testCond(arg.sum == 10);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ClientSyncAutoUnsub(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 received  = 0;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsSubscription_AutoUnsubscribe(sub, 10));

    for (int i=0; (s == NATS_OK) && (i<100); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");

    IFOK(s, natsConnection_Flush(nc));

    test("Get correct error: ");
    for (int i=0; (s == NATS_OK) && (i<100); i++)
    {
        s = natsSubscription_NextMsg(&msg, sub, 10);
        if (s == NATS_OK)
        {
            received++;
            natsMsg_Destroy(msg);
        }
    }
    testCond(s == NATS_MAX_DELIVERED_MSGS);

    test("Received no more than max: ");
    testCond(received == 10);

    test("IsValid should be false: ");
    testCond((sub != NULL) && !natsSubscription_IsValid(sub));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_ClientAutoUnsubAndReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    opts = _createReconnectOptions();
    if ((opts == NULL)
        || ((s = _createDefaultThreadArgsForCbTests(&arg)) != NATS_OK))
    {
        FAIL("Unable to setup test!");
    }

    arg.status = NATS_OK;
    arg.control= 9;

    s = natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsSubscription_AutoUnsubscribe(sub, 10));

    // Send less than the max
    for (int i=0; (s == NATS_OK) && (i<5); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");
    IFOK(s, natsConnection_Flush(nc));

    // Restart the server
    _stopServer(serverPid);
    serverPid = _startServer("nats://127.0.0.1:22222", "-p 22222", true);
    CHECK_SERVER_STARTED(serverPid);

    // Wait for reconnect
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);

    // Now send more than the max
    for (int i=0; (s == NATS_OK) && (i<50); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");
    IFOK(s, natsConnection_Flush(nc));

    nats_Sleep(10);

    test("Received no more than max: ");
    testCond((s == NATS_OK) && (arg.sum == 10));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}


static void
test_NextMsgOnClosedSub(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Setup: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsSubscription_Unsubscribe(sub));
    testCond(s == NATS_OK);

    test("Get correct error: ");
    s = natsSubscription_NextMsg(&msg, sub, 1000);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
_nextMsgKickedOut(void *closure)
{
    natsSubscription    *sub = (natsSubscription*) closure;
    natsMsg             *msg = NULL;

    (void) natsSubscription_NextMsg(&msg, sub, 10000);
}

static void
test_CloseSubRelease(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsThread          *t        = NULL;
    natsThread          *subs[3];
    natsPid             serverPid = NATS_INVALID_PID;
    int64_t             start, end;
    int                 i;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));

    for (i=0; i<3; i++)
        s = natsThread_Create(&(subs[i]), _nextMsgKickedOut, (void*) sub);

    start = nats_Now();

    IFOK(s, natsThread_Create(&t, _closeConnWithDelay, (void*) nc));

    for (i=0; i<3; i++)
    {
        if (subs[i] != NULL)
        {
            natsThread_Join(subs[i]);
            natsThread_Destroy(subs[i]);
        }
    }

    end = nats_Now();

    test("Test that NexMsg was kicked out properly: ");
    testCond((s != NATS_TIMEOUT)
             && ((end - start) <= 1000));

    natsThread_Join(t);
    natsThread_Destroy(t);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_IsValidSubscriber(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));

    test("Sub is valid: ");
    testCond((s == NATS_OK) && natsSubscription_IsValid(sub));

    test("Publish some msgs: ");
    for (int i=0; (s == NATS_OK) && (i<10); i++)
        s = natsConnection_PublishString(nc, "foo", "hello");

    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Received msg ok: ")
    s = natsSubscription_NextMsg(&msg, sub, 200);
    testCond((s == NATS_OK) && (msg != NULL));

    natsMsg_Destroy(msg);

    test("Unsub: ");
    s = natsSubscription_Unsubscribe(sub);
    testCond(s == NATS_OK);

    test("Received msg should fail after unsubscribe: ")
    s = natsSubscription_NextMsg(&msg, sub, 200);
    testCond(s != NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SlowSubscriber(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsOptions         *opts     = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int64_t             start, end;

    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
        s = natsOptions_SetMaxPendingMsgs(opts, total);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Setup: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));

    for (int i=0;
        (s == NATS_OK) && (i < total + 100); i++)
    {
        s = natsConnection_PublishString(nc, "foo", "hello");
    }
    testCond(s == NATS_OK);

    test("Check flush returns before timeout: ");
    start = nats_Now();

    (void) natsConnection_FlushTimeout(nc, 5000);

    end = nats_Now();

    testCond((end - start) < 5000);

    test("NextMsg should report error: ");
    // Make sure NextMsg returns an error to indicate slow consumer
    s = natsSubscription_NextMsg(&msg, sub, 200);
    testCond(s != NATS_OK);

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SlowAsyncSubscriber(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsOptions         *opts     = NULL;
    const char          *lastErr  = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int64_t             start, end;
    struct threadArg    arg;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetMaxPendingMsgs(opts, total));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    s = _createDefaultThreadArgsForCbTests(&arg);
    if ( s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 7;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));

    for (int i=0;
        (s == NATS_OK) && (i < (total + 100)); i++)
    {
        s = natsConnection_PublishString(nc, "foo", "hello");
    }

    test("Check Publish does not fail due to SlowConsumer: ");
    testCond(s == NATS_OK);

    test("Check flush returns before timeout: ");
    start = nats_Now();

    s = natsConnection_FlushTimeout(nc, 5000);

    end = nats_Now();

    testCond((end - start) < 5000);

    test("Flush should not report an error: ");
    testCond(s == NATS_OK);

    // Make sure the callback blocks before checking for slow consumer
    natsMutex_Lock(arg.m);

    while ((s != NATS_TIMEOUT) && !(arg.msgReceived))
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);

    natsMutex_Unlock(arg.m);

    test("Last Error should be SlowConsumer: ");
    testCond((s == NATS_OK)
             && natsConnection_GetLastError(nc, &lastErr) == NATS_SLOW_CONSUMER);

    // Release the sub
    natsMutex_Lock(arg.m);

    // Unblock the wait
    arg.closed = true;

    // And destroy the subscription here so that the next msg callback
    // is not invoked.
    natsSubscription_Destroy(sub);

    natsCondition_Signal(arg.c);
    arg.msgReceived = false;
    natsMutex_Unlock(arg.m);

    // Let the callback finish
    natsMutex_Lock(arg.m);
    while (!arg.msgReceived)
        natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    if (valgrind)
        nats_Sleep(900);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
_slowConsErrCB(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    struct threadArg *arg    = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    if (err == NATS_SLOW_CONSUMER)
    {
        arg->sum++;
        natsCondition_Signal(arg->c);
    }
    natsMutex_Unlock(arg->m);
}

static void
test_SlowConsumerCB(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsSubscription    *sub    = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetMaxPendingMsgs(opts, 1));
    IFOK(s, natsOptions_SetErrorHandler(opts, _slowConsErrCB, (void*) &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Create sub: ")
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"))
    testCond(s == NATS_OK);

    test("Publish 2 messages: ");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg1"));
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg2"));
    testCond(s == NATS_OK);

    test("Error handler invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum != 1))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Produce 1 message: ");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg3"));
    testCond(s == NATS_OK);

    test("Check handler is not invoked: ");
    nats_Sleep(50);
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum != 1))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsSubscription_Destroy(sub);
    natsOptions_Destroy(opts);
    _stopServer(pid);
    _destroyDefaultThreadArgs(&arg);
}

static void
test_PendingLimitsDeliveredAndDropped(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    const char          *lastErr  = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int                 sent      = total + 20;
    int                 msgsLimit = 0;
    int                 bytesLimit= 0;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int64_t             dropped   = 0;
    int64_t             delivered = 0;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 7;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    test("Settings, invalid args, NULL sub: ");
    s = natsSubscription_SetPendingLimits(NULL, 1, 1);
    testCond(s != NATS_OK);

    test("Settings, invalid args, zero msgs: ");
    s = natsSubscription_SetPendingLimits(sub, 0, 1);
    testCond(s != NATS_OK);

    test("Settings, invalid args, zero bytes: ");
    s = natsSubscription_SetPendingLimits(sub, 1, 0);
    testCond(s != NATS_OK);

    test("Check pending limits, NULL sub: ");
    s = natsSubscription_GetPendingLimits(NULL, &msgsLimit, &bytesLimit);
    testCond(s != NATS_OK);

    test("Check pending limits, other params NULL are OK: ");
    s = natsSubscription_GetPendingLimits(sub, NULL, NULL);
    testCond(s == NATS_OK);

    test("Check pending limits, msgsLimit NULL is OK: ");
    s = natsSubscription_GetPendingLimits(sub, NULL, &bytesLimit);
    testCond((s == NATS_OK) && (bytesLimit == NATS_OPTS_DEFAULT_MAX_PENDING_MSGS * 1024));

    test("Check pending limits, msgsLibytesLimitmit NULL is OK: ");
    s = natsSubscription_GetPendingLimits(sub, &msgsLimit, NULL);
    testCond((s == NATS_OK) && (msgsLimit == NATS_OPTS_DEFAULT_MAX_PENDING_MSGS));

    test("Set negative value for msgs OK: ");
    s = natsSubscription_SetPendingLimits(sub, -1, 100);
    testCond(s == NATS_OK);

    test("Set negative value for bytes OK: ");
    s = natsSubscription_SetPendingLimits(sub, 100, -1);
    testCond(s == NATS_OK);

    test("Set negative values OK: ");
    s = natsSubscription_SetPendingLimits(sub, -10, -10);
    testCond(s == NATS_OK);

    test("Get pending with negative values returned OK: ");
    s = natsSubscription_GetPendingLimits(sub, &msgsLimit, &bytesLimit);
    testCond((s == NATS_OK) && (msgsLimit == -10) && (bytesLimit == -10));

    msgsLimit = 0;
    bytesLimit = 0;

    test("Set valid values: ");
    s = natsSubscription_SetPendingLimits(sub, total, total * 1024);
    testCond(s == NATS_OK);

    test("Check pending limits: ");
    s = natsSubscription_GetPendingLimits(sub, &msgsLimit, &bytesLimit);
    testCond((s == NATS_OK) && (msgsLimit == total) && (bytesLimit == total * 1024));

    for (int i=0;
        (s == NATS_OK) && (i < sent); i++)
    {
        s = natsConnection_PublishString(nc, "foo", "hello");
    }
    IFOK(s, natsConnection_Flush(nc));

    // Make sure the callback blocks before checking for slow consumer
    natsMutex_Lock(arg.m);

    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);

    natsMutex_Unlock(arg.m);

    test("Last Error should be SlowConsumer: ");
    testCond((s == NATS_OK)
             && natsConnection_GetLastError(nc, &lastErr) == NATS_SLOW_CONSUMER);

    // Check the pending values
    test("Check pending values, NULL sub: ");
    s = natsSubscription_GetPending(NULL, &msgs, &bytes);
    testCond(s != NATS_OK);

    test("Check pending values, NULL msgs: ");
    s = natsSubscription_GetPending(sub, NULL, &bytes);
    testCond(s == NATS_OK);

    test("Check pending values, NULL bytes: ");
    s = natsSubscription_GetPending(sub, &msgs, NULL);
    testCond(s == NATS_OK);

    msgs = 0;
    bytes = 0;

    test("Check pending values: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK)
             && ((msgs == total) || (msgs == total - 1))
             && ((bytes == total * 5) || (bytes == (total - 1) * 5)));

    test("Check dropped: NULL sub: ");
    s = natsSubscription_GetDropped(NULL, &dropped);
    testCond(s != NATS_OK);

    test("Check dropped, NULL msgs: ");
    s = natsSubscription_GetDropped(sub, NULL);
    testCond(s != NATS_OK);

    msgs = 0;
    test("Check dropped: ");
    s = natsSubscription_GetDropped(sub, &dropped);
    testCond((s == NATS_OK)
            && ((dropped == (int64_t)(sent - total))
                || (dropped == (int64_t)(sent - total - 1))));

    test("Check delivered: NULL sub: ");
    s = natsSubscription_GetDelivered(NULL, &delivered);
    testCond(s != NATS_OK);

    test("Check delivered: NULL msgs: ");
    s = natsSubscription_GetDelivered(sub, NULL);
    testCond(s != NATS_OK);

    msgs = 0;
    test("Check delivered: ");
    s = natsSubscription_GetDelivered(sub, &delivered);
    testCond((s == NATS_OK) && (delivered == 1));

    test("Check get stats pending: ");
    s = natsSubscription_GetStats(sub, &msgs, &bytes, NULL, NULL, NULL, NULL);
    testCond((s == NATS_OK)
              && ((msgs == total) || (msgs == total - 1))
              && ((bytes == total * 5) || (bytes == (total - 1) * 5)));

    test("Check get stats max pending: ");
    s = natsSubscription_GetStats(sub, NULL, NULL, &msgs, &bytes, NULL, NULL);
    testCond((s == NATS_OK)
             && (msgs >= total - 1) && (msgs <= total)
             && (bytes >= (total - 1) * 5) && (bytes <= total * 5));

    test("Check get stats delivered: ");
    s = natsSubscription_GetStats(sub, NULL, NULL, NULL, NULL, &delivered, NULL);
    testCond((s == NATS_OK) && (delivered == 1));

    test("Check get stats dropped: ");
    s = natsSubscription_GetStats(sub, NULL, NULL, NULL, NULL, NULL, &dropped);
    testCond((s == NATS_OK)
             && ((dropped == (int64_t) (sent - total))
                 || (dropped == (int64_t) (sent - total - 1))));

    test("Check get stats all NULL: ");
    s = natsSubscription_GetStats(sub, NULL, NULL, NULL, NULL, NULL, NULL);
    testCond(s == NATS_OK);

    // Release the sub
    natsMutex_Lock(arg.m);

    // Unblock the wait
    arg.closed = true;

    // And close the subscription here so that the next msg callback
    // is not invoked.
    natsSubscription_Unsubscribe(sub);

    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    // All these calls should fail with a closed subscription
    test("SetPendingLimit on closed sub: ");
    s = natsSubscription_SetPendingLimits(sub, 1, 1);
    testCond(s != NATS_OK);

    test("GetPendingLimit on closed sub: ");
    s = natsSubscription_GetPendingLimits(sub, NULL, NULL);
    testCond(s != NATS_OK);

    test("GetPending on closed sub: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond(s != NATS_OK);

    test("GetDelivered on closed sub: ");
    s = natsSubscription_GetDelivered(sub, &delivered);
    testCond(s != NATS_OK);

    test("GetDropped on closed sub: ");
    s = natsSubscription_GetDropped(sub, &dropped);
    testCond(s != NATS_OK);

    test("Check get stats on closed sub: ");
    s = natsSubscription_GetStats(sub, NULL, NULL, NULL, NULL, NULL, NULL);
    testCond(s != NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_PendingLimitsWithSyncSub(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 msgsLimit = 0;
    int                 bytesLimit= 0;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int64_t             dropped   = 0;
    int64_t             delivered = 0;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    IFOK(s, natsSubscription_SetPendingLimits(sub, 10000, 10));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    test("Check pending limits: ");
    s = natsSubscription_GetPendingLimits(sub, &msgsLimit, &bytesLimit);
    testCond((s == NATS_OK) && (msgsLimit == 10000) && (bytesLimit == 10));

    test("Can publish: ");
    s = natsConnection_PublishString(nc, "foo", "abcde");
    IFOK(s, natsConnection_PublishString(nc, "foo", "abcdefghijklmnopqrstuvwxyz"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    // Check the pending values
    test("Check pending values: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 1) && (bytes == 5));

    msgs = 0;
    test("Check dropped: ");
    s = natsSubscription_GetDropped(sub, &dropped);
    testCond((s == NATS_OK) && (dropped == 1));

    test("Can publish small: ");
    s = natsConnection_PublishString(nc, "foo", "abc");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Receive first msg: ");
    s = natsSubscription_NextMsg(&msg, sub, 1000);
    testCond((s == NATS_OK)
             && (msg != NULL)
             && (strcmp(natsMsg_GetData(msg), "abcde") == 0));

    msgs = 0;
    test("Check delivered: ");
    s = natsSubscription_GetDelivered(sub, &delivered);
    testCond((s == NATS_OK) && (delivered == 1));

    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_AsyncSubscriptionPending(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int                 mlen      = 10;
    int                 totalSize = total * mlen;
    uint64_t            queuedMsgs= 0;
    int                 i;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 7;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    // Check with invalid args
    test("Call MaxPending with invalid args: NULL sub: ");
    s = natsSubscription_GetMaxPending(NULL, &msgs, &bytes);
    testCond(s != NATS_OK);

    test("Call MaxPending with invalid args: other NULL params OK: ");
    s = natsSubscription_GetMaxPending(sub, NULL, &bytes);
    IFOK(s, natsSubscription_GetMaxPending(sub, &msgs, NULL));
    IFOK(s, natsSubscription_GetMaxPending(sub, NULL, NULL));
    testCond(s == NATS_OK);

    for (i = 0; (s == NATS_OK) && (i < total); i++)
        s = natsConnection_PublishString(nc, "foo", "0123456789");

    IFOK(s, natsConnection_Flush(nc));

    // Wait that a message is received, so checks are safe
    natsMutex_Lock(arg.m);

    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);

    natsMutex_Unlock(arg.m);

    // Test old way
    test("Test queued msgs old way: ");
    s = natsSubscription_QueuedMsgs(sub, &queuedMsgs);
    testCond((s == NATS_OK)
            && (((int)queuedMsgs == total) || ((int)queuedMsgs == total - 1)));

    // New way, make sure the same and check bytes.
    test("Test new way: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK)
             && ((msgs == total) || (msgs == total - 1))
             && ((bytes == totalSize) || (bytes == totalSize - mlen)));


    // Make sure max has been set. Since we block after the first message is
    // received, MaxPending should be >= total - 1 and <= total
    test("Check max pending: ");
    s = natsSubscription_GetMaxPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK)
             && ((msgs <= total) && (msgs >= total-1))
             && ((bytes <= totalSize) && (bytes >= totalSize-mlen)));

    test("Check ClearMaxPending: ");
    s = natsSubscription_ClearMaxPending(sub);
    if (s == NATS_OK)
        s = natsSubscription_GetMaxPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsSubscription_Unsubscribe(sub);
    natsCondition_Signal(arg.c);
    arg.msgReceived = false;
    natsMutex_Unlock(arg.m);

    // These calls should fail once the subscription is closed.
    test("Check MaxPending on closed sub: ");
    s = natsSubscription_GetMaxPending(sub, &msgs, &bytes);
    testCond(s != NATS_OK);

    test("Check ClearMaxPending on closed sub: ");
    s = natsSubscription_ClearMaxPending(sub);
    testCond(s != NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    natsMutex_Lock(arg.m);
    while (!arg.msgReceived)
        natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_AsyncSubscriptionPendingDrain(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int64_t             delivered = 0;
    int                 i;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.string = "0123456789";
    arg.control= 1;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    for (i = 0; (s == NATS_OK) && (i < total); i++)
        s = natsConnection_PublishString(nc, "foo", arg.string);

    IFOK(s, natsConnection_Flush(nc));

    test("Wait for all delivered: ");
    msgs = 0;
    for (i=0; (s == NATS_OK) && (i<500); i++)
    {
        s = natsSubscription_GetDelivered(sub, &delivered);
        if ((s == NATS_OK) && (delivered == (int64_t) total))
            break;

        nats_Sleep(10);
    }
    testCond((s == NATS_OK) && (delivered == (int64_t) total));

    test("Check pending: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    natsSubscription_Unsubscribe(sub);

    test("Check Delivered on closed sub: ");
    s = natsSubscription_GetDelivered(sub, &delivered);
    testCond(s != NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_SyncSubscriptionPending(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsMsg             *msg      = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int                 mlen      = 10;
    int                 totalSize = total * mlen;
    uint64_t            queuedMsgs= 0;
    int                 i;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    // Check with invalid args
    test("Call MaxPending with invalid args: NULL sub: ");
    s = natsSubscription_GetMaxPending(NULL, &msgs, &bytes);
    testCond(s != NATS_OK);

    test("Call MaxPending with invalid args: other NULL params OK: ");
    s = natsSubscription_GetMaxPending(sub, NULL, &bytes);
    IFOK(s, natsSubscription_GetMaxPending(sub, &msgs, NULL));
    IFOK(s, natsSubscription_GetMaxPending(sub, NULL, NULL));
    testCond(s == NATS_OK);

    for (i = 0; (s == NATS_OK) && (i < total); i++)
        s = natsConnection_PublishString(nc, "foo", "0123456789");

    IFOK(s, natsConnection_Flush(nc));

    // Test old way
    test("Test queued msgs old way: ");
    s = natsSubscription_QueuedMsgs(sub, &queuedMsgs);
    testCond((s == NATS_OK)
            && (((int)queuedMsgs == total) || ((int)queuedMsgs == total - 1)));

    // New way, make sure the same and check bytes.
    test("Test new way: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK)
             && ((msgs == total) || (msgs == total - 1))
             && ((bytes == totalSize) || (bytes == totalSize - mlen)));


    // Make sure max has been set. Since we block after the first message is
    // received, MaxPending should be >= total - 1 and <= total
    test("Check max pending: ");
    s = natsSubscription_GetMaxPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK)
             && ((msgs <= total) && (msgs >= total-1))
             && ((bytes <= totalSize) && (bytes >= totalSize-mlen)));

    test("Check ClearMaxPending: ");
    s = natsSubscription_ClearMaxPending(sub);
    IFOK(s, natsSubscription_GetMaxPending(sub, &msgs, &bytes));
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    // Drain all but one
    for (i=0; (s == NATS_OK) && (i<total-1); i++)
    {
        s = natsSubscription_NextMsg(&msg, sub, 1000);
        if (s == NATS_OK)
            natsMsg_Destroy(msg);
    }

    test("Check pending: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 1) && (bytes == mlen));

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_SyncSubscriptionPendingDrain(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsMsg             *msg      = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 total     = 100;
    int                 msgs      = 0;
    int                 bytes     = 0;
    int64_t             delivered = 0;
    int                 i;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));

    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    for (i = 0; (s == NATS_OK) && (i < total); i++)
        s = natsConnection_PublishString(nc, "foo", "0123456789");

    IFOK(s, natsConnection_Flush(nc));

    test("Wait for all delivered: ");
    do
    {
        do
        {
            s = natsSubscription_NextMsg(&msg, sub, 10);
            if (s == NATS_OK)
                natsMsg_Destroy(msg);
        }
        while (s == NATS_OK);

        s = natsSubscription_GetDelivered(sub, &delivered);
        if ((s == NATS_OK) && (delivered == (int64_t) total))
            break;

        nats_Sleep(100);
        i++;
    }
    while ((s == NATS_OK) && (i < 50));
    testCond((s == NATS_OK) && (delivered == (int64_t) total));

    test("Check pending: ");
    s = natsSubscription_GetPending(sub, &msgs, &bytes);
    testCond((s == NATS_OK) && (msgs == 0) && (bytes == 0));

    natsSubscription_Unsubscribe(sub);

    test("Check Delivered on closed sub: ");
    s = natsSubscription_GetDelivered(sub, &delivered);
    testCond(s != NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
_asyncErrCb(natsConnection *nc, natsSubscription *sub, natsStatus err, void* closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);

    if (arg->sum == 1)
    {
        natsMutex_Unlock(arg->m);
        return;
    }

    arg->sum = 1;

    if (sub != arg->sub)
        arg->status = NATS_ERR;

    if ((arg->status == NATS_OK) && (err != NATS_SLOW_CONSUMER))
        arg->status = NATS_ERR;

    arg->closed = true;
    arg->done = true;
    natsCondition_Signal(arg->c);

    natsMutex_Unlock(arg->m);
}

static void
test_AsyncErrHandler(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 7;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetURL(opts, NATS_DEFAULT_URL));
    IFOK(s, natsOptions_SetMaxPendingMsgs(opts, 10));
    IFOK(s, natsOptions_SetErrorHandler(opts, _asyncErrCb, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to create options for test AsyncErrHandler");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "async_test", _recvTestString, (void*) &arg));

    natsMutex_Lock(arg.m);
    arg.sub = sub;
    natsMutex_Unlock(arg.m);

    for (int i=0;
        (s == NATS_OK) && (i < (opts->maxPendingMsgs + 100)); i++)
    {
        s = natsConnection_PublishString(nc, "async_test", "hello");
    }
    IFOK(s, natsConnection_Flush(nc));

    // Wait for async err callback
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.done)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    test("Aync fired properly, and all checks are good: ");
    testCond((s == NATS_OK)
             && arg.done
             && arg.closed
             && (arg.status == NATS_OK));

    natsOptions_Destroy(opts);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
_responseCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);

    arg->closed = true;
    arg->done = true;
    natsCondition_Signal(arg->c);

    natsMutex_Unlock(arg->m);

    natsMsg_Destroy(msg);
}

static void
_startCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    natsInbox           *response = NULL;
    natsStatus          s;

    natsMutex_Lock(arg->m);

    s = natsInbox_Create(&response);
    IFOK(s, natsConnection_Subscribe(&(arg->sub), nc, response, _responseCb, (void*) arg));
    IFOK(s, natsConnection_PublishRequestString(nc, "helper", response, "Help Me!"));

    if (s != NATS_OK)
        arg->status = s;

    // We need to destroy the inbox. It has been copied by the
    // natsConnection_Subscribe() call.
    natsInbox_Destroy(response);

    natsMutex_Unlock(arg->m);

    natsMsg_Destroy(msg);
}

static void
test_AsyncSubscriberStarvation(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsSubscription    *sub2     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 4;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "helper",
                                     _recvTestString, (void*) &arg));
    IFOK(s, natsConnection_Subscribe(&sub2, nc, "start",
                                     _startCb, (void*) &arg));

    IFOK(s, natsConnection_PublishString(nc, "start", "Begin"));
    IFOK(s, natsConnection_Flush(nc));

    // Wait for end of test
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.done)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    test("Test not stalled in cb waiting for other cb: ");
    testCond((s == NATS_OK)
             && arg.done
             && (arg.status == NATS_OK));

    natsSubscription_Destroy(arg.sub);
    natsSubscription_Destroy(sub);
    natsSubscription_Destroy(sub2);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_AsyncSubscriberOnClose(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsSubscription    *sub2     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 seen      = 0;
    int                 checks    = 0;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.status = NATS_OK;
    arg.control= 8;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo",
                                     _recvTestString, (void*) &arg));

    for (int i=0; (s == NATS_OK) && (i < 10); i++)
        s = natsConnection_PublishString(nc, "foo", "Hello World");

    IFOK(s, natsConnection_Flush(nc));

    // Wait to receive the first message
    test("Wait for first message: ");
    natsMutex_Lock(arg.m);
    while ((s == NATS_OK) && (arg.sum != 1))
    {
        natsMutex_Unlock(arg.m);
        nats_Sleep(100);
        natsMutex_Lock(arg.m);
        if (checks++ > 10)
            s = NATS_ILLEGAL_STATE;
    }
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsConnection_Close(nc);

    // Release callbacks
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Broadcast(arg.c);
    natsMutex_Unlock(arg.m);

    // Wait for some time
    nats_Sleep(100);

    natsMutex_Lock(arg.m);
    seen = arg.sum;
    natsMutex_Unlock(arg.m);

    test("Make sure only one callback fired: ");
    testCond(seen == 1);

    natsSubscription_Destroy(sub);
    natsSubscription_Destroy(sub2);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_NextMsgCallOnAsyncSub(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsMsg             *msg      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Setup: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, NULL))
    testCond(s == NATS_OK);

    test("NextMsg should fail for async sub: ");
    s = natsSubscription_NextMsg(&msg, sub, 1000);
    testCond((s != NATS_OK) && (msg == NULL));

    natsSubscription_Destroy(sub);

    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
testSubOnComplete(void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->status = (arg->control == 2 ? NATS_OK : NATS_ERR);
    arg->done = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
testOnCompleteMsgHandler(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->control = 1;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);

    // Sleep here so that main thread invokes Unsubscribe() and we make sure that the
    // onComplete is not invoked until this function returns.
    nats_Sleep(500);

    natsMutex_Lock(arg->m);
    arg->control = 2;
    natsMutex_Unlock(arg->m);

    natsMsg_Destroy(msg);
}

static void
test_SubOnComplete(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsSubscription    *sub      = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Invalid arg: ");
    s = natsSubscription_SetOnCompleteCB(NULL, testSubOnComplete, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Connect + sub: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_SubscribeSync(&sub, nc, "foo"));
    testCond(s == NATS_OK);

    test("Invalid sub (NULL): ");
    s= natsSubscription_SetOnCompleteCB(sub, testSubOnComplete, NULL);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    natsSubscription_Unsubscribe(sub);
    test("Invalid sub (sync): ");
    s = natsSubscription_SetOnCompleteCB(sub, testSubOnComplete, NULL);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    natsSubscription_Destroy(sub);
    sub = NULL;
    arg.status = NATS_ERR;
    test("SetOnCompleteCB ok: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", testOnCompleteMsgHandler, &arg);
    IFOK(s, natsSubscription_SetOnCompleteCB(sub, testSubOnComplete, &arg));
    testCond(s == NATS_OK);
    test("SetOnCompleteCB to NULL ok: ");
    s = natsSubscription_SetOnCompleteCB(sub, NULL, NULL);
    if (s == NATS_OK)
    {
        natsSub_Lock(sub);
        s = (((sub->onCompleteCB == NULL) && (sub->onCompleteCBClosure == NULL)) ? NATS_OK : NATS_ERR);
        natsSub_Unlock(sub);
    }
    testCond(s == NATS_OK);
    test("OnComplete invoked after last message: ");
    s = natsSubscription_SetOnCompleteCB(sub, testSubOnComplete, &arg);
    IFOK(s, natsConnection_PublishString(nc, "foo", "hello"));
    IFOK(s, natsConnection_Flush(nc));
    if (s == NATS_OK)
    {
        // Wait for message handler to be invoked
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.control != 1))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    IFOK(s, natsSubscription_Unsubscribe(sub));
    if (s == NATS_OK)
    {
        // Now wait for the onComplete to return.
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);

        // This will be OK if complete callback was invoked after msg handler returned,
        // otherwise will be NATS_ERR;
        s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);
}

static void
test_ServersOption(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    char                buffer[128];
    int                 serversCount;

    serversCount = sizeof(testServers) / sizeof(char *);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    test("Connect should fail with NATS_NO_SERVER: ");
    s = natsConnection_Connect(&nc, opts);
    testCond((nc == NULL) && (s == NATS_NO_SERVER));

    test("Connect with list of servers should fail with NATS_NO_SERVER: ");
    s = natsOptions_SetServers(opts, testServers, serversCount);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((nc == NULL) && (s == NATS_NO_SERVER));

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    buffer[0] = '\0';
    test("Can connect to first: ")
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer)));
    testCond((s == NATS_OK)
             && (buffer[0] != '\0')
             && (strcmp(buffer, testServers[0]) == 0));

    natsConnection_Destroy(nc);
    nc = NULL;

    _stopServer(serverPid);
    serverPid = NATS_INVALID_PID;

    // Make sure we can connect to a non first server if running
    serverPid = _startServer("nats://127.0.0.1:1223", "-p 1223", true);
    CHECK_SERVER_STARTED(serverPid);

    buffer[0] = '\0';
    test("Can connect to second: ")
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer)));
    testCond((s == NATS_OK)
             && (buffer[0] != '\0')
             && (strcmp(buffer, testServers[1]) == 0));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _stopServer(serverPid);
}

static void
test_AuthServers(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid1= NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    char                buffer[128];
    const char          *plainServers[] = {"nats://127.0.0.1:1222",
                                           "nats://127.0.0.1:1224"};
    const char          *authServers[] = {"nats://127.0.0.1:1222",
                                          "nats://ivan:foo@127.0.0.1:1224"};
    int serversCount = 2;

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetServers(opts, plainServers, serversCount));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid1 = _startServer("nats://127.0.0.1:1222", "-p 1222 --user ivan --pass foo", false);
    CHECK_SERVER_STARTED(serverPid1);

    serverPid2 = _startServer("nats://127.0.0.1:1224", "-p 1224 --user ivan --pass foo", false);
    if (serverPid2 == NATS_INVALID_PID)
        _stopServer(serverPid1);
    CHECK_SERVER_STARTED(serverPid2);

    nats_Sleep(500);

    test("Connect fails due to auth error: ");
    s = natsConnection_Connect(&nc, opts);
    testCond((s == NATS_CONNECTION_AUTH_FAILED) && (nc == NULL));

    buffer[0] = '\0';
    test("Connect succeeds with correct servers list: ")
    s = natsOptions_SetServers(opts, authServers, serversCount);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_OK)
             && (nc != NULL)
             && (natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer)) == NATS_OK)
             && (strcmp(buffer, authServers[1]) == 0));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _stopServer(serverPid1);
    _stopServer(serverPid2);
}

static void
test_AuthFailToReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid1= NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    natsPid             serverPid3= NATS_INVALID_PID;
    char                buffer[64];
    const char          *servers[] = {"nats://127.0.0.1:22222",
                                      "nats://127.0.0.1:22223",
                                      "nats://127.0.0.1:22224"};
    struct threadArg    args;

    int serversCount = 3;

    s = _createDefaultThreadArgsForCbTests(&args);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetServers(opts, servers, serversCount));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &args));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));

    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid1 = _startServer("nats://127.0.0.1:22222", "-p 22222", false);
    CHECK_SERVER_STARTED(serverPid1);

    serverPid2 = _startServer("nats://127.0.0.1:22223", "-p 22223 --user ivan --pass foo", false);
    if (serverPid2 == NATS_INVALID_PID)
        _stopServer(serverPid1);
    CHECK_SERVER_STARTED(serverPid2);

    serverPid3 = _startServer("nats://127.0.0.1:22224", "-p 22224", false);
    if (serverPid3 == NATS_INVALID_PID)
    {
        _stopServer(serverPid1);
        _stopServer(serverPid2);
    }
    CHECK_SERVER_STARTED(serverPid3);

    nats_Sleep(1000);

    test("Connect should succeed: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    // Stop the server which will trigger the reconnect
    _stopServer(serverPid1);
    serverPid1 = NATS_INVALID_PID;


    // The client will try to connect to the second server, and that
    // should fail. It should then try to connect to the third and succeed.

    // Wait for the reconnect CB.
    test("Reconnect callback should be triggered: ")
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT)
           && !(args.reconnected))
    {
        s = natsCondition_TimedWait(args.c, args.m, 5000);
    }
    natsMutex_Unlock(args.m);
    testCond((s == NATS_OK) && args.reconnected);

    test("Connection should not be closed: ");
    testCond(natsConnection_IsClosed(nc) == false);

    buffer[0] = '\0';
    test("Should have connected to third server: ");
    s = natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer));
    testCond((s == NATS_OK) && (buffer[0] != '\0')
             && (strcmp(buffer, servers[2]) == 0));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid2);
    _stopServer(serverPid3);
}

static void
test_BasicClusterReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid1= NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    char                buffer[128];
    int                 serversCount;
    int64_t             reconnectTimeStart, reconnectTime;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(testServers) / sizeof(char*);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_IPResolutionOrder(opts, 4));
    IFOK(s, natsOptions_SetTimeout(opts, 500));
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid1 = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid1);

    serverPid2 = _startServer("nats://127.0.0.1:1224", "-p 1224", true);
    if (serverPid2 == NATS_INVALID_PID)
        _stopServer(serverPid1);
    CHECK_SERVER_STARTED(serverPid2);

    test("Check connected to the right server: ");
    s = natsConnection_Connect(&nc, opts);

    _stopServer(serverPid1);

    // wait for disconnect
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    reconnectTimeStart = nats_Now();

    // wait for reconnect
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 3000);
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK)
             && (natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer)) == NATS_OK)
             && (strcmp(buffer, testServers[2]) == 0));

    // Make sure we did not wait on reconnect for default time.
    // Reconnect should be fast since it will be a switch to the
    // second server and not be dependent on server restart time.
    reconnectTime = nats_Now() - reconnectTimeStart;

    test("Check reconnect time did not take too long: ");
#if _WIN32
    testCond(reconnectTime <= 1300);
#else
    testCond(reconnectTime <= 100);
#endif
    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _waitForConnClosed(&arg);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid2);
}

static const char*
_reconnectTokenHandler(void* closure)
{
    const char *token;
    struct threadArg *args      = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    token = args->tokens[args->tokenCallCount % (sizeof(args->tokens)/sizeof(char*))];
    args->tokenCallCount++;
    natsMutex_Unlock(args->m);

    return token;
}

static void
test_ReconnectWithTokenHandler(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid1= NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    natsPid             serverPid3= NATS_INVALID_PID;
    char                buffer[64];
    const char          *servers[] = {"nats://127.0.0.1:22222",
                                      "nats://127.0.0.1:22223",
                                      "nats://127.0.0.1:22224"};
    struct threadArg    args;

    int serversCount = 3;

    s = _createDefaultThreadArgsForCbTests(&args);
    args.tokenCallCount = 0;
    args.tokens[0] = "token1";
    args.tokens[1] = "badtoken";
    args.tokens[2] = "token3";

    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetServers(opts, servers, serversCount));
    IFOK(s, natsOptions_SetTokenHandler(opts, _reconnectTokenHandler, (void*) &args));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &args));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));

    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid1 = _startServer("nats://token1@127.0.0.1:22222", "-p 22222 --auth token1", true);
    CHECK_SERVER_STARTED(serverPid1);

    serverPid2 = _startServer("nats://user:foo@127.0.0.1:22223", "-p 22223 --user ivan --pass foo", true);
    if (serverPid2 == NATS_INVALID_PID)
        _stopServer(serverPid1);
    CHECK_SERVER_STARTED(serverPid2);

    serverPid3 = _startServer("nats://token3@127.0.0.1:22224", "-p 22224 --auth token3", true);
    if (serverPid3 == NATS_INVALID_PID)
    {
        _stopServer(serverPid1);
        _stopServer(serverPid2);
    }
    CHECK_SERVER_STARTED(serverPid3);

    test("Connect should succeed: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    // Stop the server which will trigger the reconnect
    _stopServer(serverPid1);
    serverPid1 = NATS_INVALID_PID;


    // The client will try to connect to the second server, and that
    // should fail. It should then try to connect to the third and succeed.

    // Wait for the reconnect CB.
    test("Reconnect callback should be triggered: ")
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT)
           && !(args.reconnected))
    {
        s = natsCondition_TimedWait(args.c, args.m, 5000);
    }
    natsMutex_Unlock(args.m);
    testCond((s == NATS_OK) && args.reconnected);

    test("Connection should not be closed: ");
    testCond(natsConnection_IsClosed(nc) == false);

    buffer[0] = '\0';
    test("Should have connected to third server: ");
    s = natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer));
    testCond((s == NATS_OK) && (buffer[0] != '\0')
             && (strcmp(buffer, servers[2]) == 0));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid2);
    _stopServer(serverPid3);
}

#define NUM_CLIENTS (100)

struct hashCount
{
    int count;

};

static void
test_HotSpotReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc[NUM_CLIENTS];
    natsOptions         *opts     = NULL;
    natsPid             serverPid1= NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    natsPid             serverPid3= NATS_INVALID_PID;
    char                buffer[128];
    int                 serversCount;
    natsStrHash         *cs = NULL;
    struct threadArg    arg;
    struct hashCount    *count = NULL;

#if _WIN32
    test("Skip when running on Windows: ");
    testCond(true);
    return;
#endif

    memset(nc, 0, sizeof(nc));

    s = natsStrHash_Create(&cs, 4);
    IFOK(s, _createDefaultThreadArgsForCbTests(&arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(testServers) / sizeof(char*);

    serverPid1 = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid1);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount))
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    for (int i=0; (s == NATS_OK) && (i<NUM_CLIENTS); i++)
    {
        s = natsConnection_Connect(&(nc[i]), opts);
        IFOK(s, natsConnection_GetConnectedUrl(nc[i], buffer, sizeof(buffer)));
        if ((s == NATS_OK)
            && (strcmp(buffer, testServers[0]) != 0))
        {
            s = NATS_ERR;
        }
    }
    if (s == NATS_OK)
    {
        serverPid2 = _startServer("nats://127.0.0.1:1224", "-p 1224", true);
        serverPid3 = _startServer("nats://127.0.0.1:1226", "-p 1226", true);

        if ((serverPid2 == NATS_INVALID_PID)
            || (serverPid3 == NATS_INVALID_PID))
        {
            _stopServer(serverPid1);
            _stopServer(serverPid2);
            _stopServer(serverPid3);
            FAIL("Unable to start or verify that the server was started!");
        }
    }

    _stopServer(serverPid1);

    // Wait on all reconnects
    test("Check all reconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.reconnects != NUM_CLIENTS))
        s = natsCondition_TimedWait(arg.c, arg.m, 10000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.reconnects == NUM_CLIENTS);

    // Walk the clients and calculate how many of each..
    for (int i=0; (s == NATS_OK) && (i<NUM_CLIENTS); i++)
    {
        if (nc[i] == NULL)
        {
            s = NATS_ERR;
            break;
        }

        buffer[0] = '\0';
        s = natsConnection_GetConnectedUrl(nc[i], buffer, sizeof(buffer));
        if (s == NATS_OK)
        {
            count = (struct hashCount*) natsStrHash_Get(cs, buffer);
            if (count == NULL)
            {
                count = (struct hashCount*) calloc(1, sizeof(struct hashCount));
                if (count == NULL)
                    s = NATS_NO_MEMORY;
            }

            if (s == NATS_OK)
            {
                count->count++;
                s = natsStrHash_Set(cs, buffer, true, (void*) count, NULL);
            }
        }

        natsConnection_Close(nc[i]);
    }

    test("Check correct number of servers: ");
    testCond((s == NATS_OK) && (natsStrHash_Count(cs) == 2));

    if (s == NATS_OK)
    {
        // numClients  = 100
        // numServers  = 2
        // expected    = numClients / numServers
        // v           = expected * 0.3

        natsStrHashIter     iter;
        struct hashCount    *val;
        int                 total;
        int                 delta;
        int                 v = (int) (((float)NUM_CLIENTS / 2) * 0.30);
        void                *p = NULL;

        natsStrHashIter_Init(&iter, cs);
        while (natsStrHashIter_Next(&iter, NULL, &p))
        {
            val = (struct hashCount*) p;
            total = val->count;

            delta = ((NUM_CLIENTS / 2) - total);
            if (delta < 0)
                delta *= -1;

            if (delta > v)
                s = NATS_ERR;

            free(val);
        }

        test("Check variance: ");
        testCond(s == NATS_OK);
    }

    for (int i=0; i<NUM_CLIENTS; i++)
        natsConnection_Destroy(nc[i]);

    natsStrHash_Destroy(cs);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid2);
    _stopServer(serverPid3);
}

static void
test_ProperReconnectDelay(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 serversCount;
    struct threadArg    arg;

#if _WIN32
    test("Skip when running on Windows: ");
    testCond(true);
    return;
#endif

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(testServers) / sizeof(char*);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    // wait for disconnect
    test("Wait for disconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    // Wait, want to make sure we don't spin on reconnect to non-existent servers.
    nats_Sleep(1000);

    // Make sure we are still reconnecting..
    test("ClosedCB should not be invoked: ")
    natsMutex_Lock(arg.m);
    testCond(arg.closed == false);
    natsMutex_Unlock(arg.m);

    test("Should still be reconnecting: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_RECONNECTING);

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    // Now that the connection is destroyed, the callback will be invoked.
    // Make sure that we wait until then before destroying 'arg'.
    _waitForConnClosed(&arg);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ProperFalloutAfterMaxAttempts(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 serversCount;
    struct threadArg    arg;

#if _WIN32
    test("Skip when running on Windows: ");
    testCond(true);
    return;
#endif

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(testServers) / sizeof(char*);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 5));
    IFOK(s, natsOptions_SetReconnectWait(opts, 25));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    // wait for disconnect
    test("Wait for disconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    // wait for closed
    test("Wait for closed: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.closed);

    test("Disconnected should have been called only once: ");
    testCond((s == NATS_OK) && arg.disconnects == 1);

    test("Connection should be closed: ")
    testCond((s == NATS_OK)
             && natsConnection_IsClosed(nc));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_StopReconnectAfterTwoAuthErr(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsPid             serverPid2= NATS_INVALID_PID;
    const char          *servers[]= {"nats://127.0.0.1:1222", "nats://127.0.0.1:1223"};
    natsStatistics      *stats    = NULL;
    int                 serversCount;
    struct threadArg    arg;

//#if _WIN32
//    test("Skip when running on Windows: ");
//    testCond(true);
//    return;
//#endif

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(servers) / sizeof(char*);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetReconnectWait(opts, 25));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetServers(opts, servers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
#if _WIN32
    IFOK(s, natsOptions_SetTimeout(opts, 500));
#endif
    IFOK(s, natsStatistics_Create(&stats));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    serverPid2 = _startServer("nats://127.0.0.1:1223", "-p 1223 -user ivan -pass secret", true);
    if (serverPid == NATS_INVALID_PID)
        _stopServer(serverPid);
    CHECK_SERVER_STARTED(serverPid2);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    // wait for disconnect
    test("Wait for disconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    // wait for closed
    test("Wait for closed: ");
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    // Make sure we have not exceeded MaxReconnect
    test("Check reconnect twice: ");
    s = natsConnection_GetStats(nc, stats);
    testCond((s == NATS_OK)
             && (stats != NULL)
             && (stats->reconnects == 2));

    // Disconnect CB only from disconnect from server 1.
    test("Disconnected should have been called once: ");
    testCond((s == NATS_OK) && arg.disconnects == 1);

    test("Connection should be closed: ")
    testCond((s == NATS_OK)
             && natsConnection_IsClosed(nc));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);
    natsStatistics_Destroy(stats);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid2);
}

static void
test_TimeoutOnNoServer(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 serversCount;
    int64_t             startWait, timedWait;
    struct threadArg    arg;

#if _WIN32
    test("Skip when running on Windows: ");
    testCond(true);
    return;
#endif

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serversCount = sizeof(testServers) / sizeof(char*);

    // 1000 milliseconds total time wait
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    _stopServer(serverPid);

    // wait for disconnect
    test("Wait for disconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.disconnected);

    startWait = nats_Now();

    // wait for closed
    test("Wait for closed: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000 + serversCount*50);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && arg.closed);

    timedWait = nats_Now() - startWait;

    // The client will try to reconnect to serversCount servers.
    // It will do that for MaxReconnects==10 times.
    // For a server that has been already tried, it should sleep
    // ReconnectWait==100ms. When a server is not running, the connect
    // failure on UNIXes should be fast, still account for that.
    test("Check wait time for closed cb: ");
    testCond(timedWait <= ((opts->maxReconnect * opts->reconnectWait) + serversCount*opts->maxReconnect*50));

    natsOptions_Destroy(opts);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_PingReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int                 serversCount;
    int64_t             disconnectedAt, reconnectedAt, pingCycle;
    struct threadArg    arg;

#if _WIN32
    test("Skip when running on Windows: ");
    testCond(true);
    return;
#endif

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    arg.control = 9;

    serversCount = sizeof(testServers) / sizeof(char*);

    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetReconnectWait(opts, 200));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetPingInterval(opts, 50));
    IFOK(s, natsOptions_SetMaxPingsOut(opts, -1));
    IFOK(s, natsOptions_SetServers(opts, testServers, serversCount));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));

    if (s != NATS_OK)
        FAIL("Unable to create options for test ServerOptions");

    serverPid = _startServer("nats://127.0.0.1:1222", "-p 1222", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Pings cause reconnects: ")
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.reconnects != 4))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && (arg.reconnects == 4));

    natsConnection_Destroy(nc);

    for (int i=0; i<(4-1); i++)
    {
        disconnectedAt = arg.disconnectedAt[i];
        reconnectedAt = arg.reconnectedAt[i];

        pingCycle = reconnectedAt - disconnectedAt;
        if (pingCycle > 2 * opts->pingInterval)
        {
            s = NATS_ERR;
            break;
        }
    }

    test("Reconnect due to ping cycle correct: ");
    testCond(s == NATS_OK);

    // Wait for connection closed before destroying arg.
    natsMutex_Lock(arg.m);
    while (!arg.closed)
        natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);

    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(serverPid);

}

static void
test_GetServers(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsPid             s1Pid = NATS_INVALID_PID;
    natsPid             s2Pid = NATS_INVALID_PID;
    natsPid             s3Pid = NATS_INVALID_PID;
    char                **servers = NULL;
    int                 count     = 0;

    s1Pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -cluster nats://127.0.0.1:5222 -cluster_name abc", true);
    CHECK_SERVER_STARTED(s1Pid);

    s2Pid = _startServer("nats://127.0.0.1:4223", "-a 127.0.0.1 -p 4223 -cluster nats://127.0.0.1:5223 -cluster_name abc -routes nats://127.0.0.1:5222", true);
    if (s2Pid == NATS_INVALID_PID)
        _stopServer(s1Pid);
    CHECK_SERVER_STARTED(s2Pid);

    s3Pid = _startServer("nats://127.0.0.1:4224", "-a 127.0.0.1 -p 4224 -cluster nats://127.0.0.1:5224 -cluster_name abc -routes nats://127.0.0.1:5222", true);
    if (s3Pid == NATS_INVALID_PID)
    {
        _stopServer(s1Pid);
        _stopServer(s2Pid);
    }
    CHECK_SERVER_STARTED(s3Pid);

    test("Get Servers: ");
    s = natsConnection_ConnectTo(&conn, "nats://127.0.0.1:4222");
    IFOK(s, natsConnection_GetServers(conn, &servers, &count));
    if (s == NATS_OK)
    {
        int i;

        // Be tolerant that if we were to connect to an older
        // server, we would just get 1 url back.
        if ((count != 1) && (count != 3))
            s = nats_setError(NATS_ERR, "Unexpected number of servers: %d instead of 1 or 3", count);

        for (i=0; (s == NATS_OK) && (i < count); i++)
        {
            if ((strcmp(servers[i], "nats://127.0.0.1:4222") != 0)
                    && (strcmp(servers[i], "nats://127.0.0.1:4223") != 0)
                    && (strcmp(servers[i], "nats://127.0.0.1:4224") != 0))
            {
                s = nats_setError(NATS_ERR, "Unexpected server URL: %s", servers[i]);
            }
        }

        for (i=0; i<count; i++)
            free(servers[i]);
        free(servers);
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(conn);
    conn = NULL;

    _stopServer(s3Pid);
    _stopServer(s2Pid);
    _stopServer(s1Pid);

    s1Pid = NATS_INVALID_PID;
    s1Pid = _startServer("nats://ivan:password@127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -user ivan -pass password", true);
    CHECK_SERVER_STARTED(s1Pid);

    test("Get Servers does not return credentials: ");
    s = natsConnection_ConnectTo(&conn, "nats://ivan:password@127.0.0.1:4222");
    IFOK(s, natsConnection_GetServers(conn, &servers, &count));
    if (s == NATS_OK)
    {
        if (count != 1)
            s = nats_setError(NATS_ERR, "Unexpected number of servers: %d instead of 1", count);
        else if (strcmp(servers[0], "nats://127.0.0.1:4222") != 0)
            s = nats_setError(NATS_ERR, "Unexpected server URL: %s", servers[0]);

        free(servers[0]);
        free(servers);
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(conn);
    _stopServer(s1Pid);
}

static void
test_GetDiscoveredServers(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsPid             s1Pid = NATS_INVALID_PID;
    natsPid             s2Pid = NATS_INVALID_PID;
    char                **servers = NULL;
    int                 count     = 0;

    s1Pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -cluster nats://127.0.0.1:5222 -cluster_name abc", true);
    CHECK_SERVER_STARTED(s1Pid);

    s2Pid = _startServer("nats://127.0.0.1:4223", "-a 127.0.0.1 -p 4223 -cluster nats://127.0.0.1:5223 -cluster_name abc -routes nats://127.0.0.1:5222", true);
    if (s2Pid == NATS_INVALID_PID)
        _stopServer(s1Pid);
    CHECK_SERVER_STARTED(s2Pid);

    test("GetDiscoveredServers: ");
    s = natsConnection_ConnectTo(&conn, "nats://127.0.0.1:4222");
    IFOK(s, natsConnection_GetDiscoveredServers(conn, &servers, &count));
    if (s == NATS_OK)
    {
        int i;

        // Be tolerant that if we were to connect to an older
        // server, we would get nothing
        if (count > 1)
            s = nats_setError(NATS_ERR, "Unexpected number of servers: %d instead of 1 or 0", count);

        for (i=0; (s == NATS_OK) && (i < count); i++)
        {
            if (strcmp(servers[i], "nats://127.0.0.1:4223") != 0)
                s = nats_setError(NATS_ERR, "Unexpected server URL: %s", servers[i]);
        }

        for (i=0; i<count; i++)
            free(servers[i]);
        free(servers);
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(conn);
    conn = NULL;

    _stopServer(s2Pid);
    _stopServer(s1Pid);
}

static void
_discoveredServersCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->sum++;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
test_DiscoveredServersCb(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsPid             s1Pid = NATS_INVALID_PID;
    natsPid             s2Pid = NATS_INVALID_PID;
    natsPid             s3Pid = NATS_INVALID_PID;
    natsOptions         *opts = NULL;
    struct threadArg    arg;
    int                 invoked = 0;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetDiscoveredServersCB(opts, _discoveredServersCb, &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    s1Pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -cluster nats-route://127.0.0.1:5222 -cluster_name abc", true);
    CHECK_SERVER_STARTED(s1Pid);

    s2Pid = _startServer("nats://127.0.0.1:4223", "-a 127.0.0.1 -p 4223 -cluster nats-route://127.0.0.1:5223 -cluster_name abc -routes nats-route://127.0.0.1:5222", true);
    if (s2Pid == NATS_INVALID_PID)
        _stopServer(s1Pid);
    CHECK_SERVER_STARTED(s2Pid);

    test("DiscoveredServersCb not triggered on initial connect: ");
    s = natsConnection_Connect(&conn, opts);
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum == 0))
        s = natsCondition_TimedWait(arg.c, arg.m, 500);
    invoked = arg.sum;
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_TIMEOUT) && (invoked == 0));
    s = NATS_OK;

    s3Pid = _startServer("nats://127.0.0.1:4224", "-a 127.0.0.1 -p 4224 -cluster nats-route://127.0.0.1:5224 -cluster_name abc -routes nats-route://127.0.0.1:5222", true);
    if (s3Pid == NATS_INVALID_PID)
    {
        _stopServer(s1Pid);
        _stopServer(s2Pid);
    }
    CHECK_SERVER_STARTED(s3Pid);

    test("DiscoveredServersCb triggered on new server joining the cluster: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum == 0))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    invoked = arg.sum;
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && (invoked == 1));

    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    _stopServer(s3Pid);
    _stopServer(s2Pid);
    _stopServer(s1Pid);

    _destroyDefaultThreadArgs(&arg);
}

static void
_serverSendsINFOAfterPONG(void *closure)
{
    struct threadArg    *arg    = (struct threadArg*) closure;
    natsStatus          s       = NATS_OK;
    natsSock            sock    = NATS_SOCK_INVALID;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    // We will hand run a fake server that will send an INFO protocol
    // right after sending the initial PONG.

    s = _startMockupServer(&sock, "127.0.0.1", "4222");

    natsMutex_Lock(arg->m);
    arg->status = s;
    arg->done = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);

    if ((s == NATS_OK)
            && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
                    || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        const char* info = "INFO {}\r\n";

        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
    }
    if (s == NATS_OK)
    {
        char buffer[1024];

        memset(buffer, 0, sizeof(buffer));

        // Read connect and ping commands sent from the client
        s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
        IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
    }
    // Send PONG + INFO
    if (s == NATS_OK)
    {
        char buffer[1024];

        snprintf(buffer, sizeof(buffer), "PONG\r\nINFO {\"connect_urls\":[\"127.0.0.1:4222\",\"me:1\"]}\r\n");

        s = natsSock_WriteFully(&ctx, buffer, (int) strlen(buffer));
    }

    // Wait for a signal from the client thread.
    natsMutex_Lock(arg->m);
    while (!arg->closed)
        natsCondition_Wait(arg->c, arg->m);
    arg->status = s;
    natsMutex_Unlock(arg->m);

    natsSock_Close(ctx.fd);
    natsSock_Close(sock);
}

static void
test_ReceiveINFORightAfterFirstPONG(void)
{
    natsStatus          s       = NATS_OK;
    natsThread          *t      = NULL;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    struct threadArg arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetAllowReconnect(opts, false));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Verify that INFO right after PONG is ok: ");

    s = natsThread_Create(&t, _serverSendsINFOAfterPONG, (void*) &arg);
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while (!arg.done)
            natsCondition_Wait(arg.c, arg.m);
        s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        int     i, j;
        char    **servers    = NULL;
        int     serversCount = 0;
        bool    ok           = false;

        for (i = 0; i < 100; i++)
        {
            s = natsConnection_GetDiscoveredServers(nc, &servers, &serversCount);
            if (s != NATS_OK)
                break;

            ok = ((serversCount == 1)
                    && (strcmp(servers[0], "nats://me:1") == 0));

            for (j = 0; j < serversCount; j++)
                free(servers[j]);
            free(servers);

            if (ok)
                break;

            nats_Sleep(15);
            s = NATS_ERR;
        }
    }
    if (t != NULL)
    {
        natsMutex_Lock(arg.m);
        arg.closed = true;
        natsCondition_Signal(arg.c);
        natsMutex_Unlock(arg.m);

        natsThread_Join(t);
        natsThread_Destroy(t);

        natsMutex_Lock(arg.m);
        if ((s == NATS_OK) && (arg.status != NATS_OK))
            s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
}

static void
test_ServerPoolUpdatedOnClusterUpdate(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsPid             s1Pid = NATS_INVALID_PID;
    natsPid             s2Pid = NATS_INVALID_PID;
    natsPid             s3Pid = NATS_INVALID_PID;
    natsOptions         *opts = NULL;
    struct threadArg    arg;
    int                 invoked = 0;
    bool                restartS2 = false;

    if (!serverVersionAtLeast(1,0,7))
    {
        char txt[200];

        snprintf(txt, sizeof(txt), "Skipping since requires server version of at least 1.0.7, got %s: ", serverVersion);
        test(txt);
        testCond(true);
        return;
    }

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if ((opts == NULL)
            || (natsOptions_SetURL(opts, "nats://127.0.0.1:4222") != NATS_OK)
            || (natsOptions_SetDiscoveredServersCB(opts, _discoveredServersCb, &arg))
            || (natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg) != NATS_OK)
            || (natsOptions_SetClosedCB(opts, _closedCb, &arg) != NATS_OK))
    {
        FAIL("Unable to create reconnect options!");
    }

    s1Pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -cluster nats://127.0.0.1:6222 -cluster_name abc -routes nats://127.0.0.1:6223,nats://127.0.0.1:6224", true);
    CHECK_SERVER_STARTED(s1Pid);

    test("Connect ok: ");
    s = natsConnection_Connect(&conn, opts);
    testCond(s == NATS_OK);

    s2Pid = _startServer("nats://127.0.0.1:4223", "-a 127.0.0.1 -p 4223 -cluster nats://127.0.0.1:6223 -cluster_name abc -routes nats://127.0.0.1:6222,nats://127.0.0.1:6224", true);
    if (s2Pid == NATS_INVALID_PID)
        _stopServer(s1Pid);
    CHECK_SERVER_STARTED(s2Pid);

    test("DiscoveredServersCb triggered: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum == 0))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    invoked = arg.sum;
    arg.sum = 0;
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && (invoked == 1));

    {
        const char *urls[] = {"127.0.0.1:4222", "127.0.0.1:4223"};

        test("Check pool: ");
        s = _checkPool(conn, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
        testCond(s == NATS_OK);
    }

    s3Pid = _startServer("nats://127.0.0.1:4224", "-a 127.0.0.1 -p 4224 -cluster nats://127.0.0.1:6224 -cluster_name abc -routes nats://127.0.0.1:6222,nats://127.0.0.1:6223", true);
    if (s3Pid == NATS_INVALID_PID)
    {
        _stopServer(s1Pid);
        _stopServer(s2Pid);
    }
    CHECK_SERVER_STARTED(s3Pid);

    test("DiscoveredServersCb triggered: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum == 0))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    invoked = arg.sum;
    arg.sum = 0;
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && (invoked == 1));

    {
        const char *urls[] = {"127.0.0.1:4222", "127.0.0.1:4223", "127.0.0.1:4224"};
        test("Check pool: ");
        s = _checkPool(conn, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
        testCond(s == NATS_OK);
    }

    // Stop s1. Since this was passed to the Connect() call, this one should
    // still be present.
    _stopServer(s1Pid);
    s1Pid = NATS_INVALID_PID;

    test("Wait for reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    arg.reconnected = false;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    {
        const char *urls[] = {"127.0.0.1:4222", "127.0.0.1:4223", "127.0.0.1:4224"};

        test("Check pool: ");
        s = _checkPool(conn, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
        testCond(s == NATS_OK);
    }

    {
        const char *urls[] = {"127.0.0.1:4222", ""};
        int port = 0;

        // Check the server we reconnected to.
        natsMutex_Lock(conn->mu);
        port = conn->cur->url->port;
        natsMutex_Unlock(conn->mu);

        if (port == 4223)
        {
            urls[1] = "127.0.0.1:4224";
            _stopServer(s2Pid);
            s2Pid = NATS_INVALID_PID;
            restartS2 = true;
        }
        else
        {
            urls[1] = "127.0.0.1:4223";
            _stopServer(s3Pid);
            s3Pid = NATS_INVALID_PID;
        }

        test("Wait for reconnect: ");
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.reconnected)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        arg.reconnected = false;
        natsMutex_Unlock(arg.m);
        testCond(s == NATS_OK);

        // The implicit server that we just shutdown should have been removed from the pool
        if (s == NATS_OK)
        {
            test("Check pool: ");
            s = _checkPool(conn, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
            testCond(s == NATS_OK);
        }
    }

    {
        const char *urls[] = {"127.0.0.1:4222", "127.0.0.1:4223", "127.0.0.1:4224"};

        if (restartS2)
        {
            s2Pid = _startServer("nats://127.0.0.1:4223", "-a 127.0.0.1 -p 4223 -cluster nats://127.0.0.1:6223 -cluster_name abc -routes nats://127.0.0.1:6222,nats://127.0.0.1:6224", true);
            if (s2Pid == NATS_INVALID_PID)
                _stopServer(s3Pid);
            CHECK_SERVER_STARTED(s2Pid);
        }
        else
        {
            s3Pid = _startServer("nats://127.0.0.1:4224", "-a 127.0.0.1 -p 4224 -cluster nats://127.0.0.1:6224 -cluster_name abc -routes nats://127.0.0.1:6222,nats://127.0.0.1:6223", true);
            if (s3Pid == NATS_INVALID_PID)
                _stopServer(s2Pid);
            CHECK_SERVER_STARTED(s3Pid);
        }
        // Since this is not a "new" server, the DiscoveredServersCB won't be invoked.

        // Checking the pool may fail for a while.
        test("Check pool: ");
        s = _checkPool(conn, (char**)urls, (int)(sizeof(urls)/sizeof(char*)));
        testCond(s == NATS_OK);
    }

    natsConnection_Close(conn);
    _waitForConnClosed(&arg);
    natsConnection_Destroy(conn);
    conn = NULL;

    // Restart s1
    s1Pid = _startServer("nats://127.0.0.1:4222", "-a 127.0.0.1 -p 4222 -cluster nats://127.0.0.1:6222 -cluster_name abc -routes nats://127.0.0.1:6223,nats://127.0.0.1:6224", true);
    if (s1Pid == NATS_INVALID_PID)
    {
        _stopServer(s2Pid);
        _stopServer(s3Pid);
    }
    CHECK_SERVER_STARTED(s1Pid);

    // We should have all 3 servers running now...
    test("Connect ok: ");
    s = natsConnection_Connect(&conn, opts);
    testCond(s == NATS_OK);

    {
        int     i;
        natsSrv *srvrs[3];

        test("Server pool size should be 3: ");
        natsMutex_Lock(conn->mu);
        s = (conn->srvPool->size == 3 ? NATS_OK : NATS_ERR);
        natsMutex_Unlock(conn->mu);
        testCond(s == NATS_OK);

        // Save references to servers from pool
        natsMutex_Lock(conn->mu);
        for (i=0; i<3; i++)
            srvrs[i] = conn->srvPool->srvrs[i];
        natsMutex_Unlock(conn->mu);

        for (i=0; (s == NATS_OK) && (i<9); i++)
        {
            natsMutex_Lock(conn->mu);
            natsSock_Shutdown(conn->sockCtx.fd);
            natsMutex_Unlock(conn->mu);

            test("Wait for reconnect: ");
            natsMutex_Lock(arg.m);
            while ((s != NATS_TIMEOUT) && !arg.reconnected)
                s = natsCondition_TimedWait(arg.c, arg.m, 2000);
            arg.reconnected = false;
            natsMutex_Unlock(arg.m);
            testCond(s == NATS_OK);
        }

        {
            int j;

            natsMutex_Lock(conn->mu);
            test("Server pool size should be 3: ");
            s = (conn->srvPool->size == 3 ? NATS_OK : NATS_ERR);
            natsMutex_Unlock(conn->mu);
            testCond(s == NATS_OK);

            test("Servers in pool have not been replaced: ");
            natsMutex_Lock(conn->mu);
            for (i=0; (s == NATS_OK) && (i<3); i++)
            {
                natsSrv *srv = conn->srvPool->srvrs[i];

                s = NATS_ERR;
                for (j=0; j<3; j++)
                {
                    if (srvrs[j] == srv)
                    {
                        s = NATS_OK;
                        break;
                    }
                }
            }
            natsMutex_Unlock(conn->mu);
            testCond(s == NATS_OK);
        }

        natsConnection_Close(conn);
        _waitForConnClosed(&arg);
    }

    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    _stopServer(s3Pid);
    _stopServer(s2Pid);
    _stopServer(s1Pid);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ReconnectJitter(void)
{
    natsStatus          s       = NATS_OK;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    int64_t             start   = 0;
    int64_t             dur     = 0;
    struct threadArg arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Default jitter values: ");
    natsMutex_Lock(opts->mu);
    s = (((opts->reconnectJitter == NATS_OPTS_DEFAULT_RECONNECT_JITTER)
            && (opts->reconnectJitterTLS == NATS_OPTS_DEFAULT_RECONNECT_JITTER_TLS)) ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(opts->mu);
    testCond(s == NATS_OK);

    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4222");
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetReconnectWait(opts, 50));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 500, 0));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));

    pid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    // Shutdown server
    _stopServer(pid);
    pid = NATS_INVALID_PID;

    // Wait for several iterations of failed attempts and make
    // sure that overall wait time is a bit more than just
    // the number of reconnect attempts.
    start = nats_Now();
    nats_Sleep(400);
	pid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
    CHECK_SERVER_STARTED(pid);

    test("Check jitter: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    if (s == NATS_OK)
        dur = nats_Now() - start;
    natsMutex_Unlock(arg.m);
    testCond((s == NATS_OK) && (dur >=500));

    natsConnection_Destroy(nc);
    nc = NULL;

    // Use a long reconnect wait
    s = natsOptions_SetReconnectWait(opts, 10*60*1000); // 10 minutes
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    // Cause a disconnect
    _stopServer(pid);
    pid = NATS_INVALID_PID;
    // Wait a bit for the reconnect loop to go into wait mode.
    nats_Sleep(50);
    pid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
    CHECK_SERVER_STARTED(pid);
    // Now close and expect the reconnect thread to return..
    natsConnection_Close(nc);

    test("Wait for closed: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Check reconnect thread done: ");
    natsConn_Lock(nc);
    s = (nc->reconnectThread == NULL ? NATS_OK : NATS_ERR);
    natsConn_Unlock(nc);
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _stopServer(pid);
    _destroyDefaultThreadArgs(&arg);
}

static int64_t
_customReconnectDelayCB(natsConnection *nc, int attempts, void *closure)
{
    struct threadArg *arg  = (struct threadArg*) closure;
    int64_t          delay = 0;

    natsMutex_Lock(arg->m);
    if (attempts != arg->control)
    {
        arg->status = NATS_ERR;
        natsCondition_Signal(arg->c);
    }
    else
    {
        arg->control++;
        if (attempts <= 4)
            delay = 100;
        else
            natsConnection_Close(nc);
    }
    natsMutex_Unlock(arg->m);
    return delay;
}

static void
test_CustomReconnectDelay(void)
{
    natsStatus          s       = NATS_OK;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    int64_t             start   = 0;
    int64_t             dur     = 0;
    struct threadArg arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
#if _WIN32
    IFOK(s, natsOptions_SetTimeout(opts, 100));
#endif
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetCustomReconnectDelay(opts, _customReconnectDelayCB, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    arg.control = 1;
    arg.status  = NATS_OK;

    pid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

	// Cause disconnect
    _stopServer(pid);
    pid = NATS_INVALID_PID;

	// We should be trying to reconnect 4 times
	start = nats_Now();

	// Wait on error or completion of test.
    test("Check custom delay cb: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed && (arg.status == NATS_OK))
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);
    IFOK(s, arg.status);
    if (s == NATS_OK)
        dur = nats_Now()-start;
    natsMutex_Unlock(arg.m);
#if _WIN32
    testCond((s == NATS_OK) && (dur <= 1000));
#else
    testCond((s == NATS_OK) && (dur <= 500));
#endif

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
}

static void
_lameDuckDiscoveredCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;
    natsStatus          s;
    char                **servers = NULL;
    int                 count     = 0;

    natsMutex_Lock(arg->m);
    s = natsConnection_GetDiscoveredServers(nc, &servers, &count);
    if (s == NATS_OK)
    {
        int i;

        if ((count != 1) || (strcmp(servers[0], "nats://127.0.0.1:1234") != 0))
            arg->status = NATS_ERR;

        for (i=0; i<count; i++)
            free(servers[i]);
        free(servers);
    }
    // Use this flag to indicate that discovered cb was invoked.
    arg->done = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
_lameDuckCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    // Use this flag to indicate that LDM cb was invoked.
    arg->disconnected = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
_lameDuckMockupServerThread(void *closure)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    struct threadArg    *arg = (struct threadArg*) closure;
    char                buffer[1024];
    const char          *ldm[] = {"INFO {\"ldm\":true}\r\n", "INFO {\"connect_urls\":[\"127.0.0.1:1234\"],\"ldm\":true}\r\n"};
    natsSockCtx         ctx;
    int                 i;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _startMockupServer(&sock, "127.0.0.1", "4222");
    natsMutex_Lock(arg->m);
    arg->status = s;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);

    for (i=0; (s == NATS_OK) && (i<2); i++)
    {
        if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
                || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
        {
            s = NATS_SYS_ERROR;
        }
        if (s == NATS_OK)
        {
            const char *info = "INFO {\"server_id\":\"foobar\"}\r\n";

            // Send INFO.
            s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
            if (s == NATS_OK)
            {
                memset(buffer, 0, sizeof(buffer));

                // Read connect and ping commands sent from the client
                s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
                IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
            }
            // Send PONG
            IFOK(s, natsSock_WriteFully(&ctx,_PONG_PROTO_, _PONG_PROTO_LEN_));
            if (s == NATS_OK)
            {
                // Wait a bit and then send a INFO with LDM
                nats_Sleep(100);
                s = natsSock_WriteFully(&ctx, ldm[i], (int) strlen(ldm[i]));
                // Wait for client to close
                if (s == NATS_OK)
                    natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            }
            natsSock_Close(ctx.fd);
        }
    }

    natsSock_Close(sock);
}

static void
test_LameDuckMode(void)
{
    natsStatus          s       = NATS_OK;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsThread          *t      = NULL;
    int                 i;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetDiscoveredServersCB(opts, _lameDuckDiscoveredCb, (void*) &arg));
    IFOK(s, natsOptions_SetLameDuckModeCB(opts, _lameDuckCb, (void*) &arg));
    // IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    // Set this to error, the mock server should set it to OK
    // if it can start successfully.
    arg.status = NATS_ERR;
    s = natsThread_Create(&t, _lameDuckMockupServerThread, (void*) &arg);
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }

    for (i=0; i<2; i++)
    {
        test("Connect: ");
        s = natsConnection_Connect(&nc, opts);
        testCond(s == NATS_OK);

        test("Lame duck callback invoked: ");
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.disconnected)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
        testCond(s == NATS_OK);

        if (i == 0)
        {
            test("Discovered not invoked: ");
            natsMutex_Lock(arg.m);
            while ((s != NATS_TIMEOUT) && !arg.done)
                s = natsCondition_TimedWait(arg.c, arg.m, 200);
            if ((arg.status == NATS_OK) && (s == NATS_TIMEOUT))
                s = NATS_OK;
            natsMutex_Unlock(arg.m);
            testCond(s == NATS_OK);
        }
        else
        {
            test("Discovered servers ok: ");
            natsMutex_Lock(arg.m);
            while ((s != NATS_TIMEOUT) && !arg.done)
                s = natsCondition_TimedWait(arg.c, arg.m, 2000);
            IFOK(s, arg.status);
            natsMutex_Unlock(arg.m);
            testCond(s == NATS_OK);
        }

        natsConnection_Destroy(nc);
        nc = NULL;
    }

    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
}

static void
test_Version(void)
{
    const char *str = NULL;

    test("Compatibility: ");
    testCond(nats_CheckCompatibility() == true);

    test("Version string: ");
    str = nats_GetVersion();
    testCond((str != NULL)
             && (strcmp(str, LIB_NATS_VERSION_STRING) == 0));

    test("Version number: ");
    testCond(nats_GetVersionNumber() == LIB_NATS_VERSION_NUMBER);
}

static void
test_VersionMatchesTag(void)
{
    natsStatus  s = NATS_OK;
    const char  *tag;

    tag = getenv("TRAVIS_TAG");
    if ((tag == NULL) || (tag[0] == '\0'))
    {
        test("Skipping test since no tag detected: ");
        testCond(true);
        return;
    }
    test("Check tag and version match: ");
    // We expect a tag of the form vX.Y.Z. If that's not the case,
    // we need someone to have a look. So fail if first letter is not
    // a `v`
    if (tag[0] != 'v')
        s = NATS_ERR;
    else
    {
        // Strip the `v` from the tag for the version comparison.
        s = (strcmp(nats_GetVersion(), tag+1) == 0 ? NATS_OK : NATS_ERR);
    }
    testCond(s == NATS_OK);
}

static void
_openCloseAndWaitMsgCB(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    nats_Sleep(300);
    natsMsg_Destroy(msg);
    natsMutex_Lock(arg->m);
    arg->results[0]++;
    natsMutex_Unlock(arg->m);
}

static void
_openCloseAndWaitConnClosedCB(natsConnection *nc, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->sum++;
    natsMutex_Unlock(arg->m);
}

static void
_openCloseAndWaitCloseFromThread(void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->status = nats_CloseAndWait(0);
    arg->done   = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
_openCloseAndWaitThread(void *closure)
{
    nats_Sleep(300);
    natsLib_Release();
}

static void
test_OpenCloseAndWait(void)
{
    natsStatus          s;
    natsConnection      *nc  = NULL;
    natsOptions         *opts= NULL;
    natsSubscription    *sub = NULL;
    natsPid             pid  = NATS_INVALID_PID;
    int                 i;
    natsThread          *t   = NULL;
    struct threadArg    arg;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    if (_createDefaultThreadArgsForCbTests(&arg) != NATS_OK)
        FAIL("Unable to setup test");

    // First close the library since it is opened in main
    test("Close to prepare for test: ");
    s = nats_CloseAndWait(0);
    testCond(s == NATS_OK);

    test("Open/Close in loop: ");
    for (i=0;i<2;i++)
    {
        s = nats_Open(-1);
        IFOK(s, natsOptions_Create(&opts));
        IFOK(s, natsOptions_SetClosedCB(opts, _openCloseAndWaitConnClosedCB, (void*)&arg));
        IFOK(s, natsConnection_Connect(&nc, opts));
        IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _openCloseAndWaitMsgCB, (void*)&arg));
        IFOK(s, natsConnection_PublishString(nc, "foo", "hello"));
        IFOK(s, natsConnection_Flush(nc));
        if (s == NATS_OK)
        {
            for (;;)
            {
                natsMutex_Lock(arg.m);
                if (arg.results[0] == (i+1))
                {
                    natsMutex_Unlock(arg.m);
                    break;
                }
                natsMutex_Unlock(arg.m);
                nats_Sleep(100);
            }

            natsSubscription_Destroy(sub);
            natsConnection_Destroy(nc);
            natsOptions_Destroy(opts);
            nats_CloseAndWait(0);
        }
    }
    testCond(s == NATS_OK);

    test("Check async cb count: ");
    natsMutex_Lock(arg.m);
    testCond(arg.sum == 2);
    natsMutex_Unlock(arg.m);

    test("Check msgs count: ");
    natsMutex_Lock(arg.m);
    testCond(arg.results[0] == 2);
    natsMutex_Unlock(arg.m);

    test("Close while not opened returns error: ");
    s = nats_CloseAndWait(0);
    testCond(s == NATS_NOT_INITIALIZED);

    // Re-open for rest of test
    nats_Open(-1);

    test("Check Close from thread returns error: ");
    s = natsThread_Create(&t, _openCloseAndWaitCloseFromThread, (void*)&arg);
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done)
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        s = (arg.status == NATS_ILLEGAL_STATE ? NATS_OK : NATS_ERR);
        natsMutex_Unlock(arg.m);

        natsThread_Join(t);
        natsThread_Destroy(t);
        t = NULL;
    }
    testCond(s == NATS_OK);

    test("No timeout: ");
    natsLib_Retain();
    s = natsThread_Create(&t, _openCloseAndWaitThread, NULL);
    IFOK(s, nats_CloseAndWait(0));
    testCond(s == NATS_OK);

    natsThread_Join(t);
    natsThread_Destroy(t);
    t = NULL;
    nats_Open(-1);

    test("Timeout: ");
    natsLib_Retain();
    s = natsThread_Create(&t, _openCloseAndWaitThread, NULL);
    IFOK(s, nats_CloseAndWait(100));
    testCond(s == NATS_TIMEOUT);

    // Now wait for thread to exit
    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
    _stopServer(pid);
}

static void
_testGetLastErrInThread(void *arg)
{
    natsStatus  getLastErrSts;
    natsOptions *opts = NULL;
    const char  *getLastErr = NULL;

    test("Check that new thread has get last err clear: ");
    getLastErr = nats_GetLastError(&getLastErrSts);
    testCond((getLastErr == NULL) && (getLastErrSts == NATS_OK));

    natsOptions_Destroy(opts);
}

static void
test_GetLastError(void)
{
    natsStatus  s, getLastErrSts;
    natsOptions *opts = NULL;
    const char  *getLastErr = NULL;
    natsThread  *t = NULL;
    char        stackBuf[256];
    FILE        *stackFile = NULL;

    stackBuf[0] = '\0';

    test("Check GetLastError returns proper status: ");
    s = natsOptions_SetAllowReconnect(NULL, false);

    getLastErr = nats_GetLastError(&getLastErrSts);

    testCond((s == getLastErrSts)
             && (getLastErr != NULL)
             && (strstr(getLastErr, "Invalid") != NULL));

    test("Check GetLastErrorStack with invalid args: ");
    s = nats_GetLastErrorStack(NULL, 10);
    if (s != NATS_OK)
        s = nats_GetLastErrorStack(stackBuf, 0);
    testCond(s == NATS_INVALID_ARG);

    test("Check GetLastErrorStack returns proper insufficient buffer: ");
    s = nats_GetLastErrorStack(stackBuf, 10);
    testCond(s == NATS_INSUFFICIENT_BUFFER);

    test("Check GetLastErrorStack: ");
    s = nats_GetLastErrorStack(stackBuf, sizeof(stackBuf));
    testCond((s == NATS_OK)
             && (strlen(stackBuf) > 0)
             && (strstr(stackBuf, "natsOptions_SetAllowReconnect") != NULL));

    test("Check PrintStack: ");
    stackBuf[0] = '\0';
    stackFile = fopen("stack.txt", "w");
    if (stackFile == NULL)
        FAIL("Unable to create a file for print stack test");

    s = NATS_OK;
    nats_PrintLastErrorStack(stackFile);
    fclose(stackFile);
    stackFile = fopen("stack.txt", "r");
    s = (fgets(stackBuf, sizeof(stackBuf), stackFile) == NULL ? NATS_ERR : NATS_OK);
    if ((s == NATS_OK)
        && ((strlen(stackBuf) == 0)
            || (strstr(stackBuf, "Invalid Argument") == NULL)))
    {
        s = NATS_ERR;
    }
    s = (fgets(stackBuf, sizeof(stackBuf), stackFile) == NULL ? NATS_ERR : NATS_OK);
    if (s == NATS_OK)
        s = (fgets(stackBuf, sizeof(stackBuf), stackFile) == NULL ? NATS_ERR : NATS_OK);
    if ((s == NATS_OK)
        && ((strlen(stackBuf) == 0)
            || (strstr(stackBuf, "natsOptions_SetAllowReconnect") == NULL)))
    {
        s = NATS_ERR;
    }
    testCond(s == NATS_OK);
    fclose(stackFile);
    remove("stack.txt");

    test("Check the error not cleared until next error occurs: ");
    s = natsOptions_Create(&opts);

    getLastErr = nats_GetLastError(&getLastErrSts);

    testCond((s == NATS_OK)
             && (getLastErrSts != NATS_OK)
             && (getLastErr != NULL)
             && (strstr(getLastErr, "Invalid") != NULL));

    s = natsThread_Create(&t, _testGetLastErrInThread, NULL);
    if (s == NATS_OK)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsOptions_Destroy(opts);

    nats_clearLastError();
    stackBuf[0] = '\0';

    test("Check stack not updated when asked: ");
    nats_doNotUpdateErrStack(true);
    s = natsConnection_Publish(NULL, NULL, NULL, 0);
    nats_GetLastErrorStack(stackBuf, sizeof(stackBuf));
    testCond((s != NATS_OK)
             && (stackBuf[0] == '\0'));

    test("Check call reentrant: ");
    nats_doNotUpdateErrStack(true);
    nats_doNotUpdateErrStack(false);
    s = natsConnection_Publish(NULL, NULL, NULL, 0);
    nats_GetLastErrorStack(stackBuf, sizeof(stackBuf));
    testCond((s != NATS_OK)
             && (stackBuf[0] == '\0'));

    nats_doNotUpdateErrStack(false);

    test("Check stack updates again: ");
    s = natsConnection_Publish(NULL, NULL, NULL, 0);
    nats_GetLastErrorStack(stackBuf, sizeof(stackBuf));
    testCond((s != NATS_OK)
             && (stackBuf[0] != '\0'));

    nats_clearLastError();
}

static void
test_StaleConnection(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;
    int                 i;
    const char          *stale_conn_err = "-ERR 'Stale Connection'\r\n";

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&(arg.opts)));
    IFOK(s, natsOptions_SetReconnectWait(arg.opts, 20));
    IFOK(s, natsOptions_SetReconnectJitter(arg.opts, 0, 0));
    IFOK(s, natsOptions_SetMaxReconnect(arg.opts, 100));
    IFOK(s, natsOptions_SetDisconnectedCB(arg.opts, _disconnectedCb, &arg));
    IFOK(s, natsOptions_SetReconnectedCB(arg.opts, _reconnectedCb, &arg));
    IFOK(s, natsOptions_SetClosedCB(arg.opts, _closedCb, &arg));
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    arg.control = 5;

    test("Behavior of connection on Stale Connection: ")

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    for (i = 0; (i < 2) && (s == NATS_OK); i++)
    {
        if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
                || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
        {
            s = NATS_SYS_ERROR;
        }
        if (s == NATS_OK)
        {
            char info[1024];

            strncpy(info,
                    "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n",
                    sizeof(info));

            // Send INFO.
            s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
            if (s == NATS_OK)
            {
                char buffer[1024];

                memset(buffer, 0, sizeof(buffer));

                // Read connect and ping commands sent from the client
                s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
                IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
            }
            // Send PONG
            IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));

            if ((s == NATS_OK) && (i == 0))
            {
                // Wait a tiny, and simulate a Stale Connection
                nats_Sleep(50);
                s = natsSock_WriteFully(&ctx, stale_conn_err,
                                        (int)strlen(stale_conn_err));

                // The client should try to reconnect. When getting the
                // disconnected callback, wait for the disconnect cb
                natsMutex_Lock(arg.m);
                while ((s != NATS_TIMEOUT) && !(arg.disconnected))
                    s = natsCondition_TimedWait(arg.c, arg.m, 5000);
                natsMutex_Unlock(arg.m);
            }
            else if (s == NATS_OK)
            {
                natsMutex_Lock(arg.m);
                // We should check on !arg.closed here, but unfortunately,
                // on Windows, the thread calling natsConnection_Close()
                // would be blocked waiting for the readLoop thread to
                // exit, which it would not because fd shutdown is not
                // causing a socket error if the server is not closing
                // its side.
                while ((s != NATS_TIMEOUT) && (arg.disconnects != 2))
                    s = natsCondition_TimedWait(arg.c, arg.m, 5000);
                natsMutex_Unlock(arg.m);
            }

            natsSock_Close(ctx.fd);
        }
    }
    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsMutex_Lock(arg.m);
    IFOK(s, arg.status);
    if (s == NATS_OK)
    {
        // Wait for closed CB
        while ((s != NATS_TIMEOUT) && !(arg.closed))
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        IFOK(s, arg.status);
    }
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_OK)
             && (arg.disconnects == 2)
             && (arg.reconnects == 1)
             && (arg.closed));

    _destroyDefaultThreadArgs(&arg);
}

static void
test_ServerErrorClosesConnection(void)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    natsThread          *t = NULL;
    struct threadArg    arg;
    natsSockCtx         ctx;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&(arg.opts)));
    IFOK(s, natsOptions_SetReconnectWait(arg.opts, 20));
    IFOK(s, natsOptions_SetReconnectJitter(arg.opts, 0, 0));
    IFOK(s, natsOptions_SetMaxReconnect(arg.opts, 100));
    IFOK(s, natsOptions_SetDisconnectedCB(arg.opts, _disconnectedCb, &arg));
    IFOK(s, natsOptions_SetReconnectedCB(arg.opts, _reconnectedCb, &arg));
    IFOK(s, natsOptions_SetClosedCB(arg.opts, _closedCb, &arg));
    if (s != NATS_OK)
        FAIL("@@ Unable to setup test!");

    arg.control = 6;
    arg.string  = "Any Error";

    test("Behavior of connection on Server Error: ")

    s = _startMockupServer(&sock, "localhost", "4222");

    // Start the thread that will try to connect to our server...
    IFOK(s, natsThread_Create(&t, _connectToMockupServer, (void*) &arg));

    if ((s == NATS_OK)
        && (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK)))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        strncpy(info,
                "INFO {\"server_id\":\"foobar\",\"version\":\"latest\",\"go\":\"latest\",\"host\":\"localhost\",\"port\":4222,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n",
                sizeof(info));

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            char buffer[1024];

            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));

        if (s == NATS_OK)
        {
            // Wait a tiny, and simulate an error sent by the server
            nats_Sleep(50);

            snprintf(info, sizeof(info), "-ERR '%s'\r\n", arg.string);
            s = natsSock_WriteFully(&ctx, info, (int)strlen(info));
        }

        // Wait for the client to be done.
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !(arg.closed))
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        natsMutex_Unlock(arg.m);

        natsSock_Close(ctx.fd);
    }
    natsSock_Close(sock);

    // Wait for the client to finish.
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsMutex_Lock(arg.m);
    if (s == NATS_OK)
    {
        // Wait for closed CB
        while ((s != NATS_TIMEOUT) && !(arg.closed))
            s = natsCondition_TimedWait(arg.c, arg.m, 5000);
        IFOK(s, arg.status);
    }
    natsMutex_Unlock(arg.m);

    testCond((s == NATS_ERR)
             && (arg.disconnects == 1)
             && (arg.reconnects == 0)
             && (arg.closed));

    _destroyDefaultThreadArgs(&arg);
}

static void
test_NoEcho(void)
{
    natsStatus          s;
    natsOptions         *opts = NULL;
    natsConnection      *conn = NULL;
    natsSubscription    *sub  = NULL;
    natsPid             pid   = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetNoEcho(opts, true));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    arg.control = 0;
    arg.string = "test";
    test("Setup: ");
    s = natsConnection_Connect(&conn, opts);
    IFOK(s, natsConnection_Subscribe(&sub, conn, "foo", _recvTestString, (void*)&arg));
    IFOK(s, natsConnection_PublishString(conn, "foo", arg.string));
    IFOK(s, natsConnection_Flush(conn));
    // repeat
    IFOK(s, natsConnection_Flush(conn));
    testCond(s == NATS_OK);

    test("NoEcho: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 500);
    natsMutex_Unlock(arg.m);
    // Message should not be received.
    testCond(s == NATS_TIMEOUT);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
_startMockupServerThread(void *closure)
{
    natsStatus          s = NATS_OK;
    natsSock            sock = NATS_SOCK_INVALID;
    struct threadArg    *arg = (struct threadArg*) closure;
    natsSockCtx         ctx;
    testCheckInfoCB     checkInfoCB = NULL;

    memset(&ctx, 0, sizeof(natsSockCtx));

    s = _startMockupServer(&sock, "localhost", "4222");
    natsMutex_Lock(arg->m);
    arg->status = s;
    natsCondition_Signal(arg->c);
    checkInfoCB = arg->checkInfoCB;
    natsMutex_Unlock(arg->m);

    if (((ctx.fd = accept(sock, NULL, NULL)) == NATS_SOCK_INVALID)
            || (natsSock_SetCommonTcpOptions(ctx.fd) != NATS_OK))
    {
        s = NATS_SYS_ERROR;
    }
    if (s == NATS_OK)
    {
        char info[1024];

        snprintf(info, sizeof(info), "%s", arg->string);

        // Send INFO.
        s = natsSock_WriteFully(&ctx, info, (int) strlen(info));
        if (s == NATS_OK)
        {
            char buffer[1024];

            memset(buffer, 0, sizeof(buffer));

            // Read connect and ping commands sent from the client
            s = natsSock_ReadLine(&ctx, buffer, sizeof(buffer));
            if ((s == NATS_OK) && (checkInfoCB != NULL))
                s = (*checkInfoCB)(buffer);
            IFOK(s, natsSock_ReadLine(&ctx, buffer, sizeof(buffer)));
        }
        // Send PONG
        IFOK(s, natsSock_WriteFully(&ctx, _PONG_PROTO_, _PONG_PROTO_LEN_));

        if (s == NATS_OK)
        {
            // Wait for client to tell us it is done
            natsMutex_Lock(arg->m);
            while ((s != NATS_TIMEOUT) && !(arg->done))
                s = natsCondition_TimedWait(arg->c, arg->m, 10000);
            natsMutex_Unlock(arg->m);
        }
        natsSock_Close(ctx.fd);
    }

    natsSock_Close(sock);
}

static void
test_NoEchoOldServer(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsOptions         *opts = NULL;
    natsThread          *t    = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetNoEcho(opts, true));
    if (s == NATS_OK)
    {
        // Set this to error, the mock server should set it to OK
        // if it can start successfully.
        arg.status = NATS_ERR;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("NoEcho with old server: ");
    s = natsConnection_Connect(&conn, opts);
    testCond(s == NATS_NO_SERVER_SUPPORT);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsOptions_Destroy(opts);
    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_DrainSub(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsSubscription    *sub= NULL;
    natsSubscription    *sub2 = NULL;
    natsSubscription    *sub3 = NULL;
    natsOptions         *opts = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    arg.control = 8;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect and create subscriptions: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsConnection_SubscribeSync(&sub2, nc, "foo"));
    IFOK(s, natsConnection_SubscribeSync(&sub3, nc, "foo"));
    IFOK(s, natsSubscription_AutoUnsubscribe(sub3, 2));
    testCond(s == NATS_OK);

    test("WaitForDrainCompletion returns invalid arg: ");
    s = natsSubscription_WaitForDrainCompletion(NULL, 2000);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("WaitForDrainCompletion returns illegal state: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 2000);
    testCond(s == NATS_ILLEGAL_STATE);
    nats_clearLastError();

    test("Send 2 messages: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Call Drain on subscription: ");
    // Pass 0 or negative value to represent "for ever" timeout.
    s = natsSubscription_DrainTimeout(sub, -1);
    testCond(s == NATS_OK);

    test("Call Drain a second time is ok: ");
    s = natsSubscription_Drain(sub);
    testCond(s == NATS_OK);

    test("Drain sync subs: ");
    s = natsSubscription_Drain(sub2);
    IFOK(s, natsSubscription_Drain(sub3));
    testCond(s == NATS_OK);

    test("Wait for Drain times out: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 10);
    if (s == NATS_TIMEOUT)
        s = natsSubscription_WaitForDrainCompletion(sub2, 10);
    testCond(s == NATS_TIMEOUT);
    nats_clearLastError();

    test("Send 1 more message: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    testCond(s == NATS_OK);

    // Unblock the callback.
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    test("Wait for Drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub, -1);
    testCond(s == NATS_OK);

    // Wait a bit and make sure that we did not receive the 3rd msg
    test("Third message not received: ");
    nats_Sleep(100);
    natsMutex_Lock(arg.m);
    if ((s == NATS_OK) && (arg.sum != 2))
        s = NATS_ERR;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Drain on closed sub fails: ");
    s = natsSubscription_Drain(sub);
    testCond(s == NATS_INVALID_SUBSCRIPTION);
    nats_clearLastError();

    test("Consume sync messages: ");
    {
        natsMsg *msg = NULL;
        int     i;

        s = NATS_OK;
        for (i=0; (s == NATS_OK) && (i<2); i++)
        {
            s = natsSubscription_NextMsg(&msg, sub2, 2000);
            natsMsg_Destroy(msg);
            msg = NULL;
        }
        for (i=0; (s == NATS_OK) && (i<2); i++)
        {
            s = natsSubscription_NextMsg(&msg, sub3, 2000);
            natsMsg_Destroy(msg);
            msg = NULL;
        }
    }
    testCond(s == NATS_OK);

    test("Wait for drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub2, 1000);
    IFOK(s, natsSubscription_WaitForDrainCompletion(sub3, 1000));
    testCond(s == NATS_OK);

    // Repeat async test with auto-unsubscribe
    natsSubscription_Destroy(sub);
    sub = NULL;
    natsMutex_Lock(arg.m);
    arg.sum    = 0;
    arg.closed = false;
    natsMutex_Unlock(arg.m);

    test("Async sub with auto-unsub: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*)&arg);
    IFOK(s, natsSubscription_AutoUnsubscribe(sub, 2));
    testCond(s == NATS_OK);

    test("Send 2 messages: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Check drain status with invalid arg: ");
    s = natsSubscription_DrainCompletionStatus(NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Check drain status fails: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_ILLEGAL_STATE);

    test("Call Drain on subscription: ");
    s = natsSubscription_Drain(sub);
    testCond(s == NATS_OK);

    test("Send 1 more message: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    testCond(s == NATS_OK);

    // Unblock the callback.
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    test("Wait for Drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub, -1);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_OK);

    // Wait a bit and make sure that we did not receive the 3rd msg
    test("Third message not received: ");
    nats_Sleep(100);
    natsMutex_Lock(arg.m);
    s = (arg.sum == 2 ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    // Close connection
    natsConnection_Close(nc);

    test("Drain on closed conn fails: ");
    s = natsSubscription_Drain(sub);
    if (s == NATS_CONNECTION_CLOSED)
        s = natsSubscription_Drain(sub2);
    if (s == NATS_CONNECTION_CLOSED)
        s = natsSubscription_Drain(sub3);
    testCond(s == NATS_CONNECTION_CLOSED);

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsSubscription_Destroy(sub2);
    sub2 = NULL;
    natsSubscription_Destroy(sub3);
    sub3 = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;

    natsMutex_Lock(arg.m);
    arg.sum    = 0;
    arg.closed = false;
    natsMutex_Unlock(arg.m);

    test("Connect and create sub: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Send 2 messages: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Disconnect: ");
    _stopServer(pid);
    testCond(s == NATS_OK);

    test("Wait for disconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Call Drain on subscriptions: ");
    s = natsSubscription_DrainTimeout(sub, 500);
    testCond(s == NATS_OK);

    // Unblock the callback.
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    test("Wait for Drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub, -1);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    // Since the flush has timed-out, this is what the status will be.
    testCond(s == NATS_TIMEOUT);
    s = NATS_OK;

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;
    natsOptions_Destroy(opts);
    opts = NULL;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    natsMutex_Lock(arg.m);
    arg.sum    = 0;
    arg.closed = false;
    natsMutex_Unlock(arg.m);

    test("Create options for global msg delivery: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_UseGlobalMessageDelivery(opts, true));
    testCond(s == NATS_OK);

    test("Connect and create sub: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*)&arg));
    IFOK(s, natsConnection_Subscribe(&sub2, nc, "foo", _recvTestString, (void*)&arg));
    IFOK(s, natsSubscription_AutoUnsubscribe(sub, 2));
    testCond(s == NATS_OK);

    test("Send 2 messages: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Call Drain on subscriptions: ");
    s = natsSubscription_Drain(sub);
    IFOK(s, natsSubscription_Drain(sub2));
    testCond(s == NATS_OK);

    // Unblock the callback.
    nats_Sleep(250);
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    test("Wait for Drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub, -1);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsSubscription_Destroy(sub2);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
_msgCBForDrainSubTest(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMsg_Destroy(msg);
    natsMutex_Lock(arg->m);
    if (++arg->sum == 1)
    {
        // Signal that we got the message
        natsCondition_Signal(arg->c);

        // Wait for main thread to switch to drain mode
        while (!arg->done)
            natsCondition_Wait(arg->c, arg->m);

        // Report unsubscribe status
        arg->status = natsSubscription_Unsubscribe(sub);
    }
    natsMutex_Unlock(arg->m);
}

static void
_drainSubCompleteCB(void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    if (arg->sum == 1)
    {
        arg->closed = true;
        natsCondition_Signal(arg->c);
    }
    natsMutex_Unlock(arg->m);
}

static void
test_DrainSubStops(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsSubscription    *sub= NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    arg;
    int                 i;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect and create subscriptions: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _msgCBForDrainSubTest, (void*) &arg));
    IFOK(s, natsSubscription_SetOnCompleteCB(sub, _drainSubCompleteCB, (void*) &arg))
    testCond(s == NATS_OK);

    test("Send 10 messages: ");
    for (i=0; (s == NATS_OK) && (i<10); i++)
        s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Wait for 1st message to be received: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum != 1))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Drain subscription: ");
    s = natsSubscription_Drain(sub);
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Wait for drain completion: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 0);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    test("Check that drain stopped on unsubscribe: ");
    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    // Get the status from unsubscribe in the callback
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    sub = NULL;

    natsMutex_Lock(arg.m);
    arg.done    = false;
    arg.closed  = false;
    arg.sum     = 0;
    natsMutex_Unlock(arg.m);

    test("Create subscription: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _msgCBForDrainSubTest, (void*) &arg);
    IFOK(s, natsSubscription_SetOnCompleteCB(sub, _drainSubCompleteCB, (void*) &arg))
    testCond(s == NATS_OK);

    test("Send 10 messages: ");
    for (i=0; (s == NATS_OK) && (i<10); i++)
        s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Wait for 1st message to be received: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum != 1))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Drain connection: ");
    s = natsConnection_Drain(nc);
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Wait for Drain to complete: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 0);
    testCond(s == NATS_OK);

    test("Check that drain stopped on unsubscribe: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    // Get the status from unsubscribe in the callback
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
test_DrainSubRaceOnAutoUnsub(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsSubscription    *sub= NULL;
    natsPid             pid = NATS_INVALID_PID;
    int                 i;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    testCond(s == NATS_OK);

    testDrainAutoUnsubRace = true;

    test("Drain with auto-unsub race: ");
    for (i=0; (s == NATS_OK) && (i<500); i++)
    {
        s = natsConnection_Subscribe(&sub, nc, "foo", _dummyMsgHandler, NULL);
        IFOK(s, natsSubscription_AutoUnsubscribe(sub, 1));
        IFOK(s, natsConnection_PublishString(nc, "foo", "msg"));
        nats_Sleep(1);
        if (s == NATS_OK)
        {
            s = natsSubscription_Drain(sub);
            // Here, it is possible that the subscription is already
            // invalid. In which case, don't attempt to wait for completion.
            if (s == NATS_INVALID_SUBSCRIPTION)
            {
                s = NATS_OK;
                nats_clearLastError();
            }
            else
            {
                IFOK(s, natsSubscription_WaitForDrainCompletion(sub, -1));
                IFOK(s, natsSubscription_DrainCompletionStatus(sub));
            }
        }
        natsSubscription_Destroy(sub);
        sub = NULL;
    }
    testCond(s == NATS_OK);

    testDrainAutoUnsubRace = false;

    natsConnection_Destroy(nc);
    _stopServer(pid);
}

static void
test_DrainSubNotResentOnReconnect(void)
{
    natsStatus          s;
    natsConnection      *nc   = NULL;
    natsSubscription    *sub  = NULL;
    natsOptions         *opts = NULL;
    natsPid             pid   = NATS_INVALID_PID;
    natsStatistics      stats;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetMaxReconnect(opts, -1));
    IFOK(s, natsOptions_SetReconnectWait(opts, 10));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    natsMutex_Lock(arg.m);
    arg.control = 8;
    natsMutex_Unlock(arg.m);

    test("Connect and create subscription: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    testCond(s == NATS_OK);

    test("Send 1 message: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Wait for message to be received: ");
    nats_Sleep(150);
    testCond(s == NATS_OK);

    test("Drain subscription: ");
    s = natsSubscription_Drain(sub);
    testCond(s == NATS_OK);

    test("Disconnect: ");
    nats_Sleep(250);
    _stopServer(pid);
    testCond(s == NATS_OK);

    test("Restart server: ");
    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(s == NATS_OK);

    test("Wait for reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Release cb: ");
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Wait for drain completion: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 0);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_OK);

    test("Send new message: ");
    s = natsConnection_PublishString(nc, "foo", "msg");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Msg not received by connection: ");
    s = natsConnection_GetStats(nc, &stats);
    IFOK(s, (stats.inMsgs == 1 ? NATS_OK : NATS_ERR));
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
    _stopServer(pid);
}

static void
_drainConnBarSub(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    args->results[1]++;
    if (args->results[1] == args->results[0])
    {
        args->done = true;
        natsCondition_Broadcast(args->c);
    }
    natsMutex_Unlock(args->m);
    natsMsg_Destroy(msg);
}

static void
_drainConnFooSub(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    nats_Sleep(10);
    natsMutex_Lock(args->m);
    args->sum++;
    if (args->status == NATS_OK)
        args->status = natsConnection_PublishString(nc, natsMsg_GetReply(msg), "Stop bugging me");
    natsMutex_Unlock(args->m);
    natsMsg_Destroy(msg);
}

static void
_drainConnErrHandler(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;
    const char       *lastError = NULL;
    natsStatus       s = NATS_OK;

    natsMutex_Lock(args->m);
    // Since error handler is async, there is no guarantee that
    // natsConnection_GetLastError() returns the error we expect.
    // Only check if the error matches `err`.
    if (err == NATS_TIMEOUT)
    {
        s = natsConnection_GetLastError(nc, &lastError);
        if ((s != NATS_TIMEOUT)
                || (strstr(lastError, args->string) != NULL))
        {
            args->done = true;
            natsCondition_Broadcast(args->c);
        }
    }
    natsMutex_Unlock(args->m);
}

static void
test_DrainConn(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsSubscription    *sub    = NULL;
    natsConnection      *nc2    = NULL;
    natsSubscription    *sub2   = NULL;
    natsSubscription    *sub3   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    int                 expected= 50;
    int64_t             start   = 0;
    int                 i;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*)&arg));
    IFOK(s, natsOptions_SetErrorHandler(opts, _drainConnErrHandler, (void*) &arg));
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        natsOptions_Destroy(opts);
        FAIL("Unable to setup test");
    }

    arg.results[0] = expected;
    arg.string = "Drain error";

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Drain with invalid NULL: ");
    s = natsConnection_Drain(NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Drain with no sub/pub ok: ");
    s = natsConnection_Drain(nc);
    testCond(s == NATS_OK);

    test("Closed CB invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    arg.closed = false;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("No async error reported: ");
    natsMutex_Lock(arg.m);
    s = (arg.done == false ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_ConnectTo(&nc2, "nats://127.0.0.1:4222"));
    testCond(s == NATS_OK);

    test("Create listener for responses on bar: ");
    s = natsConnection_Subscribe(&sub2, nc2, "bar", _drainConnBarSub, (void*) &arg);
    testCond(s == NATS_OK);

    test("Create slow consumer for responder: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _drainConnFooSub, (void*) &arg);
    testCond(s == NATS_OK);

    test("Send messages: ");
    for (i=0; (s == NATS_OK) && (i<expected); i++)
        s = natsConnection_PublishRequestString(nc, "foo", "bar", "Slow Slow");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Drain connection: ");
    start = nats_Now();
    // 0 or Negative means "wait for ever".
    s = natsConnection_DrainTimeout(nc, -1);
    testCond(s == NATS_OK);

    test("Check IsDraining: ");
    s = (natsConnection_IsDraining(nc) ? NATS_OK : NATS_ERR);
    testCond(s == NATS_OK);

    test("Second drain ok: ");
    s = natsConnection_Drain(nc);
    testCond(s == NATS_OK);

    test("Cannot create new subs: ");
    s = natsConnection_Subscribe(&sub3, nc, "foo", _dummyMsgHandler, NULL);
    testCond(s == NATS_DRAINING);
    nats_clearLastError();

    test("Publish should be ok: ");
    s = natsConnection_PublishString(nc, "baz", "should work");
    testCond(s == NATS_OK);

    test("Closed CB should be invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Drain took as expected: ");
    s = ((nats_Now() - start) >= 10*expected ? NATS_OK : NATS_ERR);
    testCond(s == NATS_OK);

    test("Received all messages: ");
    natsMutex_Lock(arg.m);
    s = ((arg.sum == expected) ? NATS_OK : NATS_ERR);
    if (s == NATS_OK)
        s = arg.status;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("All responses received: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.done)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    if ((s == NATS_OK) && (arg.results[1] != expected))
        s = NATS_ERR;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Check sub drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_OK);

    test("Check IsDraining: ");
    s = (natsConnection_IsDraining(nc) ? NATS_ERR : NATS_OK);
    testCond(s == NATS_OK);

    test("Drain after closed should fail: ");
    s = natsConnection_DrainTimeout(nc, 1);
    testCond(s == NATS_CONNECTION_CLOSED);
    nats_clearLastError();

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;

    natsMutex_Lock(arg.m);
    arg.done   = false;
    arg.sum    = 0;
    arg.string = "timeout";
    natsMutex_Unlock(arg.m);

    test("Connect and subscribe: ");
    s = natsConnection_Connect(&nc, opts);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _drainConnFooSub, (void*) &arg));
    testCond(s == NATS_OK);

    test("Publish: ");
    for (i=0;(s==NATS_OK) && i<25;i++)
        s = natsConnection_PublishString(nc, "foo", "hello");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Drain timeout: ");
    s = natsConnection_DrainTimeout(nc, 10);
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && !arg.done)
            s = natsCondition_TimedWait(arg.c, arg.m, 1000);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    test("Wait for subscription to drain: ");
    s = natsSubscription_WaitForDrainCompletion(sub, -1);
    testCond(s == NATS_OK);

    test("Check sub drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    // Since the connection drain timed-out, we should report as a timeout,
    // not as the connection closed.
    testCond(s == NATS_TIMEOUT);

    natsSubscription_Destroy(sub);
    sub = NULL;
    natsSubscription_Destroy(sub2);
    natsSubscription_Destroy(sub3);
    natsConnection_Destroy(nc);
    nc = NULL;
    natsConnection_Destroy(nc2);
    nc2 = NULL;
    natsOptions_Destroy(opts);

    natsMutex_Lock(arg.m);
    arg.closed = false;
    arg.sum    = 0;
    arg.control= 8;
    natsMutex_Unlock(arg.m);

    test("Connect and create sub: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_Subscribe(&sub, nc, "foo", _recvTestString, (void*) &arg));
    IFOK(s, natsConnection_ConnectTo(&nc2, NATS_DEFAULT_URL));
    testCond(s == NATS_OK);

    test("Send messages: ");
    s = natsConnection_PublishString(nc, "foo", "msg1");
    IFOK(s, natsConnection_PublishString(nc, "foo", "msg2"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Drain: ");
    s = natsConnection_DrainTimeout(nc, 10000);
    testCond(s == NATS_OK);

    test("Drain sub directly should fail: ");
    s = natsSubscription_Drain(sub);
    testCond(s == NATS_DRAINING);
    nats_clearLastError();
    s = NATS_OK;

    test("Disconnect: ");
    _stopServer(pid);
    testCond(s == NATS_OK);

    nats_Sleep(100);

    test("Drain while disconnected fails: ");
    s = natsConnection_Drain(nc2);
    testCond(s == NATS_ILLEGAL_STATE);
    nats_clearLastError();
    s = NATS_OK;

    test("Release cb: ");
    natsMutex_Lock(arg.m);
    arg.closed = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Wait for completion: ");
    s = natsSubscription_WaitForDrainCompletion(sub, 1000);
    testCond(s == NATS_OK);

    test("Check drain status: ");
    s = natsSubscription_DrainCompletionStatus(sub);
    testCond(s == NATS_CONNECTION_CLOSED);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsConnection_Destroy(nc2);

    // Since the drain timed-out and closed the connection,
    // the subscription will be closed but there is no guarantee
    // that the callback is not in the middle of execution at that
    // time. So to avoid valgrind reports, sleep a bit before
    // destroying sub's closure.
    nats_Sleep(100);
    _destroyDefaultThreadArgs(&arg);
}

static void
_noDoubleCloseCb(natsConnection *nc, void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->sum++;
    arg->closed = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
_noDoubleCbSubCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    nats_Sleep(200);
    natsMsg_Destroy(msg);
}

static void
test_NoDoubleConnClosedOnDrain(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsSubscription    *sub    = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetClosedCB(opts, _noDoubleCloseCb, (void*)&arg));
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        natsOptions_Destroy(opts);
        FAIL("Unable to setup test");
    }

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Create sub: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _noDoubleCbSubCb, (void*)&arg);
    testCond(s == NATS_OK);

    test("Publish msg: ");
    s = natsConnection_PublishString(nc, "foo", "hello");
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Drain: ");
    s = natsConnection_Drain(nc);
    testCond(s == NATS_OK);

    nats_Sleep(200);
    test("Closing: ");
    natsConnection_Close(nc);
    testCond(s == NATS_OK);

    test("Wait for close CB: ");
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    // Now wait for connection close and make sure it was invoked once.
    test("Check closeCb invoked once: ")
    nats_Sleep(300);
    natsMutex_Lock(arg.m);
    s = (arg.sum == 1 ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
    _stopServer(pid);
}

static void
test_GetClientID(void)
{
    natsStatus          s;
    natsPid             pid1    = NATS_INVALID_PID;
    natsPid             pid2    = NATS_INVALID_PID;
    natsConnection      *nc1    = NULL;
    natsConnection      *nc2    = NULL;
    natsOptions         *opts   = NULL;
    uint64_t            cid     = 0;
    uint64_t            newcid  = 0;
    natsThread          *t      = NULL;
    struct threadArg    arg;

    if (!serverVersionAtLeast(1,2,0))
    {
        char txt[200];

        snprintf(txt, sizeof(txt), "Skipping since requires server version of at least 1.2.0, got %s: ", serverVersion);
        test(txt);
        testCond(true);
        return;
    }
    pid1 = _startServer("nats://127.0.0.1:4222", "-cluster nats://127.0.0.1:6222 -cluster_name abc", true);
    CHECK_SERVER_STARTED(pid1);

    test("Create nc1: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetDiscoveredServersCB(opts, _discoveredServersCb, (void*)&arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*)&arg));
    IFOK(s, natsConnection_Connect(&nc1, opts));
    testCond(s == NATS_OK);

    test("GetClientID for nc1: ");
    s = natsConnection_GetClientID(nc1, &cid);
    testCond((s == NATS_OK) && (cid != 0));

    test("Wait for discovered callback: ");
    pid2 = _startServer("nats://127.0.0.1:4223", "-p 4223 -cluster nats://127.0.0.1:6223 -cluster_name abc -routes nats://127.0.0.1:6222", true);
    CHECK_SERVER_STARTED(pid2);

    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum != 1))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    s = (arg.sum == 1 ? NATS_OK: NATS_ERR);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Check CID same: ");
    s = natsConnection_GetClientID(nc1, &newcid);
    testCond((s == NATS_OK) && (newcid == cid));

    test("Connect to server 2: ");
    s = natsConnection_ConnectTo(&nc2, "nats://127.0.0.1:4223");
    testCond(s == NATS_OK);

    test("Stop server 1: ");
    _stopServer(pid1);
    testCond(s == NATS_OK);

    test("Wait for nc1 to reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 4000);
    s = (arg.reconnected ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Check CID is different: ");
    s = natsConnection_GetClientID(nc1, &newcid);
    testCond((s == NATS_OK) && (newcid != cid));

    // Stop clients and remaining server
    natsConnection_Destroy(nc1);
    natsConnection_Destroy(nc2);
    natsOptions_Destroy(opts);
    _stopServer(pid2);

    // Now have dummy server that returns no CID and check we get expected error.
    nc1 = NULL;
    arg.status = NATS_ERR;
    arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
    s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        s = arg.status;
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("CID not supported: ");
    s = natsConnection_ConnectTo(&nc1, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_GetClientID(nc1, &cid));
    testCond((s == NATS_NO_SERVER_SUPPORT) && (cid == 0));

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Destroy(nc1);

    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_GetClientIP(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    char                *ip       = NULL;
    natsThread          *t    = NULL;
    struct threadArg    arg;

    test("Check server version: ");
    if (!serverVersionAtLeast(2,1,6))
    {
        char txt[200];

        snprintf(txt, sizeof(txt), "Skipping since requires server version of at least 2.1.6, got %s: ", serverVersion);
        test(txt);
        testCond(true);
        return;
    }
    testCond(true);

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Get client IP - no conn: ");
    s = natsConnection_GetClientIP(NULL, &ip);
    testCond(s == NATS_INVALID_ARG);

    test("Get client IP - no ip loc: ");
    s = natsConnection_GetClientIP(nc, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Get client IP: ");
    s = natsConnection_GetClientIP(nc, &ip);
    testCond((s == NATS_OK) && (strcmp(ip, "127.0.0.1")==0));
    free(ip);
    ip = NULL;

    natsConnection_Close(nc);
    test("Get client IP after conn closed: ");
    s = natsConnection_GetClientIP(nc, &ip);
    testCond((s == NATS_CONNECTION_CLOSED) && (ip == NULL));

    natsConnection_Destroy(nc);
    nc = NULL;

    _stopServer(serverPid);

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        // Set this to error, the mock server should set it to OK
        // if it can start successfully.
        arg.status = NATS_ERR;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Get client IP with old server: ");
    s = natsConnection_GetClientIP(nc, &ip);
    testCond ((s == NATS_NO_SERVER_SUPPORT) && (ip == NULL));

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Close(nc);
    natsConnection_Destroy(nc);

    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_GetRTT(void)
{
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    int64_t             rtt       = 0;

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetReconnectWait(opts, 10));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    test("Get RTT - no conn: ");
    s = natsConnection_GetRTT(NULL, &rtt);
    testCond(s == NATS_INVALID_ARG);

    test("Get RTT - no rtt loc: ");
    s = natsConnection_GetRTT(nc, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Get RTT: ");
    s = natsConnection_GetRTT(nc, &rtt);
    // Check that it is below 500ms...
    testCond((s == NATS_OK) && (rtt/1000000 <= 500));

    _stopServer(serverPid);

    test("Get RTT while not connected: ");
    s = natsConnection_GetRTT(nc, &rtt);
    testCond(s == NATS_CONNECTION_DISCONNECTED);

    natsConnection_Close(nc);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
}

static void
test_GetLocalIPAndPort(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    char                *ip     = NULL;
    int                 port    = 0;
    struct threadArg    arg;

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    test("Get Local IP and Port - no conn: ");
    s = natsConnection_GetLocalIPAndPort(NULL, &ip, &port);
    testCond(s == NATS_INVALID_ARG);

    test("Get Local IP and Port - no ip loc: ");
    s = natsConnection_GetLocalIPAndPort(nc, NULL, &port);
    testCond(s == NATS_INVALID_ARG);

    test("Get Local IP and Port - no port loc: ");
    s = natsConnection_GetLocalIPAndPort(nc, &ip, NULL);
    testCond(s == NATS_INVALID_ARG);

    nats_clearLastError();
    test("Get Local IP and Port: ");
    s = natsConnection_GetLocalIPAndPort(nc, &ip, &port);
    testCond((s == NATS_OK)
                && ((ip != NULL) && (strcmp(ip, "127.0.0.1") == 0))
                && (port != 0));
    free(ip);

    test("Wait for disconnect: ");
    s = NATS_OK;
    _stopServer(pid);
    natsMutex_Lock(arg.m);
    while ((s == NATS_OK) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Get Local IP and Port while disconnected: ");
    s = natsConnection_GetLocalIPAndPort(nc, &ip, &port);
    testCond(s == NATS_CONNECTION_DISCONNECTED);
    nats_clearLastError();

    // Close connection
    natsConnection_Close(nc);
    test("Get Local IP and Port with closed connection: ");
    s = natsConnection_GetLocalIPAndPort(nc, &ip, &port);
    testCond(s == NATS_CONNECTION_CLOSED);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    _destroyDefaultThreadArgs(&arg);
}

static natsStatus
_userJWTCB(char **userJWT, char **customErrTxt, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    if (closure != NULL)
    {
        bool done = true;

        natsMutex_Lock(arg->m);
        if (arg->string != NULL)
            *customErrTxt = strdup(arg->string);
        else if (arg->nc != NULL)
            natsConnection_Destroy(arg->nc);
        else
            done = false;
        natsMutex_Unlock(arg->m);

        if (done)
        {
            if (*customErrTxt != NULL)
                return NATS_ERR;
            return NATS_OK;
        }
    }

    *userJWT = strdup("some user jwt");

    return NATS_OK;
}

static natsStatus
_sigCB(char **customErrTxt, unsigned char **psig, int *sigLen, const char* nonce, void *closure)
{
    const unsigned char correctSign[] = {
            155, 157,   8, 183,  93, 154,  78,   7,
            219,  39,  11,  16, 134, 231,  46, 142,
            168,  87, 110, 202, 187, 180, 179,  62,
             49, 255, 225,  74,  48,  80, 176, 111,
            248, 162, 121, 188, 203, 101, 100, 195,
            162,  70, 213, 182, 220,  14,  71, 113,
             93, 239, 141, 131,  66, 190, 237, 127,
            104, 191, 138, 217, 227,   1,  92,  14,
    };
    unsigned char *sig = NULL;

    if (closure != NULL)
    {
        struct threadArg *arg = (struct threadArg*) closure;
        bool             done = true;

        natsMutex_Lock(arg->m);
        if (arg->string != NULL)
            *customErrTxt = strdup(arg->string);
        else if (arg->nc != NULL)
            natsConnection_Destroy(arg->nc);
        else
            done = false;
        natsMutex_Unlock(arg->m);

        if (done)
        {
            if (*customErrTxt != NULL)
                return NATS_ERR;

            return NATS_OK;
        }
    }

    sig = malloc(NATS_CRYPTO_SIGN_BYTES);
    memcpy(sig, correctSign, NATS_CRYPTO_SIGN_BYTES);
    *psig = sig;
    if (sigLen != NULL)
        *sigLen = NATS_CRYPTO_SIGN_BYTES;

    return NATS_OK;
}

static natsStatus
_checkJWTAndSigCB(char *buffer)
{
    // UserJWT callback should have returned this...
    if (strstr(buffer, "some user jwt") == NULL)
        return NATS_ERR;

    // The server is sending the nonce "nonce" and we
    // use a seed that should have produced a signature
    // that converted to base64 should be:
    if (strstr(buffer, "m50It12aTgfbJwsQhucujqhXbsq7tLM-Mf_hSjBQsG_4onm8y2Vkw6JG1bbcDkdxXe-Ng0K-7X9ov4rZ4wFcDg") == NULL)
        return NATS_ERR;

    return NATS_OK;
}

static void
test_UserCredsCallbacks(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsOptions         *opts2  = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    natsThread          *t      = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    if (s != NATS_OK)
        FAIL("Unable to create options for test UserCredsCallbacks");

    test("Invalid arg 1: ");
    s = natsOptions_SetUserCredentialsCallbacks(NULL, _dummyUserJWTCb, NULL, _dummySigCb, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Invalid arg 2: ");
    s = natsOptions_SetUserCredentialsCallbacks(opts, NULL, NULL, _dummySigCb, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Clone: ");
    s = natsOptions_SetUserCredentialsCallbacks(opts, _dummyUserJWTCb, (void*) 1, _dummySigCb, (void*) 2);
    if (s == NATS_OK)
    {
        opts2 = natsOptions_clone(opts);
        if (opts2 == NULL)
            s = NATS_NO_MEMORY;
    }
    IFOK(s, natsOptions_SetUserCredentialsCallbacks(opts, NULL, NULL, NULL, NULL));
    testCond((s == NATS_OK)
                && (opts2->userJWTHandler == _dummyUserJWTCb)
                && (opts2->userJWTClosure == (void*) 1)
                && (opts2->sigHandler == _dummySigCb)
                && (opts2->sigClosure == (void*) 2));

    natsOptions_Destroy(opts2);

    // We first check that we get error when callbacks do return error.
    // For that part, we don't need that the server sends the nonce.

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    // Create a connection. The user JWT callback is going to return
    // an error, ensure connection fails.
    test("UserJWTCB returns error: ");
    natsMutex_Lock(arg.m);
    arg.string = "some jwt error";
    arg.nc = NULL;
    natsMutex_Unlock(arg.m);
    s = natsOptions_SetUserCredentialsCallbacks(opts, _userJWTCB, (void*) &arg, _sigCB, NULL);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "some jwt error") != NULL));

    s = NATS_OK;
    nats_clearLastError();
    test("SignatureCB returns error: ");
    natsMutex_Lock(arg.m);
    arg.string = "some sig error";
    arg.nc = NULL;
    natsMutex_Unlock(arg.m);
    s = natsOptions_SetUserCredentialsCallbacks(opts, _userJWTCB, NULL, _sigCB, (void*) &arg);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "some sig error") != NULL));

    s = NATS_OK;
    nats_clearLastError();
    test("UserJWTCB destroys connection: ");
    natsMutex_Lock(arg.m);
    arg.string = NULL;
    arg.nc = NULL;
    arg.closed = false;
    natsMutex_Unlock(arg.m);
    s = natsOptions_SetUserCredentialsCallbacks(opts, _userJWTCB, (void*) &arg, _sigCB, NULL);
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        arg.nc = nc;
        natsMutex_Unlock(arg.m);

        _stopServer(pid);
        pid = _startServer("nats://127.0.0.1:4222", NULL, true);
        CHECK_SERVER_STARTED(pid);
    }
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (!arg.closed))
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    nc = NULL;

    s = NATS_OK;
    nats_clearLastError();
    test("SigCB destroys connection: ");
    natsMutex_Lock(arg.m);
    arg.string = NULL;
    arg.nc = NULL;
    arg.closed = false;
    natsMutex_Unlock(arg.m);
    s = natsOptions_SetUserCredentialsCallbacks(opts, _userJWTCB, NULL, _sigCB, (void*) &arg);
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsConnection_Connect(&nc, opts));
    if (s == NATS_OK)
    {
        natsMutex_Lock(arg.m);
        arg.nc = nc;
        natsMutex_Unlock(arg.m);

        _stopServer(pid);
        pid = _startServer("nats://127.0.0.1:4222", NULL, true);
        CHECK_SERVER_STARTED(pid);
    }
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (!arg.closed))
        s = natsCondition_TimedWait(arg.c, arg.m, 5000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    nc = NULL;

    _stopServer(pid);

    // Start fake server that will send predefined "nonce" so we can check
    // that connection is sending appropriate jwt and signature.

    // Set this to error, the mock server should set it to OK
    // if it can start successfully.
    arg.status = NATS_ERR;
    arg.checkInfoCB = _checkJWTAndSigCB;
    arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576,\"nonce\":\"nonce\"}\r\n";
    s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Connect sends proper JWT and Signature: ");
    natsOptions_Destroy(opts);
    opts = NULL;
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetUserCredentialsCallbacks(opts, _userJWTCB, NULL, _sigCB, NULL));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    natsThread_Join(t);
    natsThread_Destroy(t);
    _destroyDefaultThreadArgs(&arg);
}

static void
test_UserCredsFromFiles(void)
{
    natsStatus          s       = NATS_OK;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsOptions         *opts2  = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    natsThread          *t      = NULL;
    int                 i;
    struct threadArg    arg;
    const char          *ucfn   = "user.creds";
    const char          *sfn    = "seed.txt";
    const char          *snhfn  = "seednh.txt";
    const char          *nusfn  = "nouors.txt";
    FILE                *f      = NULL;

    f = fopen(ucfn, "w");
    if (f == NULL)
        s = NATS_ERR;
    else
    {
        int res = fputs("-----BEGIN NATS USER JWT----\nsome user jwt\n-----END NATS USER JWT-----\n\n-----BEGIN USER NKEY SEED-----\nSUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY\n-----END USER NKEY SEED-----\n", f);
        if (res < 0)
            s = NATS_ERR;

        if (fclose(f) != 0)
            s = NATS_ERR;
        f = NULL;
    }
    if (s == NATS_OK)
    {
        f = fopen(sfn, "w");
        if (f == NULL)
            s = NATS_ERR;
        else
        {
            int res = fputs("-----BEGIN USER NKEY SEED-----\nSUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY\n-----END USER NKEY SEED-----\n", f);
            if (res < 0)
                s = NATS_ERR;

            if (fclose(f) != 0)
                s = NATS_ERR;
            f = NULL;
        }
    }
    if (s == NATS_OK)
    {
        f = fopen(snhfn, "w");
        if (f == NULL)
            s = NATS_ERR;
        else
        {
            int res = fputs("This file does not have the proper header\nand also has spaces before the seed:\n  \tSUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY\nthis should work\n", f);
            if (res < 0)
                s = NATS_ERR;

            if (fclose(f) != 0)
                s = NATS_ERR;
            f = NULL;
        }
    }
    if (s == NATS_OK)
    {
        f = fopen(nusfn, "w");
        if (f == NULL)
            s = NATS_ERR;
        else
        {
            int res = fputs("This file does not have a jwt\nnor a valid seed\n", f);
            if (res < 0)
                s = NATS_ERR;

            if (fclose(f) != 0)
                s = NATS_ERR;
            f = NULL;
        }
    }
    if (s != NATS_OK)
        FAIL("Unable to create creds test files");

    s = natsOptions_Create(&opts);
    if (s != NATS_OK)
        FAIL("Unable to create options for test UserCredsFromFiles");

    test("Invalid arg 1: ");
    s = natsOptions_SetUserCredentialsFromFiles(NULL, "foo", "bar");
    testCond(s == NATS_INVALID_ARG);

    test("Invalid arg 2: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, NULL, "bar");
    testCond(s == NATS_INVALID_ARG);

    test("Invalid arg 3: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "", "bar");
    testCond(s == NATS_INVALID_ARG);

    test("Clone: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", "bar");
    if (s == NATS_OK)
    {
        opts2 = natsOptions_clone(opts);
        if (opts2 == NULL)
            s = NATS_NO_MEMORY;
    }
    IFOK(s, natsOptions_SetUserCredentialsFromFiles(opts, NULL, NULL));
    testCond((s == NATS_OK)
                && (opts2->userCreds != NULL)
                && (opts2->userJWTHandler == natsConn_userFromFile)
                && (opts2->userJWTClosure == (void*) opts2->userCreds)
                && (opts2->sigHandler == natsConn_signatureHandler)
                && (opts2->sigClosure == (void*) opts2->userCreds));
    natsOptions_Destroy(opts2);

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("UserOrChainedFile not found: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "userCredsNotFound", NULL);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "error opening file 'userCredsNotFound'") != NULL));

    // Use a file that contains no userJWT..
    test("UserOrChainedFile has no JWT: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "list.txt", NULL);
    IFOK(s, natsConnection_Connect(&nc, opts));
    // Since we return the whole content of the file when we don't find
    // the key for the user, but we don't for seed, the error we'll get
    // is about no user seed found.
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "no nkey user seed found") != NULL));

    test("SeedFile not found: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, ucfn, "seedFileNotFound");
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "error opening file 'seedFileNotFound'") != NULL));

    // Use a seed file that contains no seed..
    test("SeedFile has no seed: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, ucfn, "list.txt");
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond((s == NATS_ERR)
                && (strstr(nats_GetLastError(NULL), "no nkey user seed found") != NULL));

    _stopServer(pid);

    for (i=0; i<3; i++)
    {
        // Start fake server that will send predefined "nonce" so we can check
        // that connection is sending appropriate jwt and signature.
        s = _createDefaultThreadArgsForCbTests(&arg);
        if (s == NATS_OK)
        {
            // Set this to error, the mock server should set it to OK
            // if it can start successfully.
            arg.done = false;
            arg.status = NATS_ERR;
            arg.checkInfoCB = _checkJWTAndSigCB;
            arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576,\"nonce\":\"nonce\"}\r\n";
            s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
        }
        if (s == NATS_OK)
        {
            // Wait for server to be ready
            natsMutex_Lock(arg.m);
            while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
                s = natsCondition_TimedWait(arg.c, arg.m, 2000);
            natsMutex_Unlock(arg.m);
        }
        if (s != NATS_OK)
        {
            if (t != NULL)
            {
                natsThread_Join(t);
                natsThread_Destroy(t);
            }
            natsOptions_Destroy(opts);
            _destroyDefaultThreadArgs(&arg);
            FAIL("Unable to setup test");
        }

        s = NATS_OK;
        if (i == 0)
        {
            test("Connect with chained file: ");
            s = natsOptions_SetUserCredentialsFromFiles(opts, ucfn, NULL);
        }
        else if (i == 1)
        {
            test("Connect with user and seed files: ");
            s = natsOptions_SetUserCredentialsFromFiles(opts, ucfn, sfn);
        }
        else if (i == 2)
        {
            test("Connect with user and seed files (seed does not contain header): ");
            s = natsOptions_SetUserCredentialsFromFiles(opts, ucfn, snhfn);
        }
        IFOK(s, natsConnection_Connect(&nc, opts));
        testCond(s == NATS_OK);

        // Notify mock server we are done
        natsMutex_Lock(arg.m);
        arg.done = true;
        natsCondition_Signal(arg.c);
        natsMutex_Unlock(arg.m);

        natsConnection_Destroy(nc);
        nc = NULL;

        natsThread_Join(t);
        natsThread_Destroy(t);
        t = NULL;

        _destroyDefaultThreadArgs(&arg);
    }
    natsOptions_Destroy(opts);

    remove(ucfn);
    remove(sfn);
    remove(snhfn);
    remove(nusfn);
}

static natsStatus
_checkNKeyAndSig(char *buffer)
{
    // NKey should have been included
    if (strstr(buffer, "pubKey") == NULL)
        return NATS_ERR;

    // The server is sending the nonce "nonce" and we
    // use a seed that should have produced a signature
    // that converted to base64 should be:
    if (strstr(buffer, "m50It12aTgfbJwsQhucujqhXbsq7tLM-Mf_hSjBQsG_4onm8y2Vkw6JG1bbcDkdxXe-Ng0K-7X9ov4rZ4wFcDg") == NULL)
        return NATS_ERR;

    return NATS_OK;
}

static void
test_NKey(void)
{
    natsStatus          s;
    natsOptions         *opts  = NULL;
    natsOptions         *opts2 = NULL;
    natsConnection      *nc    = NULL;
    natsThread          *t     = NULL;
    struct threadArg    arg;

    s = natsOptions_Create(&opts);
    if (s != NATS_OK)
        FAIL("Failed to setup test");

    test("Invalid arg 1: ");
    s = natsOptions_SetNKey(NULL, "pubkey", _dummySigCb, (void*) 1);
    testCond(s == NATS_INVALID_ARG);

    test("Invalid arg 2: ");
    s = natsOptions_SetNKey(opts, "pubkey", NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Clone: ");
    s = natsOptions_SetNKey(opts, "pubkey", _dummySigCb, (void*) 1);
    if (s == NATS_OK)
    {
        opts2 = natsOptions_clone(opts);
        if (opts2 == NULL)
            s = NATS_NO_MEMORY;
    }
    IFOK(s, natsOptions_SetNKey(opts, NULL, NULL, NULL));
    testCond((s == NATS_OK)
                && (opts2->nkey != NULL)
                && (strcmp(opts2->nkey, "pubkey") == 0)
                && (opts2->sigHandler == _dummySigCb)
                && (opts2->sigClosure == (void*) 1));
    natsOptions_Destroy(opts2);

    test("NKey erase JWT: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", "bar");
    IFOK(s, natsOptions_SetNKey(opts, "pubkey2", _dummySigCb, (void*) 2));
    testCond((s == NATS_OK)
                && (opts->nkey != NULL)
                && (strcmp(opts->nkey, "pubkey2") == 0)
                && (opts->userJWTHandler == NULL)
                && (opts->userJWTClosure == NULL)
                && (opts->sigHandler == _dummySigCb)
                && (opts->sigClosure == (void*) 2));

    test("UserCreds erase NKey: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", "bar");
    testCond((s == NATS_OK)
                && (opts->nkey == NULL)
                && (opts->userJWTHandler == natsConn_userFromFile)
                && (opts->userJWTClosure == (void*) opts->userCreds)
                && (opts->sigHandler == natsConn_signatureHandler)
                && (opts->sigClosure == (void*) opts->userCreds));

    // Start fake server that will send predefined "nonce" so we can check
    // that connection is sending appropriate jwt and signature.
    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        // Set this to error, the mock server should set it to OK
        // if it can start successfully.
        arg.done = false;
        arg.status = NATS_ERR;
        arg.checkInfoCB = _checkNKeyAndSig;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576,\"nonce\":\"nonce\"}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("NKey works ok: ");
    s = natsOptions_SetNKey(opts, "pubKey", _sigCB, NULL);
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Destroy(nc);
    nc = NULL;

    natsThread_Join(t);
    natsThread_Destroy(t);
    t = NULL;

    _destroyDefaultThreadArgs(&arg);

    natsOptions_Destroy(opts);
}

static natsStatus
_checkNKeyFromSeed(char *buffer)
{
    // NKey should have been included
    if (strstr(buffer, "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4") == NULL)
        return NATS_ERR;

    // The server is sending the nonce "nonce" and we
    // use a seed that should have produced a signature
    // that converted to base64 should be:
    if (strstr(buffer, "AVfpO7Pw3rc56hoO1OJcFxXUCfBmO2qouchBchSlL45Fuur9zS15UzytEI1QC5wwVG7uiHIdqyfmOS6uPrwqCg") == NULL)
        return NATS_ERR;

    return NATS_OK;
}

static void
test_NKeyFromSeed(void)
{
    natsStatus          s;
    natsOptions         *opts  = NULL;
    natsOptions         *opts2 = NULL;
    natsConnection      *nc    = NULL;
    natsThread          *t     = NULL;
    FILE                *f     = NULL;
    struct threadArg    arg;

    s = natsOptions_Create(&opts);
    if (s != NATS_OK)
        FAIL("Failed to setup test");

    test("Invalid arg 1: ");
    s = natsOptions_SetNKeyFromSeed(NULL, "pubkey", "seed.file");
    testCond(s == NATS_INVALID_ARG);

    test("Invalid arg 2: ");
    s = natsOptions_SetNKeyFromSeed(opts, "pubkey", NULL);
    testCond(s == NATS_INVALID_ARG);

    nats_clearLastError();

    test("Clone: ");
    s = natsOptions_SetNKeyFromSeed(opts, "pubkey", "seed.file");
    if (s == NATS_OK)
    {
        opts2 = natsOptions_clone(opts);
        if (opts2 == NULL)
            s = NATS_NO_MEMORY;
    }
    IFOK(s, natsOptions_SetNKeyFromSeed(opts, NULL, NULL));
    testCond((s == NATS_OK)
                && (opts2->nkey != NULL)
                && (strcmp(opts2->nkey, "pubkey") == 0)
                && (opts2->sigHandler == natsConn_signatureHandler)
                && (opts2->sigClosure == (void*) opts2->userCreds)
                && (opts2->userCreds != NULL)
                && (opts2->userCreds->seedFile != NULL)
                && (strcmp(opts2->userCreds->seedFile, "seed.file") == 0));
    natsOptions_Destroy(opts2);

    test("NKeyFromSeed erase JWT: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", "bar");
    IFOK(s, natsOptions_SetNKeyFromSeed(opts, "pubkey2", "seed.file"));
    testCond((s == NATS_OK)
                && (opts->nkey != NULL)
                && (strcmp(opts->nkey, "pubkey2") == 0)
                && (opts->userJWTHandler == NULL)
                && (opts->userJWTClosure == NULL)
                && (opts->sigHandler == natsConn_signatureHandler)
                && (opts->sigClosure == (void*) opts->userCreds)
                && (opts->userCreds != NULL)
                && (opts->userCreds->seedFile != NULL)
                && (strcmp(opts->userCreds->seedFile, "seed.file") == 0));

    test("UserCreds erase NKeyFromSeed: ");
    s = natsOptions_SetUserCredentialsFromFiles(opts, "foo", NULL);
    testCond((s == NATS_OK)
                && (opts->nkey == NULL)
                && (opts->userJWTHandler == natsConn_userFromFile)
                && (opts->userJWTClosure == (void*) opts->userCreds)
                && (opts->sigHandler == natsConn_signatureHandler)
                && (opts->sigClosure == (void*) opts->userCreds)
                && (opts->userCreds != NULL)
                && (opts->userCreds->seedFile == NULL));

    // Start fake server that will send predefined "nonce" so we can check
    // that connection is sending appropriate jwt and signature.
    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        // Set this to error, the mock server should set it to OK
        // if it can start successfully.
        arg.done = false;
        arg.status = NATS_ERR;
        arg.checkInfoCB = _checkNKeyFromSeed;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576,\"nonce\":\"nonce\"}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        natsOptions_Destroy(opts);
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("NKeyFromSeed works ok: ");
    f = fopen("seed.file", "w");
    if (f == NULL)
        s = NATS_ERR;
    else
    {
        fputs("SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY\n", f);
        fclose(f);
        f = NULL;
    }
    IFOK(s, natsOptions_SetNKeyFromSeed(opts, "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4", "seed.file"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Destroy(nc);
    nc = NULL;

    natsThread_Join(t);
    natsThread_Destroy(t);
    t = NULL;

    _destroyDefaultThreadArgs(&arg);

    natsOptions_Destroy(opts);

    remove("seed.file");
}

static void
test_ConnSign(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    const char          *ucfn   = "user.creds";
    FILE                *f      = NULL;
    unsigned char       sig[64];
    const unsigned char expected[] = {
            155, 157,   8, 183,  93, 154,  78,   7,
            219,  39,  11,  16, 134, 231,  46, 142,
            168,  87, 110, 202, 187, 180, 179,  62,
             49, 255, 225,  74,  48,  80, 176, 111,
            248, 162, 121, 188, 203, 101, 100, 195,
            162,  70, 213, 182, 220,  14,  71, 113,
             93, 239, 141, 131,  66, 190, 237, 127,
            104, 191, 138, 217, 227,   1,  92,  14,
    };

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect ok: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    testCond(s == NATS_OK);

    test("Can't sign without user creds: ");
    s = natsConnection_Sign(nc, (const unsigned char*) "payload", 7, sig);
    testCond((s == NATS_ERR)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), "unable to sign") != NULL));

    natsConnection_Destroy(nc);
    nc = NULL;

    s = NATS_OK;
    test("Set proper option: ");
    f = fopen(ucfn, "w");
    if (f == NULL)
        s = NATS_ERR;
    else
    {
        int res = fputs("SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY\n", f);
        if (res < 0)
            s = NATS_ERR;

        if (fclose(f) != 0)
            s = NATS_ERR;
        f = NULL;
    }
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetUserCredentialsFromFiles(opts, ucfn, ucfn));
    testCond(s == NATS_OK);

    test("Connect ok: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Sign with NULL message: ");
    s = natsConnection_Sign(nc, NULL, 0, sig);
    testCond(s == NATS_OK);

    test("Sign message: ");
    s = natsConnection_Sign(nc, (const unsigned char*) "nonce", 5, sig);
    testCond((s == NATS_OK)
                && (sig != NULL)
                && (memcmp((void*) sig, expected, sizeof(expected)) == 0));

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(pid);

    remove(ucfn);
}

static void
test_WriteDeadline(void)
{
    natsStatus          s;
    natsOptions         *opts = NULL;
    natsConnection      *nc   = NULL;
    natsThread          *t    = NULL;
    char                data[1024] = {0};
    struct threadArg    arg;

    test("Create options: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetAllowReconnect(opts, false));
    testCond(s == NATS_OK);

    test("Set invalid write deadline: ");
    s = natsOptions_SetWriteDeadline(opts, -1);
    testCond(s == NATS_INVALID_ARG);

    test("Start mock server: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        arg.status = NATS_ERR;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    test("Write deadline kicks publish out: ");
    s = natsOptions_SetIOBufSize(opts, 100);
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, &arg));
    IFOK(s, natsOptions_SetWriteDeadline(opts, 1));
    IFOK(s, natsConnection_Connect(&nc, opts));
    while (s == NATS_OK)
        s = natsConnection_Publish(nc, "foo", data, sizeof(data));
    testCond(s == NATS_TIMEOUT);

    test("Caused a disconnect: ");
    // Since we are not allowing for reconnect, we should
    // get the closed CB.
    natsMutex_Lock(arg.m);
    s = NATS_OK;
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    natsThread_Join(t);
    natsThread_Destroy(t);

    _destroyDefaultThreadArgs(&arg);
}

static void
_publish(void *arg)
{
    natsConnection  *nc = (natsConnection*) arg;
    char            data[1024] = {0};
    int             i;
    natsStatus      s = NATS_OK;

    for (i=0; ((s == NATS_OK) && (i<1000)); i++)
        s = natsConnection_Publish(nc, "foo", data, sizeof(data));

}

static void
test_NoPartialOnReconnect(void)
{
    natsStatus          s;
    natsOptions         *opts = NULL;
    natsConnection      *nc   = NULL;
    natsThread          *t    = NULL;
    natsThread          *t2   = NULL;
    natsPid             pid   = NATS_INVALID_PID;
    struct threadArg    arg;
    const char          *servers[2] = {"nats://127.0.0.1:4222", "nats://127.0.0.1:4223"};

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("unable to setup test");

    test("Create options: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetAllowReconnect(opts, true));
    IFOK(s, natsOptions_SetReconnectWait(opts, 10));
    IFOK(s, natsOptions_SetReconnectJitter(opts, 0, 0));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 10000));
    IFOK(s, natsOptions_SetServers(opts, servers, 2));
    IFOK(s, natsOptions_SetNoRandomize(opts, true));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &arg));
    testCond(s == NATS_OK);

    test("Start real backup server: ");
    pid = _startServer("nats://127.0.0.1:4223", "-p 4223", true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Start mock server: ");
    if (s == NATS_OK)
    {
        arg.status = NATS_ERR;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    testCond(s == NATS_OK);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK)

    test("Start Publish: ");
    s = natsThread_Create(&t2, _publish, (void*) nc);
    testCond(s == NATS_OK);

    nats_Sleep(1000);

    test("Kill server: ");
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Wait for reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !(arg.reconnected))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    if (t2 != NULL)
    {
        natsThread_Join(t2);
        natsThread_Destroy(t2);
    }

    test("Check no proto error: ");
    {
        const char *le = NULL;
        s = natsConnection_GetLastError(nc, &le);
    }
    testCond(s == NATS_OK);

    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
_stopServerInThread(void *closure)
{
    natsPid pid = *((natsPid*) closure);

    nats_Sleep(150);
    _stopServer(pid);
}

static void
test_ReconnectFailsPendingRequest(void)
{
    natsStatus          s;
    natsOptions         *opts = NULL;
    natsConnection      *nc   = NULL;
    natsSubscription    *sub  = NULL;
    natsMsg             *msg  = NULL;
    natsThread          *t    = NULL;
    natsPid             pid   = NATS_INVALID_PID;
    bool                failr = false;
    int                 iter;

    for (iter=1; iter<=2; iter++)
    {
        failr = (iter == 2 ? true : false);

        test("Create options: ");
        s = natsOptions_Create(&opts);
        IFOK(s, natsOptions_SetFailRequestsOnDisconnect(opts, failr));
        testCond(s == NATS_OK);

        test("Start server: ");
        pid = _startServer("nats://127.0.0.1:4222", "-p 4222", true);
        CHECK_SERVER_STARTED(pid);
        testCond(true);

        test("Connect: ");
        s = natsConnection_Connect(&nc, opts);
        testCond(s == NATS_OK);

        test("Create service provider: ");
        s = natsConnection_SubscribeSync(&sub, nc, "requests");
        testCond(s == NATS_OK);

        test("Start thread that will stop server: ");
        s = natsThread_Create(&t, _stopServerInThread, (void*) &pid);
        testCond(s == NATS_OK);

        test((failr ? "Fails due to disconnect: " : "Fails due to timeout: "));
        s = natsConnection_RequestString(&msg, nc, "requests", "help", 300);
        testCond(s == (failr ? NATS_CONNECTION_DISCONNECTED : NATS_TIMEOUT));

        natsThread_Join(t);
        natsThread_Destroy(t);
        t = NULL;
        natsSubscription_Destroy(sub);
        sub = NULL;
        natsConnection_Destroy(nc);
        nc = NULL;
        natsOptions_Destroy(opts);
        opts = NULL;
    }
}

static void
test_HeadersNotSupported(void)
{
    natsStatus          s;
    natsConnection      *conn = NULL;
    natsMsg             *msg  = NULL;
    natsMsg             *reply= NULL;
    natsThread          *t    = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        // Set this to error, the mock server should set it to OK
        // if it can start successfully.
        arg.status = NATS_ERR;
        arg.string = "INFO {\"server_id\":\"22\",\"version\":\"latest\",\"go\":\"latest\",\"port\":4222,\"max_payload\":1048576}\r\n";
        s = natsThread_Create(&t, _startMockupServerThread, (void*) &arg);
    }
    if (s == NATS_OK)
    {
        // Wait for server to be ready
        natsMutex_Lock(arg.m);
        while ((s != NATS_TIMEOUT) && (arg.status != NATS_OK))
            s = natsCondition_TimedWait(arg.c, arg.m, 2000);
        natsMutex_Unlock(arg.m);
    }
    if (s != NATS_OK)
    {
        if (t != NULL)
        {
            natsThread_Join(t);
            natsThread_Destroy(t);
        }
        _destroyDefaultThreadArgs(&arg);
        FAIL("Unable to setup test");
    }

    test("Headers not supported with old server: ");
    s = natsConnection_ConnectTo(&conn, NATS_DEFAULT_URL);
    IFOK(s, natsConnection_HasHeaderSupport(conn));
    testCond(s == NATS_NO_SERVER_SUPPORT);

    test("Create msg with heades: ");
    s = natsMsg_Create(&msg, "foo", NULL, "body", 4);
    IFOK(s, natsMsgHeader_Set(msg, "Header", "Hello Headers!"));
    testCond(s == NATS_OK);

    test("Publish fails: ");
    s = natsConnection_PublishMsg(conn, msg);
    testCond(s == NATS_NO_SERVER_SUPPORT);

    test("Request fails: ");
    s = natsConnection_RequestMsg(&reply, conn, msg, 1000);
    testCond((s == NATS_NO_SERVER_SUPPORT) && (reply == NULL));

    natsConnection_Destroy(conn);

    // Notify mock server we are done
    natsMutex_Lock(arg.m);
    arg.done = true;
    natsCondition_Signal(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(t);
    natsThread_Destroy(t);

    natsMsg_Destroy(msg);
    natsMsg_Destroy(reply);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_HeadersBasic(void)
{
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsPid             pid     = NATS_INVALID_PID;
    natsMsg             *msg    = NULL;
    natsMsg             *rmsg   = NULL;
    natsSubscription    *sub    = NULL;
    const char          *val    = NULL;

    if (!serverVersionAtLeast(2, 2, 0))
    {
        char txt[200];

        snprintf(txt, sizeof(txt), "Skipping since requires server version of at least 2.2.0, got %s: ", serverVersion);
        test(txt);
        testCond(true);
        return;
    }

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect ok: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    testCond(s == NATS_OK);

    test("Headers supported: ");
    s = natsConnection_HasHeaderSupport(nc);
    testCond(s == NATS_OK);

    test("Create sub: ");
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    testCond(s == NATS_OK);

    test("Create msg with headers: ");
    s = natsMsg_Create(&msg, "foo", NULL, "body", 4);
    IFOK(s, natsMsgHeader_Set(msg, "Headers", "Hello Headers!"))
    testCond(s == NATS_OK);

    test("Publish with headers ok: ");
    s = natsConnection_PublishMsg(nc, msg);
    testCond(s == NATS_OK);

    test("Receive msg: ")
    s = natsSubscription_NextMsg(&rmsg, sub, 1000);
    testCond((s == NATS_OK) && (rmsg != NULL));

    test("Resend msg without lift: ");
    s = natsConnection_PublishMsg(nc, rmsg);
    testCond(s == NATS_OK);
    natsMsg_Destroy(rmsg);
    rmsg = NULL;

    test("Receive msg: ")
    s = natsSubscription_NextMsg(&rmsg, sub, 1000);
    testCond((s == NATS_OK) && (rmsg != NULL));

    test("Check headers: ");
    s = natsMsgHeader_Get(rmsg, "Headers", &val);
    testCond((s == NATS_OK)
                && (val != NULL) && (strcmp(val, "Hello Headers!") == 0)
                && (natsMsg_GetDataLength(rmsg) == 4)
                && (strncmp(natsMsg_GetData(msg), "body", 4) == 0));

    natsMsg_Destroy(rmsg);
    rmsg = NULL;
    test("Value with CRLFs replaced with spaces: ");
    s = natsMsgHeader_Set(msg, "Headers", "value1\r\nvalue2\r\nvalue3");
    IFOK(s, natsConnection_PublishMsg(nc, msg));
    IFOK(s, natsSubscription_NextMsg(&rmsg, sub, 1000));
    IFOK(s, natsMsgHeader_Get(rmsg, "Headers", &val));
    testCond((s == NATS_OK)
                && (val != NULL) && (strcmp(val, "value1  value2  value3") == 0)
                && (natsMsg_GetDataLength(rmsg) == 4)
                && (strncmp(natsMsg_GetData(msg), "body", 4) == 0));

    natsMsg_Destroy(msg);
    natsMsg_Destroy(rmsg);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    _stopServer(pid);
}

static natsStatus
_evLoopAttach(void **userData, void *loop, natsConnection *nc, natsSock socket)
{
    struct threadArg *arg = (struct threadArg *) loop;

    natsMutex_Lock(arg->m);
    *userData = arg;
    arg->nc = nc;
    arg->sock = socket;
    arg->attached++;
    arg->doRead = true;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);

    return NATS_OK;
}

static natsStatus
_evLoopRead(void *userData, bool add)
{
    struct threadArg *arg = (struct threadArg *) userData;

    natsMutex_Lock(arg->m);
    arg->doRead = add;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);

    return NATS_OK;
}

static natsStatus
_evLoopWrite(void *userData, bool add)
{
    struct threadArg *arg = (struct threadArg *) userData;

    natsMutex_Lock(arg->m);
    arg->doWrite = add;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);

    return NATS_OK;
}

static natsStatus
_evLoopDetach(void *userData)
{
    struct threadArg *arg = (struct threadArg *) userData;

    natsMutex_Lock(arg->m);
    arg->detached++;
    natsCondition_Broadcast(arg->c);
    natsMutex_Unlock(arg->m);

    return NATS_OK;
}

static void
_eventLoop(void *closure)
{
    struct threadArg *arg = (struct threadArg *) closure;
    natsSock         sock = NATS_SOCK_INVALID;
    natsConnection   *nc  = NULL;
    bool             read = false;
    bool             write= false;
    bool             stop = false;

    while (!stop)
    {
        nats_Sleep(100);
        natsMutex_Lock(arg->m);
        while (!arg->evStop && ((sock = arg->sock) == NATS_SOCK_INVALID))
            natsCondition_Wait(arg->c, arg->m);
        stop = arg->evStop;
        nc = arg->nc;
        read = arg->doRead;
        write = arg->doWrite;
        natsMutex_Unlock(arg->m);

        if (!stop)
        {
            if (read)
                natsConnection_ProcessReadEvent(nc);
            if (write)
                natsConnection_ProcessWriteEvent(nc);
        }
    }
}

static void
test_EventLoop(void)
{
    natsStatus          s;
    natsConnection      *nc         = NULL;
    natsOptions         *opts       = NULL;
    natsSubscription    *sub        = NULL;
    natsMsg             *msg        = NULL;
    natsPid             pid         = NATS_INVALID_PID;
    struct threadArg    arg;

    test("Set options: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 100));
    IFOK(s, natsOptions_SetReconnectWait(opts, 50));
    IFOK(s, natsOptions_SetEventLoop(opts, (void*) &arg,
                                     _evLoopAttach,
                                     _evLoopRead,
                                     _evLoopWrite,
                                     _evLoopDetach));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    testCond(s == NATS_OK);

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Start event loop: ");
    natsMutex_Lock(arg.m);
    arg.sock = NATS_SOCK_INVALID;
    natsMutex_Unlock(arg.m);
    s = natsThread_Create(&arg.t, _eventLoop, (void*) &arg);
    testCond(s == NATS_OK);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK)

    test("Create sub: ");
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    testCond(s == NATS_OK);

    test("Stop server and wait for disconnect: ");
    _stopServer(pid);
    pid = NATS_INVALID_PID;
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Restart server: ");
    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(s == NATS_OK);

    test("Wait for reconnect: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Publish: ");
    s = natsConnection_PublishString(nc, "foo", "bar");
    testCond(s == NATS_OK);

    test("Check msg received: ");
    s = natsSubscription_NextMsg(&msg, sub, 1000);
    testCond(s == NATS_OK);
    natsMsg_Destroy(msg);

    test("Close and wait for close cb: ");
    natsConnection_Close(nc);
    _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    natsMutex_Lock(arg.m);
    arg.evStop = true;
    natsCondition_Broadcast(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(arg.t);
    natsThread_Destroy(arg.t);

    test("Check ev loop: ");
    natsMutex_Lock(arg.m);
    if (arg.attached != 2 || !arg.detached)
        s = NATS_ERR;
    testCond(s == NATS_OK);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
test_EventLoopRetryOnFailedConnect(void)
{
    natsStatus          s;
    natsConnection      *nc         = NULL;
    natsOptions         *opts       = NULL;
    natsSubscription    *sub        = NULL;
    natsMsg             *msg        = NULL;
    natsPid             pid         = NATS_INVALID_PID;
    struct threadArg    arg;

    test("Set options: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 100));
    IFOK(s, natsOptions_SetReconnectWait(opts, 50));
    IFOK(s, natsOptions_SetRetryOnFailedConnect(opts,
                                                true,
                                                _connectedCb,
                                                (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsOptions_SetEventLoop(opts, (void*) &arg,
                                     _evLoopAttach,
                                     _evLoopRead,
                                     _evLoopWrite,
                                     _evLoopDetach));
    testCond(s == NATS_OK);

    test("Start event loop: ");
    natsMutex_Lock(arg.m);
    arg.sock = NATS_SOCK_INVALID;
    natsMutex_Unlock(arg.m);
    s = natsThread_Create(&arg.t, _eventLoop, (void*) &arg);
    testCond(s == NATS_OK);

    test("Start connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_NOT_YET_CONNECTED);
    if (s == NATS_NOT_YET_CONNECTED)
        s = NATS_OK;

    test("Create sub: ");
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    testCond(s == NATS_OK);

    test("Start server: ");
    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(s == NATS_OK);

    test("Check connected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.connected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Publish: ");
    s = natsConnection_PublishString(nc, "foo", "bar");
    testCond(s == NATS_OK);

    test("Check msg received: ");
    s = natsSubscription_NextMsg(&msg, sub, 1000);
    testCond(s == NATS_OK);
    natsMsg_Destroy(msg);

    test("Close and wait for close cb: ");
    natsConnection_Close(nc);
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    natsMutex_Lock(arg.m);
    arg.evStop = true;
    natsCondition_Broadcast(arg.c);
    natsMutex_Unlock(arg.m);

    natsThread_Join(arg.t);
    natsThread_Destroy(arg.t);

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
}

static void
test_EventLoopTLS(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc         = NULL;
    natsOptions         *opts       = NULL;
    natsPid             pid         = NATS_INVALID_PID;
    struct threadArg    arg;

    test("Set options: ");
    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetURL(opts, "nats://localhost:4443"));
    IFOK(s, natsOptions_SkipServerVerification(opts, true));
    IFOK(s, natsOptions_SetSecure(opts, true));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 100));
    IFOK(s, natsOptions_SetReconnectWait(opts, 50));
    IFOK(s, natsOptions_SetDisconnectedCB(opts, _disconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, (void*) &arg));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &arg));
    IFOK(s, natsOptions_SetEventLoop(opts, (void*) &arg,
                                     _evLoopAttach,
                                     _evLoopRead,
                                     _evLoopWrite,
                                     _evLoopDetach));
    testCond(s == NATS_OK);

    test("Start server: ");
    pid = _startServer("nats://127.0.0.1:4443", "-config tls.conf -DV", true);
    CHECK_SERVER_STARTED(pid);
    testCond(s == NATS_OK);

    test("Start event loop: ");
    natsMutex_Lock(arg.m);
    arg.sock = NATS_SOCK_INVALID;
    natsMutex_Unlock(arg.m);
    s = natsThread_Create(&arg.t, _eventLoop, (void*) &arg);
    testCond(s == NATS_OK);

    test("Connect: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Disconnect: ");
    _stopServer(pid);
    pid = NATS_INVALID_PID;
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.disconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Restart server: ");
    pid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(pid);
    testCond(s == NATS_OK);

    test("Check reconnected: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.reconnected)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Shutdown evLoop: ");
    natsMutex_Lock(arg.m);
    arg.evStop = true;
    natsCondition_Broadcast(arg.c);
    natsMutex_Unlock(arg.m);
    natsThread_Join(arg.t);
    natsThread_Destroy(arg.t);
    testCond(s == NATS_OK);

    test("Close and wait for close cb: ");
    natsConnection_Close(nc);
    s = _waitForConnClosed(&arg);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&arg);

    _stopServer(pid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLBasic(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect switches to TLS automatically: ");
    s = natsOptions_SetURL(opts, "nats://localhost:4443");
    // For this test skip server verification
    IFOK(s, natsOptions_SkipServerVerification(opts, true));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    nc = NULL;

    test("Check connects OK with SSL options: ");
    s = natsOptions_SetSecure(opts, true);
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));

    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLVerify(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect fails if no SSL certs: ");
    s = natsOptions_SetURL(opts, "nats://localhost:4443");
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s != NATS_OK);

    test("Check that connect succeeds with proper certs: ");
    s = natsOptions_LoadCertificatesChain(opts,
                                          "certs/client-cert.pem",
                                          "certs/client-key.pem");
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLLoadCAFromMemory(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsBuffer          *certBuf  = NULL;
    struct threadArg    args;

    s = nats_ReadFile(&certBuf, 10000, "certs/ca.pem");
    IFOK(s, _createDefaultThreadArgsForCbTests(&args));
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    test("Check NULL certs: ");
    s = natsOptions_SetCATrustedCertificates(opts, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Check empty certs: ");
    s = natsOptions_SetCATrustedCertificates(opts, "");
    testCond(s == NATS_INVALID_ARG);

    test("Check invalid cert: ");
    s = natsOptions_SetCATrustedCertificates(opts, "invalid");
    testCond(s == NATS_SSL_ERROR);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect succeeds with proper certs: ");
    s = natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args);
    IFOK(s, natsOptions_SetURL(opts, "nats://localhost:4443"));
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    if (s == NATS_OK)
    {
        s = natsOptions_SetCATrustedCertificates(opts, (const char*) natsBuf_Data(certBuf));
        // Demonstrate that we can free the memory after this call..
        natsBuf_Destroy(certBuf);
    }
    IFOK(s, natsOptions_LoadCertificatesChain(opts,
                                              "certs/client-cert.pem",
                                              "certs/client-key.pem"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLCertAndKeyFromMemory(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsBuffer          *certBuf  = NULL;
    natsBuffer          *keyBuf   = NULL;
    struct threadArg    args;

    s = nats_ReadFile(&certBuf, 10000, "certs/client-cert.pem");
    IFOK(s, nats_ReadFile(&keyBuf, 10000, "certs/client-key.pem"));
    IFOK(s, _createDefaultThreadArgsForCbTests(&args));
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    test("Check NULL cert: ");
    s = natsOptions_SetCertificatesChain(opts, NULL, (const char*) natsBuf_Data(keyBuf));
    testCond(s == NATS_INVALID_ARG);

    test("Check empty cert: ");
    s = natsOptions_SetCertificatesChain(opts, "", (const char*) natsBuf_Data(keyBuf));
    testCond(s == NATS_INVALID_ARG);

    test("Check NULL key: ");
    s = natsOptions_SetCertificatesChain(opts, (const char*) natsBuf_Data(certBuf), NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Check empty key: ");
    s = natsOptions_SetCertificatesChain(opts, (const char*) natsBuf_Data(certBuf), "");
    testCond(s == NATS_INVALID_ARG);

    test("Check invalid cert: ");
    s = natsOptions_SetCertificatesChain(opts, "invalid", (const char*) natsBuf_Data(keyBuf));
    testCond(s == NATS_SSL_ERROR);

    test("Check invalid key: ");
    s = natsOptions_SetCertificatesChain(opts, (const char*) natsBuf_Data(certBuf), "invalid");
    testCond(s == NATS_SSL_ERROR);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect succeeds with proper certs: ");
    s = natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args);
    IFOK(s, natsOptions_SetURL(opts, "nats://localhost:4443"));
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetCertificatesChain(opts,
                                             (const char*) natsBuf_Data(certBuf),
                                             (const char*) natsBuf_Data(keyBuf)));
    // Demonstrate that we can free the memory after this call..
    natsBuf_Destroy(certBuf);
    natsBuf_Destroy(keyBuf);

    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tlsverify.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLVerifyHostname(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect fails if url is IP: ");
    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4443");
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_SSL_ERROR);

    test("Check that connect fails if wrong expected hostname: ");
    s = natsOptions_SetURL(opts, "nats://localhost:4443");
    IFOK(s, natsOptions_SetExpectedHostname(opts, "foo"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_SSL_ERROR);

    test("Check that connect succeeds if hostname ok and no expected hostname set: ");
    s = natsOptions_SetURL(opts, "nats://localhost:4443");
    IFOK(s, natsOptions_SetExpectedHostname(opts, NULL));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK)
    natsConnection_Destroy(nc);
    nc = NULL;

    test("Check that connect succeeds with proper expected hostname: ");
    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4443");
    IFOK(s, natsOptions_SetExpectedHostname(opts, "localhost"));
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK);

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLSkipServerVerification(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that connect fails due to server verification: ");
    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4443");
    IFOK(s, natsOptions_SetSecure(opts, true));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_SSL_ERROR);

    test("Check that connect succeeds with server verification disabled: ");
    s = natsOptions_SkipServerVerification(opts, true);
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s == NATS_OK);

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLCiphers(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        opts = _createReconnectOptions();
    if (opts == NULL)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("SetCipherSuites requires OpenSSL 1.1: ");
    s = natsOptions_SetCipherSuites(opts, "TLS_AES_128_GCM_SHA256");
#if defined(NATS_USE_OPENSSL_1_1)
    testCond(s == NATS_OK);
#else
    testCond(s == NATS_ERR);
#endif

    test("Check that connect fails if improper ciphers: ");
    s = natsOptions_SetURL(opts, "nats://localhost:4443");
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetCiphers(opts, "-ALL:RSA"));
#if defined(NATS_USE_OPENSSL_1_1)
    IFOK(s, natsOptions_SetCipherSuites(opts, ""));
#endif
    IFOK(s, natsConnection_Connect(&nc, opts));
    testCond(s != NATS_OK);

    test("Check connects OK with proper ciphers: ");
    s = natsOptions_SetCiphers(opts, "-ALL:HIGH");
#if defined(NATS_USE_OPENSSL_1_1)
    IFOK(s, natsOptions_SetCipherSuites(opts, "TLS_AES_128_GCM_SHA256"));
#endif
    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    test("Check reconnects OK: ");
    _stopServer(serverPid);

    nats_Sleep(100);

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !(args.reconnected))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_PublishString(nc, "foo", "test"));
    IFOK(s, natsConnection_Flush(nc));
    testCond(s == NATS_OK)

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

#if defined(NATS_HAS_TLS)
static void
_sslMT(void *closure)
{
    struct threadArg    *args = (struct threadArg*) closure;
    natsStatus          s = NATS_OK;
    natsConnection      *nc = NULL;
    natsSubscription    *sub = NULL;
    natsMsg             *msg = NULL;
    char                subj[64];
    int                 i;
    const char          *msgPayload = "this is a test payload";
    int                 count = 50;

    natsMutex_Lock(args->m);
    snprintf(subj, sizeof(subj), "foo.%d", ++(args->sum));
    while (!(args->current) && (s == NATS_OK))
        s = natsCondition_TimedWait(args->c, args->m, 2000);
    natsMutex_Unlock(args->m);

    if (valgrind)
        count = 10;

    for (i=0; (s == NATS_OK) && (i < count); i++)
    {
        s = natsConnection_Connect(&nc, args->opts);
        IFOK(s, natsConnection_SubscribeSync(&sub, nc, subj));
        IFOK(s, natsConnection_PublishString(nc, subj, msgPayload));
        IFOK(s, natsSubscription_NextMsg(&msg, sub, 2000));
        if (s == NATS_OK)
        {
            if (strcmp(natsMsg_GetData(msg), msgPayload) != 0)
                s = NATS_ERR;
        }

        natsMsg_Destroy(msg);
        msg = NULL;
        natsSubscription_Destroy(sub);
        sub = NULL;
        natsConnection_Destroy(nc);
        nc = NULL;
    }

    if (s != NATS_OK)
    {
        natsMutex_Lock(args->m);
        if (args->status == NATS_OK)
            args->status = s;
        natsMutex_Unlock(args->m);
    }
}

#define SSL_THREADS (3)
#endif

static void
test_SSLMultithreads(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    natsThread          *t[SSL_THREADS];
    int                 i;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
        s = natsOptions_Create(&opts);
    if (opts == NULL)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Create options: ");
    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4443");
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetExpectedHostname(opts, "localhost"));
    testCond(s == NATS_OK);

    args.opts = opts;

    for (i=0; (s == NATS_OK) && (i<SSL_THREADS); i++)
        s = natsThread_Create(&t[i], _sslMT, &args);

    test("Create connections from multiple threads using same ssl ctx: ");
    natsMutex_Lock(args.m);
    args.current = true;
    natsCondition_Broadcast(args.c);
    natsMutex_Unlock(args.m);

    for (i=0; i<SSL_THREADS; i++)
    {
        if (t[i] == NULL)
            continue;

        natsThread_Join(t[i]);
        natsThread_Destroy(t[i]);
    }

    natsMutex_Lock(args.m);
    IFOK(s, args.status);
    natsMutex_Unlock(args.m);

    testCond(s == NATS_OK);

    natsOptions_Destroy(opts);

    if (valgrind)
        nats_Sleep(900);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLConnectVerboseOption(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s == NATS_OK)
    {
        opts = _createReconnectOptions();
        if (opts == NULL)
            s = NATS_ERR;
    }
    IFOK(s, natsOptions_SetVerbose(opts, true));
    IFOK(s, natsOptions_SetReconnectedCB(opts, _reconnectedCb, &args));
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    s = natsOptions_SetURL(opts, "nats://127.0.0.1:4443");
    IFOK(s, natsOptions_SetSecure(opts, true));
    // For test purposes, we provide the CA trusted certs
    IFOK(s, natsOptions_LoadCATrustedCertificates(opts, "certs/ca.pem"));
    IFOK(s, natsOptions_SetExpectedHostname(opts, "localhost"));

    test("Check that SSL connect OK when Verbose set: ");

    IFOK(s, natsConnection_Connect(&nc, opts));
    IFOK(s, natsConnection_Flush(nc));

    testCond(s == NATS_OK);

    _stopServer(serverPid);
    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    test("Check that SSL reconnect OK when Verbose set: ");

    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.reconnected)
        s = natsCondition_TimedWait(args.c, args.m, 5000);
    natsMutex_Unlock(args.m);

    IFOK(s, natsConnection_Flush(nc));

    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    if (valgrind)
        nats_Sleep(900);

    _destroyDefaultThreadArgs(&args);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

#if defined(NATS_HAS_TLS)
static natsStatus
_elDummyAttach(void **userData, void *loop, natsConnection *nc, natsSock socket) { return NATS_OK; }

static natsStatus
_elDummyRead(void *userData, bool add) { return NATS_OK; }

static natsStatus
_elDummyWrite(void *userData, bool add) { return NATS_OK; }

static natsStatus
_elDummyDetach(void *userData) { return NATS_OK; }
#endif

static void
test_SSLSocketLeakWithEventLoop(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc       = NULL;
    natsOptions         *opts     = NULL;
    natsPid             serverPid = NATS_INVALID_PID;

    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
    {
        // Not really using a real event loop, just setting the option.
        s = natsOptions_SetEventLoop(opts, (void*) 1,
                                     _elDummyAttach,
                                     _elDummyRead,
                                     _elDummyWrite,
                                     _elDummyDetach);
    }
    IFOK(s, natsOptions_SetURL(opts, "nats://127.0.0.1:4443"));
    IFOK(s, natsOptions_SetSecure(opts, true));
    if (s != NATS_OK)
        FAIL("Unable to setup test!");

    serverPid = _startServer("nats://127.0.0.1:4443", "-config tls.conf", true);
    CHECK_SERVER_STARTED(serverPid);

    // The options have not been set properly for a successful TLS connection.
    test("Check that SSL fails: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s != NATS_OK);

    // Valgrind will tell us if we have leaked socket.

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _stopServer(serverPid);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
test_SSLReconnectWithAuthError(void)
{
#if defined(NATS_HAS_TLS)
    natsStatus          s;
    natsConnection      *nc     = NULL;
    natsOptions         *opts   = NULL;
    natsPid             pid1    = NATS_INVALID_PID;
    natsPid             pid2    = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    IFOK(s, natsOptions_Create(&opts));
    IFOK(s, natsOptions_SetSecure(opts, true));
    IFOK(s, natsOptions_SkipServerVerification(opts, true));
    IFOK(s, natsOptions_SetTimeout(opts, 250));
    IFOK(s, natsOptions_SetMaxReconnect(opts, 1000));
    IFOK(s, natsOptions_SetReconnectWait(opts, 100));
    IFOK(s, natsOptions_SetClosedCB(opts, _closedCb, (void*) &args));
    IFOK(s, natsOptions_SetURL(opts, "tls://user:pwd@127.0.0.1:4443"));
    if (opts == NULL)
        FAIL("Unable to create reconnect options!");

    pid1 = _startServer("nats://127.0.0.1:4443", "-p 4443 -cluster_name abc -cluster nats://127.0.0.1:6222 -tls -tlscert certs/server-cert.pem -tlskey certs/server-key.pem -tlscacert certs/ca.pem -user user -pass pwd", true);
    CHECK_SERVER_STARTED(pid1);

    pid2 = _startServer("nats://127.0.0.1:4444", "-p 4444 -cluster_name abc -cluster nats://127.0.0.1:6223 -routes nats://127.0.0.1:6222 -tls -tlscert certs/server-cert.pem -tlskey certs/server-key.pem -tlscacert certs/ca.pem -user user -pass pwd", true);
    CHECK_SERVER_STARTED(pid2);

    test("Connect to server1: ");
    s = natsConnection_Connect(&nc, opts);
    testCond(s == NATS_OK);

    test("Stop server1: ");
    _stopServer(pid1);
    testCond(true);

    test("Check that client stops after auth errors: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.closed)
        s = natsCondition_TimedWait(args.c, args.m, 5000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid2);
#else
    test("Skipped when built with no SSL support: ");
    testCond(true);
#endif
}

static void
rmtree(const char *path)
{
#ifdef _WIN32
    WIN32_FIND_DATA ffd;
    HANDLE          hFind = INVALID_HANDLE_VALUE;
    char            *dir  = NULL;

    if (nats_asprintf(&dir, "%s\\*", path) < 0)
        abort();

    hFind = FindFirstFile(dir, &ffd);
    if (hFind == INVALID_HANDLE_VALUE)
    {
        free(dir);
        return;
    }

    do
    {
        char *fullPath = NULL;

        if (!strcmp(ffd.cFileName, ".") || !strcmp(ffd.cFileName, ".."))
            continue;

        if (nats_asprintf(&fullPath, "%s\\%s", path, ffd.cFileName) < 0)
            abort();

        if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            rmtree(fullPath);
        else
            DeleteFile(fullPath);
        free(fullPath);
    }
    while (FindNextFile(hFind, &ffd) != 0);

    FindClose(hFind);
    RemoveDirectory(path);
    free(dir);

#else
    DIR             *dir;
    struct stat     statPath, statEntry;
    struct dirent   *entry;

    memset(&statPath, 0, sizeof(struct stat));

    stat(path, &statPath);
    if (S_ISDIR(statPath.st_mode) == 0)
        return;

    if ((dir = opendir(path)) == NULL)
        return;

    while ((entry = readdir(dir)) != NULL)
    {
        char *fullPath = NULL;

        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;

        if (nats_asprintf(&fullPath, "%s/%s", path, entry->d_name) < 0)
            abort();

        memset(&statEntry, 0, sizeof(struct stat));
        stat(fullPath, &statEntry);

        if (S_ISDIR(statEntry.st_mode) != 0)
            rmtree(fullPath);
        else
            unlink(fullPath);

        free(fullPath);
    }

    closedir(dir);
    rmdir(path);
#endif
}

static void
_makeUniqueDir(char *buf, int bufLen, const char *path)
{
    int n;

    if ((int) strlen(path) + 1 + NUID_BUFFER_LEN + 1 > bufLen)
        abort();

    n = snprintf(buf, bufLen, "%s", path);
    natsNUID_Next(buf+n, NUID_BUFFER_LEN+1);
    buf[n+NUID_BUFFER_LEN+1] = '\0';
}

static void
_createConfFile(char *buf, int bufLen, const char *content)
{
    FILE *f = NULL;

    if (5 + NUID_BUFFER_LEN + 1 > bufLen)
        abort();

    memcpy(buf, "conf_", 5);
    natsNUID_Next(buf+5, NUID_BUFFER_LEN+1);
    buf[5+NUID_BUFFER_LEN+1] = '\0';

    f = fopen(buf, "w");
    if (f == NULL)
        abort();

    fputs(content, f);
    fclose(f);
}

static void
test_JetStreamUnmarshalAccountInfo(void)
{
    natsStatus          s;
    nats_JSON           *json = NULL;
    natsJSAccountInfo   *ai   = NULL;
    const char          *missing[] = {
        "{\"memory\":1000}",
        "{\"memory\":1000,\"storage\":2000}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8,\"errors\":2}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8,\"errors\":2},\"limits\":{}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8,\"errors\":2},\"limits\":{\"max_memory\":3000}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8,\"errors\":2},\"limits\":{\"max_memory\":3000,\"max_storage\":4000}}",
        "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,\"api\":{\"total\":8,\"errors\":2},\"limits\":{\"max_memory\":3000,\"max_storage\":4000,\"max_streams\":10}}",
    };
    char tmp[512];
    int i;

    for (i=0; i<11; i++)
    {
        test("Missing fields: ");
        s = nats_JSONParse(&json, missing[i], (int) strlen(missing[i]));
        IFOK(s, natsJS_unmarshalAccountInfo(&ai, json));
        testCond((s == NATS_NOT_FOUND) && (ai == NULL));
        nats_JSONDestroy(json);
        json = NULL;
    }

    test("Account info complete: ");
    snprintf(tmp, sizeof(tmp), "{\"memory\":1000,\"storage\":2000,\"streams\":5,\"consumers\":7,"\
        "\"domain\":\"MyDomain\","\
        "\"api\":{\"total\":8,\"errors\":2},"\
        "\"limits\":{\"max_memory\":3000,\"max_storage\":4000,\"max_streams\":10,\"max_consumers\":20}}");
    s = nats_JSONParse(&json, tmp, (int) strlen(tmp));
    IFOK(s, natsJS_unmarshalAccountInfo(&ai, json));
    testCond((s == NATS_OK) && (ai != NULL)
                && (ai->Memory == 1000)
                && (ai->Store == 2000)
                && (ai->Streams == 5)
                && (ai->Consumers == 7)
                && (ai->Domain != NULL)
                && (strcmp(ai->Domain, "MyDomain") == 0)
                && (ai->API.Total == 8)
                && (ai->API.Errors == 2)
                && (ai->Limits.MaxMemory == 3000)
                && (ai->Limits.MaxStore == 4000)
                && (ai->Limits.MaxStreams == 10)
                && (ai->Limits.MaxConsumers == 20));
    nats_JSONDestroy(json);
    natsJSAccountInfo_Destroy(ai);
}

static void
test_JetStreamUnmarshalStreamConfig(void)
{
    natsStatus          s;
    nats_JSON           *json = NULL;
    natsJSStreamConfig  *sc   = NULL;
    const char          *missing[] = {
        "{\"name\":\"TEST\"}",
        "{\"name\":\"TEST\",\"retention\":\"limits\"}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5,\"max_msgs\":10}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000,\"max_age\":20000000}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000,\"max_age\":20000000,\"discard\":\"new\"}",
        "{\"name\":\"TEST\",\"retention\":\"limits\",\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000,\"max_age\":20000000,\"discard\":\"new\",\"storage\":\"memory\"}",
    };
    char tmp[1024];
    int i;

    for (i=0; i<8; i++)
    {
        test("Missing fields: ");
        s = nats_JSONParse(&json, missing[i], (int) strlen(missing[i]));
        IFOK(s, natsJS_unmarshalStreamConfig(&sc, json));
        testCond((s == NATS_NOT_FOUND) == (sc == NULL));
        nats_JSONDestroy(json);
        json = NULL;
    }

    test("Stream config with all required: ");
    snprintf(tmp, sizeof(tmp), "{\"name\":\"TEST\",\"retention\":\"workqueue\","\
        "\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000,\"max_msgs_per_subject\":1,\"max_age\":20000000,"\
        "\"discard\":\"new\",\"storage\":\"memory\",\"num_replicas\":3}");
    s = nats_JSONParse(&json, tmp, (int) strlen(tmp));
    IFOK(s, natsJS_unmarshalStreamConfig(&sc, json));
    testCond((s == NATS_OK) && (sc != NULL)
                && (strcmp(sc->Name, "TEST") == 0)
                && (sc->Retention == natsJS_WorkQueuePolicy)
                && (sc->MaxConsumers == 5)
                && (sc->MaxMsgs == 10)
                && (sc->MaxBytes == 1000)
                && (sc->MaxAge == 20000000)
                && (sc->MaxMsgsPerSubject == 1)
                && (sc->Discard == natsJS_DiscardNew)
                && (sc->Storage == natsJS_MemoryStorage)
                && (sc->Replicas == 3));
    natsJS_destroyStreamConfig(sc);
    sc = NULL;
    nats_JSONDestroy(json);
    json = NULL;

    test("Stream config with all: ");
    if (snprintf(tmp, sizeof(tmp), "{\"name\":\"TEST\",\"subjects\":[\"foo\",\"bar\"],"\
        "\"retention\":\"workqueue\",\"max_consumers\":5,\"max_msgs\":10,\"max_bytes\":1000,"\
        "\"max_age\":20000000,\"max_msg_size\":1024,\"max_msgs_per_subject\":1,\"discard\":\"new\",\"storage\":\"memory\","\
        "\"num_replicas\":3,\"no_ack\":true,\"template_owner\":\"owner\","\
        "\"duplicate_window\":100000000000,\"placement\":{\"cluster\":\"cluster\",\"tags\":[\"tag1\",\"tag2\"]},"\
        "\"mirror\":{\"name\":\"TEST2\",\"opt_start_seq\":10,\"filter_subject\":\"foo\",\"external\":{\"api\":\"my_prefix\",\"deliver\":\"deliver_prefix\"}},"\
        "\"sources\":[{\"name\":\"TEST3\",\"opt_start_seq\":20,\"filter_subject\":\"bar\",\"external\":{\"api\":\"my_prefix2\",\"deliver\":\"deliver_prefix2\"}}]}") >= (int) sizeof(tmp))
    {
        abort();
    }
    s = nats_JSONParse(&json, tmp, (int) strlen(tmp));
    IFOK(s, natsJS_unmarshalStreamConfig(&sc, json));
    testCond((s == NATS_OK) && (sc != NULL)
                && (sc->Subjects != NULL)
                && (sc->SubjectsLen == 2)
                && (strcmp(sc->Subjects[0], "foo") == 0)
                && (strcmp(sc->Subjects[1], "bar") == 0)
                && (sc->MaxMsgsPerSubject == 1)
                && (sc->MaxMsgSize == 1024)
                && (sc->NoAck)
                && (strcmp(sc->Template, "owner") == 0)
                && (sc->Duplicates == 100000000000)
                && (sc->Placement != NULL)
                && (strcmp(sc->Placement->Cluster, "cluster") == 0)
                && (sc->Placement->TagsLen == 2)
                && (strcmp(sc->Placement->Tags[0], "tag1") == 0)
                && (strcmp(sc->Placement->Tags[1], "tag2") == 0)
                && (sc->Mirror != NULL)
                && (strcmp(sc->Mirror->Name, "TEST2") == 0)
                && (sc->Mirror->OptStartSeq == 10)
                && (strcmp(sc->Mirror->FilterSubject, "foo") == 0)
                && (sc->Mirror->External != NULL)
                && (strcmp(sc->Mirror->External->APIPrefix, "my_prefix") == 0)
                && (strcmp(sc->Mirror->External->DeliverPrefix, "deliver_prefix") == 0)
                && (sc->SourcesLen == 1)
                && (strcmp(sc->Sources[0]->Name, "TEST3") == 0)
                && (sc->Sources[0]->OptStartSeq == 20)
                && (strcmp(sc->Sources[0]->FilterSubject, "bar") == 0)
                && (sc->Sources[0]->External != NULL)
                && (strcmp(sc->Sources[0]->External->APIPrefix, "my_prefix2") == 0)
                && (strcmp(sc->Sources[0]->External->DeliverPrefix, "deliver_prefix2") == 0));
    natsJS_destroyStreamConfig(sc);
    sc = NULL;
    nats_JSONDestroy(json);
    json = NULL;
}

static void
test_JetStreamMarshalStreamConfig(void)
{
    natsStatus           s;
    natsJSStreamConfig   sc;
    natsJSPlacement      p;
    natsJSStreamSource   m;
    natsJSExternalStream esm;
    natsJSStreamSource   s1;
    natsJSExternalStream esmS1;
    natsJSStreamSource   s2;
    natsJSExternalStream esmS2;
    const char           *subjects[]    = {"foo", "bar"};
    const char           *tags[]        = {"tag1", "tag2"};
    natsJSStreamSource   *sources[]     = {&s1, &s2};
    natsBuffer           *buf           = NULL;
    nats_JSON            *json          = NULL;
    natsJSStreamConfig   *rsc           = NULL;

    natsJSStreamConfig_Init(&sc);
    sc.Name = "MyStream";
    sc.Subjects = subjects;
    sc.SubjectsLen = 2;
    sc.Retention = natsJS_InterestPolicy;
    sc.MaxConsumers = 1;
    sc.MaxMsgs = 2;
    sc.MaxBytes = 3;
    sc.MaxAge = 4;
    sc.MaxMsgSize = 5;
    sc.Duplicates = 6;
    sc.MaxMsgsPerSubject = 1;
    sc.Discard = natsJS_DiscardNew;
    sc.Storage = natsJS_MemoryStorage;
    sc.Replicas = 3;
    sc.NoAck = true;
    sc.Template = "template";

    natsJSPlacement_Init(&p);
    p.Cluster = "MyCluster";
    p.Tags = tags;
    p.TagsLen = 2;
    sc.Placement = &p;

    natsJSStreamSource_Init(&m);
    m.Name = "AStream";
    m.OptStartSeq = 100;
    m.FilterSubject = "foo";
    natsJSExternalStream_Init(&esm);
    esm.APIPrefix = "mirror.prefix";
    esm.DeliverPrefix = "deliver.prefix";
    m.External = &esm;
    sc.Mirror = &m;

    natsJSStreamSource_Init(&s1);
    s1.Name = "StreamOne";
    s1.OptStartSeq = 10;
    s1.FilterSubject = "stream.one";
    natsJSExternalStream_Init(&esmS1);
    esmS1.APIPrefix = "source1.prefix";
    esmS1.DeliverPrefix = "source1.deliver.prefix";
    s1.External = &esmS1;

    natsJSStreamSource_Init(&s2);
    s2.Name = "StreamTwo";
    s2.OptStartSeq = 20;
    s2.FilterSubject = "stream.two";
    natsJSExternalStream_Init(&esmS2);
    esmS2.APIPrefix = "source2.prefix";
    esmS2.DeliverPrefix = "source2.deliver.prefix";
    s2.External = &esmS2;

    sc.Sources = sources;
    sc.SourcesLen = 2;

    test("Marshal stream config: ");
    s = natsJS_marshalStreamConfig(&buf, &sc);
    testCond((s == NATS_OK) && (buf != NULL) && (natsBuf_Len(buf) > 0));

    test("Verify with unmarshal: ");
    s = nats_JSONParse(&json, natsBuf_Data(buf), natsBuf_Len(buf));
    IFOK(s, natsJS_unmarshalStreamConfig(&rsc, json));
    testCond((s == NATS_OK) && (rsc != NULL)
                && (strcmp(rsc->Name, "MyStream") == 0)
                && (rsc->SubjectsLen == 2)
                && (rsc->Subjects != NULL)
                && (strcmp(rsc->Subjects[0], "foo") == 0)
                && (strcmp(rsc->Subjects[1], "bar") == 0)
                && (rsc->Retention == natsJS_InterestPolicy)
                && (rsc->MaxConsumers == 1)
                && (rsc->MaxMsgs == 2)
                && (rsc->MaxBytes == 3)
                && (rsc->MaxAge == 4)
                && (rsc->MaxMsgSize == 5)
                && (rsc->MaxMsgsPerSubject == 1)
                && (rsc->Duplicates == 6)
                && (rsc->Discard == natsJS_DiscardNew)
                && (rsc->Storage == natsJS_MemoryStorage)
                && (rsc->Replicas == 3)
                && rsc->NoAck
                && (strcmp(rsc->Template, "template") == 0)
                && (rsc->Placement != NULL)
                && (strcmp(rsc->Placement->Cluster, "MyCluster") == 0)
                && (rsc->Placement->TagsLen == 2)
                && (rsc->Placement->Tags != NULL)
                && (strcmp(rsc->Placement->Tags[0], "tag1") == 0)
                && (strcmp(rsc->Placement->Tags[1], "tag2") == 0)
                && (rsc->Mirror != NULL)
                && (strcmp(rsc->Mirror->Name, "AStream") == 0)
                && (rsc->Mirror->OptStartSeq == 100)
                && (strcmp(rsc->Mirror->FilterSubject, "foo") == 0)
                && (rsc->Mirror->External != NULL)
                && (strcmp(rsc->Mirror->External->APIPrefix, "mirror.prefix") == 0)
                && (strcmp(rsc->Mirror->External->DeliverPrefix, "deliver.prefix") == 0)
                && (rsc->SourcesLen == 2)
                && (rsc->Sources != NULL)
                && (strcmp(rsc->Sources[0]->Name, "StreamOne") == 0)
                && (rsc->Sources[0]->OptStartSeq == 10)
                && (strcmp(rsc->Sources[0]->FilterSubject, "stream.one") == 0)
                && (rsc->Sources[0]->External != NULL)
                && (strcmp(rsc->Sources[0]->External->APIPrefix, "source1.prefix") == 0)
                && (strcmp(rsc->Sources[0]->External->DeliverPrefix, "source1.deliver.prefix") == 0)
                && (strcmp(rsc->Sources[1]->Name, "StreamTwo") == 0)
                && (rsc->Sources[1]->OptStartSeq == 20)
                && (strcmp(rsc->Sources[1]->FilterSubject, "stream.two") == 0)
                && (rsc->Sources[1]->External != NULL)
                && (strcmp(rsc->Sources[1]->External->APIPrefix, "source2.prefix") == 0)
                && (strcmp(rsc->Sources[1]->External->DeliverPrefix, "source2.deliver.prefix") == 0));
    natsJS_destroyStreamConfig(rsc);
    natsBuf_Destroy(buf);
    nats_JSONDestroy(json);
}

static void
test_JetStreamContext(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsJS              *js = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsJSOptions       o;
    natsJSAccountInfo   *ai = NULL;
    natsJSErrCode       jerr = 0;
    char                datastore[256] = {'\0'};
    char                cmdLine[1024] = {'\0'};

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");

    pid = _startServer("nats://127.0.0.1:4222", "", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Check JS context invalid args: ")
    s = natsJS_NewContext(NULL, nc, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_NewContext(&js, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Init options (bad args): ");
    s = natsJSOptions_Init(NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Check JS context with negative wait: ")
    natsJSOptions_Init(&o);
    o.Wait = -10;
    s = natsJS_NewContext(&js, nc, &o);
    testCond((s == NATS_INVALID_ARG)
                && (strstr(nats_GetLastError(NULL), "negative") != NULL));
    o.Wait = 0;
    nats_clearLastError();

    test("Get context: ");
    s = natsJS_NewContext(&js, nc, NULL);
    testCond(s == NATS_OK);

    test("Get account info (bad args): ");
    s = natsJS_GetAccountInfo(NULL, js, NULL, NULL);
        s = natsJS_GetAccountInfo(&ai, NULL, NULL, NULL);
    testCond((s == NATS_INVALID_ARG) && (ai == NULL));
    nats_clearLastError();

    test("Get account fail: ");
    s = natsJS_GetAccountInfo(&ai, js, NULL, &jerr);
    testCond((s == NATS_NO_RESPONDERS) && (ai == NULL) && (jerr == JSNotEnabledErr));
    nats_clearLastError();
    jerr = 0;

    natsJS_DestroyContext(js);
    js = NULL;
    natsConnection_Destroy(nc);
    nc = NULL;

    // Restart the server with JS enabled
    _stopServer(pid);
    pid = NATS_INVALID_PID;
    test("Start JS server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Get context set unknown prefix: ");
    o.Prefix = "MyPrefix";
    s = natsJS_NewContext(&js, nc, &o);
    testCond(s == NATS_OK);

    test("Get account info with unknown prefix: ");
    s = natsJS_GetAccountInfo(&ai, js, NULL, &jerr);
    testCond((s == NATS_NO_RESPONDERS) && (ai == NULL) && (jerr == JSNotEnabledErr));
    nats_clearLastError();
    jerr = 0;

    test("Get account info override prefix: ");
    o.Prefix = jsDefaultAPIPrefix;
    s = natsJS_GetAccountInfo(&ai, js, &o, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    test("Prefix with trailing '.' works: ");
    o.Prefix = "$JS.API.";
    s = natsJS_GetAccountInfo(&ai, js, &o, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    natsJS_DestroyContext(js);
    js = NULL;

    test("Create context with prefix with traling '.': ");
    s = natsJS_NewContext(&js, nc, &o);
    IFOK(s, natsJS_GetAccountInfo(&ai, js, NULL, &jerr));
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (js != NULL)
                // Verify we don't touch the provided prefix
                && (strcmp(o.Prefix, "$JS.API.") == 0)
                && (ai != NULL)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    natsJS_DestroyContext(js);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    rmtree(datastore);
}

static void
test_JetStreamContextDomain(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsJS              *js = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsJSOptions       o;
    natsJSAccountInfo   *ai = NULL;
    natsJSErrCode       jerr = 0;
    char                datastore[256] = {'\0'};
    char                cmdLine[1024] = {'\0'};
    char                confFile[256] = {'\0'};

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");
    _createConfFile(confFile, sizeof(confFile),
        " jetstream: { domain: ABC }\n"
    );

    test("Start JS server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s -c %s", datastore, confFile);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Create context with domain: ");
    natsJSOptions_Init(&o);
    o.Domain = "ABC";
    s = natsJS_NewContext(&js, nc, &o);
    if (s == NATS_OK)
    {
        natsJS_lock(js);
        // The option "Domain" should create the following prefix.
        // The JS internal option should not have domain set.
        if ((strcmp(js->opts.Prefix, "$JS.ABC.API") != 0)
            || (js->opts.Domain != NULL))
        {
            s = NATS_ERR;
        }
        natsJS_unlock(js);
    }
    testCond((s == NATS_OK) && (js != NULL));

    test("Get account: ");
    s = natsJS_GetAccountInfo(&ai, js, NULL, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (strcmp(ai->Domain, "ABC") == 0)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    test("Get account with wrong domain override: ");
    o.Domain = "DEF";
    s = natsJS_GetAccountInfo(&ai, js, &o, &jerr);
    testCond((s == NATS_NO_RESPONDERS) && (ai == NULL) && (jerr == JSNotEnabledErr));
    nats_clearLastError();
    jerr = 0;

    natsJS_DestroyContext(js);
    js = NULL;

    test("Create context with mix prefix/domain: ");
    natsJSOptions_Init(&o);
    o.Prefix = "should.not.be.used";
    o.Domain = "ABC";
    s = natsJS_NewContext(&js, nc, &o);
    testCond((s == NATS_OK) && (js != NULL));

    test("Get account: ");
    s = natsJS_GetAccountInfo(&ai, js, NULL, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (strcmp(ai->Domain, "ABC") == 0)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    natsJS_DestroyContext(js);
    js = NULL;

    test("Create context without domain: ");
    natsJSOptions_Init(&o);
    s = natsJS_NewContext(&js, nc, &o);
    testCond((s == NATS_OK) && (js != NULL));

    test("Get account with domain override: ");
    o.Domain = "ABC";
    s = natsJS_GetAccountInfo(&ai, js, &o, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (strcmp(ai->Domain, "ABC") == 0)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    test("Get account with domain override (prefix option ignored): ");
    o.Prefix = "should.not.be.used";
    o.Domain = "ABC";
    s = natsJS_GetAccountInfo(&ai, js, &o, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (ai != NULL)
                && (strcmp(ai->Domain, "ABC") == 0)
                && (ai->Limits.MaxMemory == -1)
                && (ai->Limits.MaxStore == -1));
    natsJSAccountInfo_Destroy(ai);
    ai = NULL;

    natsJS_DestroyContext(js);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    rmtree(datastore);
    remove(confFile);
}

static void
test_JetStreamMgtStreams(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsJS              *js = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsJSStreamInfo    *si = NULL;
    natsJSStreamConfig  cfg;
    natsJSErrCode       jerr = 0;
    natsMsg             *resp = NULL;
    natsSubscription    *sub  = NULL;
    const char          *subjects[] = {"foo", "bar"};
    char                datastore[256] = {'\0'};
    char                cmdLine[1024] = {'\0'};
    natsJSOptions       o;
    natsJSPurgeOptions  po;

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");

    test("Start JS server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Get context: ");
    s = natsJS_NewContext(&js, nc, NULL);
    testCond(s == NATS_OK);

    test("Stream config init, bad args: ");
    s = natsJSStreamConfig_Init(NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Stream config init: ");
    s = natsJSStreamConfig_Init(&cfg);
    testCond(s == NATS_OK);

    test("Add stream, bad args: ");
    s = natsJS_AddStream(&si, NULL, &cfg, NULL, NULL);
    testCond((s == NATS_INVALID_ARG) && (si == NULL));
    nats_clearLastError();

    test("Stream name required: ");
    s = natsJS_AddStream(&si, js, NULL, NULL, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_AddStream(&si, js, &cfg, NULL, NULL);
    testCond((s == NATS_INVALID_ARG)
                && (si == NULL)
                && (strstr(nats_GetLastError(NULL), "stream name is required") != NULL));
    nats_clearLastError();

    test("Invalid stream name: ");
    cfg.Name = "invalid.stream.name";
    s = natsJS_AddStream(&si, js, &cfg, NULL, NULL);
    testCond((s == NATS_INVALID_ARG)
                && (si == NULL)
                && (strstr(nats_GetLastError(NULL), "invalid stream name") != NULL));
    nats_clearLastError();

    test("Create basic: ");
    cfg.Name = "TEST";
    s = natsJS_AddStream(&si, js, &cfg, NULL, &jerr);
    testCond((s == NATS_OK)
                && (si != NULL)
                && (si->Config != NULL)
                && (strcmp(si->Config->Name, "TEST") == 0)
                && (jerr == 0));
    natsJSStreamInfo_Destroy(si);
    si = NULL;

    test("Create failed because already used: ");
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 2;
    s = natsJS_AddStream(&si, js, &cfg, NULL, &jerr);
    testCond((s == NATS_ERR)
                && (si == NULL)
                && (strstr(nats_GetLastError(NULL), "already in use") != NULL)
                && ((jerr == 0) || (jerr == JSStreamNameExistErr)));
    nats_clearLastError();

    jerr = 0;
    // Reset config
    natsJSStreamConfig_Init(&cfg);
    test("Add stream without getting stream info back: ");
    cfg.Name = "TEST2";
    s = natsJS_AddStream(NULL, js, &cfg, NULL, &jerr);
    testCond((s == NATS_OK) && (jerr == 0) && (si == NULL));

    test("Update stream: ");
    cfg.MaxMsgs = 1000;
    s = natsJS_UpdateStream(&si, js, &cfg, NULL, &jerr);
    testCond((s == NATS_OK) && (jerr == 0)
                && (si != NULL)
                && (si->Config != NULL)
                && (si->Config->MaxMsgs == 1000));
    natsJSStreamInfo_Destroy(si);
    si = NULL;

    test("Send 1 message: ");
    s = natsConnection_Request(&resp, nc, "TEST2", "hello", 5, 1000);
    testCond(s == NATS_OK);
    natsMsg_Destroy(resp);
    resp = NULL;

    test("Get stream info (bad args): ");
    s = natsJS_GetStreamInfo(NULL, NULL, "TEST2", NULL, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_GetStreamInfo(NULL, js, "TEST2", NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Get stream info (stream name missing): ");
    s = natsJS_GetStreamInfo(&si, js, NULL, NULL, NULL);
    testCond((s == NATS_INVALID_ARG)
                && (si == NULL)
                && (strstr(nats_GetLastError(NULL), "stream name is required") != NULL));
    nats_clearLastError();

    test("Get stream info: ");
    s = natsJS_GetStreamInfo(&si, js, "TEST2", NULL, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (si != NULL)
                && (si->Config != NULL)
                && (strcmp(si->Config->Subjects[0], "TEST2") == 0)
                && (si->State.Msgs == 1));
    natsJSStreamInfo_Destroy(si);
    si = NULL;

    test("Purge stream (bad args): ");
    s = natsJS_PurgeStream(NULL, "TEST2", NULL, &jerr);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Purge stream (stream name missing): ");
    s = natsJS_PurgeStream(js, NULL, NULL, &jerr);
    testCond((s == NATS_INVALID_ARG)
                && (jerr == 0)
                && (strstr(nats_GetLastError(NULL), "stream name is required") != NULL));
    nats_clearLastError();

    test("Purge stream: ");
    s = natsJS_PurgeStream(js, "TEST2", NULL, &jerr);
    testCond((s == NATS_OK) && (jerr == 0));

    test("Get stream info (verify purged): ");
    s = natsJS_GetStreamInfo(&si, js, "TEST2", NULL, &jerr);
    testCond((s == NATS_OK)
                && (jerr == 0)
                && (si != NULL)
                && (si->Config != NULL)
                && (strcmp(si->Config->Subjects[0], "TEST2") == 0)
                && (si->State.Msgs == 0));
    natsJSStreamInfo_Destroy(si);
    si = NULL;

    test("Create stream: ");
    natsJSStreamConfig_Init(&cfg);
    cfg.Name = "TEST3";
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 2;
    s = natsJS_AddStream(NULL, js, &cfg, NULL, &jerr);
    testCond((s == NATS_OK) && (jerr == 0));

    test("Create sub to check purge req: ");
    s = natsConnection_SubscribeSync(&sub, nc, "$JS.API.STREAM.PURGE.TEST3");
    testCond(s == NATS_OK);

    test("Purge options init (bad args): ");
    s = natsJSPurgeOptions_Init(NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Purge with options (subj+seq): ");
    natsJSPurgeOptions_Init(&po);
    // Will purge only messages from "foo"
    po.Subject = "foo";
    // Purge up-to but do not include this sequence.
    po.Sequence = 4;
    natsJSOptions_Init(&o);
    o.Purge = &po;
    // We care only on the outbound request, not the result of the API call.
    natsJS_PurgeStream(js, "TEST3", &o, NULL);
    nats_clearLastError();
    s = natsSubscription_NextMsg(&resp, sub, 1000);
    testCond((s == NATS_OK)
                && (resp != NULL)
                && (strncmp("{\"filter\":\"foo\",\"seq\":4}",
                            natsMsg_GetData(resp),
                            natsMsg_GetDataLength(resp)) == 0));
    natsMsg_Destroy(resp);
    resp = NULL;

    test("Purge with options (seq and keep mutually exclusive): ");
    natsJSPurgeOptions_Init(&po);
    po.Subject = "bar";
    po.Sequence = 8;
    po.Keep = 2;
    natsJSOptions_Init(&o);
    o.Purge = &po;
    s = natsJS_PurgeStream(js, "TEST3", &o, &jerr);
    testCond((s == NATS_INVALID_ARG)
                && (jerr == 0)
                && (strstr(nats_GetLastError(NULL), "exclusive") != NULL));
    nats_clearLastError();

    test("Check no request was sent: ");
    s = natsSubscription_NextMsg(&resp, sub, 500);
    testCond((s == NATS_TIMEOUT) && (resp == NULL));
    nats_clearLastError();

    test("Purge with options (subj+keep): ");
    natsJSPurgeOptions_Init(&po);
    po.Subject = "bar";
    // Keep 2 messages in the stream's bar subject space.
    po.Keep = 2;
    natsJSOptions_Init(&o);
    o.Purge = &po;
    // We care only on the outbound request, not the result of the API call.
    natsJS_PurgeStream(js, "TEST3", &o, NULL);
    nats_clearLastError();
    s = natsSubscription_NextMsg(&resp, sub, 1000);
    testCond((s == NATS_OK)
                && (resp != NULL)
                && (strncmp("{\"filter\":\"bar\",\"keep\":2}",
                            natsMsg_GetData(resp),
                            natsMsg_GetDataLength(resp)) == 0));
    natsMsg_Destroy(resp);
    resp = NULL;

    test("Delete stream (bad args): ");
    s = natsJS_DeleteStream(NULL, "TEST2", NULL, &jerr);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Delete stream (stream name missing): ");
    s = natsJS_DeleteStream(js, NULL, NULL, &jerr);
    testCond((s == NATS_INVALID_ARG)
                && (jerr == 0)
                && (strstr(nats_GetLastError(NULL), "stream name is required") != NULL));
    nats_clearLastError();

    test("Delete stream: ");
    s = natsJS_DeleteStream(js, "TEST2", NULL, &jerr);
    testCond((s == NATS_OK) && (jerr == 0));

    test("Get stream info (not found): ");
    s = natsJS_GetStreamInfo(&si, js, "TEST2", NULL, &jerr);
    testCond((s == NATS_NOT_FOUND)
                && (si == NULL)
                && (nats_GetLastError(NULL) == NULL)
                && ((jerr == 0) || (jerr == JSStreamNotFoundErr)));

    natsJS_DestroyContext(js);
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    rmtree(datastore);
}

static void
test_JetStreamPublish(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsJS              *js = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsJSStreamConfig  cfg;
    natsJSPubOptions    opts;
    natsJSErrCode       jerr = 0;
    natsJSPubAck        *pa = NULL;
    natsMsg             *msg = NULL;
    char                datastore[256] = {'\0'};
    char                cmdLine[1024] = {'\0'};
    const char          *subjects[] = {"foo"};

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");

    test("Start JS server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Get context: ");
    s = natsJS_NewContext(&js, nc, NULL);
    testCond(s == NATS_OK);

    test("Stream config init: ");
    s = natsJSStreamConfig_Init(&cfg);
    testCond(s == NATS_OK);

    test("Add stream: ");
    cfg.Name = "TEST";
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    s = natsJS_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    test("Publish bad args: ");
    s = natsJS_Publish(NULL, NULL, NULL, "hello", 5, NULL, &jerr);
    if (s == NATS_INVALID_ARG)
        s = natsJS_Publish(NULL, js, NULL, "hello", 5, NULL, &jerr);
    if (s == NATS_INVALID_ARG)
        s = natsJS_Publish(NULL, js, "", "hello", 5, NULL, &jerr);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Options init bad args: ");
    s = natsJSPubOptions_Init(NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Publish bad ttl option: ");
    s = natsJSPubOptions_Init(&opts);
    if (s == NATS_OK)
    {
        opts.MaxWait = -10;
        s = natsJS_Publish(NULL, js, "foo", "hello", 5, &opts, &jerr);
    }
    testCond((s == NATS_INVALID_ARG)
                && (strstr(nats_GetLastError(NULL), "negative") != NULL));
    nats_clearLastError();

    test("Publish data: ");
    opts.MaxWait = 3000;
    s = natsJS_Publish(NULL, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0));

    test("Publish data with pubAck: ");
    opts.MsgId = "msg2";
    s = natsJS_Publish(&pa, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0) && (pa != NULL)
                && (strcmp(pa->Stream, "TEST") == 0)
                && (pa->Sequence == 2)
                && !pa->Duplicate);
    natsJSPubAck_Destroy(pa);
    pa = NULL;

    test("Publish message with same msgID: ");
    s = natsJS_Publish(&pa, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0) && (pa != NULL) && pa->Duplicate);
    natsJSPubAck_Destroy(pa);
    pa = NULL;

    test("Publish with wrong expected stream: ");
    opts.MsgId = NULL;
    opts.ExpectStream = "WRONG";
    s = natsJS_Publish(&pa, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamNotMatchErr)) && (pa == NULL));
    jerr = 0;

    test("Publish with wrong expected sequence: ");
    opts.MsgId = NULL;
    opts.ExpectStream = NULL;
    opts.ExpectLastSeq = 4;
    s = natsJS_Publish(&pa, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamWrongLastSequenceErr)) && (pa == NULL));
    jerr = 0;

    test("Publish with wrong expected message ID: ");
    opts.MsgId = NULL;
    opts.ExpectStream = NULL;
    opts.ExpectLastSeq = 0;
    opts.ExpectLastMsgId = "WRONG";
    s = natsJS_Publish(&pa, js, "foo", "hello", 5, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamWrongLastMsgIDErr)) && (pa == NULL));
    jerr = 0;

    // ---- Same than above but with PublishMsg variant
    test("Recreate stream: ");
    s = natsJS_DeleteStream(js, "TEST", NULL, NULL);
    IFOK(s, natsJS_AddStream(NULL, js, &cfg, NULL, NULL));
    testCond(s == NATS_OK);

    test("Publish bad args: ");
    s = natsJS_PublishMsg(NULL, NULL, NULL, NULL, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishMsg(NULL, js, NULL, NULL, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishMsg(NULL, NULL, msg, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Create msg: ");
    s = natsMsg_Create(&msg, "foo", NULL, "hello", 5);
    testCond(s == NATS_OK);

    test("Publish bad ttl option: ");
    s = natsJSPubOptions_Init(&opts);
    if (s == NATS_OK)
    {
        opts.MaxWait = -10;
        s = natsJS_PublishMsg(NULL, js, msg, &opts, &jerr);
    }
    testCond((s == NATS_INVALID_ARG)
                && (strstr(nats_GetLastError(NULL), "negative") != NULL));
    nats_clearLastError();

    test("Publish data: ");
    opts.MaxWait = 3000;
    s = natsJS_PublishMsg(NULL, js, msg, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0));

    test("Publish data with pubAck: ");
    opts.MsgId = "msg2";
    s = natsJS_PublishMsg(&pa, js, msg, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0) && (pa != NULL)
                && (strcmp(pa->Stream, "TEST") == 0)
                && (pa->Sequence == 2)
                && !pa->Duplicate);
    natsJSPubAck_Destroy(pa);
    pa = NULL;
    natsMsg_Destroy(msg);
    msg = NULL;
    natsMsg_Create(&msg, "foo", NULL, "hello", 5);

    test("Publish message with same msgID: ");
    s = natsJS_PublishMsg(&pa, js, msg, &opts, &jerr);
    testCond((s == NATS_OK) && (jerr == 0) && (pa != NULL) && pa->Duplicate);
    natsJSPubAck_Destroy(pa);
    pa = NULL;
    natsMsg_Destroy(msg);
    msg = NULL;
    natsMsg_Create(&msg, "foo", NULL, "hello", 5);

    test("Publish with wrong expected stream: ");
    opts.MsgId = NULL;
    opts.ExpectStream = "WRONG";
    s = natsJS_PublishMsg(&pa, js, msg, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamNotMatchErr)) && (pa == NULL));
    jerr = 0;
    natsMsg_Destroy(msg);
    msg = NULL;
    natsMsg_Create(&msg, "foo", NULL, "hello", 5);

    test("Publish with wrong expected sequence: ");
    opts.MsgId = NULL;
    opts.ExpectStream = NULL;
    opts.ExpectLastSeq = 4;
    s = natsJS_PublishMsg(&pa, js, msg, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamWrongLastSequenceErr)) && (pa == NULL));
    jerr = 0;
    natsMsg_Destroy(msg);
    msg = NULL;
    natsMsg_Create(&msg, "foo", NULL, "hello", 5);

    test("Publish with wrong expected message ID: ");
    opts.MsgId = NULL;
    opts.ExpectStream = NULL;
    opts.ExpectLastSeq = 0;
    opts.ExpectLastMsgId = "WRONG";
    s = natsJS_PublishMsg(&pa, js, msg, &opts, &jerr);
    testCond((s == NATS_ERR) && ((jerr == 0) || (jerr == JSStreamWrongLastMsgIDErr)) && (pa == NULL));
    jerr = 0;
    natsMsg_Destroy(msg);
    msg = NULL;

    natsJS_DestroyContext(js);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    rmtree(datastore);
}

static void
_jsPubAckErrHandler(natsJS *js, natsJSPubAckErr *pae, void *closure)
{
    struct threadArg    *args = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    args->sum++;
    if (strcmp(natsMsg_GetData(pae->Msg), "fail1") == 0)
    {
        if ((pae->Err == NATS_ERR)
            && ((pae->ErrCode == 0) || (pae->ErrCode == JSStreamWrongLastMsgIDErr))
            && (strstr(pae->ErrText, "wrong last") != NULL))
        {
            args->status = NATS_ERR;
        }
    }
    else if (strcmp(natsMsg_GetData(pae->Msg), "fail2") == 0)
    {
        // Resend only once
        if (args->sum == 2)
            natsJS_PublishMsgAsync(js, &(pae->Msg), NULL);
    }
    else if (strcmp(natsMsg_GetData(pae->Msg), "fail3") == 0)
    {
        while (!args->done)
            natsCondition_Wait(args->c, args->m);

        // Destroy context
        natsJS_DestroyContext(js);
        // Notify that we are done
        args->closed = true;
        natsCondition_Broadcast(args->c);
    }
    else if (strcmp(natsMsg_GetData(pae->Msg), "block") == 0)
    {
        while (!args->done)
            natsCondition_Wait(args->c, args->m);
        nats_Sleep(500);
    }
    natsMutex_Unlock(args->m);
}

static void
test_JetStreamPublishAsync(void)
{
    natsStatus          s;
    natsConnection      *nc = NULL;
    natsSubscription    *sub= NULL;
    natsJS              *js = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsJSOptions       o;
    natsJSStreamConfig  cfg;
    natsJSPubOptions    opts;
    natsMsg             *msg = NULL;
    natsMsg             *cmsg = NULL;
    const char          *val = NULL;
    const char          **keys = NULL;
    int                 keysCount = 0;
    char                datastore[256] = {'\0'};
    char                cmdLine[1024] = {'\0'};
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");

    test("Start JS server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    testCond(s == NATS_OK);

    test("Create control sub: ");
    s = natsConnection_SubscribeSync(&sub, nc, "foo");
    testCond(s == NATS_OK);

    test("Prepare JS options: ");
    s = natsJSOptions_Init(&o);
    if (s == NATS_OK)
    {
        o.PublishAsyncErrHandler        = _jsPubAckErrHandler;
        o.PublishAsyncErrHandlerClosure = &args;
    }
    testCond(s == NATS_OK);

    test("Get context: ");
    s = natsJS_NewContext(&js, nc, &o);
    testCond(s == NATS_OK);

    test("Stream config init: ");
    s = natsJSStreamConfig_Init(&cfg);
    testCond(s == NATS_OK);

    test("Add stream: ");
    cfg.Name = "foo";
    s = natsJS_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    test("Publish bad args: ");
    s = natsJS_PublishAsync(NULL, NULL, "hello", 5, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishAsync(js, NULL, "hello", 5, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishAsync(js, "", "hello", 5, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishAsync(NULL, "foo", "hello", 5, NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Publish data: ");
    s = natsJS_PublishAsync(js, "foo", "ok1", 3, NULL);
    IFOK(s, natsJS_PublishAsyncComplete(js, NULL));
    testCond(s == NATS_OK);

    test("Check pub msg no header and reply set: ");
    s = natsSubscription_NextMsg(&cmsg, sub, 1000);
    testCond((s == NATS_OK) && (cmsg != NULL)
                && !nats_IsStringEmpty(natsMsg_GetReply(cmsg))
                && (natsMsgHeader_Keys(cmsg, &keys, &keysCount) == NATS_NOT_FOUND));
    natsMsg_Destroy(cmsg);

    test("Publish msg (bad args): ");
    s = natsJS_PublishMsgAsync(NULL, NULL, NULL);
    if (s == NATS_INVALID_ARG)
        s = natsJS_PublishMsgAsync(NULL, &msg, NULL);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Reply subject should not be set: ");
    s = natsMsg_Create(&msg, "foo", "bar", "baz", 3);
    IFOK(s, natsJS_PublishMsgAsync(js, &msg, NULL));
    testCond((s == NATS_INVALID_ARG) && (msg != NULL)
                && (strstr(nats_GetLastError(NULL), "reply subject should not be set") != NULL));
    nats_clearLastError();
    natsMsg_Destroy(msg);
    msg = NULL;

    test("Publish msg: ");
    s = natsJSPubOptions_Init(&opts);
    if (s == NATS_OK)
        opts.MsgId = "msgID";
    IFOK(s, natsMsg_Create(&msg, "foo", NULL, "ok2", 3));
    IFOK(s, natsJS_PublishMsgAsync(js, &msg, &opts));
    // Check that library took ownership of message by checking
    // that msg is now NULL.
    testCond((s == NATS_OK) && (msg == NULL));

    test("Check pub msg reply set: ");
    s = natsSubscription_NextMsg(&cmsg, sub, 1000);
    testCond((s == NATS_OK) && (cmsg != NULL)
                && !nats_IsStringEmpty(natsMsg_GetReply(cmsg)));

    test("Check msg ID header set: ");
    s = natsMsgHeader_Get(cmsg, jsMsgIdHdr, &val);
    testCond((s == NATS_OK) && (strcmp(val, "msgID") == 0));
    natsMsg_Destroy(cmsg);
    val = NULL;

    test("Wait for complete (bad args): ");
    opts.MaxWait = -1000;
    s = natsJS_PublishAsyncComplete(js, &opts);
    testCond(s == NATS_INVALID_ARG);
    nats_clearLastError();

    test("Wait for complete: ");
    opts.MaxWait = 1000;
    s = natsJS_PublishAsyncComplete(js, &opts);
    testCond(s == NATS_OK);

    test("Send fails due to wrong last ID: ");
    args.status = NATS_OK;
    opts.MsgId = NULL;
    opts.ExpectLastMsgId = "wrong";
    s = natsMsg_Create(&msg, "foo", NULL, "fail1", 5);
    IFOK(s, natsJS_PublishMsgAsync(js, &msg, &opts));
    testCond((s == NATS_OK) && (msg == NULL));

    test("Check pub msg reply set: ");
    s = natsSubscription_NextMsg(&cmsg, sub, 1000);
    testCond((s == NATS_OK) && (cmsg != NULL)
                && !nats_IsStringEmpty(natsMsg_GetReply(cmsg)));

    test("Check msg ID header not set: ");
    s = natsMsgHeader_Get(cmsg, jsMsgIdHdr, &val);
    testCond((s == NATS_NOT_FOUND) && (val == NULL));

    test("Check expect last msg ID header set: ");
    s = natsMsgHeader_Get(cmsg, jsExpectedLastMsgIdHdr, &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "wrong") == 0));
    natsMsg_Destroy(cmsg);
    val = NULL;

    test("Wait for complete: ");
    s = natsJS_PublishAsyncComplete(js, NULL);
    testCond(s == NATS_OK);

    test("Check cb got proper failure: ")
    natsMutex_Lock(args.m);
    s = ((args.status == NATS_ERR) && (args.sum == 1) ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    test("Send new failed message, will be resent in cb: ");
    s = natsJS_PublishAsync(js, "foo", "fail2", 5, &opts);
    testCond(s == NATS_OK);

    test("Wait complete: ")
    s = natsJS_PublishAsyncComplete(js, NULL);
    if (s == NATS_OK)
    {
        natsMutex_Lock(args.m);
        s = (args.sum == 3 ? NATS_OK : NATS_ERR);
        natsMutex_Unlock(args.m);
    }
    testCond(s == NATS_OK);

    test("Send new failed messages which will block cb: ");
    s = natsJS_PublishAsync(js, "foo", "fail3", 5, &opts);
    // Send another message, which should not be delivered to CB
    // since we will destroy context from CB on releasing CB
    // after fail3 msg is processed.
    IFOK(s, natsJS_PublishAsync(js, "foo", "fail4", 5, &opts));
    testCond(s == NATS_OK);

    test("Check complete timeout: ");
    opts.MaxWait = 100;
    s = natsJS_PublishAsyncComplete(js, &opts);
    testCond(s == NATS_TIMEOUT);
    nats_clearLastError();

    test("Release cb which will destroy context: ");
    s = NATS_OK;
    natsMutex_Lock(args.m);
    args.done = true;
    natsCondition_Broadcast(args.c);
    while ((s != NATS_TIMEOUT) && !args.closed)
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    test("Check that last msg was not delivered to CB: ");
    natsMutex_Lock(args.m);
    // cb has seen: fail1, fail2 twice, fail3, so sum == 4
    s = (args.sum == 4 ? NATS_OK: NATS_ERR);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    js = NULL;
    natsJSOptions_Init(&o);
    test("Stall wait bad args: ");
    o.PublishAsyncStallWait = -10;
    s = natsJS_NewContext(&js, nc, &o);
    testCond((s == NATS_INVALID_ARG) && (js == NULL));
    nats_clearLastError();

    test("Recreate context: ");
    o.PublishAsyncMaxPending = 1;
    o.PublishAsyncStallWait  = 100;
    s = natsJS_NewContext(&js, nc, &o);
    testCond(s == NATS_OK);

    test("Block CB: ");
    natsMutex_Lock(args.m);
    args.done = false;
    args.closed = false;
    natsMutex_Unlock(args.m);
    // Pass options so that we add an expected last msg ID which will
    // cause failure.
    s = natsJS_PublishAsync(js, "foo", "block", 5, &opts);
    testCond(s == NATS_OK);

    test("Send should fail due to stall: ");
    s = natsJS_PublishAsync(js, "foo", "stalled", 7, NULL);
    testCond((s == NATS_TIMEOUT)
                && (strstr(nats_GetLastError(NULL), "too many outstanding") != NULL));
    nats_clearLastError();

    // Release CB, which will sleep a bit before returning, so that we
    // have time to start a publish here that we will check gets unstalled.
    // Artificially increase the stallWait so that we don't flap on Travis/etc..
    natsMutex_Lock(js->mu);
    js->opts.PublishAsyncStallWait = 10000;
    natsMutex_Unlock(js->mu);
    // should unstall the publish that we are going to make here.
    natsMutex_Lock(args.m);
    args.done = true;
    natsCondition_Broadcast(args.c);
    natsMutex_Unlock(args.m);

    test("Pub will stall: ");
    s = natsJS_PublishAsync(js, "foo", "ok", 2, NULL);
    testCond(s == NATS_OK);

    test("Wait complete: ");
    s = natsJS_PublishAsyncComplete(js, NULL);
    testCond(s == NATS_OK);

    test("Msg needs to be destroyed on failure: ");
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(nc);
    nc = NULL;
    s = natsMsg_Create(&msg, "foo", NULL, "conclosed", 9);
    IFOK(s, natsJS_PublishMsgAsync(js, &msg, NULL));
    testCond((s == NATS_CONNECTION_CLOSED) && (msg != NULL));
    nats_clearLastError();

    test("Msg destroy: ");
    natsMsg_Destroy(msg);
    testCond(true);

    natsJS_DestroyContext(js);
    _destroyDefaultThreadArgs(&args);
    _stopServer(pid);
    rmtree(datastore);
}

#if defined(NATS_HAS_STREAMING)

static int
_roundUp(int val)
{
    return ((val + (MEMALIGN-1))/MEMALIGN)*MEMALIGN;
}

static void
test_StanPBufAllocator(void)
{
    natsPBufAllocator   *a = NULL;
    natsStatus          s;
    char                *ptr1;
    char                *ptr2;
    char                *ptr3;
    char                *ptr4;
    char                *oldBuf;
    int                 oldCap;
    int                 expectedProtoSize;
    int                 expectedOverhead;
    int                 expectedUsed;
    int                 expectedRemaining;
    int                 expectedCap;
    int                 prevUsed;

    test("Create: ");
    s = natsPBufAllocator_Create(&a, 10, 2);
    expectedProtoSize = MEMALIGN + _roundUp(10);
    expectedOverhead  = (MEMALIGN * 2) + 2 + (2 * (MEMALIGN-1));
    testCond((s == NATS_OK)
            && (a->protoSize == expectedProtoSize)
            && (a->overhead == expectedOverhead)
            && (a->base.alloc != NULL)
            && (a->base.free != NULL)
            && (a->base.allocator_data == a));

    test("Prepare: ");
    natsPBufAllocator_Prepare(a, 20);
    expectedCap = expectedProtoSize + expectedOverhead + 20;
    testCond((a->buf != NULL)
            && (a->cap == expectedCap)
            && (a->remaining == a->cap)
            && (a->used == 0));

    test("Alloc 1: ");
    ptr1 = (char*) a->base.alloc((void*)a, 10);
    expectedUsed = MEMALIGN + _roundUp(10);
    expectedRemaining = expectedCap - expectedUsed;
    testCond((ptr1 != NULL)
            && ((ptr1-MEMALIGN) == a->buf)
            && ((ptr1-MEMALIGN)[0] == '0')
            && (a->used == expectedUsed)
            && (a->remaining == expectedRemaining));

    test("Alloc 2: ");
    ptr2 = (char*) a->base.alloc((void*)a, 5);
    prevUsed           = expectedUsed;
    expectedUsed      += MEMALIGN + _roundUp(5);
    expectedRemaining  = expectedCap - expectedUsed;
    testCond((ptr2 != ptr1)
            && ((ptr2-MEMALIGN) == (a->buf + prevUsed))
            && ((ptr2-MEMALIGN)[0] == '0')
            && (a->used == expectedUsed)
            && (a->remaining == expectedRemaining));

    test("Alloc 3: ");
    ptr3 = (char*) a->base.alloc((void*)a, 3);
    prevUsed           = expectedUsed;
    expectedUsed      += MEMALIGN + _roundUp(3);
    expectedRemaining  = expectedCap - expectedUsed;
    testCond((ptr3 != ptr2)
            && ((ptr3-MEMALIGN) == (a->buf + prevUsed))
            && ((ptr3-MEMALIGN)[0] == '0')
            && (a->used == expectedUsed)
            && (a->remaining == expectedRemaining));

    test("Alloc 4: ");
    ptr4 = (char*) a->base.alloc((void*)a, 50);
    testCond((ptr4 != ptr3)
            && (((ptr4-MEMALIGN) < a->buf) || ((ptr4-MEMALIGN) > (a->buf+a->cap)))
            && ((ptr4-MEMALIGN)[0] == '1')
            && (a->used == expectedUsed)
            && (a->remaining == expectedRemaining));

    // Free out of order, just make sure it does not crash
    // and valgrind will make sure that we freed ptr4.
    test("Free 2: ");
    a->base.free((void*) a, (void*) ptr2);
    testCond(1);

    test("Free 1: ");
    a->base.free((void*) a, (void*) ptr1);
    testCond(1);

    test("Free 4: ");
    a->base.free((void*) a, (void*) ptr4);
    testCond(1);

    test("Free 3: ");
    a->base.free((void*) a, (void*) ptr3);
    testCond(1);

    // Call prepare again with smaller buffer, buf should
    // remain same, but used/remaining should be updated.
    oldBuf = a->buf;
    oldCap = a->cap;
    test("Prepare with smaller buffer: ");
    natsPBufAllocator_Prepare(a, 5);
    testCond((a->buf == oldBuf)
            && (a->cap == oldCap)
            && (a->remaining == a->cap)
            && (a->used == 0));

    test("Prepare requires expand: ");
    natsPBufAllocator_Prepare(a, 100);
    // Realloc may or may not make a->buf be different...
    expectedCap = expectedProtoSize + expectedOverhead + 100;
    testCond((a->buf != NULL)
            && (a->cap == expectedCap)
            && (a->remaining == a->cap)
            && (a->used == 0));

    test("Destroy: ");
    natsPBufAllocator_Destroy(a);
    testCond(1);
}

static void
_stanConnLostCB(stanConnection *sc, const char *errorTxt, void *closure)
{
    struct threadArg *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->closed = true;
    arg->status = NATS_OK;
    if ((arg->string != NULL) && (strcmp(errorTxt, arg->string) != 0))
        arg->status = NATS_ERR;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
}

static void
test_StanConnOptions(void)
{
    natsStatus      s;
    stanConnOptions *opts = NULL;
    stanConnOptions *clone= NULL;
    natsOptions     *no   = NULL;

    test("Create option: ");
    s = stanConnOptions_Create(&opts);
    testCond(s == NATS_OK);

    test("Has default values: ");
    testCond(
            (opts->connTimeout == STAN_CONN_OPTS_DEFAULT_CONN_TIMEOUT) &&
            (opts->connectionLostCB == stanConn_defaultConnLostHandler) &&
            (opts->connectionLostCBClosure == NULL) &&
            (strcmp(opts->discoveryPrefix, STAN_CONN_OPTS_DEFAULT_DISCOVERY_PREFIX) == 0) &&
            (opts->maxPubAcksInFlightPercentage == STAN_CONN_OPTS_DEFAULT_MAX_PUB_ACKS_INFLIGHT_PERCENTAGE) &&
            (opts->maxPubAcksInflight == STAN_CONN_OPTS_DEFAULT_MAX_PUB_ACKS_INFLIGHT) &&
            (opts->ncOpts == NULL) &&
            (opts->pingInterval == STAN_CONN_OPTS_DEFAULT_PING_INTERVAL) &&
            (opts->pingMaxOut == STAN_CONN_OPTS_DEFAULT_PING_MAX_OUT) &&
            (opts->pubAckTimeout == STAN_CONN_OPTS_DEFAULT_PUB_ACK_TIMEOUT) &&
            (opts->url == NULL));

    test("Check invalid connection wait: ");
    s = stanConnOptions_SetConnectionWait(opts, -10);
    if (s != NATS_OK)
        s = stanConnOptions_SetConnectionWait(opts, 0);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid discovery prefix: ");
    s = stanConnOptions_SetDiscoveryPrefix(opts, NULL);
    if (s != NATS_OK)
        s = stanConnOptions_SetDiscoveryPrefix(opts, "");
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid max pub acks: ");
    s = stanConnOptions_SetMaxPubAcksInflight(opts, -1, 1);
    if (s != NATS_OK)
        s = stanConnOptions_SetMaxPubAcksInflight(opts, 0, 1);
    if (s != NATS_OK)
        s = stanConnOptions_SetMaxPubAcksInflight(opts, 10, -1);
    if (s != NATS_OK)
        s = stanConnOptions_SetMaxPubAcksInflight(opts, 10, 0);
    if (s != NATS_OK)
        s = stanConnOptions_SetMaxPubAcksInflight(opts, 10, 2);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid pings: ");
    s = stanConnOptions_SetPings(opts, -1, 10);
    if (s != NATS_OK)
        s = stanConnOptions_SetPings(opts, 0, 10);
    if (s != NATS_OK)
        s = stanConnOptions_SetPings(opts, 1, -1);
    if (s != NATS_OK)
        s = stanConnOptions_SetPings(opts, 1, 0);
    if (s != NATS_OK)
        s = stanConnOptions_SetPings(opts, 1, 1);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid pub ack wait: ");
    s = stanConnOptions_SetPubAckWait(opts, -1);
    if (s != NATS_OK)
        s = stanConnOptions_SetPubAckWait(opts, 0);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Set values: ");
    s = stanConnOptions_SetConnectionWait(opts, 10000);
    IFOK(s, stanConnOptions_SetDiscoveryPrefix(opts, "myPrefix"));
    IFOK(s, stanConnOptions_SetMaxPubAcksInflight(opts, 10, (float) 0.8));
    IFOK(s, stanConnOptions_SetPings(opts, 1, 10));
    IFOK(s, stanConnOptions_SetPubAckWait(opts, 2000));
    IFOK(s, stanConnOptions_SetURL(opts, "nats://me:1"));
    IFOK(s, stanConnOptions_SetConnectionLostHandler(opts, _stanConnLostCB, (void*) 1));
    testCond((s == NATS_OK) &&
            (opts->connTimeout == 10000) &&
            (strcmp(opts->discoveryPrefix, "myPrefix") == 0) &&
            (opts->maxPubAcksInFlightPercentage == (float) 0.8) &&
            (opts->maxPubAcksInflight == 10) &&
            (opts->pingInterval == 1) &&
            (opts->pingMaxOut == 10) &&
            (opts->pubAckTimeout == 2000) &&
            (strcmp(opts->url, "nats://me:1") == 0) &&
            (opts->connectionLostCB == _stanConnLostCB) &&
            (opts->connectionLostCBClosure == (void*) 1)
            );

    test("Set NATS options: ");
    s = natsOptions_Create(&no);
    IFOK(s, natsOptions_SetMaxPendingMsgs(no, 1000));
    IFOK(s, stanConnOptions_SetNATSOptions(opts, no));
    // change value from no after setting to stan opts
    // check options were cloned.
    IFOK(s, natsOptions_SetMaxPendingMsgs(no, 2000));
    testCond((s == NATS_OK) &&
            (opts->ncOpts != NULL) && // set
            (opts->ncOpts != no) &&   // not a reference
            (opts->ncOpts->maxPendingMsgs == 1000) // original value
            );

    test("Check clone: ");
    s = stanConnOptions_clone(&clone, opts);
    // Change values from original, check that clone
    // keeps original values.
    IFOK(s, stanConnOptions_SetConnectionWait(opts, 3000));
    IFOK(s, stanConnOptions_SetDiscoveryPrefix(opts, "xxxxx"));
    IFOK(s, stanConnOptions_SetMaxPubAcksInflight(opts, 100, (float) 0.2));
    IFOK(s, stanConnOptions_SetPings(opts, 10, 20));
    IFOK(s, stanConnOptions_SetPubAckWait(opts, 3000));
    IFOK(s, stanConnOptions_SetURL(opts, "nats://metoo:1"));
    IFOK(s, stanConnOptions_SetConnectionLostHandler(opts, NULL, NULL));
    IFOK(s, stanConnOptions_SetNATSOptions(opts, NULL));
    testCond((s == NATS_OK) &&
            (clone != opts) &&
            (clone->connTimeout == 10000) &&
            (strcmp(clone->discoveryPrefix, "myPrefix") == 0) &&
            (clone->maxPubAcksInFlightPercentage == (float) 0.8) &&
            (clone->maxPubAcksInflight == 10) &&
            (clone->pingInterval == 1) &&
            (clone->pingMaxOut == 10) &&
            (clone->pubAckTimeout == 2000) &&
            (strcmp(clone->url, "nats://me:1") == 0) &&
            (clone->connectionLostCB == _stanConnLostCB) &&
            (clone->connectionLostCBClosure == (void*) 1) &&
            (clone->ncOpts != NULL) &&
            (clone->ncOpts != no) &&
            (clone->ncOpts->maxPendingMsgs == 1000)
            );

    test("Check cb and NATS options can be set to NULL: ");
    testCond(
            (opts->ncOpts == NULL) &&
            (opts->connectionLostCB == NULL) &&
            (opts->connectionLostCBClosure == NULL));

    test("Check URL can be set to NULL: ");
    s = stanConnOptions_SetURL(opts, NULL);
    testCond(s == NATS_OK);

    test("Check clone ok after destroy original: ");
    stanConnOptions_Destroy(opts);
    testCond((s == NATS_OK) &&
                (clone->connTimeout == 10000) &&
                (strcmp(clone->discoveryPrefix, "myPrefix") == 0) &&
                (clone->maxPubAcksInFlightPercentage == (float) 0.8) &&
                (clone->maxPubAcksInflight == 10) &&
                (clone->pingInterval == 1) &&
                (clone->pingMaxOut == 10) &&
                (clone->pubAckTimeout == 2000) &&
                (strcmp(clone->url, "nats://me:1") == 0) &&
                (clone->connectionLostCB == _stanConnLostCB) &&
                (clone->connectionLostCBClosure == (void*) 1) &&
                (clone->ncOpts != NULL) &&
                (clone->ncOpts != no) &&
                (clone->ncOpts->maxPendingMsgs == 1000)
                );

    natsOptions_Destroy(no);
    stanConnOptions_Destroy(clone);
}

static void
test_StanSubOptions(void)
{
    natsStatus      s;
    stanSubOptions  *opts = NULL;
    stanSubOptions  *clone= NULL;
    int64_t         now   = 0;

    test("Create Options: ");
    s = stanSubOptions_Create(&opts);
    testCond(s == NATS_OK);

    test("Default values: ");
    testCond(
            (opts->ackWait == STAN_SUB_OPTS_DEFAULT_ACK_WAIT) &&
            (opts->durableName == NULL) &&
            (opts->manualAcks == false) &&
            (opts->maxInflight == STAN_SUB_OPTS_DEFAULT_MAX_INFLIGHT) &&
            (opts->startAt == PB__START_POSITION__NewOnly) &&
            (opts->startSequence == 0) &&
            (opts->startTime == 0)
            );

    test("Check invalid ackwait: ");
    s = stanSubOptions_SetAckWait(opts, -1);
    if (s != NATS_OK)
        s = stanSubOptions_SetAckWait(opts, 0);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid maxinflight: ");
    s = stanSubOptions_SetMaxInflight(opts, -1);
    if (s != NATS_OK)
        s = stanSubOptions_SetMaxInflight(opts, 0);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid start seq: ");
    s = stanSubOptions_StartAtSequence(opts, 0);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid start time: ");
    s = stanSubOptions_StartAtTime(opts, -1);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check invalid start time: ");
    s = stanSubOptions_StartAtTimeDelta(opts, -1);
    testCond(s != NATS_OK);
    nats_clearLastError();

    test("Check set values: ");
    s = stanSubOptions_SetAckWait(opts, 1000);
    IFOK(s, stanSubOptions_SetDurableName(opts, "myDurable"));
    IFOK(s, stanSubOptions_SetManualAckMode(opts, true));
    IFOK(s, stanSubOptions_SetMaxInflight(opts, 200));
    testCond((s == NATS_OK) &&
            (opts->ackWait == 1000) &&
            (strcmp(opts->durableName, "myDurable") == 0) &&
            (opts->manualAcks == true) &&
            (opts->maxInflight == 200)
            );

    now = nats_Now();
    test("Check start at time delta: ");
    s = stanSubOptions_StartAtTimeDelta(opts, 20000);
    testCond((s == NATS_OK) &&
            (opts->startAt == PB__START_POSITION__TimeDeltaStart) &&
                ((opts->startTime >= now-20200) &&
                 (opts->startTime <= now-19800))
            );

    test("Check start at time: ");
    s = stanSubOptions_StartAtTime(opts, 1234567890);
    testCond((s == NATS_OK) &&
            (opts->startAt == PB__START_POSITION__TimeDeltaStart) &&
            (opts->startTime == 1234567890)
            );

    test("Check start at seq: ");
    s = stanSubOptions_StartAtSequence(opts, 100);
    testCond((s == NATS_OK) &&
            (opts->startAt == PB__START_POSITION__SequenceStart) &&
            (opts->startSequence == 100)
            );

    test("Check deliver all avail: ");
    s = stanSubOptions_DeliverAllAvailable(opts);
    testCond((s == NATS_OK) && (opts->startAt == PB__START_POSITION__First));

    test("Check clone: ");
    s = stanSubOptions_clone(&clone, opts);
    // Change values of opts to show that this does not affect
    // the clone
    IFOK(s, stanSubOptions_SetAckWait(opts, 20000));
    IFOK(s, stanSubOptions_SetDurableName(opts, NULL));
    IFOK(s, stanSubOptions_SetManualAckMode(opts, false));
    IFOK(s, stanSubOptions_SetMaxInflight(opts, 4000));
    IFOK(s, stanSubOptions_StartAtSequence(opts, 100));
    testCond((s == NATS_OK) &&
            (clone != opts) &&
            (clone->ackWait == 1000) &&
            (strcmp(clone->durableName, "myDurable") == 0) &&
            (clone->manualAcks == true) &&
            (clone->maxInflight == 200) &&
            (clone->startAt == PB__START_POSITION__First)
            );

    test("Check clone ok after destroy original: ");
    stanSubOptions_Destroy(opts);
    testCond((s == NATS_OK) &&
            (clone != opts) &&
            (clone->ackWait == 1000) &&
            (strcmp(clone->durableName, "myDurable") == 0) &&
            (clone->manualAcks == true) &&
            (clone->maxInflight == 200) &&
            (clone->startAt == PB__START_POSITION__First)
            );

    stanSubOptions_Destroy(clone);
}

static void
test_StanMsg(void)
{
    test("GetSequence with NULL msg: ");
    testCond(stanMsg_GetSequence(NULL) == 0);

    test("GetData with NULL msg: ");
    testCond(stanMsg_GetData(NULL) == NULL);

    test("GetDataLength with NULL msg: ");
    testCond(stanMsg_GetDataLength(NULL) == 0);

    test("GetTimestamp with NULL msg: ");
    testCond(stanMsg_GetTimestamp(NULL) == 0);

    test("IsRedelivered with NULL msg: ");
    testCond(stanMsg_IsRedelivered(NULL) == false);

    stanMsg_Destroy(NULL);
}

static void
test_StanServerNotReachable(void)
{
    natsStatus      s;
    stanConnection  *sc = NULL;
    stanConnOptions *opts = NULL;
    natsPid         serverPid = NATS_INVALID_PID;
    int64_t         now = 0;
    int64_t         elapsed = 0;

    s = stanConnOptions_Create(&opts);
    IFOK(s, stanConnOptions_SetURL(opts, "nats://127.0.0.1:4222"));
    IFOK(s, stanConnOptions_SetConnectionWait(opts, 250));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    serverPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(serverPid);

    test("Connect fails if no streaming server running: ");
    now = nats_Now();
    s = stanConnection_Connect(&sc, clusterName, clientName, opts);
    elapsed = nats_Now()-now;
    if (serverVersionAtLeast(2, 2, 0))
    {
        testCond((s == NATS_NO_RESPONDERS) &&
                 (strstr(nats_GetLastError(NULL), STAN_ERR_CONNECT_REQUEST_NO_RESP) != NULL));
    }
    else
    {
        testCond((s == NATS_TIMEOUT) &&
                 (strstr(nats_GetLastError(NULL), STAN_ERR_CONNECT_REQUEST_TIMEOUT) != NULL) &&
                 (elapsed < 2000));
    }

    stanConnOptions_Destroy(opts);

    _stopServer(serverPid);
}

static void
test_StanBasicConnect(void)
{
    natsStatus      s;
    stanConnection  *sc = NULL;
    natsPid         pid = NATS_INVALID_PID;
    stanConnOptions *opts   = NULL;
    natsOptions     *nopts  = NULL;

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Basic connect: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("Connection close: ");
    s = stanConnection_Close(sc);
    testCond(s == NATS_OK);

    test("Connection double close: ");
    s = stanConnection_Close(sc);
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);
    sc = NULL;

    _stopServer(pid);
    pid = NATS_INVALID_PID;

    pid = _startStreamingServer("nats://127.0.0.1:4223", "-p 4223", true);
    CHECK_SERVER_STARTED(pid);

    test("Connect with non default stan URL: ");
    s = stanConnOptions_Create(&opts);
    IFOK(s, stanConnOptions_SetURL(opts, "nats://127.0.0.1:4223"));
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, opts));
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);
    sc = NULL;

    test("stan URL takes precedence: ");
    s = natsOptions_Create(&nopts);
    IFOK(s, natsOptions_SetURL(nopts, "nats://127.0.0.1:4224")); // wrong URL
    IFOK(s, stanConnOptions_SetNATSOptions(opts, nopts));
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, opts));
    // Should connect because it should use the one from stanConnOptions_SetURL.
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);
    sc = NULL;

    test("If no stan URL set, uses NATS URL: ");
    s = stanConnOptions_SetURL(opts, NULL);
    IFOK(s, natsOptions_SetURL(nopts, "nats://127.0.0.1:4223"));
    IFOK(s, stanConnOptions_SetNATSOptions(opts, nopts));
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, opts));
    // Should connect because it should use the one from StanURL.
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);
    natsOptions_Destroy(nopts);

    _stopServer(pid);
}

static void
test_StanConnectError(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanConnection      *sc2 = NULL;
    natsPid             nPid = NATS_INVALID_PID;
    natsPid             sPid = NATS_INVALID_PID;
    stanConnOptions     *opts = NULL;

    nPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(nPid);

    sPid = _startStreamingServer("nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222", true);
    CHECK_SERVER_STARTED(sPid);

    test("Check connect response error: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    IFOK(s, stanConnection_Connect(&sc2, clusterName, clientName, NULL));
    testCond((s == NATS_ERR) &&
            (strstr(nats_GetLastError(NULL), "clientID already registered") != NULL));

    test("Check wrong discovery prefix: ");
    s = stanConnOptions_Create(&opts);
    IFOK(s, stanConnOptions_SetDiscoveryPrefix(opts, "wrongprefix"));
    IFOK(s, stanConnOptions_SetConnectionWait(opts, 500));
    IFOK(s, stanConnection_Connect(&sc2, clusterName, "newClient", opts));
    testCond(serverVersionAtLeast(2, 2, 0) ? (s == NATS_NO_RESPONDERS) : (s == NATS_TIMEOUT));

    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);

    _stopServer(sPid);
    _stopServer(nPid);
}


static void
test_StanBasicPublish(void)
{
    natsStatus      s;
    stanConnection  *sc = NULL;
    natsPid         pid = NATS_INVALID_PID;

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Basic publish: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    IFOK(s, stanConnection_Publish(sc, "foo", (const void*) "hello", 5));
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);

    _stopServer(pid);
}

static void
_stanPubAckHandler(const char *guid, const char *errTxt, void* closure)
{
    struct threadArg *args= (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    args->status = NATS_OK;
    if (errTxt != NULL)
    {
        if ((args->string == NULL) || (strstr(errTxt, args->string) == NULL))
            args->status = NATS_ERR;
    }
    else if (args->string != NULL)
    {
        args->status = NATS_ERR;
    }
    args->msgReceived = true;
    natsCondition_Signal(args->c);
    natsMutex_Unlock(args->m);
}

static void
test_StanBasicPublishAsync(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Basic publish async: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    IFOK(s, stanConnection_PublishAsync(sc, "foo", (const void*) "hello", 5,
                                        _stanPubAckHandler, (void*) &args));
    testCond(s == NATS_OK);

    test("PubAck callback report no error: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.msgReceived)
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    IFOK(s, args.status);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
test_StanPublishTimeout(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    struct threadArg    args;
    natsPid             nPid = NATS_INVALID_PID;
    natsPid             sPid = NATS_INVALID_PID;
    stanConnOptions     *opts = NULL;

    s = _createDefaultThreadArgsForCbTests(&args);
    IFOK(s, stanConnOptions_Create(&opts));
    IFOK(s, stanConnOptions_SetPubAckWait(opts, 50));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    nPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(nPid);

    sPid = _startStreamingServer("nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222", true);
    CHECK_SERVER_STARTED(sPid);

    // First connect, then once that's done, shutdown streaming server
    s = stanConnection_Connect(&sc, clusterName, clientName, opts);

    _stopServer(sPid);

    if (s != NATS_OK)
    {
        _stopServer(nPid);
        FAIL("Not able to create connection for this test");
    }

    args.string = STAN_ERR_PUB_ACK_TIMEOUT;

    test("Check publish async timeout");
    s = stanConnection_PublishAsync(sc, "foo", (const void*) "hello", 5,
                                    _stanPubAckHandler, (void*) &args);
    testCond(s == NATS_OK);

    test("PubAck callback report pub ack timeout error: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.msgReceived)
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    IFOK(s, args.status);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    // Speed up test by closing stan's nc connection to avoid timing out on conn close
    stanConnClose(sc, false);

    stanConnOptions_Destroy(opts);
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(nPid);
}

static void
_stanPublishAsyncThread(void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;
    int i;

    for (i = 0; i < 10; i++)
        stanConnection_PublishAsync(args->sc, "foo", (const void*)"hello", 5, NULL, NULL);
}

static void
_stanPublishSyncThread(void *closure)
{
    stanConnection *sc = (stanConnection*) closure;

    stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
}

static void
test_StanPublishMaxAcksInflight(void)
{
    natsStatus          s;
    stanConnection      *sc1 = NULL;
    stanConnection      *sc2 = NULL;
    struct threadArg    args;
    natsPid             nPid = NATS_INVALID_PID;
    natsPid             sPid = NATS_INVALID_PID;
    stanConnOptions     *opts = NULL;
    natsThread          *t = NULL;
    natsThread          *pts[10];
    int                 i;
    natsConnection      *nc = NULL;

    for (i=0;i<10;i++)
        pts[i] = NULL;

    s = _createDefaultThreadArgsForCbTests(&args);
    IFOK(s, stanConnOptions_Create(&opts));
    IFOK(s, stanConnOptions_SetMaxPubAcksInflight(opts, 5, 1.0));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    nPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(nPid);

    sPid = _startStreamingServer("nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222", true);
    CHECK_SERVER_STARTED(sPid);

    // First connect, then once that's done, shutdown streaming server
    s = stanConnection_Connect(&sc1, clusterName, clientName, opts);
    IFOK(s, stanConnection_Connect(&sc2, clusterName, "otherClient", opts));
    if (s != NATS_OK)
    {
        stanConnection_Destroy(sc1);
        stanConnection_Destroy(sc2);
        _stopServer(sPid);
        _stopServer(nPid);
        FAIL("Not able to create connection for this test");
    }

    _stopServer(sPid);

    // grap nc first
    natsMutex_Lock(sc1->mu);
    nc = sc1->nc;
    natsMutex_Unlock(sc1->mu);

    test("Check max inflight: ");
    args.sc = sc1;
    // Retain the connection
    stanConn_retain(sc1);
    s = natsThread_Create(&t, _stanPublishAsyncThread, (void*) &args);
    if (s == NATS_OK)
    {
        int i = 0;
        // Check that pubAckMap size is never greater than 5.
        for (i=0; (s == NATS_OK) && (i<10); i++)
        {
            nats_Sleep(100);
            natsMutex_Lock(sc1->pubAckMu);
            s = (natsStrHash_Count(sc1->pubAckMap) <= 5 ? NATS_OK : NATS_ERR);
            natsMutex_Unlock(sc1->pubAckMu);
        }
    }
    testCond(s == NATS_OK);

    test("Close unblock: ");
    natsConn_close(nc);
    nc = NULL;
    stanConnection_Destroy(sc1);
    natsThread_Join(t);
    natsThread_Destroy(t);
    stanConn_release(sc1);
    testCond(s == NATS_OK);

    // Repeat test with sync publishers

    // grap nc first
    natsMutex_Lock(sc2->mu);
    nc = sc2->nc;
    natsMutex_Unlock(sc2->mu);

    test("Check max inflight: ");
    // Retain the connection
    stanConn_retain(sc2);
    for (i=0; (s == NATS_OK) && (i<10); i++)
        s = natsThread_Create(&(pts[i]), _stanPublishSyncThread, (void*) sc2);
    if (s == NATS_OK)
    {
        int i = 0;
        // Check that pubAckMap size is never greater than 5.
        for (i=0; (s == NATS_OK) && (i<10); i++)
        {
            nats_Sleep(100);
            natsMutex_Lock(sc2->pubAckMu);
            s = (natsStrHash_Count(sc2->pubAckMap) <= 5 ? NATS_OK : NATS_ERR);
            natsMutex_Unlock(sc2->pubAckMu);
        }
    }
    testCond(s == NATS_OK);

    test("Close unblock: ");
    natsConn_close(nc);
    nc = NULL;
    stanConnection_Destroy(sc2);
    for (i = 0; i<10; i++)
    {
        if (pts[i] != NULL)
        {
            natsThread_Join(pts[i]);
            natsThread_Destroy(pts[i]);
        }
    }
    stanConn_release(sc2);
    testCond(s == NATS_OK);

    stanConnOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(nPid);
}

static void
_dummyStanMsgHandler(stanConnection *sc, stanSubscription *sub, const char *channel,
                     stanMsg *msg, void* closure)
{
    stanMsg_Destroy(msg);
}

static void
_stanMsgHandlerBumpSum(stanConnection *sc, stanSubscription *sub, const char *channel,
                      stanMsg *msg, void* closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    if (!stanMsg_IsRedelivered(msg))
        args->sum++;
    else
        args->redelivered++;
    natsCondition_Broadcast(args->c);
    natsMutex_Unlock(args->m);

    stanMsg_Destroy(msg);
}

static void
test_StanBasicSubscription(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *sub = NULL;
    stanSubscription    *subf = NULL;
    natsPid             pid = NATS_INVALID_PID;

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Basic subscibe: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    testCond(s == NATS_OK);

    test("Close connection: ")
    s = stanConnection_Close(sc);
    testCond(s == NATS_OK);

    test("Subscribe should fail after conn closed: ");
    s = stanConnection_Subscribe(&subf, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    testCond(s == NATS_CONNECTION_CLOSED);

    test("Subscribe should fail after conn closed: ");
    s = stanConnection_QueueSubscribe(&subf, sc, "foo", "bar", _dummyStanMsgHandler, NULL, NULL);
    testCond(s == NATS_CONNECTION_CLOSED);

    stanSubscription_Destroy(sub);
    stanConnection_Destroy(sc);

    _stopServer(pid);
}

static void
test_StanSubscriptionCloseAndUnsubscribe(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *sub = NULL;
    stanSubscription    *sub2 = NULL;
    natsPid             pid = NATS_INVALID_PID;
    natsPid             spid = NATS_INVALID_PID;
    char                *cs = NULL;
    stanConnOptions     *opts = NULL;

    s = stanConnOptions_Create(&opts);
    IFOK(s, stanConnOptions_SetConnectionWait(opts, 250));
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    spid = _startStreamingServer("nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222", true);
    CHECK_SERVER_STARTED(spid);

    s = stanConnection_Connect(&sc, clusterName, clientName, opts);
    if (s != NATS_OK)
    {
        _stopServer(spid);
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Unsubscribe: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    IFOK(s, stanSubscription_Unsubscribe(sub));
    testCond(s == NATS_OK);

    stanSubscription_Destroy(sub);
    sub = NULL;

    test("Close: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    IFOK(s, stanSubscription_Close(sub));
    testCond(s == NATS_OK);

    stanSubscription_Destroy(sub);
    sub = NULL;

    test("Close not supported: ");
    // Simulate that we connected to an older server
    natsMutex_Lock(sc->mu);
    cs = sc->subCloseRequests;
    sc->subCloseRequests = NULL;
    natsMutex_Unlock(sc->mu);
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    IFOK(s, stanSubscription_Close(sub));
    testCond((s == NATS_NO_SERVER_SUPPORT) &&
            (strstr(nats_GetLastError(NULL), STAN_ERR_SUB_CLOSE_NOT_SUPPORTED) != NULL));

    stanSubscription_Destroy(sub);
    sub = NULL;

    natsMutex_Lock(sc->mu);
    sc->subCloseRequests = cs;
    natsMutex_Unlock(sc->mu);

    test("Close/Unsub timeout: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    IFOK(s, stanConnection_Subscribe(&sub2, sc, "foo", _dummyStanMsgHandler, NULL, NULL));

    // Stop the serer
    _stopServer(spid);

    if (s == NATS_OK)
    {
        s = stanSubscription_Close(sub);
        if (s != NATS_OK)
            s = stanSubscription_Unsubscribe(sub2);
    }
    if (serverVersionAtLeast(2, 2, 0))
    {
        testCond((s == NATS_NO_RESPONDERS) &&
                 (strstr(nats_GetLastError(NULL), "no streaming server was listening") != NULL));
    }
    else
    {
        testCond((s == NATS_TIMEOUT) &&
                 (strstr(nats_GetLastError(NULL), "request timeout") != NULL));
    }
    stanSubscription_Destroy(sub);
    stanSubscription_Destroy(sub2);

    stanConnClose(sc, false);
    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);

    _stopServer(pid);
}

static void
test_StanDurableSubscription(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *dur = NULL;
    stanSubOptions      *opts = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    args;
    int                 i;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Error setting up test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Send some messages: ");
    for (i=0; (s == NATS_OK) && (i<3); i++)
        s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Basic durable subscibe: ");
    s = stanSubOptions_Create(&opts);
    IFOK(s, stanSubOptions_SetDurableName(opts, "dur"));
    IFOK(s, stanSubOptions_DeliverAllAvailable(opts));
    IFOK(s, stanConnection_Subscribe(&dur, sc, "foo", _stanMsgHandlerBumpSum, &args, opts));
    testCond(s == NATS_OK);

    test("Check 3 messages received: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 3))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    // Wait a bit to give a chance for the server to process acks.
    nats_Sleep(500);

    test("Close connection: ");
    s = stanConnection_Close(sc);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(dur);
    dur = NULL;
    stanConnection_Destroy(sc);
    sc = NULL;

    test("Connect again: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("Send 2 more messages: ");
    for (i=0; (s == NATS_OK) && (i<2); i++)
        s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Recreate durable with start seq 1: ");
    s = stanSubOptions_StartAtSequence(opts, 1);
    IFOK(s, stanConnection_Subscribe(&dur, sc, "foo", _stanMsgHandlerBumpSum, &args, opts));
    testCond(s == NATS_OK);

    test("Check 5 messages total are received: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 5))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    testCond(s == NATS_OK);
    test("Check no redelivered: ");
    testCond((s == NATS_OK) && (args.redelivered == 0));
    natsMutex_Unlock(args.m);

    stanSubscription_Destroy(dur);
    stanSubOptions_Destroy(opts);
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
test_StanBasicQueueSubscription(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *qsub1 = NULL;
    stanSubscription    *qsub2 = NULL;
    stanSubscription    *qsub3 = NULL;
    stanSubOptions      *opts = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Error setting up test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Basic queue subscibe: ");
    s = stanConnection_QueueSubscribe(&qsub1, sc, "foo", "bar", _stanMsgHandlerBumpSum, &args, NULL);
    IFOK(s, stanConnection_QueueSubscribe(&qsub2, sc, "foo", "bar", _stanMsgHandlerBumpSum, &args, NULL));
    testCond(s == NATS_OK);

    // Test that durable and non durable queue subscribers with
    // same name can coexist and they both receive the same message.
    test("New durable queue sub with same queue name: ");
    s = stanSubOptions_Create(&opts);
    IFOK(s, stanSubOptions_SetDurableName(opts, "durable-queue-sub"));
    IFOK(s, stanConnection_QueueSubscribe(&qsub3, sc, "foo", "bar", _stanMsgHandlerBumpSum, &args, opts));
    testCond(s == NATS_OK);

    test("Check published message ok: ");
    s = stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
    testCond(s == NATS_OK);

    test("Check 1 message published is received once per group: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 2))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(qsub1);
    stanSubscription_Destroy(qsub2);
    stanSubscription_Destroy(qsub3);
    stanSubOptions_Destroy(opts);
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
test_StanDurableQueueSubscription(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *dur = NULL;
    stanSubOptions      *opts = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    args;
    int                 i;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Error setting up test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Send some messages: ");
    for (i=0; (s == NATS_OK) && (i<3); i++)
        s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Basic durable subscibe: ");
    s = stanSubOptions_Create(&opts);
    IFOK(s, stanSubOptions_SetDurableName(opts, "dur"));
    IFOK(s, stanSubOptions_DeliverAllAvailable(opts));
    IFOK(s, stanConnection_QueueSubscribe(&dur, sc, "foo", "bar", _stanMsgHandlerBumpSum, &args, opts));
    testCond(s == NATS_OK);

    test("Check 3 messages received: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 3))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    // Give a chance for the server to process those acks
    nats_Sleep(500);

    test("Close connection: ");
    s = stanConnection_Close(sc);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(dur);
    dur = NULL;
    stanConnection_Destroy(sc);
    sc = NULL;

    test("Connect again: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("Send 2 more messages: ");
    for (i=0; (s == NATS_OK) && (i<2); i++)
        s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Recreate durable with start seq 1: ");
    s = stanSubOptions_StartAtSequence(opts, 1);
    IFOK(s, stanConnection_QueueSubscribe(&dur, sc, "foo", "bar", _stanMsgHandlerBumpSum, &args, opts));
    testCond(s == NATS_OK);

    test("Check 5 messages total are received: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 5))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    testCond(s == NATS_OK);
    test("Check no redelivered: ");
    testCond((s == NATS_OK) && (args.redelivered == 0));
    natsMutex_Unlock(args.m);

    stanSubscription_Destroy(dur);
    stanSubOptions_Destroy(opts);
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
_stanCheckRecvStanMsg(stanConnection *sc, stanSubscription *sub, const char *channel,
        stanMsg *msg, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    if (strcmp(channel, args->channel) != 0)
        args->status = NATS_ERR;
    if ((args->status == NATS_OK) && (strncmp(args->string, (char*) stanMsg_GetData(msg), strlen(args->string)) != 0))
        args->status = NATS_ERR;
    if ((args->status == NATS_OK) && (stanMsg_GetDataLength(msg) != 5))
        args->status = NATS_ERR;
    if ((args->status == NATS_OK) && (stanMsg_GetSequence(msg) == 0))
        args->status = NATS_ERR;
    if ((args->status == NATS_OK) && (stanMsg_GetTimestamp(msg) == 0))
        args->status = NATS_ERR;
    stanMsg_Destroy(msg);
    args->done = true;
    natsCondition_Signal(args->c);
    natsMutex_Unlock(args->m);
}

static void
test_StanCheckReceivedvMsg(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *sub = NULL;
    natsPid             pid = NATS_INVALID_PID;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Error setting up test");
    args.channel = "foo";
    args.string = "hello";

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Create sub: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _stanCheckRecvStanMsg, (void*) &args, NULL);
    testCond(s == NATS_OK);

    test("Send a message: ");
    s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Check message received: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && !args.done)
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    s = args.status;
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(sub);
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
_stanManualAck(stanConnection *sc, stanSubscription *sub, const char *channel,
               stanMsg *msg, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;
    natsStatus s;

    natsMutex_Lock(args->m);
    // control 1 means auto-ack, so expect ack to fail
    s = stanSubscription_AckMsg(sub, msg);
    args->status = NATS_OK;
    if ((args->control == 1) &&
            (s != NATS_ERR) &&
            (strstr(nats_GetLastError(NULL), STAN_ERR_MANUAL_ACK) == NULL))
    {
        args->status = NATS_ERR;
    }
    else if ((args->control == 2) && (s != NATS_OK))
    {
        args->status = NATS_ERR;
    }
    args->sum++;
    natsCondition_Signal(args->c);
    stanMsg_Destroy(msg);
    natsMutex_Unlock(args->m);
}

static void
_stanGetMsg(stanConnection *sc, stanSubscription *sub, const char *channel,
            stanMsg *msg, void *closure)
{
    struct threadArg *args = (struct threadArg*) closure;

    natsMutex_Lock(args->m);
    args->sMsg = msg;
    args->msgReceived = true;
    natsCondition_Signal(args->c);
    natsMutex_Unlock(args->m);
}

static void
test_StanSubscriptionAckMsg(void)
{
    natsStatus          s;
    stanConnection      *sc = NULL;
    stanSubscription    *sub = NULL;
    stanSubscription    *sub2 = NULL;
    natsPid             pid = NATS_INVALID_PID;
    stanSubOptions      *opts = NULL;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    IFOK(s, stanSubOptions_Create(&opts));
    IFOK(s, stanSubOptions_SetManualAckMode(opts, true));
    if (s != NATS_OK)
        FAIL("Error setting up test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    if (s != NATS_OK)
    {
        _stopServer(pid);
        FAIL("Unable to create connection for this test");
    }

    test("Create sub with auto-ack: ");
    args.control = 1;
    s = stanConnection_Subscribe(&sub, sc, "foo", _stanManualAck, (void*) &args, NULL);
    testCond(s == NATS_OK);

    test("Publish message: ");
    s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Check manual ack fails: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 1))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    if (s == NATS_OK)
        s = args.status;
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(sub);
    sub = NULL;

    natsMutex_Lock(args.m);
    args.control = 2;
    args.sum     = 0;
    natsMutex_Unlock(args.m);

    test("Create sub with manual ack: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _stanManualAck, (void*) &args, opts);
    testCond(s == NATS_OK);

    test("Publish message: ");
    s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Check manual ack ok: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 1))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    if (s == NATS_OK)
        s = args.status;
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    stanSubscription_Destroy(sub);
    sub = NULL;

    test("Create sub and get message: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _stanGetMsg, (void*) &args, NULL);
    IFOK(s, stanConnection_Publish(sc, "foo", (const void*) "hello", 5));
    if (s == NATS_OK)
    {
        natsMutex_Lock(args.m);
        while ((s != NATS_TIMEOUT) && !args.msgReceived)
            s = natsCondition_TimedWait(args.c, args.m, 2000);
        natsMutex_Unlock(args.m);
    }
    testCond(s == NATS_OK)

    test("Create sub with manual ack: ");
    s = stanConnection_Subscribe(&sub2, sc, "foo", _dummyStanMsgHandler, NULL, opts);
    testCond(s == NATS_OK);

    test("NULL Sub should fail: ");
    s = stanSubscription_AckMsg(NULL, args.sMsg);
    testCond(s == NATS_INVALID_ARG);

    test("NULL Msg should fail: ");
    s = stanSubscription_AckMsg(sub2, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Sub acking not own message fails: ");
    s = stanSubscription_AckMsg(sub2, args.sMsg);
    testCond((s == NATS_ILLEGAL_STATE)
                && (nats_GetLastError(NULL) != NULL)
                && (strstr(nats_GetLastError(NULL), STAN_ERR_SUB_NOT_OWNER) != NULL));

    natsMutex_Lock(args.m);
    if (args.sMsg != NULL)
        stanMsg_Destroy(args.sMsg);
    natsMutex_Unlock(args.m);

    stanSubscription_Destroy(sub);
    stanSubscription_Destroy(sub2);
    stanConnection_Destroy(sc);
    stanSubOptions_Destroy(opts);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
test_StanPings(void)
{
    natsStatus          s;
    natsPid             pid = NATS_INVALID_PID;
    stanConnection      *sc = NULL;
    stanConnOptions     *opts = NULL;
    natsConnection      *nc = NULL;
    natsSubscription    *psub = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        testAllowMillisecInPings = true;
        s = stanConnOptions_Create(&opts);
    }
    IFOK(s, stanConnOptions_SetPings(opts, -50, 5));
    if (s == NATS_OK)
    {
        arg.string = STAN_ERR_MAX_PINGS;
        s = stanConnOptions_SetConnectionLostHandler(opts, _stanConnLostCB, (void*) &arg);
    }
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        stanConnOptions_Destroy(opts);
        testAllowMillisecInPings = false;
        FAIL("Unable to setup test");
    }

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    if (pid == NATS_INVALID_PID)
    {
        testAllowMillisecInPings = false;
        FAIL("Unable to start server");
    }

    // Create NATS Subscription on pings subject and count the
    // outgoing pings.
    test("Pings are sent: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    // Use _recvTestString with control == 3 to increment sum up to 10
    if (s == NATS_OK)
    {
        char psubject[256];

        snprintf(psubject, sizeof(psubject), "%s.%s.pings", STAN_CONN_OPTS_DEFAULT_DISCOVERY_PREFIX, clusterName);
        arg.control = 3;
        s = natsConnection_Subscribe(&psub, nc, psubject, _recvTestString, (void*) &arg);
    }

    // Connect to Stan
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, opts));

    // We should start receiving PINGs
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && (arg.sum < 10))
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    if ((s == NATS_OK) && (arg.sum < 10))
        s = NATS_ERR;
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    natsSubscription_Destroy(psub);
    natsConnection_Destroy(nc);

    test("Connection lost handler invoked: ");
    _stopServer(pid);
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Connection closed: ");
    natsMutex_Lock(sc->mu);
    s = (sc->closed ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(sc->mu);
    testCond(s == NATS_OK);

    stanConnClose(sc, false);
    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);
    testAllowMillisecInPings = false;

    _destroyDefaultThreadArgs(&arg);
}

static void
test_StanPingsNoResponder(void)
{
    natsStatus          s;
    natsPid             nPid = NATS_INVALID_PID;
    natsPid             sPid = NATS_INVALID_PID;
    stanConnection      *sc = NULL;
    stanConnOptions     *opts = NULL;
    struct threadArg    arg;

    nPid = _startServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(nPid);

    sPid = _startStreamingServer("nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222", true);
    CHECK_SERVER_STARTED(sPid);

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        testAllowMillisecInPings = true;
        s = stanConnOptions_Create(&opts);
    }
    IFOK(s, stanConnOptions_SetPings(opts, -50, 5));
    IFOK(s, stanConnOptions_SetConnectionLostHandler(opts, _stanConnLostCB, (void*) &arg));
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        stanConnOptions_Destroy(opts);
        testAllowMillisecInPings = false;
        FAIL("Unable to setup test");
    }

    // Connect to Stan
    test("Connects ok: ")
    s = stanConnection_Connect(&sc, clusterName, clientName, opts);
    testCond(s == NATS_OK);

    // Shutdown the streaming server, but not NATS Server
    _stopServer(sPid);

    // We should get the connection lost callback invoked.
    test("Connection lost: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);
    testAllowMillisecInPings = false;

    _stopServer(nPid);
    _destroyDefaultThreadArgs(&arg);
}

static void
test_StanConnectionLostHandlerNotSet(void)
{
    natsStatus          s;
    natsPid             pid = NATS_INVALID_PID;
    stanConnection      *sc = NULL;
    stanConnOptions     *opts = NULL;
    natsOptions         *nOpts = NULL;
    struct threadArg    arg;

    testAllowMillisecInPings = true;

    s = _createDefaultThreadArgsForCbTests(&arg);
    IFOK(s, natsOptions_Create(&nOpts));
    IFOK(s, natsOptions_SetClosedCB(nOpts, _closedCb, (void*)&arg));
    IFOK(s, stanConnOptions_Create(&opts));
    IFOK(s, stanConnOptions_SetNATSOptions(opts, nOpts));
    IFOK(s, stanConnOptions_SetPings(opts, -50, 5));
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        stanConnOptions_Destroy(opts);
        testAllowMillisecInPings = false;
        FAIL("Unable to setup test");
    }

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    if (pid == NATS_INVALID_PID)
    {
        testAllowMillisecInPings = false;
        FAIL("Unable to start server");
    }

    test("Connect: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, opts);
    testCond(s == NATS_OK);

    // Stop server and wait for closed notification
    _stopServer(pid);

    test("Connection closed: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Connection closed: ");
    natsMutex_Lock(sc->mu);
    s = (sc->closed ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(sc->mu);
    testCond(s == NATS_OK);

    stanConnClose(sc, false);
    stanConnection_Destroy(sc);
    stanConnOptions_Destroy(opts);
    natsOptions_Destroy(nOpts);
    testAllowMillisecInPings = false;

    _destroyDefaultThreadArgs(&arg);
}

static void
test_StanPingsUnblockPubCalls(void)
{
    natsStatus          s;
    natsPid             pid = NATS_INVALID_PID;
    stanConnection      *sc = NULL;
    stanConnOptions     *opts = NULL;
    natsThread          *t    = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s == NATS_OK)
    {
        testAllowMillisecInPings = true;
        s = stanConnOptions_Create(&opts);
    }
    IFOK(s, stanConnOptions_SetMaxPubAcksInflight(opts, 1, 1.0));
    IFOK(s, stanConnOptions_SetPings(opts, -100, 5));
    if (s == NATS_OK)
    {
        arg.string = STAN_ERR_MAX_PINGS;
        s = stanConnOptions_SetConnectionLostHandler(opts, _stanConnLostCB, (void*) &arg);
    }
    if (s != NATS_OK)
    {
        _destroyDefaultThreadArgs(&arg);
        stanConnOptions_Destroy(opts);
        testAllowMillisecInPings = false;
        FAIL("Unable to setup test");
    }

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    if (pid == NATS_INVALID_PID)
    {
        testAllowMillisecInPings = false;
        FAIL("Unable to start server");
    }

    test("Connect: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, opts);
    testCond(s == NATS_OK);

    _stopServer(pid);

    natsThread_Create(&t, _stanPublishAsyncThread, (void*) &arg);

    test("Sync publish released: ");
    s = stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
    testCond(s != NATS_OK);
    nats_clearLastError();
    s = NATS_OK;

    test("Async publish released: ");
    if (t != NULL)
    {
        natsThread_Join(t);
        natsThread_Destroy(t);
    }
    testCond(s == NATS_OK);

    test("Connection lost handler invoked: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.closed)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    IFOK(s, arg.status);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Connection closed: ");
    natsMutex_Lock(sc->mu);
    s = (sc->closed ? NATS_OK : NATS_ERR);
    natsMutex_Unlock(sc->mu);
    testCond(s == NATS_OK);

    stanConnClose(sc, false);
    stanConnection_Destroy(sc);

    stanConnOptions_Destroy(opts);
    testAllowMillisecInPings = false;

    _destroyDefaultThreadArgs(&arg);
}

static void
test_StanGetNATSConnection(void)
{
    natsStatus          s;
    natsPid             pid     = NATS_INVALID_PID;
    stanConnection      *sc     = NULL;
    stanSubscription    *ssub   = NULL;
    natsConnection      *nc     = NULL;
    natsConnection      *nc2    = NULL;
    natsSubscription    *nsub   = NULL;
    struct threadArg    args;

    s = _createDefaultThreadArgsForCbTests(&args);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Connect: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("Create sub: ");
    s = stanConnection_Subscribe(&ssub, sc, "foo", _stanMsgHandlerBumpSum, (void*) &args, NULL);
    testCond(s == NATS_OK);

    test("Get NATS Connection: ");
    s = stanConnection_GetNATSConnection(sc, &nc);
    testCond((s == NATS_OK) && (nc != NULL));

    test("Create sub & pub with NATS OK: ");
    s = natsConnection_Subscribe(&nsub, nc, "foo", _dummyMsgHandler, NULL);
    IFOK(s, natsConnection_PublishString(nc, "foo", "hello"));
    testCond(s == NATS_OK);

    test("Invalid to drain: ");
    s = natsConnection_Drain(nc);
    if (s == NATS_ILLEGAL_STATE)
        s = natsConnection_DrainTimeout(nc, 1000);
    if ((s == NATS_ILLEGAL_STATE)
            && (nats_GetLastError(NULL) != NULL)
            && (strstr(nats_GetLastError(NULL), "owned") != NULL))
    {
        s = NATS_OK;
        nats_clearLastError();
    }
    testCond(s == NATS_OK);

    test("Closing NATS Conn has no effect: ");
    natsConnection_Close(nc);
    s = stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
    testCond(s == NATS_OK);

    test("Destroying NATS Conn has no effect: ");
    natsConnection_Destroy(nc);
    s = stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
    testCond(s == NATS_OK);

    test("Calling release more than get does not crash: ");
    stanConnection_ReleaseNATSConnection(sc);
    stanConnection_ReleaseNATSConnection(sc);
    s = stanConnection_Publish(sc, "foo", (const void*)"hello", 5);
    testCond(s == NATS_OK);

    test("Should have received 3 messages: ");
    natsMutex_Lock(args.m);
    while ((s != NATS_TIMEOUT) && (args.sum != 3))
        s = natsCondition_TimedWait(args.c, args.m, 2000);
    natsMutex_Unlock(args.m);
    testCond(s == NATS_OK);

    nc2 = NULL;
    test("Invalid arg (sc == NULL): ");
    s = stanConnection_GetNATSConnection(NULL, &nc2);
    testCond((s == NATS_INVALID_ARG) && (nc2 == NULL));
    nats_clearLastError();

    test("Invalid arg (nc == NULL): ");
    s = stanConnection_GetNATSConnection(sc, NULL);
    testCond((s == NATS_INVALID_ARG) && (nc2 == NULL));
    nats_clearLastError();

    test("GetNATSConnection on closed connection fails: ");
    s = stanConnection_Close(sc);
    IFOK(s, stanConnection_GetNATSConnection(sc, &nc2));
    testCond(s == NATS_CONNECTION_CLOSED);
    nats_clearLastError();

    natsSubscription_Destroy(nsub);
    stanSubscription_Destroy(ssub);
    // Valgrind will tell us if the NATS connection/STAN connection
    // were properly released.
    stanConnection_Destroy(sc);

    _destroyDefaultThreadArgs(&args);

    _stopServer(pid);
}

static void
test_StanNoRetryOnFailedConnect(void)
{
    natsStatus      s;
    natsOptions     *opts  = NULL;
    stanConnOptions *sopts = NULL;
    stanConnection  *sc    = NULL;

    test("RetryOnFailedConnect not supported: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_SetRetryOnFailedConnect(opts, true, _dummyConnHandler, (void*)1));
    IFOK(s, stanConnOptions_Create(&sopts));
    IFOK(s, stanConnOptions_SetNATSOptions(sopts, opts));
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, sopts));
    testCond(s == NATS_NO_SERVER);

    natsOptions_Destroy(opts);
    stanConnOptions_Destroy(sopts);
}

static bool
_subDlvThreadPooled(natsSubscription *sub)
{
    bool pooled;
    natsSub_Lock(sub);
    pooled = (sub->libDlvWorker != NULL);
    natsSub_Unlock(sub);
    return pooled;
}

static void
test_StanInternalSubsNotPooled(void)
{
    natsStatus          s;
    natsPid             pid         = NATS_INVALID_PID;
    natsOptions         *opts       = NULL;
    stanConnOptions     *sopts      = NULL;
    stanSubscription    *sub        = NULL;
    stanConnection      *sc         = NULL;
    natsSubscription    *hbSub      = NULL;
    natsSubscription    *ackSub     = NULL;
    natsSubscription    *pingSub    = NULL;
    natsSubscription    *inboxSub   = NULL;

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Can connet: ");
    s = natsOptions_Create(&opts);
    IFOK(s, natsOptions_UseGlobalMessageDelivery(opts, true));
    IFOK(s, stanConnOptions_Create(&sopts));
    IFOK(s, stanConnOptions_SetNATSOptions(sopts, opts));
    IFOK(s, stanConnection_Connect(&sc, clusterName, clientName, sopts));
    testCond(s == NATS_OK);

    test("Create sub: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
    testCond(s == NATS_OK);

    stanConn_Lock(sc);
    hbSub   = sc->hbSubscription;
    ackSub  = sc->ackSubscription;
    pingSub = sc->pingSub;
    stanConn_Unlock(sc);

    stanSub_Lock(sub);
    inboxSub = sub->inboxSub;
    stanSub_Unlock(sub);

    test("HBSub not pooled: ");
    testCond((s == NATS_OK) && !_subDlvThreadPooled(hbSub));

    test("AckSub not pooled: ");
    testCond((s == NATS_OK) && !_subDlvThreadPooled(ackSub));

    test("PingSub not pooled: ");
    testCond((s == NATS_OK) && !_subDlvThreadPooled(pingSub));

    test("InboxSub pooled: ");
    testCond((s == NATS_OK) && _subDlvThreadPooled(inboxSub));

    natsOptions_Destroy(opts);
    stanConnOptions_Destroy(sopts);
    stanSubscription_Destroy(sub);
    stanConnection_Destroy(sc);

    _stopServer(pid);
}

static void
_stanSubOnComplete(void *closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    if (arg->control == 1)
    {
        arg->done = true;
        natsCondition_Signal(arg->c);
    }
    natsMutex_Unlock(arg->m);
}

static void
_stanSubOnCompleteMsgCB(stanConnection *sc, stanSubscription *sub, const char *channel,
    stanMsg *msg, void* closure)
{
    struct threadArg    *arg = (struct threadArg*) closure;

    natsMutex_Lock(arg->m);
    arg->msgReceived = true;
    natsCondition_Signal(arg->c);
    natsMutex_Unlock(arg->m);
    // Sleep to simulate callbac doing some processing and let the
    // main thread close the subscription.
    nats_Sleep(500);
    // Update a field that _stanSubOnComplete will ensure is set to
    // prove that the onComplete is invoked after the msg callback
    // has returned.
    natsMutex_Lock(arg->m);
    arg->control = 1;
    natsMutex_Unlock(arg->m);

    stanMsg_Destroy(msg);
}

static void
test_StanSubOnComplete(void)
{
    natsStatus          s;
    natsPid             pid         = NATS_INVALID_PID;
    stanConnection      *sc         = NULL;
    stanSubscription    *sub        = NULL;
    struct threadArg    arg;

    s = _createDefaultThreadArgsForCbTests(&arg);
    if (s != NATS_OK)
        FAIL("Unable to setup test");

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Can connet: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("SetOnComplete error: ");
    s = stanSubscription_SetOnCompleteCB(NULL, _stanSubOnComplete, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Create sub: ");
    s = stanConnection_Subscribe(&sub, sc, "foo", _stanSubOnCompleteMsgCB, &arg, NULL);
    testCond(s == NATS_OK);

    test("SetOnComplete ok: ");
    s = stanSubscription_SetOnCompleteCB(sub, _stanSubOnComplete, &arg);
    testCond(s == NATS_OK);

    test("Remove onComplete: ");
    s = stanSubscription_SetOnCompleteCB(sub, NULL, NULL);
    if (s == NATS_OK)
    {
        stanSub_Lock(sub);
        if ((sub->onCompleteCB != NULL) || (sub->onCompleteCBClosure != NULL))
            s = NATS_ERR;
        stanSub_Unlock(sub);
    }
    testCond(s == NATS_OK);

    test("SetOnComplete ok: ");
    s = stanSubscription_SetOnCompleteCB(sub, _stanSubOnComplete, &arg);
    testCond(s == NATS_OK);

    test("Publish msg: ");
    s = stanConnection_Publish(sc, "foo", (const void*) "hello", 5);
    testCond(s == NATS_OK);

    test("Wait for message to be received: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.msgReceived)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("Close subscription: ")
    s = stanSubscription_Close(sub);
    testCond(s == NATS_OK);

    test("Ensure onComplete invoked after cb returned: ");
    natsMutex_Lock(arg.m);
    while ((s != NATS_TIMEOUT) && !arg.done)
        s = natsCondition_TimedWait(arg.c, arg.m, 2000);
    natsMutex_Unlock(arg.m);
    testCond(s == NATS_OK);

    test("SetOnComplete after close: ");
    s = stanSubscription_SetOnCompleteCB(sub, _stanSubOnComplete, &arg);
    testCond(s == NATS_INVALID_SUBSCRIPTION);

    stanSubscription_Destroy(sub);
    stanConnection_Destroy(sc);
    _stopServer(pid);

    _destroyDefaultThreadArgs(&arg);
}

static void
test_StanSubTimeout(void)
{
    natsStatus          s;
    natsPid             pid         = NATS_INVALID_PID;
    stanConnection      *sc         = NULL;
    stanSubscription    *sub        = NULL;
    natsConnection      *nc         = NULL;
    natsSubscription    *ncSub      = NULL;
    const char          *closeSubj  = NULL;
    natsMsg             *resp       = NULL;
    int                 i           = 0;
    Pb__SubscriptionRequest *r      = NULL;

    pid = _startStreamingServer("nats://127.0.0.1:4222", NULL, true);
    CHECK_SERVER_STARTED(pid);

    test("Can connect: ");
    s = stanConnection_Connect(&sc, clusterName, clientName, NULL);
    testCond(s == NATS_OK);

    test("Get NATS subscription: ");
    s = stanConnection_GetNATSConnection(sc, &nc);
    testCond(s == NATS_OK);

    test("Setup NATS sub: ");
    natsMutex_Lock(sc->mu);
    closeSubj = (const char*) sc->subCloseRequests;
    natsMutex_Unlock(sc->mu);
    s = natsConnection_SubscribeSync(&ncSub, nc, closeSubj);
    testCond(s == NATS_OK);

    // Artificially lower the timeout to make sure it fails.
    natsMutex_Lock(sc->mu);
    sc->opts->connTimeout=1;
    natsMutex_Unlock(sc->mu);

    test("Subscribe should timeout: ");
    // Try to get the timeout...
    for (i=0;i<50;i++)
    {
        s = stanConnection_Subscribe(&sub, sc, "foo", _dummyStanMsgHandler, NULL, NULL);
        if (s == NATS_TIMEOUT)
            break;
        stanSubscription_Destroy(sub);
        sub = NULL;
    }
    // We don't want to fail the thest if we did not get a timeout.
    testCond((s == NATS_OK) || (s == NATS_TIMEOUT));

    // However, proceed with the rest of the test only if it was a timeout.
    if (s == NATS_TIMEOUT)
    {
        test("Check sub close request sent: ");
        s = natsSubscription_NextMsg(&resp, ncSub, 1000);
        testCond((s == NATS_OK) && (resp != NULL));

        test("Check request: ");
        r = pb__subscription_request__unpack(NULL,
                                            (size_t) natsMsg_GetDataLength(resp),
                                            (const uint8_t*) natsMsg_GetData(resp));
        testCond((r != NULL)
                    && (strcmp(r->clientid, clientName) == 0)
                    && (strcmp(r->subject, "foo") == 0)
                    && (r->inbox != NULL));
        if (r != NULL)
            pb__subscription_request__free_unpacked(r, NULL);

        natsMsg_Destroy(resp);

        if (valgrind)
            nats_Sleep(900);
    }

    natsSubscription_Destroy(ncSub);
    stanSubscription_Destroy(sub);
    stanConnection_ReleaseNATSConnection(sc);
    stanConnection_Destroy(sc);
    _stopServer(pid);
}

#endif

typedef void (*testFunc)(void);

typedef struct __testInfo
{
    const char  *name;
    testFunc    func;

} testInfo;

static testInfo allTests[] =
{
    // Building blocks
    {"Version",                         test_Version},
    {"VersionMatchesTag",               test_VersionMatchesTag},
    {"OpenCloseAndWait",                test_OpenCloseAndWait},
    {"natsNowAndSleep",                 test_natsNowAndSleep},
    {"natsAllocSprintf",                test_natsAllocSprintf},
    {"natsStrCaseStr",                  test_natsStrCaseStr},
    {"natsSnprintf",                    test_natsSnprintf},
    {"natsBuffer",                      test_natsBuffer},
    {"natsParseInt64",                  test_natsParseInt64},
    {"natsParseControl",                test_natsParseControl},
    {"natsNormalizeErr",                test_natsNormalizeErr},
    {"natsMutex",                       test_natsMutex},
    {"natsThread",                      test_natsThread},
    {"natsCondition",                   test_natsCondition},
    {"natsTimer",                       test_natsTimer},
    {"natsUrl",                         test_natsUrl},
    {"natsCreateStringFromBuffer",      test_natsCreateStringFromBuffer},
    {"natsHash",                        test_natsHash},
    {"natsHashing",                     test_natsHashing},
    {"natsStrHash",                     test_natsStrHash},
    {"natsInbox",                       test_natsInbox},
    {"natsOptions",                     test_natsOptions},
    {"natsSock_ConnectTcp",             test_natsSock_ConnectTcp},
    {"natsSock_IPOrder",                test_natsSock_IPOrder},
    {"natsSock_ReadLine",               test_natsSock_ReadLine},
    {"natsJSON",                        test_natsJSON},
    {"natsErrWithLongText",             test_natsErrWithLongText},
    {"natsErrStackMoreThanMaxFrames",   test_natsErrStackMoreThanMaxFrames},
    {"natsMsg",                         test_natsMsg},
    {"natsBase32",                      test_natsBase32Decode},
    {"natsBase64",                      test_natsBase64Encode},
    {"natsCRC16",                       test_natsCRC16},
    {"natsKeys",                        test_natsKeys},
    {"natsReadFile",                    test_natsReadFile},
    {"natsGetJWTOrSeed",                test_natsGetJWTOrSeed},
    {"natsHostIsIP",                    test_natsHostIsIP},
    {"natsWaitReady",                   test_natsWaitReady},
    {"natsSign",                        test_natsSign},
    {"HeadersLift",                     test_natsMsgHeadersLift},
    {"HeadersAPIs",                     test_natsMsgHeaderAPIs},

    // Package Level Tests

    {"ReconnectServerStats",            test_ReconnectServerStats},
    {"ParseStateReconnectFunctionality",test_ParseStateReconnectFunctionality},
    {"ServersRandomize",                test_ServersRandomize},
    {"SelectNextServer",                test_SelectNextServer},
    {"ParserPing",                      test_ParserPing},
    {"ParserErr",                       test_ParserErr},
    {"ParserOK",                        test_ParserOK},
    {"ParseINFO",                       test_ParseINFO},
    {"ParserShouldFail",                test_ParserShouldFail},
    {"ParserSplitMsg",                  test_ParserSplitMsg},
    {"ProcessMsgArgs",                  test_ProcessMsgArgs},
    {"LibMsgDelivery",                  test_LibMsgDelivery},
    {"AsyncINFO",                       test_AsyncINFO},
    {"RequestPool",                     test_RequestPool},
    {"NoFlusherIfSendAsapOption",       test_NoFlusherIfSendAsap},
    {"HeadersAndSubPendingBytes",       test_HeadersAndSubPendingBytes},

    // Public API Tests

    {"DefaultConnection",               test_DefaultConnection},
    {"SimplifiedURLs",                  test_SimplifiedURLs},
    {"IPResolutionOrder",               test_IPResolutionOrder},
    {"UseDefaultURLIfNoServerSpecified",test_UseDefaultURLIfNoServerSpecified},
    {"ConnectToWithMultipleURLs",       test_ConnectToWithMultipleURLs},
    {"ConnectionWithNULLOptions",       test_ConnectionWithNullOptions},
    {"ConnectionToWithNullURLs",        test_ConnectionToWithNullURLs},
    {"ConnectionStatus",                test_ConnectionStatus},
    {"ConnClosedCB",                    test_ConnClosedCB},
    {"CloseDisconnectedCB",             test_CloseDisconnectedCB},
    {"ServerStopDisconnectedCB",        test_ServerStopDisconnectedCB},
    {"ClosedConnections",               test_ClosedConnections},
    {"ConnectVerboseOption",            test_ConnectVerboseOption},
    {"ReconnectThreadLeak",             test_ReconnectThreadLeak},
    {"ReconnectTotalTime",              test_ReconnectTotalTime},
    {"ReconnectDisallowedFlags",        test_ReconnectDisallowedFlags},
    {"ReconnectAllowedFlags",           test_ReconnectAllowedFlags},
    {"ConnCloseBreaksReconnectLoop",    test_ConnCloseBreaksReconnectLoop},
    {"BasicReconnectFunctionality",     test_BasicReconnectFunctionality},
    {"ExtendedReconnectFunctionality",  test_ExtendedReconnectFunctionality},
    {"QueueSubsOnReconnect",            test_QueueSubsOnReconnect},
    {"IsClosed",                        test_IsClosed},
    {"IsReconnectingAndStatus",         test_IsReconnectingAndStatus},
    {"ReconnectBufSize",                test_ReconnectBufSize},
    {"RetryOnFailedConnect",            test_RetryOnFailedConnect},
    {"NoPartialOnReconnect",            test_NoPartialOnReconnect},
    {"ReconnectFailsPendingRequests",   test_ReconnectFailsPendingRequest},

    {"ErrOnConnectAndDeadlock",         test_ErrOnConnectAndDeadlock},
    {"ErrOnMaxPayloadLimit",            test_ErrOnMaxPayloadLimit},

    {"Auth",                            test_Auth},
    {"AuthFailNoDisconnectCB",          test_AuthFailNoDisconnectCB},
    {"AuthToken",                       test_AuthToken},
    {"AuthTokenHandler",                test_AuthTokenHandler},
    {"PermViolation",                   test_PermViolation},
    {"AuthViolation",                   test_AuthViolation},
    {"AuthenticationExpired",           test_AuthenticationExpired},
    {"AuthenticationExpiredReconnect",  test_AuthenticationExpiredReconnect},
    {"ConnectedServer",                 test_ConnectedServer},
    {"MultipleClose",                   test_MultipleClose},
    {"SimplePublish",                   test_SimplePublish},
    {"SimplePublishNoData",             test_SimplePublishNoData},
    {"PublishMsg",                      test_PublishMsg},
    {"InvalidSubsArgs",                 test_InvalidSubsArgs},
    {"AsyncSubscribe",                  test_AsyncSubscribe},
    {"AsyncSubscribeTimeout",           test_AsyncSubscribeTimeout},
    {"SyncSubscribe",                   test_SyncSubscribe},
    {"PubSubWithReply",                 test_PubSubWithReply},
    {"NoResponders",                    test_NoResponders},
    {"Flush",                           test_Flush},
    {"ConnCloseDoesFlush",              test_ConnCloseDoesFlush},
    {"QueueSubscriber",                 test_QueueSubscriber},
    {"ReplyArg",                        test_ReplyArg},
    {"SyncReplyArg",                    test_SyncReplyArg},
    {"Unsubscribe",                     test_Unsubscribe},
    {"DoubleUnsubscribe",               test_DoubleUnsubscribe},
    {"SubRemovedWhileProcessingMsg",    test_SubRemovedWhileProcessingMsg},
    {"RequestTimeout",                  test_RequestTimeout},
    {"Request",                         test_Request},
    {"RequestNoBody",                   test_RequestNoBody},
    {"RequestMuxWithMappedSubject",     test_RequestMuxWithMappedSubject},
    {"OldRequest",                      test_OldRequest},
    {"SimultaneousRequests",            test_SimultaneousRequest},
    {"RequestClose",                    test_RequestClose},
    {"FlushInCb",                       test_FlushInCb},
    {"ReleaseFlush",                    test_ReleaseFlush},
    {"FlushErrOnDisconnect",            test_FlushErrOnDisconnect},
    {"Inbox",                           test_Inbox},
    {"Stats",                           test_Stats},
    {"BadSubject",                      test_BadSubject},
    {"SubBadSubjectAndQueueNames",      test_SubBadSubjectAndQueueName},
    {"ClientAsyncAutoUnsub",            test_ClientAsyncAutoUnsub},
    {"ClientSyncAutoUnsub",             test_ClientSyncAutoUnsub},
    {"ClientAutoUnsubAndReconnect",     test_ClientAutoUnsubAndReconnect},
    {"NextMsgOnClosedSub",              test_NextMsgOnClosedSub},
    {"CloseSubRelease",                 test_CloseSubRelease},
    {"IsValidSubscriber",               test_IsValidSubscriber},
    {"SlowSubscriber",                  test_SlowSubscriber},
    {"SlowAsyncSubscriber",             test_SlowAsyncSubscriber},
    {"SlowConsumerCb",                  test_SlowConsumerCB},
    {"PendingLimitsDeliveredAndDropped",test_PendingLimitsDeliveredAndDropped},
    {"PendingLimitsWithSyncSub",        test_PendingLimitsWithSyncSub},
    {"AsyncSubscriptionPending",        test_AsyncSubscriptionPending},
    {"AsyncSubscriptionPendingDrain",   test_AsyncSubscriptionPendingDrain},
    {"SyncSubscriptionPending",         test_SyncSubscriptionPending},
    {"SyncSubscriptionPendingDrain",    test_SyncSubscriptionPendingDrain},
    {"AsyncErrHandler",                 test_AsyncErrHandler},
    {"AsyncSubscriberStarvation",       test_AsyncSubscriberStarvation},
    {"AsyncSubscriberOnClose",          test_AsyncSubscriberOnClose},
    {"NextMsgCallOnAsyncSub",           test_NextMsgCallOnAsyncSub},
    {"SubOnComplete",                   test_SubOnComplete},
    {"GetLastError",                    test_GetLastError},
    {"StaleConnection",                 test_StaleConnection},
    {"ServerErrorClosesConnection",     test_ServerErrorClosesConnection},
    {"NoEcho",                          test_NoEcho},
    {"NoEchoOldServer",                 test_NoEchoOldServer},
    {"DrainSub",                        test_DrainSub},
    {"DrainSubStops",                   test_DrainSubStops},
    {"DrainSubRaceOnAutoUnsub",         test_DrainSubRaceOnAutoUnsub},
    {"DrainSubNotResentOnReconnect",    test_DrainSubNotResentOnReconnect},
    {"DrainConn",                       test_DrainConn},
    {"NoDoubleCloseCbOnDrain",          test_NoDoubleConnClosedOnDrain},
    {"GetClientID",                     test_GetClientID},
    {"GetClientIP",                     test_GetClientIP},
    {"GetRTT",                          test_GetRTT},
    {"GetLocalIPAndPort",               test_GetLocalIPAndPort},
    {"UserCredsCallbacks",              test_UserCredsCallbacks},
    {"UserCredsFromFiles",              test_UserCredsFromFiles},
    {"NKey",                            test_NKey},
    {"NKeyFromSeed",                    test_NKeyFromSeed},
    {"ConnSign",                        test_ConnSign},
    {"WriteDeadline",                   test_WriteDeadline},
    {"HeadersNotSupported",             test_HeadersNotSupported},
    {"HeadersBasic",                    test_HeadersBasic},
    {"EventLoop",                       test_EventLoop},
    {"EventLoopRetryOnFailedConnect",   test_EventLoopRetryOnFailedConnect},
    {"EventLoopTLS",                    test_EventLoopTLS},
    {"SSLBasic",                        test_SSLBasic},
    {"SSLVerify",                       test_SSLVerify},
    {"SSLCAFromMemory",                 test_SSLLoadCAFromMemory},
    {"SSLCertAndKeyFromMemory",         test_SSLCertAndKeyFromMemory},
    {"SSLVerifyHostname",               test_SSLVerifyHostname},
    {"SSLSkipServerVerification",       test_SSLSkipServerVerification},
    {"SSLCiphers",                      test_SSLCiphers},
    {"SSLMultithreads",                 test_SSLMultithreads},
    {"SSLConnectVerboseOption",         test_SSLConnectVerboseOption},
    {"SSLSocketLeakEventLoop",          test_SSLSocketLeakWithEventLoop},
    {"SSLReconnectWithAuthError",       test_SSLReconnectWithAuthError},

    // Clusters Tests

    {"ServersOption",                   test_ServersOption},
    {"AuthServers",                     test_AuthServers},
    {"AuthFailToReconnect",             test_AuthFailToReconnect},
    {"ReconnectWithTokenHandler",       test_ReconnectWithTokenHandler},
    {"BasicClusterReconnect",           test_BasicClusterReconnect},
    {"HotSpotReconnect",                test_HotSpotReconnect},
    {"ProperReconnectDelay",            test_ProperReconnectDelay},
    {"ProperFalloutAfterMaxAttempts",   test_ProperFalloutAfterMaxAttempts},
    {"StopReconnectAfterTwoAuthErr",    test_StopReconnectAfterTwoAuthErr},
    {"TimeoutOnNoServer",               test_TimeoutOnNoServer},
    {"PingReconnect",                   test_PingReconnect},
    {"GetServers",                      test_GetServers},
    {"GetDiscoveredServers",            test_GetDiscoveredServers},
    {"DiscoveredServersCb",             test_DiscoveredServersCb},
    {"INFOAfterFirstPONGisProcessedOK", test_ReceiveINFORightAfterFirstPONG},
    {"ServerPoolUpdatedOnClusterUpdate",test_ServerPoolUpdatedOnClusterUpdate},
    {"ReconnectJitter",                 test_ReconnectJitter},
    {"CustomReconnectDelay",            test_CustomReconnectDelay},
    {"LameDuckMode",                    test_LameDuckMode},

    {"JetStreamUnmarshalAccInfo",       test_JetStreamUnmarshalAccountInfo},
    {"JetStreamUnmarshalStreamCfg",     test_JetStreamUnmarshalStreamConfig},
    {"JetStreamMarshalStreamCfg",       test_JetStreamMarshalStreamConfig},
    {"JetStreamContext",                test_JetStreamContext},
    {"JetStreamDomain",                 test_JetStreamContextDomain},
    {"JetStreamMgtStreams",             test_JetStreamMgtStreams},
    {"JetStreamPublish",                test_JetStreamPublish},
    {"JetStreamPublishAsync",           test_JetStreamPublishAsync},

#if defined(NATS_HAS_STREAMING)
    {"StanPBufAllocator",               test_StanPBufAllocator},
    {"StanConnOptions",                 test_StanConnOptions},
    {"StanSubOptions",                  test_StanSubOptions},
    {"StanMsg",                         test_StanMsg},
    {"StanServerNotReachable",          test_StanServerNotReachable},
    {"StanBasicConnect",                test_StanBasicConnect},
    {"StanConnectError",                test_StanConnectError},
    {"StanBasicPublish",                test_StanBasicPublish},
    {"StanBasicPublishAsync",           test_StanBasicPublishAsync},
    {"StanPublishTimeout",              test_StanPublishTimeout},
    {"StanPublishMaxAcksInflight",      test_StanPublishMaxAcksInflight},
    {"StanBasicSubscription",           test_StanBasicSubscription},
    {"StanSubscriptionCloseAndUnsub",   test_StanSubscriptionCloseAndUnsubscribe},
    {"StanDurableSubscription",         test_StanDurableSubscription},
    {"StanBasicQueueSubscription",      test_StanBasicQueueSubscription},
    {"StanDurableQueueSubscription",    test_StanDurableQueueSubscription},
    {"StanCheckReceivedMsg",            test_StanCheckReceivedvMsg},
    {"StanSubscriptionAckMsg",          test_StanSubscriptionAckMsg},
    {"StanPings",                       test_StanPings},
    {"StanPingsNoResponder",            test_StanPingsNoResponder},
    {"StanConnectionLostHandlerNotSet", test_StanConnectionLostHandlerNotSet},
    {"StanPingsUnblockPublishCalls",    test_StanPingsUnblockPubCalls},
    {"StanGetNATSConnection",           test_StanGetNATSConnection},
    {"StanNoRetryOnFailedConnect",      test_StanNoRetryOnFailedConnect},
    {"StanInternalSubsNotPooled",       test_StanInternalSubsNotPooled},
    {"StanSubOnComplete",               test_StanSubOnComplete},
    {"StanSubTimeout",                  test_StanSubTimeout},

#endif

};

static int  maxTests = (int) (sizeof(allTests)/sizeof(testInfo));

static void
generateList(void)
{
    FILE    *list = fopen("list.txt", "w");
    int     i;

    if (list == NULL)
    {
        printf("@@ Unable to create file 'list.txt': %d\n", errno);
        return;
    }

    printf("Number of tests: %d\n", maxTests);

    for (i=0; i<maxTests; i++)
        fprintf(list, "%s\n", allTests[i].name);

    fflush(list);
    fclose(list);
}

int main(int argc, char **argv)
{
    const char *envStr;
    int testStart   = 0;
    int testEnd     = 0;
    int i;

    if (argc == 1)
    {
        generateList();
        return 0;
    }

    if (argc == 3)
    {
        testStart = atoi(argv[1]);
        testEnd   = atoi(argv[2]);
    }

    if ((argc != 3)
        || (testStart < 0) || (testStart >= maxTests)
        || (testEnd < 0) || (testEnd >= maxTests)
        || (testStart > testEnd))
    {
        printf("@@ Usage: %s [start] [end] (0 .. %d)\n", argv[0], (maxTests - 1));
        return 1;
    }

    envStr = getenv("NATS_TEST_TRAVIS");
    if ((envStr != NULL) && (envStr[0] != '\0'))
        runOnTravis = true;

    envStr = getenv("NATS_TEST_VALGRIND");
    if ((envStr != NULL) && (envStr[0] != '\0'))
    {
        valgrind = true;
        printf("Test running in VALGRIND mode.\n");
    }

    envStr = getenv("NATS_TEST_KEEP_SERVER_OUTPUT");
    if ((envStr != NULL) && (envStr[0] != '\0'))
    {
        keepServerOutput = true;
        printf("Test prints server's output.\n");
    }

    envStr = getenv("NATS_TEST_SERVER_EXE");
    if ((envStr != NULL) && (envStr[0] != '\0'))
    {
        natsServerExe = envStr;
        printf("Test using server executable: %s\n", natsServerExe);
    }

    envStr = getenv("NATS_TEST_STREAMING_SERVER_EXE");
    if ((envStr != NULL) && (envStr[0] != '\0'))
    {
        natsStreamingServerExe = envStr;
        printf("Test using server executable: %s\n", natsStreamingServerExe);
    }

    envStr = getenv("NATS_TEST_SERVER_VERSION");
    if ((envStr != NULL) && (envStr[0] != '\0'))
    {
        serverVersion = envStr;
        printf("Test server version: %s\n", serverVersion);
    }

    if (nats_Open(-1) != NATS_OK)
    {
        printf("@@ Unable to run tests: unable to initialize the library!\n");
        return 1;
    }

    if (natsMutex_Create(&slMu) != NATS_OK)
    {
        printf("@@ Unable to create lock for servers map\n");
        return 1;
    }
    if (natsHash_Create(&slMap, 4) != NATS_OK)
    {
        natsMutex_Destroy(slMu);
        printf("@@ Unable to create lock for servers map\n");
        return 1;
    }

    // Execute tests
    for (i=testStart; (i<=testEnd) && !failed; i++)
    {
#ifdef _WIN32
        printf("\n== %s ==\n", allTests[i].name);
#else
        printf("\033[0;34m\n== %s ==\n\033[0;0m", allTests[i].name);
#endif
        (*(allTests[i].func))();
    }

#ifdef _WIN32
    if (logHandle != NULL)
    {
        CloseHandle(logHandle);
        DeleteFile(LOGFILE_NAME);
    }
#else
    remove(LOGFILE_NAME);
#endif

    // Shutdown servers that are still running likely due to failed test
    {
        natsHashIter iter;
        int64_t      key;

        natsMutex_Lock(slMu);
        natsHashIter_Init(&iter, slMap);

        while (natsHashIter_Next(&iter, &key, NULL))
            _stopServer((natsPid) key);

        natsHash_Destroy(slMap);
        natsMutex_Destroy(slMu);
    }

    // Makes valgrind happy
    nats_CloseAndWait((failed ? 1 : 2000));

    if (failed)
    {
        printf("*** TEST FAILED ***\n");
        return 1;
    }

    printf("ALL PASSED\n");
    return 0;
}
