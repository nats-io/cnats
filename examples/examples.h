// Copyright 2015-2018 The NATS Authors
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

#ifndef EXAMPLES_H_
#define EXAMPLES_H_

#include <nats.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#define strcasecmp  _stricmp
#define strdup      _strdup
#else
#include <strings.h>
#include <signal.h>
#endif

#define STATS_IN        0x1
#define STATS_OUT       0x2
#define STATS_COUNT     0x4

#define MAX_SERVERS     (10)

bool        async   = true;
const char  *subj   = "foo";
const char  *payload= "hello";
const char  *name   = "worker";
int64_t     total   = 1000000;

volatile int64_t count   = 0;
volatile int64_t dropped = 0;
int64_t          start   = 0;
volatile int64_t elapsed = 0;
bool             print   = false;
int64_t          timeout = 10000; // 10 seconds.

natsOptions      *opts   = NULL;

const char       *certFile = NULL;
const char       *keyFile  = NULL;

const char  *cluster    = "test-cluster";
const char  *clientID   = "client";
const char  *qgroup     = NULL;
const char  *durable    = NULL;
bool        deliverAll  = false;
bool        deliverLast = true;
uint64_t    deliverSeq  = 0;
bool        unsubscribe = false;

const char  *stream     = "foo";

static natsStatus
printStats(int mode, natsConnection *conn, natsSubscription *sub,
           natsStatistics *stats)
{
    natsStatus  s           = NATS_OK;
    uint64_t    inMsgs      = 0;
    uint64_t    inBytes     = 0;
    uint64_t    outMsgs     = 0;
    uint64_t    outBytes    = 0;
    uint64_t    reconnected = 0;
    int         pending     = 0;
    int64_t     delivered   = 0;
    int64_t     sdropped    = 0;

    s = natsConnection_GetStats(conn, stats);
    if (s == NATS_OK)
        s = natsStatistics_GetCounts(stats, &inMsgs, &inBytes,
                                     &outMsgs, &outBytes, &reconnected);
    if ((s == NATS_OK) && (sub != NULL))
    {
        s = natsSubscription_GetStats(sub, &pending, NULL, NULL, NULL,
                                      &delivered, &sdropped);

        // Since we use AutoUnsubscribe(), when the max has been reached,
        // the subscription is automatically closed, so this call would
        // return "Invalid Subscription". Ignore this error.
        if (s == NATS_INVALID_SUBSCRIPTION)
        {
            s = NATS_OK;
            pending = 0;
        }
    }

    if (s == NATS_OK)
    {
        if (mode & STATS_IN)
        {
            printf("In Msgs: %9" PRIu64 " - "\
                   "In Bytes: %9" PRIu64 " - ", inMsgs, inBytes);
        }
        if (mode & STATS_OUT)
        {
            printf("Out Msgs: %9" PRIu64 " - "\
                   "Out Bytes: %9" PRIu64 " - ", outMsgs, outBytes);
        }
        if (mode & STATS_COUNT)
        {
            printf("Delivered: %9" PRId64 " - ", delivered);
            printf("Pending: %5d - ", pending);
            printf("Dropped: %5" PRId64 " - ", sdropped);
        }
        printf("Reconnected: %3" PRIu64 "\n", reconnected);
    }

    return s;
}

static void
printPerf(const char *perfTxt)
{
    if ((start > 0) && (elapsed == 0))
        elapsed = nats_Now() - start;

    if (elapsed <= 0)
        printf("\nNot enough messages or too fast to report performance!\n");
    else
        printf("\n%s %" PRId64 " messages in "\
               "%" PRId64 " milliseconds (%d msgs/sec)\n",
               perfTxt, count, elapsed, (int)((count * 1000) / elapsed));
}

static void
printUsageAndExit(const char *progName, const char *usage)
{
    printf("\nUsage: %s [options]\n\nThe options are:\n\n"\
"-h             prints the usage\n" \
"-s             server url(s) (list of comma separated nats urls)\n" \
"-tls           use secure (SSL/TLS) connection\n" \
"-tlscacert     trusted certificates file\n" \
"-tlscert       client certificate (PEM format only)\n" \
"-tlskey        client private key file (PEM format only)\n" \
"-tlsciphers    ciphers suite\n"
"-tlshost       server certificate's expected hostname\n" \
"-tlsskip       skip server certificate verification\n" \
"-creds         user credentials chained file\n" \
"-subj          subject (default is 'foo')\n" \
"-print         for consumers, print received messages (default is false)\n" \
"-wd            write deadline in milliseconds\n" \
                "%s\n",
                progName, usage);

    natsOptions_Destroy(opts);
    nats_Close();

    exit(1);
}

static natsStatus
parseUrls(const char *urls, natsOptions *gopts)
{
    char        *serverUrls[MAX_SERVERS];
    int         num       = 0;
    natsStatus  s         = NATS_OK;
    char        *urlsCopy = NULL;
    char        *commaPos = NULL;
    char        *ptr      = NULL;

    urlsCopy = strdup(urls);
    if (urlsCopy == NULL)
        return NATS_NO_MEMORY;

    memset(serverUrls, 0, sizeof(serverUrls));

    ptr = urlsCopy;

    do
    {
        if (num == MAX_SERVERS)
        {
            s = NATS_INSUFFICIENT_BUFFER;
            break;
        }

        serverUrls[num++] = ptr;
        commaPos = strchr(ptr, ',');
        if (commaPos != NULL)
        {
            ptr = (char*)(commaPos + 1);
            *(commaPos) = '\0';
        }
        else
        {
            ptr = NULL;
        }
    } while (ptr != NULL);

    if (s == NATS_OK)
        s = natsOptions_SetServers(gopts, (const char**) serverUrls, num);

    free(urlsCopy);

    return s;
}

static natsOptions*
parseArgs(int argc, char **argv, const char *usage)
{
    natsStatus  s       = NATS_OK;
    bool        urlsSet = false;
    int         i;

    if (natsOptions_Create(&opts) != NATS_OK)
        s = NATS_NO_MEMORY;

    for (i=1; (i<argc) && (s == NATS_OK); i++)
    {
        if ((strcasecmp(argv[i], "-h") == 0)
            || (strcasecmp(argv[i], "-help") == 0))
        {
            printUsageAndExit(argv[0], usage);
        }
        else if (strcasecmp(argv[i], "-s") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = parseUrls(argv[++i], opts);
            if (s == NATS_OK)
                urlsSet = true;
        }
        else if (strcasecmp(argv[i], "-tls") == 0)
        {
            s = natsOptions_SetSecure(opts, true);
        }
        else if (strcasecmp(argv[i], "-tlscacert") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = natsOptions_LoadCATrustedCertificates(opts, argv[++i]);
        }
        else if (strcasecmp(argv[i], "-tlscert") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            certFile = argv[++i];
        }
        else if (strcasecmp(argv[i], "-tlskey") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            keyFile = argv[++i];
        }
        else if (strcasecmp(argv[i], "-tlsciphers") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = natsOptions_SetCiphers(opts, argv[++i]);
        }
        else if (strcasecmp(argv[i], "-tlshost") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = natsOptions_SetExpectedHostname(opts, argv[++i]);
        }
        else if (strcasecmp(argv[i], "-tlsskip") == 0)
        {
            s = natsOptions_SkipServerVerification(opts, true);
        }
        else if (strcasecmp(argv[i], "-sync") == 0)
        {
            async = false;
        }
        else if (strcasecmp(argv[i], "-subj") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            subj = argv[++i];
        }
        else if (strcasecmp(argv[i], "-print") == 0)
        {
            print = true;
        }
        else if ((strcasecmp(argv[i], "-name") == 0) ||
                 (strcasecmp(argv[i], "-queue") == 0))
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            name = argv[++i];
        }
        else if (strcasecmp(argv[i], "-count") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            total = atol(argv[++i]);
        }
        else if (strcasecmp(argv[i], "-txt") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            payload = argv[++i];
        }
        else if (strcasecmp(argv[i], "-timeout") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            timeout = atol(argv[++i]);
        }
        else if (strcasecmp(argv[i], "-gd") == 0)
        {
            s = natsOptions_UseGlobalMessageDelivery(opts, true);
        }
        else if (strcasecmp(argv[i], "-c") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            cluster = argv[++i];
        }
        else if (strcasecmp(argv[i], "-id") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            clientID = argv[++i];
        }
        else if (strcasecmp(argv[i], "-last") == 0)
        {
            deliverLast = true;
        }
        else if (strcasecmp(argv[i], "-all") == 0)
        {
            deliverAll = true;
        }
        else if (strcasecmp(argv[i], "-seq") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            deliverSeq = atol(argv[++i]);
        }
        else if (strcasecmp(argv[i], "-durable") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            durable = argv[++i];
        }
        else if (strcasecmp(argv[i], "-qgroup") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            qgroup = argv[++i];
        }
        else if (strcasecmp(argv[i], "-unsubscribe") == 0)
        {
            unsubscribe = true;
        }
        else if (strcasecmp(argv[i], "-creds") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = natsOptions_SetUserCredentialsFromFiles(opts, argv[++i], NULL);
        }
        else if (strcasecmp(argv[i], "-wd") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            s = natsOptions_SetWriteDeadline(opts, atol(argv[++i]));
        }
        else if (strcasecmp(argv[i], "-stream") == 0)
        {
            if (i + 1 == argc)
                printUsageAndExit(argv[0], usage);

            stream = argv[++i];
        }
        else
        {
            printf("Unknown option: '%s'\n", argv[i]);
            printUsageAndExit(argv[0], usage);
        }
    }

    if ((s == NATS_OK) && ((certFile != NULL) || (keyFile != NULL)))
        s = natsOptions_LoadCertificatesChain(opts, certFile, keyFile);

    if ((s == NATS_OK) && !urlsSet)
        s = parseUrls(NATS_DEFAULT_URL, opts);

    if (s != NATS_OK)
    {
        printf("Error parsing arguments: %d - %s\n",
               s, natsStatus_GetText(s));

        nats_PrintLastErrorStack(stderr);

        natsOptions_Destroy(opts);
        nats_Close();
        exit(1);
    }

    return opts;
}

#endif /* EXAMPLES_H_ */
