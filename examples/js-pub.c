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

#include "examples.h"

static const char *usage = ""\
"-stream        stream name (default is 'foo')\n" \
"-txt           text to send (default is 'hello')\n" \
"-count         number of messages to send\n" \
"-sync          publish synchronously (default is async)\n";

static void
_jsPubErr(natsJS *js, natsJSPubAckErr *pae, void *closure)
{
    int *errors = (int*) closure;

    printf("Error: %d - Code: %d - Text: %s\n", pae->Err, pae->ErrCode, pae->ErrText);
    printf("Original message: %.*s\n", natsMsg_GetDataLength(pae->Msg), natsMsg_GetData(pae->Msg));

    *errors = (*errors + 1);

    // If we wanted to resend the original message, we would do something like that:
    //
    // natsJS_PublishMsgAsync(js, &(pae->Msg), NULL);
    //
    // Note that we use `&(pae->Msg)` so that the library set it to NULL if it takes
    // ownership, and the library will not destroy the message when this callback returns.

    // No need to destroy anything, everything is handled by the library.
}

int main(int argc, char **argv)
{
    natsConnection      *conn  = NULL;
    natsStatistics      *stats = NULL;
    natsOptions         *opts  = NULL;
    natsJS              *js    = NULL;
    natsJSOptions       jsOpts;
    natsJSErrCode       jerr   = 0;
    natsStatus          s;
    int                 dataLen=0;
    volatile int        errors = 0;
    bool                delStream = false;

    opts = parseArgs(argc, argv, usage);
    dataLen = (int) strlen(payload);

    s = natsConnection_Connect(&conn, opts);

    if (s == NATS_OK)
        s = natsJSOptions_Init(&jsOpts);

    if (s == NATS_OK)
    {
        if (async)
        {
            jsOpts.PublishAsync.ErrHandler           = _jsPubErr;
            jsOpts.PublishAsync.ErrHandlerClosure    = (void*) &errors;
        }
        s = natsJS_NewContext(&js, conn, &jsOpts);
    }

    if (s == NATS_OK)
    {
        natsJSStreamInfo    *si = NULL;

        // First check if the stream already exists.
        s = natsJS_GetStreamInfo(&si, js, stream, NULL, &jerr);
        if (s == NATS_NOT_FOUND)
        {
            natsJSStreamConfig  cfg;

            // Since we are the one creating this stream, we can delete at the end.
            delStream = true;

            // Initialize the configuration structure.
            natsJSStreamConfig_Init(&cfg);
            // Since we don't provide subjects, the subjects it will default to the stream name.
            cfg.Name = stream;
            // Make it a memory stream.
            cfg.Storage = natsJS_MemoryStorage;
            // Add the stream,
            s = natsJS_AddStream(&si, js, &cfg, NULL, &jerr);
        }
        if (s == NATS_OK)
        {
            printf("Stream %s has %" PRIu64 " messages (%" PRIu64 " bytes)\n",
                si->Config->Name, si->State.Msgs, si->State.Bytes);

            // Need to destroy the returned stream object.
            natsJSStreamInfo_Destroy(si);
        }
    }

    if (s == NATS_OK)
        s = natsStatistics_Create(&stats);

    if (s == NATS_OK)
    {
        printf("\nSending %" PRId64 " messages to subject '%s'\n", total, stream);
        start = nats_Now();
    }

    for (count = 0; (s == NATS_OK) && (count < total); count++)
    {
        if (async)
            s = natsJS_PublishAsync(js, stream, (const void*) payload, dataLen, NULL);
        else
        {
            natsJSPubAck *pa = NULL;

            s = natsJS_Publish(&pa, js, stream, (const void*) payload, dataLen, NULL, &jerr);
            if (s == NATS_OK)
            {
                if (pa->Duplicate)
                    printf("Got a duplicate message! Sequence=%" PRIu64 "\n", pa->Sequence);

                natsJSPubAck_Destroy(pa);
            }
        }
    }

    if ((s == NATS_OK) && async)
    {
        natsJSPubOptions    jsPubOpts;

        natsJSPubOptions_Init(&jsPubOpts);
        // Let's set it to 30 seconds, if getting "Timeout" errors,
        // this may need to be increased based on the number of messages
        // being sent.
        jsPubOpts.MaxWait = 30000;
        s = natsJS_PublishAsyncComplete(js, &jsPubOpts);
    }

    if (s == NATS_OK)
    {
        natsJSStreamInfo *si = NULL;

        elapsed = nats_Now() - start;
        printStats(STATS_OUT, conn, NULL, stats);
        printPerf("Sent");

        if (errors != 0)
            printf("There were %d asynchronous errors\n", errors);

        // Let's report some stats after the run
        s = natsJS_GetStreamInfo(&si, js, stream, NULL, &jerr);
        if (s == NATS_OK)
        {
            printf("\nStream %s has %" PRIu64 " messages (%" PRIu64 " bytes)\n",
                si->Config->Name, si->State.Msgs, si->State.Bytes);

            natsJSStreamInfo_Destroy(si);
        }
    }
    if (delStream && (js != NULL))
    {
        printf("\nDeleting stream %s: ", stream);
        s = natsJS_DeleteStream(js, stream, NULL, &jerr);
        if (s == NATS_OK)
            printf("OK!");
        printf("\n");
    }
    if (s != NATS_OK)
    {
        printf("Error: %d - %s - jerr=%d\n", s, natsStatus_GetText(s), jerr);
        nats_PrintLastErrorStack(stderr);
    }

    // Destroy all our objects to avoid report of memory leak
    natsStatistics_Destroy(stats);
    natsJS_DestroyContext(js);
    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    // To silence reports of memory still in used with valgrind
    nats_Close();

    return 0;
}
