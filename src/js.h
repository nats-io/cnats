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

#include "natsp.h"
#include "util.h"

#ifdef DEV_MODE
// For type safety

void js_lock(jsCtx *js);
void js_unlock(jsCtx *js);

#else
// We know what we are doing :-)

#define js_lock(js)     (natsMutex_Lock((js)->mu))
#define js_unlock(c)    (natsMutex_Unlock((js)->mu))

#endif // DEV_MODE


extern const char*      jsDefaultAPIPrefix;
extern const int64_t    jsDefaultRequestWait;

#define jsMsgIdHdr                  "Nats-Msg-Id"
#define jsExpectedStreamHdr         "Nats-Expected-Stream"
#define jsExpectedLastSeqHdr        "Nats-Expected-Last-Sequence"
#define jsExpectedLastSubjSeqHdr    "Nats-Expected-Last-Subject-Sequence"
#define jsExpectedLastMsgIdHdr      "Nats-Expected-Last-Msg-Id"

#define jsErrStreamNameRequired     "stream name is required"
#define jsErrConsumerNameRequired   "consumer name is required"

// jsApiAccountInfo is for obtaining general information about JetStream.
#define jsApiAccountInfo "%.*s.INFO"

// jsApiStreamCreateT is the endpoint to create new streams.
#define jsApiStreamCreateT "%.*s.STREAM.CREATE.%s"

// jsApiStreamUpdateT is the endpoint to update existing streams.
#define jsApiStreamUpdateT "%.*s.STREAM.UPDATE.%s"

// jsApiStreamPurgeT is the endpoint to purge streams.
#define jsApiStreamPurgeT "%.*s.STREAM.PURGE.%s"

// jsApiStreamDeleteT is the endpoint to delete streams.
#define jsApiStreamDeleteT "%.*s.STREAM.DELETE.%s"

// jsApiStreamInfoT is the endpoint to get information on a stream.
#define jsApiStreamInfoT "%.*s.STREAM.INFO.%s"

// jsApiConsumerCreateT is used to create consumers.
#define jsApiConsumerCreateT "%.*s.CONSUMER.CREATE.%s"

// jsApiDurableCreateT is used to create durable consumers.
#define jsApiDurableCreateT "%.*s.CONSUMER.DURABLE.CREATE.%s.%s"

// jsApiConsumerInfoT is used to get information about consumers.
#define jsApiConsumerInfoT "%.*s.CONSUMER.INFO.%s.%s"

// jsApiDeleteConsumerT is used to delete consumers.
#define jsApiConsumerDeleteT "%.*s.CONSUMER.DELETE.%s.%s"


/*
    // apiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
    apiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

    // apiConsumerListT is used to return all detailed consumer information
    apiConsumerListT = "CONSUMER.LIST.%s"

    // apiConsumerNamesT is used to return a list with all consumer names for the stream.
    apiConsumerNamesT = "CONSUMER.NAMES.%s"

    // apiStreams can lookup a stream by subject.
    apiStreams = "STREAM.NAMES"

    // apiStreamListT is the endpoint that will return all detailed stream information
    apiStreamList = "STREAM.LIST"

    // apiMsgGetT is the endpoint to get a message.
    apiMsgGetT = "STREAM.MSG.GET.%s"

    // apiMsgDeleteT is the endpoint to remove a message.
    apiMsgDeleteT = "STREAM.MSG.DELETE.%s"
*/

// Creates a subject based on the option's prefix, the subject format and its values.
#define js_apiSubj(s, o, f, ...) (nats_asprintf((s), (f), (o)->Prefix, __VA_ARGS__) < 0 ? NATS_NO_MEMORY : NATS_OK)

// Execute the JS API request if status is OK on entry. If the result is NATS_NO_RESPONDERS,
// and `errCode` is not NULL, set it to JSNotEnabledErr.
#define IFOK_JSR(s, c)  if (s == NATS_OK) { s = (c); if ((s == NATS_NO_RESPONDERS) && (errCode != NULL)) { *errCode = JSNotEnabledErr; } }

// Returns true if the API response has a Code or ErrCode that is not 0.
#define js_apiResponseIsErr(ar)	(((ar)->Error.Code != 0) || ((ar)->Error.ErrCode != 0))

struct __jsCtx
{
    natsMutex		    *mu;
    natsConnection      *nc;
    jsOptions  	    opts;
    int				    refs;
    natsCondition       *cond;
    natsStrHash         *pm;
    natsSubscription    *rsub;
    char                *rpre;
    int                 pacw;
    int                 pmcount;
    int                 stalled;
};

// jsApiError is included in all API responses if there was an error.
typedef struct __jsApiError
{
    int         Code;           //`json:"code"`
    uint16_t    ErrCode;        //`json:"err_code,omitempty"`
    char        *Description;   //`json:"description,omitempty"

} jsApiError;

// apiResponse is a standard response from the JetStream JSON API
typedef struct __jsApiResponse
{
    char            *Type;  //`json:"type"`
    jsApiError 	Error;  //`json:"error,omitempty"`

} jsApiResponse;

// Sets the options in `resOpts` based on the given `opts` and defaults to the context
// own options when some options are not specified.
// Returns also the NATS connection to be used to send the request.
// This function will get/release the context's lock.
natsStatus
js_setOpts(natsConnection **nc, bool *freePfx, jsCtx *js, jsOptions *opts, jsOptions *resOpts);

int
js_lenWithoutTrailingDot(const char *str);

natsStatus
js_unmarshalResponse(jsApiResponse *ar, nats_JSON **new_json, natsMsg *resp);

void
js_freeApiRespContent(jsApiResponse *ar);

natsStatus
js_unmarshalAccountInfo(nats_JSON *json, jsAccountInfo **new_ai);

natsStatus
js_marshalStreamConfig(natsBuffer **new_buf, jsStreamConfig *cfg);

natsStatus
js_unmarshalStreamConfig(nats_JSON *json, const char *fieldName, jsStreamConfig **new_cfg);

void
js_destroyStreamConfig(jsStreamConfig *cfg);

natsStatus
js_unmarshalStreamState(nats_JSON *pjson, const char *fieldName, jsStreamState *state);

void
js_cleanStreamState(jsStreamState *state);
