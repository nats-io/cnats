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

void natsJS_lock(natsJS *js);
void natsJS_unlock(natsJS *js);

#else
// We know what we are doing :-)

#define natsJS_lock(js)     (natsMutex_Lock((js)->mu))
#define natsJS_unlock(c)    (natsMutex_Unlock((js)->mu))

#endif // DEV_MODE


extern const char*      jsDefaultAPIPrefix;
extern const int64_t    jsDefaultRequestWait;

#define jsMsgIdHdr             "Nats-Msg-Id"
#define jsExpectedStreamHdr    "Nats-Expected-Stream"
#define jsExpectedLastSeqHdr   "Nats-Expected-Last-Sequence"
#define jsExpectedLastMsgIdHdr "Nats-Expected-Last-Msg-Id"

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

/*
    // apiConsumerCreateT is used to create consumers.
    apiConsumerCreateT = "CONSUMER.CREATE.%s"

    // apiDurableCreateT is used to create durable consumers.
    apiDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"

    // apiConsumerInfoT is used to create consumers.
    apiConsumerInfoT = "CONSUMER.INFO.%s.%s"

    // apiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
    apiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

    // apiDeleteConsumerT is used to delete consumers.
    apiConsumerDeleteT = "CONSUMER.DELETE.%s.%s"

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
#define natsJS_apiSubj(s, o, f, ...) (nats_asprintf((s), (f), (o)->Prefix, __VA_ARGS__) < 0 ? NATS_NO_MEMORY : NATS_OK)

// Execute the JS API request if status is OK on entry. If the result is NATS_NO_RESPONDERS,
// and `errCode` is not NULL, set it to JSNotEnabledErr.
#define IFOK_JSR(s, c)  if (s == NATS_OK) { s = (c); if ((s == NATS_NO_RESPONDERS) && (errCode != NULL)) { *errCode = JSNotEnabledErr; } }

// Returns true if the API response has a Code or ErrCode that is not 0.
#define natsJS_apiResponseIsErr(ar)	(((ar)->Error.Code != 0) || ((ar)->Error.ErrCode != 0))

struct __natsJS
{
    natsMutex		    *mu;
    natsConnection      *nc;
    natsJSOptions  	    opts;
    int				    refs;
    natsCondition       *cond;
    natsStrHash         *pm;
    natsSubscription    *rsub;
    char                *rpre;
    int                 pacw;
    bool                destroyed;
};

// natsJSApiError is included in all API responses if there was an error.
typedef struct __natsJSApiError
{
    int         Code;           //`json:"code"`
    uint16_t    ErrCode;        //`json:"err_code,omitempty"`
    char        *Description;   //`json:"description,omitempty"

} natsJSApiError;

// apiResponse is a standard response from the JetStream JSON API
typedef struct __natsJSApiResponse
{
    char            *Type;  //`json:"type"`
    natsJSApiError 	Error;  //`json:"error,omitempty"`

} natsJSApiResponse;

// Sets the options in `resOpts` based on the given `opts` and defaults to the context
// own options when some options are not specified.
// Returns also the NATS connection to be used to send the request.
// This function will get/release the context's lock.
natsStatus
natsJS_setOpts(natsConnection **nc, bool *freePfx, natsJS *js, natsJSOptions *opts, natsJSOptions *resOpts);

int
natsJS_lenWithoutTrailingDot(const char *str);

natsStatus
natsJS_unmarshalResponse(natsJSApiResponse *ar, nats_JSON **new_json, natsMsg *resp);

void
natsJS_freeApiRespContent(natsJSApiResponse *ar);

natsStatus
natsJS_unmarshalAccountInfo(natsJSAccountInfo **new_ai, nats_JSON *json);

natsStatus
natsJS_marshalStreamConfig(natsBuffer **new_buf, natsJSStreamConfig *cfg);

natsStatus
natsJS_unmarshalStreamConfig(natsJSStreamConfig **new_cfg, nats_JSON *json);

void
natsJS_destroyStreamConfig(natsJSStreamConfig *cfg);
