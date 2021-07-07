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
#include "mem.h"
#include "util.h"
#include "js.h"

static const char *jsRetPolicyLimitsStr      = "limits";
static const char *jsRetPolicyInterestStr    = "interest";
static const char *jsRetPolicyWorkQueueStr   = "workqueue";

static const char *jsDiscardPolicyOldStr   = "old";
static const char *jsDiscardPolicyNewStr   = "new";

static const char *jsStorageTypeFileStr    = "file";
static const char *jsStorageTypeMemStr     = "memory";

static const char *jsDeliverAllStr         = "all";
static const char *jsDeliverLastStr        = "last";
static const char *jsDeliverNewStr         = "new";
static const char *jsDeliverBySeqStr       = "by_start_sequence";
static const char *jsDeliverByTimeStr      = "by_start_time";

static const char *jsAckNoneStr            = "none";
static const char *jsAckAllStr             = "all";
static const char *jsAckExplictStr         = "explicit";

static const char *jsReplayOriginalStr     = "original";
static const char *jsReplayInstantStr      = "instant";


#define _marshalLong(b, c, f, l)    _marshalLongVal((b), (c), (f), true, (l), 0)
#define _marshalULong(b, c, f, u)   _marshalLongVal((b), (c), (f), false, 0, (u))

typedef enum
{
    jsStreamActionCreate = 1,
    jsStreamActionUpdate,
    jsStreamActionGet,

} jsStreamAction;

static natsStatus
_marshalTimeUTC(natsBuffer *buf, const char *fieldName, int64_t timeUTC)
{
    natsStatus  s  = NATS_OK;
    char        dbuf[36] = {'\0'};

    s = nats_EncodeTimeUTC(dbuf, sizeof(dbuf), timeUTC);
    if (s != NATS_OK)
    {
        if (s == NATS_INVALID_ARG)
            return NATS_UPDATE_ERR_STACK(s);

        return nats_setError(NATS_ERR, "unable to encode data for field '%s' value %" PRId64, fieldName, timeUTC);
    }

    s = natsBuf_Append(buf, ",\"", -1);
    IFOK(s, natsBuf_Append(buf, fieldName, -1));
    IFOK(s, natsBuf_Append(buf, "\":\"", -1));
    IFOK(s, natsBuf_Append(buf, dbuf, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalLongVal(natsBuffer *buf, bool comma, const char *fieldName, bool l, int64_t lval, uint64_t uval)
{
    natsStatus  s = NATS_OK;
    char        temp[32];
    const char  *start = (comma ? ",\"" : "\"");

    if (l)
        snprintf(temp, sizeof(temp), "%" PRId64, lval);
    else
        snprintf(temp, sizeof(temp), "%" PRIi64, uval);

    s = natsBuf_Append(buf, start, -1);
    IFOK(s, natsBuf_Append(buf, fieldName, -1));
    IFOK(s, natsBuf_Append(buf, "\":", -1));
    IFOK(s, natsBuf_Append(buf, temp, -1));

    return NATS_UPDATE_ERR_STACK(s);
}

//
// Stream related functions
//

static void
_destroyPlacement(jsPlacement *placement)
{
    int i;

    if (placement == NULL)
        return;

    NATS_FREE((char*)placement->Cluster);
    for (i=0; i<placement->TagsLen; i++)
        NATS_FREE((char*) placement->Tags[i]);
    NATS_FREE((char**) placement->Tags);
    NATS_FREE(placement);
}

static void
_destroyExternalStream(jsExternalStream *external)
{
    if (external == NULL)
        return;

    NATS_FREE((char*) external->APIPrefix);
    NATS_FREE((char*) external->DeliverPrefix);
    NATS_FREE(external);
}

static void
_destroyStreamSource(jsStreamSource *source)
{
    if (source == NULL)
        return;

    NATS_FREE((char*) source->Name);
    NATS_FREE((char*) source->FilterSubject);
    _destroyExternalStream(source->External);
    NATS_FREE(source);
}

void
js_destroyStreamConfig(jsStreamConfig *cfg)
{
    int i;

    if (cfg == NULL)
        return;

    NATS_FREE((char*) cfg->Name);
    for (i=0; i<cfg->SubjectsLen; i++)
        NATS_FREE((char*) cfg->Subjects[i]);
    NATS_FREE((char**) cfg->Subjects);
    NATS_FREE((char*) cfg->Template);
    _destroyPlacement(cfg->Placement);
    _destroyStreamSource(cfg->Mirror);
    for (i=0; i<cfg->SourcesLen; i++)
        _destroyStreamSource(cfg->Sources[i]);
    NATS_FREE(cfg->Sources);
    NATS_FREE(cfg);
}

static void
_destroyPeerInfo(jsPeerInfo *peer)
{
    if (peer == NULL)
        return;

    NATS_FREE(peer->Name);
    NATS_FREE(peer);
}

static void
_destroyClusterInfo(jsClusterInfo *cluster)
{
    int i;

    if (cluster == NULL)
        return;

    NATS_FREE(cluster->Name);
    NATS_FREE(cluster->Leader);
    for (i=0; i<cluster->ReplicasLen; i++)
        _destroyPeerInfo(cluster->Replicas[i]);
    NATS_FREE(cluster->Replicas);
    NATS_FREE(cluster);
}

static void
_destroyStreamSourceInfo(jsStreamSourceInfo *info)
{
    if (info == NULL)
        return;

    NATS_FREE(info->Name);
    _destroyExternalStream(info->External);
    NATS_FREE(info);
}

static void
_destroyLostStreamData(jsLostStreamData *lost)
{
    if (lost == NULL)
        return;

    NATS_FREE(lost->Msgs);
    NATS_FREE(lost);
}

void
js_cleanStreamState(jsStreamState *state)
{
    if (state == NULL)
        return;

    NATS_FREE(state->Deleted);
    _destroyLostStreamData(state->Lost);
}

void
jsStreamInfo_Destroy(jsStreamInfo *si)
{
    int i;

    if (si == NULL)
        return;

    js_destroyStreamConfig(si->Config);
    _destroyClusterInfo(si->Cluster);
    js_cleanStreamState(&(si->State));
    _destroyStreamSourceInfo(si->Mirror);
    for (i=0; i<si->SourcesLen; i++)
        _destroyStreamSourceInfo(si->Sources[i]);
    NATS_FREE(si->Sources);
    NATS_FREE(si);
}

static natsStatus
_unmarshalExternalStream(nats_JSON *json, const char *fieldName, jsExternalStream **new_external)
{
    jsExternalStream        *external = NULL;
    nats_JSON               *obj      = NULL;
    natsStatus              s;

    s = nats_JSONGetObject(json, fieldName, &obj);
    if (obj == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    external = (jsExternalStream*) NATS_CALLOC(1, sizeof(jsExternalStream));
    if (external == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(obj, "api", (char**) &(external->APIPrefix));
    IFOK(s, nats_JSONGetStr(obj, "deliver", (char**) &(external->DeliverPrefix)));

    if (s == NATS_OK)
        *new_external = external;
    else
        _destroyExternalStream(external);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalExternalStream(jsExternalStream *external, const char *fieldName, natsBuffer *buf)
{
    natsStatus s = NATS_OK;

    IFOK(s, natsBuf_Append(buf, ",\"", -1));
    IFOK(s, natsBuf_Append(buf, fieldName, -1));
    IFOK(s, natsBuf_Append(buf, "\":{\"api\":\"", -1));
    IFOK(s, natsBuf_Append(buf, external->APIPrefix, -1));
    IFOK(s, natsBuf_Append(buf, "\",\"deliver\":\"", -1));
    IFOK(s, natsBuf_Append(buf, external->DeliverPrefix, -1));
    IFOK(s, natsBuf_Append(buf, "\"}", -1));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamSource(nats_JSON *json, const char *fieldName, jsStreamSource **new_source)
{
    jsStreamSource      *source = NULL;
    nats_JSON           *obj    = NULL;
    natsStatus          s;

    if (fieldName != NULL)
    {
        s = nats_JSONGetObject(json, fieldName, &obj);
        if (obj == NULL)
            return NATS_UPDATE_ERR_STACK(s);
    }
    else
    {
        obj = json;
    }

    source = (jsStreamSource*) NATS_CALLOC(1, sizeof(jsStreamSource));
    if (source == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(obj, "name", (char**) &(source->Name));
    IFOK(s, nats_JSONGetULong(obj, "opt_start_seq", &(source->OptStartSeq)));
    IFOK(s, nats_JSONGetTime(obj, "opt_start_time", &(source->OptStartTime)));
    IFOK(s, nats_JSONGetStr(obj, "filter_subject", (char**) &(source->FilterSubject)));
    IFOK(s, _unmarshalExternalStream(obj, "external", &(source->External)));

    if (s == NATS_OK)
        *new_source = source;
    else
        _destroyStreamSource(source);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalStreamSource(jsStreamSource *source, const char *fieldName, natsBuffer *buf)
{
    natsStatus  s = NATS_OK;

    if (fieldName != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"", -1));
        IFOK(s, natsBuf_Append(buf, fieldName, -1));
        IFOK(s, natsBuf_Append(buf, "\":", -1));
    }
    IFOK(s, natsBuf_Append(buf, "{\"name\":\"", -1));
    IFOK(s, natsBuf_Append(buf, source->Name, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    if ((s == NATS_OK) && (source->OptStartSeq > 0))
        s = _marshalLong(buf, true, "opt_start_seq", source->OptStartSeq);
    if ((s == NATS_OK) && (source->OptStartTime > 0))
        IFOK(s, _marshalTimeUTC(buf, "opt_start_time", source->OptStartTime));
    if (source->FilterSubject != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"filter_subject\":\"", -1));
        IFOK(s, natsBuf_Append(buf, source->FilterSubject, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }
    if (source->External != NULL)
        IFOK(s, _marshalExternalStream(source->External, "external", buf));

    IFOK(s, natsBuf_AppendByte(buf, '}'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalPlacement(nats_JSON *json, const char *fieldName, jsPlacement **new_placement)
{
    jsPlacement         *placement = NULL;
    nats_JSON           *jpl       = NULL;
    natsStatus          s;

    s = nats_JSONGetObject(json, fieldName, &jpl);
    if (jpl == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    placement = (jsPlacement*) NATS_CALLOC(1, sizeof(jsPlacement));
    if (placement == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(jpl, "cluster", (char**) &(placement->Cluster));
    IFOK(s, nats_JSONGetArrayStr(jpl, "tags", (char***) &(placement->Tags), &(placement->TagsLen)));

    if (s == NATS_OK)
        *new_placement = placement;
    else
        _destroyPlacement(placement);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalPlacement(jsPlacement *placement, natsBuffer *buf)
{
    natsStatus  s;

    s = natsBuf_Append(buf, ",\"placement\":{\"cluster\":\"", -1);
    IFOK(s, natsBuf_Append(buf, placement->Cluster, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    if (placement->TagsLen > 0)
    {
        int i;

        IFOK(s, natsBuf_Append(buf, ",\"tags\":[\"", -1));
        for (i=0; (s == NATS_OK) && (i<placement->TagsLen); i++)
        {
            IFOK(s, natsBuf_Append(buf, placement->Tags[i], -1));
            IFOK(s, natsBuf_AppendByte(buf, '"'));
            if (i < placement->TagsLen-1)
                IFOK(s, natsBuf_Append(buf, ",\"", -1));
        }
        IFOK(s, natsBuf_AppendByte(buf, ']'));
    }
    IFOK(s, natsBuf_AppendByte(buf, '}'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalRetentionPolicy(nats_JSON *json, const char *fieldName, jsRetentionPolicy *policy)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, fieldName, &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsRetPolicyLimitsStr) == 0)
        *policy = js_LimitsPolicy;
    else if (strcmp(str, jsRetPolicyInterestStr) == 0)
        *policy = js_InterestPolicy;
    else if (strcmp(str, jsRetPolicyWorkQueueStr) == 0)
        *policy = js_WorkQueuePolicy;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal retention policy '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalRetentionPolicy(jsRetentionPolicy policy, natsBuffer *buf)
{
    natsStatus  s;
    const char  *rp = NULL;

    s = natsBuf_Append(buf, ",\"retention\":\"", -1);
    switch (policy)
    {
        case js_LimitsPolicy:       rp = jsRetPolicyLimitsStr;     break;
        case js_InterestPolicy:     rp = jsRetPolicyInterestStr;   break;
        case js_WorkQueuePolicy:    rp = jsRetPolicyWorkQueueStr;  break;
        default:
            rp = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, rp, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalDiscardPolicy(nats_JSON *json, const char *fieldName, jsDiscardPolicy *policy)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, fieldName, &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsDiscardPolicyOldStr) == 0)
        *policy = js_DiscardOld;
    else if (strcmp(str, jsDiscardPolicyNewStr) == 0)
        *policy = js_DiscardNew;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal discard policy '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalDiscardPolicy(jsDiscardPolicy policy, natsBuffer *buf)
{
    natsStatus  s;
    const char  *dp = NULL;

    s = natsBuf_Append(buf, ",\"discard\":\"", -1);
    switch (policy)
    {
        case js_DiscardOld: dp = jsDiscardPolicyOldStr;   break;
        case js_DiscardNew: dp = jsDiscardPolicyNewStr;   break;
        default:
            dp = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, dp, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStorageType(nats_JSON *json, const char *fieldName, jsStorageType *storage)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, "storage", &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsStorageTypeFileStr) == 0)
        *storage = js_FileStorage;
    else if (strcmp(str, jsStorageTypeMemStr) == 0)
        *storage = js_MemoryStorage;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal storage type '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalStorageType(jsStorageType storage, natsBuffer *buf)
{
    natsStatus  s;
    const char  *st = NULL;

    s = natsBuf_Append(buf, ",\"storage\":\"", -1);
    switch (storage)
    {
        case js_FileStorage:    st = jsStorageTypeFileStr; break;
        case js_MemoryStorage:  st = jsStorageTypeMemStr;  break;
        default:
            st = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, st, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_unmarshalStreamConfig(nats_JSON *json, const char *fieldName, jsStreamConfig **new_cfg)
{
    nats_JSON           *jcfg       = NULL;
    jsStreamConfig      *cfg        = NULL;
    nats_JSON           **sources   = NULL;
    int                 sourcesLen  = 0;
    natsStatus          s;

    if (fieldName != NULL)
    {
        s = nats_JSONGetObject(json, fieldName, &jcfg);
        if (jcfg == NULL)
            return NATS_UPDATE_ERR_STACK(s);
    }
    else
    {
        jcfg = json;
    }

    cfg = (jsStreamConfig*) NATS_CALLOC(1, sizeof(jsStreamConfig));
    if (cfg == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(jcfg, "name", (char**) &(cfg->Name));
    IFOK(s, nats_JSONGetArrayStr(jcfg, "subjects", (char***) &(cfg->Subjects), &(cfg->SubjectsLen)));
    IFOK(s, _unmarshalRetentionPolicy(jcfg, "retention", &(cfg->Retention)));
    IFOK(s, nats_JSONGetInt(jcfg, "max_consumers", &(cfg->MaxConsumers)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_msgs", &(cfg->MaxMsgs)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_bytes", &(cfg->MaxBytes)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_age", &(cfg->MaxAge)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_msgs_per_subject", &(cfg->MaxMsgsPerSubject)));
    IFOK(s, nats_JSONGetInt32(jcfg, "max_msg_size", &(cfg->MaxMsgSize)));
    IFOK(s, _unmarshalDiscardPolicy(jcfg, "discard", &(cfg->Discard)));
    IFOK(s, _unmarshalStorageType(jcfg, "storage", &(cfg->Storage)));
    IFOK(s, nats_JSONGetInt(jcfg, "num_replicas", &(cfg->Replicas)));
    IFOK(s, nats_JSONGetBool(jcfg, "no_ack", &(cfg->NoAck)));
    IFOK(s, nats_JSONGetStr(jcfg, "template_owner", (char**) &(cfg->Template)));
    IFOK(s, nats_JSONGetLong(jcfg, "duplicate_window", &(cfg->Duplicates)));
    IFOK(s, _unmarshalPlacement(jcfg, "placement", &(cfg->Placement)));
    IFOK(s, _unmarshalStreamSource(jcfg, "mirror", &(cfg->Mirror)));
    // Get the sources and unmarshal if present
    IFOK(s, nats_JSONGetArrayObject(jcfg, "sources", &sources, &sourcesLen));
    if ((s == NATS_OK) && (sources != NULL))
    {
        int i;

        cfg->Sources = (jsStreamSource**) NATS_CALLOC(sourcesLen, sizeof(jsStreamSource*));
        if (cfg->Sources == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<sourcesLen); i++)
        {
            s = _unmarshalStreamSource(sources[i], NULL, &(cfg->Sources[i]));
            if (s == NATS_OK)
                cfg->SourcesLen++;
        }
        // Free the array of JSON objects that was allocated by nats_JSONGetArrayObject.
        NATS_FREE(sources);
    }
    if (s == NATS_OK)
        *new_cfg = cfg;
    else
        js_destroyStreamConfig(cfg);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_marshalStreamConfig(natsBuffer **new_buf, jsStreamConfig *cfg)
{
    natsBuffer *buf = NULL;
    natsStatus s;

    s = natsBuf_Create(&buf, 256);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    s = natsBuf_Append(buf, "{\"name\":\"", -1);
    IFOK(s, natsBuf_Append(buf, cfg->Name, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));
    if (cfg->SubjectsLen > 0)
    {
        int i;

        IFOK(s, natsBuf_Append(buf, ",\"subjects\":[\"", -1));
        for (i=0; (s == NATS_OK) && (i<cfg->SubjectsLen); i++)
        {
            IFOK(s, natsBuf_Append(buf, cfg->Subjects[i], -1));
            IFOK(s, natsBuf_AppendByte(buf, '"'));
            if (i < cfg->SubjectsLen - 1)
                IFOK(s, natsBuf_Append(buf, ",\"", -1));
        }
        IFOK(s, natsBuf_AppendByte(buf, ']'));
    }
    IFOK(s, _marshalRetentionPolicy(cfg->Retention, buf));

    IFOK(s, _marshalLong(buf, true, "max_consumers", (int64_t) cfg->MaxConsumers));
    IFOK(s, _marshalLong(buf, true, "max_msgs", cfg->MaxMsgs));
    IFOK(s, _marshalLong(buf, true, "max_bytes", cfg->MaxBytes));
    IFOK(s, _marshalLong(buf, true, "max_age", cfg->MaxAge));
    IFOK(s, _marshalLong(buf, true, "max_msg_size", (int64_t) cfg->MaxMsgSize));
    IFOK(s, _marshalLong(buf, true, "max_msgs_per_subject", cfg->MaxMsgsPerSubject));

    IFOK(s, _marshalDiscardPolicy(cfg->Discard, buf));

    IFOK(s, _marshalStorageType(cfg->Storage, buf));

    IFOK(s, _marshalLong(buf, true, "num_replicas", (int64_t) cfg->Replicas));

    if (cfg->NoAck)
        IFOK(s, natsBuf_Append(buf, ",\"no_ack\":true", -1));

    if (cfg->Template != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"template_owner\":\"", -1));
        IFOK(s, natsBuf_Append(buf, cfg->Template, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }

    if (cfg->Duplicates != 0)
        s = _marshalLong(buf, true, "duplicate_window", cfg->Duplicates);

    if (cfg->Placement != NULL)
        IFOK(s, _marshalPlacement(cfg->Placement, buf));

    if (cfg->Mirror != NULL)
        IFOK(s, _marshalStreamSource(cfg->Mirror, "mirror", buf));

    if (cfg->SourcesLen > 0)
    {
        int i;

        IFOK(s, natsBuf_Append(buf, ",\"sources\":[", -1));
        for (i=0; (s == NATS_OK) && (i < cfg->SourcesLen); i++)
        {
            IFOK(s, _marshalStreamSource(cfg->Sources[i], NULL, buf));
            if ((s == NATS_OK) && (i < cfg->SourcesLen-1))
                IFOK(s, natsBuf_AppendByte(buf, ','));
        }
        IFOK(s, natsBuf_AppendByte(buf, ']'));
    }

    IFOK(s, natsBuf_AppendByte(buf, '}'));

    if (s == NATS_OK)
        *new_buf = buf;
    else
        natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalLostStreamData(nats_JSON *pjson, const char *fieldName, jsLostStreamData **new_lost)
{
    natsStatus              s       = NATS_OK;
    jsLostStreamData        *lost   = NULL;
    nats_JSON               *json   = NULL;

    s = nats_JSONGetObject(pjson, fieldName, &json);
    if (json == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    lost = (jsLostStreamData*) NATS_CALLOC(1, sizeof(jsLostStreamData));
    if (lost == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    IFOK(s, nats_JSONGetArrayULong(json, "msgs", &(lost->Msgs), &(lost->MsgsLen)));
    IFOK(s, nats_JSONGetULong(json, "bytes", &(lost->Bytes)));

    if (s == NATS_OK)
        *new_lost = lost;
    else
        _destroyLostStreamData(lost);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_unmarshalStreamState(nats_JSON *pjson, const char *fieldName, jsStreamState *state)
{
    nats_JSON   *json = NULL;
    natsStatus  s;

    s = nats_JSONGetObject(pjson, fieldName, &json);
    if (json == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    s = nats_JSONGetULong(json, "messages", &(state->Msgs));
    IFOK(s, nats_JSONGetULong(json, "bytes", &(state->Bytes)));
    IFOK(s, nats_JSONGetULong(json, "first_seq", &(state->FirstSeq)));
    IFOK(s, nats_JSONGetTime(json, "first_ts", &(state->FirstTime)));
    IFOK(s, nats_JSONGetULong(json, "last_seq", &(state->LastSeq)));
    IFOK(s, nats_JSONGetTime(json, "last_ts", &(state->LastTime)));
    IFOK(s, nats_JSONGetULong(json, "num_deleted", &(state->NumDeleted)));
    IFOK(s, nats_JSONGetArrayULong(json, "deleted", &(state->Deleted), &(state->DeletedLen)));
    IFOK(s, _unmarshalLostStreamData(json, "lost", &(state->Lost)));
    IFOK(s, nats_JSONGetInt(json, "consumer_count", &(state->Consumers)));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalPeerInfo(nats_JSON *json, jsPeerInfo **new_pi)
{
    jsPeerInfo      *pi = NULL;
    natsStatus      s;

    pi = (jsPeerInfo*) NATS_CALLOC(1, sizeof(jsPeerInfo));
    if (pi == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(pi->Name));
    IFOK(s, nats_JSONGetBool(json, "current", &(pi->Current)));
    IFOK(s, nats_JSONGetBool(json, "offline", &(pi->Offline)));
    IFOK(s, nats_JSONGetLong(json, "active", &(pi->Active)));
    IFOK(s, nats_JSONGetULong(json, "lag", &(pi->Lag)));

    if (s == NATS_OK)
        *new_pi = pi;
    else
        _destroyPeerInfo(pi);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalClusterInfo(nats_JSON *pjson, const char *fieldName, jsClusterInfo **new_ci)
{
    jsClusterInfo       *ci         = NULL;
    nats_JSON           *json       = NULL;
    nats_JSON           **replicas  = NULL;
    int                 replicasLen = 0;
    natsStatus          s;

    s = nats_JSONGetObject(pjson, fieldName, &json);
    if (json == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    ci = (jsClusterInfo*) NATS_CALLOC(1, sizeof(jsClusterInfo));
    if (ci == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(ci->Name));
    IFOK(s, nats_JSONGetStr(json, "leader", &(ci->Leader)));
    IFOK(s, nats_JSONGetArrayObject(json, "replicas", &replicas, &replicasLen));
    if ((s == NATS_OK) && (replicas != NULL))
    {
        int i;

        ci->Replicas = (jsPeerInfo**) NATS_CALLOC(replicasLen, sizeof(jsPeerInfo*));
        if (ci->Replicas == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<replicasLen); i++)
        {
            s = _unmarshalPeerInfo(replicas[i], &(ci->Replicas[i]));
            if (s == NATS_OK)
                ci->ReplicasLen++;
        }
        // Free the array of JSON objects that was allocated by nats_JSONGetArrayObject.
        NATS_FREE(replicas);
    }
    if (s == NATS_OK)
        *new_ci = ci;
    else
        _destroyClusterInfo(ci);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamSourceInfo(nats_JSON *pjson, const char *fieldName, jsStreamSourceInfo **new_src)
{
    nats_JSON               *json = NULL;
    jsStreamSourceInfo      *ssi  = NULL;
    natsStatus              s;

    if (fieldName != NULL)
    {
        s = nats_JSONGetObject(pjson, fieldName, &json);
        if (json == NULL)
            return NATS_UPDATE_ERR_STACK(s);
    }
    else
    {
        json = pjson;
    }

    ssi = (jsStreamSourceInfo*) NATS_CALLOC(1, sizeof(jsStreamSourceInfo));
    if (ssi == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(ssi->Name));
    IFOK(s, _unmarshalExternalStream(json, "external", &(ssi->External)));
    IFOK(s, nats_JSONGetULong(json, "lag", &(ssi->Lag)));
    IFOK(s, nats_JSONGetLong(json, "active", &(ssi->Active)));

    if (s == NATS_OK)
        *new_src = ssi;
    else
        _destroyStreamSourceInfo(ssi);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamInfo(nats_JSON *json, jsStreamInfo **new_si)
{
    jsStreamInfo        *si         = NULL;
    nats_JSON           **sources   = NULL;
    int                 sourcesLen  = 0;
    natsStatus          s;

    si = (jsStreamInfo*) NATS_CALLOC(1, sizeof(jsStreamInfo));
    if (si == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    // Get the config object
    s = js_unmarshalStreamConfig(json, "config", &(si->Config));
    IFOK(s, nats_JSONGetTime(json, "created", &(si->Created)));
    IFOK(s, js_unmarshalStreamState(json, "state", &(si->State)));
    IFOK(s, _unmarshalClusterInfo(json, "cluster", &(si->Cluster)));
    IFOK(s, _unmarshalStreamSourceInfo(json, "mirror", &(si->Mirror)));
    IFOK(s, nats_JSONGetArrayObject(json, "sources", &sources, &sourcesLen));
    if ((s == NATS_OK) && (sources != NULL))
    {
        int i;

        si->Sources = (jsStreamSourceInfo**) NATS_CALLOC(sourcesLen, sizeof(jsStreamSourceInfo*));
        if (si->Sources == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<sourcesLen); i++)
        {
            s = _unmarshalStreamSourceInfo(sources[i], NULL, &(si->Sources[i]));
            if (s == NATS_OK)
                si->SourcesLen++;
        }
        // Free the array of JSON objects that was allocated by nats_JSONGetArrayObject.
        NATS_FREE(sources);
    }

    if (s == NATS_OK)
        *new_si = si;
    else
        jsStreamInfo_Destroy(si);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamCreateResp(jsStreamInfo **new_si, natsMsg *resp, jsErrCode *errCode)
{
    nats_JSON           *json = NULL;
    jsApiResponse       ar;
    natsStatus          s;

    s = js_unmarshalResponse(&ar, &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (js_apiResponseIsErr(&ar))
    {
        if (errCode != NULL)
            *errCode = (int) ar.Error.ErrCode;

        // If the error code is JSStreamNotFoundErr then pick NATS_NOT_FOUND
        // as the error and caller will clear error stack.
        if (ar.Error.ErrCode == JSStreamNotFoundErr)
            s = NATS_NOT_FOUND;
        else
            s = nats_setError(NATS_ERR, "%s", ar.Error.Description);
    }
    else if (new_si != NULL)
    {
        // At this point we need to unmarshal the stream info itself.
        s = _unmarshalStreamInfo(json, new_si);
    }

    js_freeApiRespContent(&ar);
    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
jsStreamConfig_Init(jsStreamConfig *cfg)
{
    if (cfg == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(cfg, 0, sizeof(jsStreamConfig));
    cfg->Retention      = js_LimitsPolicy;
    cfg->MaxConsumers   = -1;
    cfg->MaxMsgs        = -1;
    cfg->MaxBytes       = -1;
    cfg->MaxMsgSize     = -1;
    cfg->Storage        = js_FileStorage;
    cfg->Discard        = js_DiscardOld;
    cfg->Replicas       = 1;
    return NATS_OK;
}

static natsStatus
_marshalStreamInfoReq(natsBuffer **new_buf)
{
    natsBuffer  *buf = NULL;
    natsStatus  s;

    s = natsBuf_Create(&buf, 30);
    IFOK(s, natsBuf_Append(buf, "{\"deleted_details\":true}", -1));

    if (s == NATS_OK)
        *new_buf = buf;
    else
        natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_addUpdateOrGet(jsStreamInfo **new_si, jsStreamAction action, jsCtx *js, jsStreamConfig *cfg, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s       = NATS_OK;
    natsBuffer          *buf    = NULL;
    char                *subj   = NULL;
    char                *req    = NULL;
    int                 reqLen  = 0;
    natsMsg             *resp   = NULL;
    natsConnection      *nc     = NULL;
    const char          *apiT   = NULL;
    bool                freePfx = false;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if ((cfg == NULL) || nats_IsStringEmpty(cfg->Name))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrStreamNameRequired);

    if (strchr(cfg->Name, '.'))
        return nats_setError(NATS_INVALID_ARG, "invalid stream name '%s' (cannot contain '.')", cfg->Name);

    switch (action)
    {
        case jsStreamActionCreate: apiT = jsApiStreamCreateT; break;
        case jsStreamActionUpdate: apiT = jsApiStreamUpdateT; break;
        case jsStreamActionGet:    apiT = jsApiStreamInfoT; break;
        default:
            abort();
    }
    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, apiT, js_lenWithoutTrailingDot(o.Prefix), o.Prefix, cfg->Name) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }

    if (action != jsStreamActionGet)
    {
        // Marshal the stream create/update request
        IFOK(s, js_marshalStreamConfig(&buf, cfg));
    }
    else if (o.Stream.Info.DeletedDetails)
    {
        // For GetStreamInfo, if there is an option to get deleted details,
        // we need to request it.
        IFOK(s, _marshalStreamInfoReq(&buf));
    }
    if ((s == NATS_OK) && (buf != NULL))
    {
        req = natsBuf_Data(buf);
        reqLen = natsBuf_Len(buf);
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, req, reqLen, o.Wait));

    // If we got a response, check for error or return the stream info result.
    IFOK(s, _unmarshalStreamCreateResp(new_si, resp, errCode));

    natsBuf_Destroy(buf);
    natsMsg_Destroy(resp);
    NATS_FREE(subj);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_AddStream(jsStreamInfo **new_si, jsCtx *js, jsStreamConfig *cfg, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus s = _addUpdateOrGet(new_si, jsStreamActionCreate, js, cfg, opts, errCode);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_UpdateStream(jsStreamInfo **new_si, jsCtx *js, jsStreamConfig *cfg, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus s = _addUpdateOrGet(new_si, jsStreamActionUpdate, js, cfg, opts, errCode);
    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_GetStreamInfo(jsStreamInfo **new_si, jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s;
    jsStreamConfig      cfg;

    if (errCode != NULL)
        *errCode = 0;

    // Check for new_si here, the js and stream name will be checked in _addUpdateOrGet.
    if (new_si == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    jsStreamConfig_Init(&cfg);
    cfg.Name = (char*) stream;

    s = _addUpdateOrGet(new_si, jsStreamActionGet, js, &cfg, opts, errCode);
    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalSuccessResp(bool *success, natsMsg *resp, jsErrCode *errCode)
{
    nats_JSON           *json = NULL;
    jsApiResponse       ar;
    natsStatus          s;

    *success = false;

    s = js_unmarshalResponse(&ar, &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (js_apiResponseIsErr(&ar))
    {
        if (errCode != NULL)
            *errCode = (int) ar.Error.ErrCode;

        // For stream or consumer not found, return NATS_NOT_FOUND instead of NATS_ERR.
        if ((ar.Error.ErrCode == JSStreamNotFoundErr)
            || (ar.Error.ErrCode == JSConsumerNotFoundErr))
        {
            s = NATS_NOT_FOUND;
        }
        else
            s = nats_setError(NATS_ERR, "%s", ar.Error.Description);
    }
    else
    {
        s = nats_JSONGetBool(json, "success", success);
    }

    js_freeApiRespContent(&ar);
    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalPurgeRequest(natsBuffer **new_buf, struct jsOptionsStreamPurge *opts)
{
    natsStatus          s       = NATS_OK;
    natsBuffer          *buf    = NULL;
    bool                comma   = false;

    if (nats_IsStringEmpty(opts->Subject) && (opts->Sequence == 0) && (opts->Keep == 0))
        return NATS_OK;

    if ((opts->Sequence > 0) && (opts->Keep > 0))
        return nats_setError(NATS_INVALID_ARG,
                             "Sequence (%" PRIu64 ") and Keep (%" PRIu64 " are mutually exclusive",
                             opts->Sequence, opts->Keep);

    s = natsBuf_Create(&buf, 128);
    IFOK(s, natsBuf_AppendByte(buf, '{'));
    if ((s == NATS_OK) && !nats_IsStringEmpty(opts->Subject))
    {
        s = natsBuf_Append(buf, "\"filter\":\"", -1);
        IFOK(s, natsBuf_Append(buf, opts->Subject, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
        comma = true;
    }
    if ((s == NATS_OK) && (opts->Sequence > 0))
        s = _marshalULong(buf, comma, "seq", opts->Sequence);

    if ((s == NATS_OK) && (opts->Keep > 0))
        s = _marshalULong(buf, comma, "keep", opts->Keep);

    IFOK(s, natsBuf_AppendByte(buf, '}'));

    if (s == NATS_OK)
        *new_buf = buf;
    else
        natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_purgeOrDelete(bool purge, jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s       = NATS_OK;
    char                *subj   = NULL;
    natsMsg             *resp   = NULL;
    natsConnection      *nc     = NULL;
    const char          *apiT   = (purge ? jsApiStreamPurgeT : jsApiStreamDeleteT);
    bool                freePfx = false;
    natsBuffer          *buf    = NULL;
    const void          *data   = NULL;
    int                 dlen    = 0;
    bool                success = false;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (nats_IsStringEmpty(stream))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrStreamNameRequired);

    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, apiT, js_lenWithoutTrailingDot(o.Prefix), o.Prefix, stream) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }
    if ((s == NATS_OK) && purge)
    {
        s = _marshalPurgeRequest(&buf, &(o.Stream.Purge));
        if ((s == NATS_OK) && (buf != NULL))
        {
            data = (const void*) natsBuf_Data(buf);
            dlen = natsBuf_Len(buf);
        }
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, js->nc, subj, data, dlen, o.Wait));

    IFOK(s, _unmarshalSuccessResp(&success, resp, errCode));
    if ((s == NATS_OK) && !success)
        s = nats_setError(NATS_ERR, "failed to %s stream '%s'", (purge ? "purge" : "delete"), stream);

    natsBuf_Destroy(buf);
    natsMsg_Destroy(resp);
    NATS_FREE(subj);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_PurgeStream(jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus s = _purgeOrDelete(true, js, stream, opts, errCode);
    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_DeleteStream(jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode)
{
    natsStatus s = _purgeOrDelete(false, js, stream, opts, errCode);
    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }
    return NATS_UPDATE_ERR_STACK(s);
}

//
// Account related functions
//

natsStatus
js_unmarshalAccountInfo(nats_JSON *json, jsAccountInfo **new_ai)
{
    natsStatus          s;
    nats_JSON           *obj = NULL;
    jsAccountInfo       *ai  = NULL;

    ai = (jsAccountInfo*) NATS_CALLOC(1, sizeof(jsAccountInfo));
    if (ai == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetULong(json, "memory", &(ai->Memory));
    IFOK(s, nats_JSONGetULong(json, "storage", &(ai->Store)));
    IFOK(s, nats_JSONGetInt(json, "streams", &(ai->Streams)));
    IFOK(s, nats_JSONGetInt(json, "consumers", &(ai->Consumers)));
    IFOK(s, nats_JSONGetStr(json, "domain", &(ai->Domain)));
    IFOK(s, nats_JSONGetObject(json, "api", &obj));
    if ((s == NATS_OK) && (obj != NULL))
    {
        IFOK(s, nats_JSONGetULong(obj, "total", &(ai->API.Total)));
        IFOK(s, nats_JSONGetULong(obj, "errors", &(ai->API.Errors)));
        obj = NULL;
    }
    IFOK(s, nats_JSONGetObject(json, "limits", &obj));
    if ((s == NATS_OK) && (obj != NULL))
    {
        IFOK(s, nats_JSONGetLong(obj, "max_memory", &(ai->Limits.MaxMemory)));
        IFOK(s, nats_JSONGetLong(obj, "max_storage", &(ai->Limits.MaxStore)));
        IFOK(s, nats_JSONGetInt(obj, "max_streams", &(ai->Limits.MaxStreams)));
        IFOK(s, nats_JSONGetInt(obj, "max_consumers", &(ai->Limits.MaxConsumers)));
        obj = NULL;
    }

    if (s == NATS_OK)
        *new_ai = ai;
    else
        jsAccountInfo_Destroy(ai);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalAccountInfoResp(jsAccountInfo **new_ai, natsMsg *resp, jsErrCode *errCode)
{
    nats_JSON           *json = NULL;
    jsApiResponse       ar;
    natsStatus          s;

    s = js_unmarshalResponse(&ar, &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (js_apiResponseIsErr(&ar))
    {
        if (errCode != NULL)
            *errCode = (int) ar.Error.ErrCode;
        s = nats_setError(NATS_ERR, "%s", ar.Error.Description);
    }
    else
        s = js_unmarshalAccountInfo(json, new_ai);

    js_freeApiRespContent(&ar);
    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_GetAccountInfo(jsAccountInfo **new_ai, jsCtx *js, jsOptions *opts, jsErrCode *errCode)
{
    natsMsg             *resp   = NULL;
    char                *subj   = NULL;
    natsConnection      *nc     = NULL;
    natsStatus          s       = NATS_OK;
    bool                freePfx = false;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if (new_ai == NULL || js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, jsApiAccountInfo, js_lenWithoutTrailingDot(o.Prefix), o.Prefix) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }
    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, NULL, 0, o.Wait));

    // If we get a response, unmarshal the response
    IFOK(s, _unmarshalAccountInfoResp(new_ai, resp, errCode));

    // Common cleanup that is done regardless of success or failure.
    NATS_FREE(subj);
    natsMsg_Destroy(resp);
    return NATS_UPDATE_ERR_STACK(s);
}

void
jsAccountInfo_Destroy(jsAccountInfo *ai)
{
    if (ai == NULL)
        return;

    NATS_FREE(ai->Domain);
    NATS_FREE(ai);
}

natsStatus
jsPlacement_Init(jsPlacement *placement)
{
    if (placement == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(placement, 0, sizeof(jsPlacement));
    return NATS_OK;
}

natsStatus
jsStreamSource_Init(jsStreamSource *source)
{
    if (source == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(source, 0, sizeof(jsStreamSource));
    return NATS_OK;

}

natsStatus
jsExternalStream_Init(jsExternalStream *external)
{
    if (external == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(external, 0, sizeof(jsExternalStream));
    return NATS_OK;
}

//
// Consumer related functions
//

static natsStatus
_marshalDeliverPolicy(natsBuffer *buf, jsDeliverPolicy p)
{
    natsStatus  s;
    const char  *dp = NULL;

    s = natsBuf_Append(buf, "\"deliver_policy\":\"", -1);
    switch (p)
    {
        case js_DeliverAll:             dp = jsDeliverAllStr;     break;
        case js_DeliverLast:            dp = jsDeliverLastStr;    break;
        case js_DeliverNew:             dp = jsDeliverNewStr;     break;
        case js_DeliverByStartSequence: dp = jsDeliverBySeqStr;   break;
        case js_DeliverByStartTime:     dp = jsDeliverByTimeStr;  break;
        default:
            dp = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, dp, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalAckPolicy(natsBuffer *buf, jsAckPolicy p)
{
    natsStatus  s;
    const char  *ap = NULL;

    s = natsBuf_Append(buf, ",\"ack_policy\":\"", -1);
    switch (p)
    {
        case js_AckNone:        ap = jsAckNoneStr;     break;
        case js_AckAll:         ap = jsAckAllStr;      break;
        case js_AckExplicit:    ap = jsAckExplictStr;  break;
        default:
            ap = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, ap, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalReplayPolicy(natsBuffer *buf, jsReplayPolicy p)
{
    natsStatus  s;
    const char  *rp = NULL;

    s = natsBuf_Append(buf, ",\"replay_policy\":\"", -1);
    switch (p)
    {
        case js_ReplayOriginal: rp = jsReplayOriginalStr;  break;
        case js_ReplayInstant:  rp = jsReplayInstantStr;   break;
        default:
            rp = "unknown";
    }
    IFOK(s, natsBuf_Append(buf, rp, -1));
    IFOK(s, natsBuf_AppendByte(buf, '"'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalConsumerCreateReq(natsBuffer **new_buf, const char *stream, jsConsumerConfig *cfg)
{
    natsStatus      s    = NATS_OK;
    natsBuffer      *buf = NULL;

    s = natsBuf_Create(&buf, 256);
    IFOK(s, natsBuf_Append(buf, "{\"stream_name\":\"", -1));
    IFOK(s, natsBuf_Append(buf, stream, -1));
    IFOK(s, natsBuf_Append(buf, "\",\"config\":{", -1));
    // Marshal something that is always present first, so that the optionals
    // will always start with a "," and we know that there will be a field before that.
    IFOK(s, _marshalDeliverPolicy(buf, cfg->DeliverPolicy));
    if ((s == NATS_OK) && (!nats_IsStringEmpty(cfg->Durable)))
    {
        s = natsBuf_Append(buf, ",\"durable_name\":\"", -1);
        IFOK(s, natsBuf_Append(buf, cfg->Durable, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }
    if ((s == NATS_OK) && (!nats_IsStringEmpty(cfg->DeliverSubject)))
    {
        s = natsBuf_Append(buf, ",\"deliver_subject\":\"", -1);
        IFOK(s, natsBuf_Append(buf, cfg->DeliverSubject, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }
    if ((s == NATS_OK) && (cfg->OptStartSeq > 0))
        s = _marshalLong(buf, true, "opt_start_seq", cfg->OptStartSeq);
    if ((s == NATS_OK) && (cfg->OptStartTime > 0))
        s = _marshalTimeUTC(buf, "opt_start_time", cfg->OptStartTime);
    IFOK(s, _marshalAckPolicy(buf, cfg->AckPolicy));
    if ((s == NATS_OK) && (cfg->AckWait > 0))
        s = _marshalLong(buf, true, "ack_wait", cfg->AckWait);
    if ((s == NATS_OK) && (cfg->MaxDeliver > 0))
        s = _marshalLong(buf, true, "max_deliver", (int64_t) cfg->MaxDeliver);
    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->FilterSubject))
    {
        s = natsBuf_Append(buf, ",\"filter_subject\":\"", -1);
        IFOK(s, natsBuf_Append(buf, cfg->FilterSubject, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }
    IFOK(s, _marshalReplayPolicy(buf, cfg->ReplayPolicy))
    if ((s == NATS_OK) && (cfg->RateLimit > 0))
        s = _marshalULong(buf, true, "rate_limit_bps", cfg->RateLimit);
    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->SampleFrequency))
    {
        s = natsBuf_Append(buf, ",\"sample_freq\":\"", -1);
        IFOK(s, natsBuf_Append(buf, cfg->SampleFrequency, -1));
        IFOK(s, natsBuf_AppendByte(buf, '"'));
    }
    if ((s == NATS_OK) && (cfg->MaxWaiting > 0))
        s = _marshalLong(buf, true, "max_waiting", (int64_t) cfg->MaxWaiting);
    if ((s == NATS_OK) && (cfg->MaxAckPending > 0))
        s = _marshalLong(buf, true, "max_ack_pending", (int64_t) cfg->MaxAckPending);
    if ((s == NATS_OK) && cfg->FlowControl)
        s = natsBuf_Append(buf, ",\"flow_control\":true", -1);
    if ((s == NATS_OK) && (cfg->Heartbeat > 0))
        s = _marshalLong(buf, true, "idle_heartbeat", cfg->Heartbeat);
    IFOK(s, natsBuf_Append(buf, "}}", -1));

    if (s == NATS_OK)
        *new_buf = buf;
    else
        natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

static void
_destroyConsumerConfig(jsConsumerConfig *cc)
{
    if (cc == NULL)
        return;

    NATS_FREE((char*) cc->Durable);
    NATS_FREE((char*) cc->DeliverSubject);
    NATS_FREE((char*) cc->FilterSubject);
    NATS_FREE((char*) cc->SampleFrequency);
    NATS_FREE(cc);
}

static natsStatus
_unmarshalDeliverPolicy(nats_JSON *json, const char *fieldName, jsDeliverPolicy *dp)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, fieldName, &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsDeliverAllStr) == 0)
        *dp = js_DeliverAll;
    else if (strcmp(str, jsDeliverLastStr) == 0)
        *dp = js_DeliverLast;
    else if (strcmp(str, jsDeliverNewStr) == 0)
        *dp = js_DeliverNew;
    else if (strcmp(str, jsDeliverBySeqStr) == 0)
        *dp = js_DeliverByStartSequence;
    else if (strcmp(str, jsDeliverByTimeStr) == 0)
        *dp = js_DeliverByStartTime;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal delivery policy '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalAckPolicy(nats_JSON *json, const char *fieldName, jsAckPolicy *ap)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, fieldName, &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsAckNoneStr) == 0)
        *ap = js_AckNone;
    else if (strcmp(str, jsAckAllStr) == 0)
        *ap = js_AckAll;
    else if (strcmp(str, jsAckExplictStr) == 0)
        *ap = js_AckExplicit;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal ack policy '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalReplayPolicy(nats_JSON *json, const char *fieldName, jsReplayPolicy *rp)
{
    natsStatus  s    = NATS_OK;
    char        *str = NULL;

    s = nats_JSONGetStr(json, fieldName, &str);
    if (str == NULL)
        return NATS_UPDATE_ERR_STACK(s);

    if (strcmp(str, jsReplayOriginalStr) == 0)
        *rp = js_ReplayOriginal;
    else if (strcmp(str, jsReplayInstantStr) == 0)
        *rp = js_ReplayInstant;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal replay policy '%s'", str);

    NATS_FREE(str);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalConsumerConfig(nats_JSON *json, const char *fieldName, jsConsumerConfig **new_cc)
{
    natsStatus              s       = NATS_OK;
    jsConsumerConfig        *cc     = NULL;
    nats_JSON               *cjson  = NULL;

    cc = (jsConsumerConfig*) NATS_CALLOC(1, sizeof(jsConsumerConfig));
    if (cc == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetObject(json, fieldName, &cjson);
    if ((s == NATS_OK) && (cjson != NULL))
    {
        s = nats_JSONGetStr(cjson, "durable_name", (char**) &(cc->Durable));
        IFOK(s, nats_JSONGetStr(cjson, "deliver_subject", (char**) &(cc->DeliverSubject)));
        IFOK(s, _unmarshalDeliverPolicy(cjson, "deliver_policy", &(cc->DeliverPolicy)));
        IFOK(s, nats_JSONGetULong(cjson, "opt_start_seq", &(cc->OptStartSeq)));
        IFOK(s, nats_JSONGetTime(cjson, "opt_start_time", &(cc->OptStartTime)));
        IFOK(s, _unmarshalAckPolicy(cjson, "ack_policy", &(cc->AckPolicy)));
        IFOK(s, nats_JSONGetLong(cjson, "ack_wait", &(cc->AckWait)));
        IFOK(s, nats_JSONGetInt(cjson, "max_deliver", &(cc->MaxDeliver)));
        IFOK(s, nats_JSONGetStr(cjson, "filter_subject", (char**) &(cc->FilterSubject)));
        IFOK(s, _unmarshalReplayPolicy(cjson, "replay_policy", &(cc->ReplayPolicy)));
        IFOK(s, nats_JSONGetULong(cjson, "rate_limit_bps", &(cc->RateLimit)));
        IFOK(s, nats_JSONGetStr(cjson, "sample_freq", (char**) &(cc->SampleFrequency)));
        IFOK(s, nats_JSONGetInt(cjson, "max_waiting", &(cc->MaxWaiting)));
        IFOK(s, nats_JSONGetInt(cjson, "max_ack_pending", &(cc->MaxAckPending)));
        IFOK(s, nats_JSONGetBool(cjson, "flow_control", &(cc->FlowControl)));
        IFOK(s, nats_JSONGetLong(cjson, "idle_heartbeat", &(cc->Heartbeat)));
    }

    if (s == NATS_OK)
        *new_cc = cc;
    else
        _destroyConsumerConfig(cc);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalSeqPair(nats_JSON *json, const char *fieldName, jsSequencePair *sp)
{
    natsStatus  s    = NATS_OK;
    nats_JSON   *spj = NULL;

    s = nats_JSONGetObject(json, fieldName, &spj);
    if ((s == NATS_OK) && (spj != NULL))
    {
        IFOK(s, nats_JSONGetULong(spj, "consumer_seq", &(sp->Consumer)));
        IFOK(s, nats_JSONGetULong(spj, "stream_seq", &(sp->Stream)));
    }
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalConsumerInfo(nats_JSON *json, jsConsumerInfo **new_ci)
{
    natsStatus          s   = NATS_OK;
    jsConsumerInfo      *ci = NULL;

    ci = (jsConsumerInfo*) NATS_CALLOC(1, sizeof(jsConsumerInfo));
    if (ci == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "stream_name", &(ci->Stream));
    IFOK(s, nats_JSONGetStr(json, "name", &(ci->Name)));
    IFOK(s, nats_JSONGetTime(json, "created", &(ci->Created)));
    IFOK(s, _unmarshalConsumerConfig(json, "config", &(ci->Config)));
    IFOK(s, _unmarshalSeqPair(json, "delivered", &(ci->Delivered)));
    IFOK(s, _unmarshalSeqPair(json, "ack_floor", &(ci->AckFloor)));
    IFOK(s, nats_JSONGetInt(json, "num_ack_pending", &(ci->NumAckPending)));
    IFOK(s, nats_JSONGetInt(json, "num_redelivered", &(ci->NumRedelivered)));
    IFOK(s, nats_JSONGetInt(json, "num_waiting", &(ci->NumWaiting)));
    IFOK(s, nats_JSONGetULong(json, "num_pending", &(ci->NumPending)));
    IFOK(s, _unmarshalClusterInfo(json, "cluster", &(ci->Cluster)));

    if (s == NATS_OK)
        *new_ci = ci;
    else
        jsConsumerInfo_Destroy(ci);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalConsumerCreateOrGetResp(jsConsumerInfo **new_ci, natsMsg *resp, jsErrCode *errCode)
{
    nats_JSON           *json = NULL;
    jsApiResponse       ar;
    natsStatus          s;

    s = js_unmarshalResponse(&ar, &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (js_apiResponseIsErr(&ar))
    {
        if (errCode != NULL)
            *errCode = (int) ar.Error.ErrCode;

        if (ar.Error.ErrCode == JSConsumerNotFoundErr)
            s = NATS_NOT_FOUND;
        else
            s = nats_setError(NATS_ERR, "%s", ar.Error.Description);
    }
    else if (new_ci != NULL)
    {
        // At this point we need to unmarshal the consumer info itself.
        s = _unmarshalConsumerInfo(json, new_ci);
    }

    js_freeApiRespContent(&ar);
    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_AddConsumer(jsConsumerInfo **new_ci, jsCtx *js,
               const char *stream, jsConsumerConfig *cfg,
               jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s       = NATS_OK;
    natsBuffer          *buf    = NULL;
    natsConnection      *nc     = NULL;
    char                *subj   = NULL;
    bool                freePfx = false;
    natsMsg             *resp   = NULL;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if ((js == NULL) || (cfg == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (nats_IsStringEmpty(stream))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrStreamNameRequired);

    if (!nats_IsStringEmpty(cfg->Durable) && (strchr(cfg->Durable, '.') != NULL))
        return nats_setError(NATS_INVALID_ARG, "invalid durable name '%s' (cannot contain '.')", cfg->Durable);

    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        int res;

        if (nats_IsStringEmpty(cfg->Durable))
            res = nats_asprintf(&subj, jsApiConsumerCreateT,
                                js_lenWithoutTrailingDot(o.Prefix), o.Prefix,
                                stream);
        else
            res = nats_asprintf(&subj, jsApiDurableCreateT,
                                js_lenWithoutTrailingDot(o.Prefix), o.Prefix,
                                stream, cfg->Durable);
        if (res < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }
    IFOK(s, _marshalConsumerCreateReq(&buf, stream, cfg));

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, natsBuf_Data(buf), natsBuf_Len(buf), o.Wait));

    // If we got a response, check for error or return the consumer info result.
    IFOK(s, _unmarshalConsumerCreateOrGetResp(new_ci, resp, errCode));

    NATS_FREE(subj);
    natsMsg_Destroy(resp);
    natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_GetConsumerInfo(jsConsumerInfo **new_ci, jsCtx *js,
                   const char *stream, const char *consumer,
                   jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s       = NATS_OK;
    char                *subj   = NULL;
    bool                freePfx = false;
    natsConnection      *nc     = NULL;
    natsMsg             *resp   = NULL;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if ((js == NULL) || (new_ci == NULL))
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (nats_IsStringEmpty(stream))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrStreamNameRequired);

    if (nats_IsStringEmpty(consumer))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrConsumerNameRequired);

    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, jsApiConsumerInfoT,
                          js_lenWithoutTrailingDot(o.Prefix), o.Prefix,
                          stream, consumer) < 0 )
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, NULL, 0, o.Wait));

    // If we got a response, check for error or return the consumer info result.
    IFOK(s, _unmarshalConsumerCreateOrGetResp(new_ci, resp, errCode));

    NATS_FREE(subj);
    natsMsg_Destroy(resp);

    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
js_DeleteConsumer(jsCtx *js, const char *stream, const char *consumer,
                  jsOptions *opts, jsErrCode *errCode)
{
    natsStatus          s = NATS_OK;
    char                *subj   = NULL;
    bool                freePfx = false;
    natsConnection      *nc     = NULL;
    natsMsg             *resp   = NULL;
    bool                success = false;
    jsOptions           o;

    if (errCode != NULL)
        *errCode = 0;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (nats_IsStringEmpty(stream))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrStreamNameRequired);

    if (nats_IsStringEmpty(consumer))
        return nats_setError(NATS_INVALID_ARG, "%s", jsErrConsumerNameRequired);

    s = js_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, jsApiConsumerDeleteT,
                          js_lenWithoutTrailingDot(o.Prefix), o.Prefix,
                          stream, consumer) < 0 )
        {
            s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, NULL, 0, o.Wait));

    // If we got a response, check for error and success result.
    IFOK(s, _unmarshalSuccessResp(&success, resp, errCode));
    if ((s == NATS_OK) && !success)
        s = nats_setError(NATS_ERR, "failed to delete consumer '%s'", consumer);

    NATS_FREE(subj);
    natsMsg_Destroy(resp);

    if (s == NATS_NOT_FOUND)
    {
        nats_clearLastError();
        return s;
    }

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
jsConsumerConfig_Init(jsConsumerConfig *cc)
{
    if (cc == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(cc, 0, sizeof(jsConsumerConfig));
    return NATS_OK;
}

void
jsConsumerInfo_Destroy(jsConsumerInfo *ci)
{
    if (ci == NULL)
        return;

    NATS_FREE(ci->Stream);
    NATS_FREE(ci->Name);
    _destroyConsumerConfig(ci->Config);
    _destroyClusterInfo(ci->Cluster);
    NATS_FREE(ci);
}
