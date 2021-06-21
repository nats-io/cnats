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

static const char *jsRetPolicyLimits      = "limits";
static const char *jsRetPolicyInterest    = "interest";
static const char *jsRetPolicyWorkQueue   = "workqueue";

static const char *jsDiscardPolicyOld   = "old";
static const char *jsDiscardPolicyNew   = "new";

static const char *jsStorageTypeFile    = "file";
static const char *jsStorageTypeMem     = "memory";

typedef enum
{
    jsStreamActionCreate = 1,
    jsStreamActionUpdate,
    jsStreamActionGet,

} jsStreamAction;

typedef struct __streamCreateResp
{
    natsJSApiResponse   ar;
    natsJSStreamInfo    *si;

} streamCreateResp, streamInfoResp;

typedef struct __accountInfoResp
{
    natsJSApiResponse   ar;
    natsJSAccountInfo   *ai;

} accountInfoResp;

typedef struct __streamPurgeOrDel
{
    natsJSApiResponse   ar;
    bool                Success;    //`json:"success,omitempty"`

} streamPurgeOrDel;

static void
_destroyPlacement(natsJSPlacement *placement)
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
_destroyExternalStream(natsJSExternalStream *external)
{
    if (external == NULL)
        return;

    NATS_FREE((char*) external->APIPrefix);
    NATS_FREE((char*) external->DeliverPrefix);
    NATS_FREE(external);
}

static void
_destroyStreamSource(natsJSStreamSource *source)
{
    if (source == NULL)
        return;

    NATS_FREE((char*) source->Name);
    NATS_FREE((char*) source->FilterSubject);
    _destroyExternalStream(source->External);
    NATS_FREE(source);
}

void
natsJS_destroyStreamConfig(natsJSStreamConfig *cfg)
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
_destroyPeerInfo(natsJSPeerInfo *peer)
{
    if (peer == NULL)
        return;

    NATS_FREE(peer->Name);
    NATS_FREE(peer);
}

static void
_destroyClusterInfo(natsJSClusterInfo *cluster)
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
_destroyStreamSourceInfo(natsJSStreamSourceInfo *info)
{
    if (info == NULL)
        return;

    NATS_FREE(info->Name);
    _destroyExternalStream(info->External);
    NATS_FREE(info);
}

void
natsJSStreamInfo_Destroy(natsJSStreamInfo *si)
{
    int i;

    if (si == NULL)
        return;

    natsJS_destroyStreamConfig(si->Config);
    NATS_FREE(si->Created);
    _destroyClusterInfo(si->Cluster);
    _destroyStreamSourceInfo(si->Mirror);
    for (i=0; i<si->SourcesLen; i++)
        _destroyStreamSourceInfo(si->Sources[i]);
    NATS_FREE(si->Sources);
    NATS_FREE(si);
}

static natsStatus
_unmarshalExternalStream(natsJSExternalStream **new_external, nats_JSON *obj)
{
    natsJSExternalStream    *external = NULL;
    natsStatus              s;

    external = (natsJSExternalStream*) NATS_CALLOC(1, sizeof(natsJSExternalStream));
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
_marshalExternalStream(natsJSExternalStream *external, const char *fieldName, natsBuffer *buf)
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
_unmarshalStreamSource(natsJSStreamSource **new_source, nats_JSON *obj)
{
    natsJSStreamSource  *source = (natsJSStreamSource*) NATS_CALLOC(1, sizeof(natsJSStreamSource));
    nats_JSON           *jext   = NULL;
    natsStatus          s;

    if (source == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(obj, "name", (char**) &(source->Name));
    IFOK_INF(s, nats_JSONGetULong(obj, "opt_start_seq", &(source->OptStartSeq)));
    IFOK_INF(s, nats_JSONGetStr(obj, "filter_subject", (char**) &(source->FilterSubject)));
    IFOK_INF(s, nats_JSONGetObject(obj, "external", &jext));
    if (jext != NULL)
        s = _unmarshalExternalStream(&(source->External), jext);

    if (s == NATS_OK)
        *new_source = source;
    else
        _destroyStreamSource(source);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalStreamSource(natsJSStreamSource *source, const char *fieldName, natsBuffer *buf)
{
    natsStatus  s = NATS_OK;
    char        temp[256];

    if (fieldName != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"", -1));
        IFOK(s, natsBuf_Append(buf, fieldName, -1));
        IFOK(s, natsBuf_Append(buf, "\":", -1));
    }
    IFOK(s, natsBuf_Append(buf, "{\"name\":\"", -1));
    IFOK(s, natsBuf_Append(buf, source->Name, -1));
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    if (source->OptStartSeq != 0)
    {
        IFOK(s, natsBuf_Append(buf, ",\"opt_start_seq\":", -1));
        snprintf(temp, sizeof(temp), "%" PRIu64, source->OptStartSeq);
        IFOK(s, natsBuf_Append(buf, temp, -1));
    }
    if (source->FilterSubject != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"filter_subject\":\"", -1));
        IFOK(s, natsBuf_Append(buf, source->FilterSubject, -1));
        IFOK(s, natsBuf_AppendByte(buf, '\"'));
    }
    if (source->External != NULL)
        IFOK(s, _marshalExternalStream(source->External, "external", buf));

    IFOK(s, natsBuf_AppendByte(buf, '}'));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalPlacement(natsJSPlacement **new_placement, nats_JSON *jpl)
{
    natsJSPlacement     *placement = (natsJSPlacement*) NATS_CALLOC(1, sizeof(natsJSPlacement));
    natsStatus          s;

    if (placement == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(jpl, "cluster", (char**) &(placement->Cluster));
    IFOK_INF(s, nats_JSONGetArrayStr(jpl, "tags", (char***) &(placement->Tags), &(placement->TagsLen)));

    if (s == NATS_OK)
        *new_placement = placement;
    else
        _destroyPlacement(placement);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalPlacement(natsJSPlacement *placement, natsBuffer *buf)
{
    natsStatus  s;

    s = natsBuf_Append(buf, ",\"placement\":{\"cluster\":\"", -1);
    IFOK(s, natsBuf_Append(buf, placement->Cluster, -1));
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    if (placement->TagsLen > 0)
    {
        int i;

        IFOK(s, natsBuf_Append(buf, ",\"tags\":[\"", -1));
        for (i=0; (s == NATS_OK) && (i<placement->TagsLen); i++)
        {
            IFOK(s, natsBuf_Append(buf, placement->Tags[i], -1));
            IFOK(s, natsBuf_AppendByte(buf, '\"'));
            if (i < placement->TagsLen-1)
                IFOK(s, natsBuf_Append(buf, ",\"", -1));
        }
        IFOK(s, natsBuf_AppendByte(buf, ']'));
    }
    IFOK(s, natsBuf_AppendByte(buf, '}'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalRetentionPolicy(natsJSRetentionPolicy *policy, char **pStr)
{
    natsStatus  s    = NATS_OK;
    char        *str = *pStr;

    if (strcmp(str, jsRetPolicyLimits) == 0)
        *policy = natsJS_LimitsPolicy;
    else if (strcmp(str, jsRetPolicyInterest) == 0)
        *policy = natsJS_InterestPolicy;
    else if (strcmp(str, jsRetPolicyWorkQueue) == 0)
        *policy = natsJS_WorkQueuePolicy;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal retention policy '%s'", str);

    NATS_FREE(*pStr);
    *pStr = NULL;

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalRetentionPolicy(natsJSRetentionPolicy policy, natsBuffer *buf)
{
    natsStatus  s;

    s = natsBuf_Append(buf, ",\"retention\":\"", -1);
    switch (policy)
    {
        case natsJS_LimitsPolicy:       IFOK(s, natsBuf_Append(buf, jsRetPolicyLimits, -1));     break;
        case natsJS_InterestPolicy:     IFOK(s, natsBuf_Append(buf, jsRetPolicyInterest, -1));   break;
        case natsJS_WorkQueuePolicy:    IFOK(s, natsBuf_Append(buf, jsRetPolicyWorkQueue, -1));  break;
        default:
            IFOK(s, natsBuf_Append(buf, "unknown", -1)); break;
    }
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalDiscardPolicy(natsJSDiscardPolicy *policy, char **pStr)
{
    natsStatus  s    = NATS_OK;
    char        *str = *pStr;

    if (strcmp(str, jsDiscardPolicyOld) == 0)
        *policy = natsJS_DiscardOld;
    else if (strcmp(str, jsDiscardPolicyNew) == 0)
        *policy = natsJS_DiscardNew;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal discard policy '%s'", str);

    NATS_FREE(*pStr);
    *pStr = NULL;

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalDiscardPolicy(natsJSDiscardPolicy policy, natsBuffer *buf)
{
    natsStatus  s;

    s = natsBuf_Append(buf, ",\"discard\":\"", -1);
    switch (policy)
    {
        case natsJS_DiscardOld: IFOK(s, natsBuf_Append(buf, jsDiscardPolicyOld, -1));    break;
        case natsJS_DiscardNew: IFOK(s, natsBuf_Append(buf, jsDiscardPolicyNew, -1));    break;
        default:
            IFOK(s, natsBuf_Append(buf, "unknown", -1)); break;
    }
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStorageType(natsJSStorageType *storage, char **pStr)
{
    natsStatus  s    = NATS_OK;
    char        *str = *pStr;

    if (strcmp(str, jsStorageTypeFile) == 0)
        *storage = natsJS_FileStorage;
    else if (strcmp(str, jsStorageTypeMem) == 0)
        *storage = natsJS_MemoryStorage;
    else
        s = nats_setError(NATS_ERR, "unable to unmarshal storage type '%s'", str);

    NATS_FREE(*pStr);
    *pStr = NULL;

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalStorageType(natsJSStorageType storage, natsBuffer *buf)
{
    natsStatus  s;

    s = natsBuf_Append(buf, ",\"storage\":\"", -1);
    switch (storage)
    {
        case natsJS_FileStorage:    IFOK(s, natsBuf_Append(buf, jsStorageTypeFile, -1));    break;
        case natsJS_MemoryStorage:  IFOK(s, natsBuf_Append(buf, jsStorageTypeMem, -1));     break;
        default:
            IFOK(s, natsBuf_Append(buf, "unknown", -1)); break;
    }
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_unmarshalStreamConfig(natsJSStreamConfig **new_cfg, nats_JSON *jcfg)
{
    natsJSStreamConfig  *cfg        = NULL;
    nats_JSON           *jpl        = NULL;
    nats_JSON           *jm         = NULL;
    nats_JSON           **sources   = NULL;
    int                 sourcesLen  = 0;
    char                *tmpStr     = NULL;
    natsStatus          s;

    cfg = (natsJSStreamConfig*) NATS_CALLOC(1, sizeof(natsJSStreamConfig));
    if (cfg == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(jcfg, "name", (char**) &(cfg->Name));
    IFOK_INF(s, nats_JSONGetArrayStr(jcfg, "subjects", (char***) &(cfg->Subjects), &(cfg->SubjectsLen)));
    IFOK(s, nats_JSONGetStr(jcfg, "retention", &tmpStr));
    IFOK(s, _unmarshalRetentionPolicy(&(cfg->Retention), &tmpStr));
    IFOK(s, nats_JSONGetInt(jcfg, "max_consumers", &(cfg->MaxConsumers)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_msgs", &(cfg->MaxMsgs)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_bytes", &(cfg->MaxBytes)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_age", &(cfg->MaxAge)));
    IFOK_INF(s, nats_JSONGetInt32(jcfg, "max_msg_size", &(cfg->MaxMsgSize)));
    IFOK(s, nats_JSONGetLong(jcfg, "max_msgs_per_subject", &(cfg->MaxMsgsPerSubject)));
    IFOK(s, nats_JSONGetStr(jcfg, "discard", &tmpStr));
    IFOK(s, _unmarshalDiscardPolicy(&(cfg->Discard), &tmpStr));
    IFOK(s, nats_JSONGetStr(jcfg, "storage", &tmpStr));
    IFOK(s, _unmarshalStorageType(&(cfg->Storage), &tmpStr));
    IFOK(s, nats_JSONGetInt(jcfg, "num_replicas", &(cfg->Replicas)));
    IFOK_INF(s, nats_JSONGetBool(jcfg, "no_ack", &(cfg->NoAck)));
    IFOK_INF(s, nats_JSONGetStr(jcfg, "template_owner", (char**) &(cfg->Template)));
    IFOK_INF(s, nats_JSONGetLong(jcfg, "duplicate_window", &(cfg->Duplicates)));
    // Get the placement object and unmarshal if present
    IFOK_INF(s, nats_JSONGetObject(jcfg, "placement", &jpl));
    if (jpl != NULL)
        s = _unmarshalPlacement(&(cfg->Placement), jpl);
    // Get the mirror object and unmarshal if present
    IFOK_INF(s, nats_JSONGetObject(jcfg, "mirror", &jm));
    if (jm != NULL)
        s = _unmarshalStreamSource(&(cfg->Mirror), jm);
    // Get the sources and unmarshal if present
    IFOK_INF(s, nats_JSONGetArrayObject(jcfg, "sources", &sources, &sourcesLen));
    if (sources != NULL)
    {
        int i;

        cfg->Sources = (natsJSStreamSource**) NATS_CALLOC(sourcesLen, sizeof(natsJSStreamSource*));
        if (cfg->Sources == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<sourcesLen); i++)
        {
            s = _unmarshalStreamSource(&(cfg->Sources[i]), sources[i]);
            if (s == NATS_OK)
                cfg->SourcesLen++;
        }
        // Free the array of JSON objects that was allocated by nats_JSONGetArrayObject.
        NATS_FREE(sources);
    }
    if (s == NATS_OK)
        *new_cfg = cfg;
    else
        natsJS_destroyStreamConfig(cfg);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_marshalStreamConfig(natsBuffer **new_buf, natsJSStreamConfig *cfg)
{
    natsBuffer *buf = NULL;
    char       temp[256];
    natsStatus s;

    s = natsBuf_Create(&buf, 256);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    s = natsBuf_Append(buf, "{\"name\":\"", -1);
    IFOK(s, natsBuf_Append(buf, cfg->Name, -1));
    IFOK(s, natsBuf_AppendByte(buf, '\"'));
    if (cfg->SubjectsLen > 0)
    {
        int i;

        IFOK(s, natsBuf_Append(buf, ",\"subjects\":[\"", -1));
        for (i=0; (s == NATS_OK) && (i<cfg->SubjectsLen); i++)
        {
            IFOK(s, natsBuf_Append(buf, cfg->Subjects[i], -1));
            IFOK(s, natsBuf_AppendByte(buf, '\"'));
            if (i < cfg->SubjectsLen - 1)
                IFOK(s, natsBuf_Append(buf, ",\"", -1));
        }
        IFOK(s, natsBuf_AppendByte(buf, ']'));
    }
    IFOK(s, _marshalRetentionPolicy(cfg->Retention, buf));

    IFOK(s, natsBuf_Append(buf, ",\"max_consumers\":", -1));
    snprintf(temp, sizeof(temp), "%d", cfg->MaxConsumers);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, natsBuf_Append(buf, ",\"max_msgs\":", -1));
    snprintf(temp, sizeof(temp), "%" PRId64, cfg->MaxMsgs);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, natsBuf_Append(buf, ",\"max_bytes\":", -1));
    snprintf(temp, sizeof(temp), "%" PRId64, cfg->MaxBytes);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, natsBuf_Append(buf, ",\"max_age\":", -1));
    snprintf(temp, sizeof(temp), "%" PRId64, cfg->MaxAge);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, natsBuf_Append(buf, ",\"max_msg_size\":", -1));
    snprintf(temp, sizeof(temp), "%d", (int) cfg->MaxMsgSize);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, natsBuf_Append(buf, ",\"max_msgs_per_subject\":", -1));
    snprintf(temp, sizeof(temp), "%d", (int) cfg->MaxMsgsPerSubject);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    IFOK(s, _marshalDiscardPolicy(cfg->Discard, buf));

    IFOK(s, _marshalStorageType(cfg->Storage, buf));

    IFOK(s, natsBuf_Append(buf, ",\"num_replicas\":", -1));
    snprintf(temp, sizeof(temp), "%d", cfg->Replicas);
    IFOK(s, natsBuf_Append(buf, temp, -1));

    if (cfg->NoAck)
        IFOK(s, natsBuf_Append(buf, ",\"no_ack\":true", -1));

    if (cfg->Template != NULL)
    {
        IFOK(s, natsBuf_Append(buf, ",\"template_owner\":\"", -1));
        IFOK(s, natsBuf_Append(buf, cfg->Template, -1));
        IFOK(s, natsBuf_AppendByte(buf, '\"'));
    }

    if (cfg->Duplicates != 0)
    {
        IFOK(s, natsBuf_Append(buf, ",\"duplicate_window\":", -1));
        snprintf(temp, sizeof(temp), "%" PRId64, cfg->Duplicates);
        IFOK(s, natsBuf_Append(buf, temp, -1));
    }

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
_unmarshalStreamState(natsJSStreamState *state, nats_JSON *json)
{
    natsStatus s;

    s = nats_JSONGetULong(json, "messages", &(state->Msgs));
    IFOK(s, nats_JSONGetULong(json, "bytes", &(state->Bytes)));
    IFOK(s, nats_JSONGetULong(json, "first_seq", &(state->FirstSeq)));
    IFOK(s, nats_JSONGetULong(json, "last_seq", &(state->LastSeq)));
    IFOK(s, nats_JSONGetInt(json, "consumer_count", &(state->Consumers)));

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalPeerInfo(natsJSPeerInfo **new_pi, nats_JSON *json)
{
    natsJSPeerInfo  *pi = NULL;
    natsStatus      s;

    pi = (natsJSPeerInfo*) NATS_CALLOC(1, sizeof(natsJSPeerInfo));
    if (pi == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(pi->Name));
    IFOK(s, nats_JSONGetBool(json, "current", &(pi->Current)));
    IFOK_INF(s, nats_JSONGetBool(json, "offline", &(pi->Offline)));
    IFOK(s, nats_JSONGetLong(json, "active", &(pi->Active)));
    IFOK_INF(s, nats_JSONGetULong(json, "lag", &(pi->Lag)));

    if (s == NATS_OK)
        *new_pi = pi;
    else
        _destroyPeerInfo(pi);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalClusterInfo(natsJSClusterInfo **new_ci, nats_JSON *json)
{
    natsJSClusterInfo   *ci         = NULL;
    nats_JSON           **replicas  = NULL;
    int                 replicasLen = 0;
    natsStatus          s;

    ci = (natsJSClusterInfo*) NATS_CALLOC(1, sizeof(natsJSClusterInfo));
    if (ci == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(ci->Name));
    IFOK_INF(s, nats_JSONGetStr(json, "leader", &(ci->Leader)));
    IFOK_INF(s, nats_JSONGetArrayObject(json, "replicas", &replicas, &replicasLen));
    if (replicas != NULL)
    {
        int i;

        ci->Replicas = (natsJSPeerInfo**) NATS_CALLOC(replicasLen, sizeof(natsJSPeerInfo*));
        if (ci->Replicas == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<replicasLen); i++)
        {
            s = _unmarshalPeerInfo(&(ci->Replicas[i]), replicas[i]);
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
_unmarshalStreamSourceInfo(natsJSStreamSourceInfo **new_src, nats_JSON *json)
{
    natsJSStreamSourceInfo  *ssi  = NULL;
    nats_JSON               *jext = NULL;
    natsStatus              s;

    ssi = (natsJSStreamSourceInfo*) NATS_CALLOC(1, sizeof(natsJSStreamSourceInfo));
    if (ssi == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetStr(json, "name", &(ssi->Name));
    IFOK(s, nats_JSONGetLong(json, "active", &(ssi->Active)));
    IFOK(s, nats_JSONGetULong(json, "lag", &(ssi->Lag)));
    IFOK_INF(s, nats_JSONGetObject(json, "external", &jext));
    if (jext != NULL)
        s = _unmarshalExternalStream(&(ssi->External), jext);

    if (s == NATS_OK)
        *new_src = ssi;
    else
        _destroyStreamSourceInfo(ssi);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamInfo(natsJSStreamInfo **new_si, nats_JSON *json)
{
    natsJSStreamInfo    *si         = NULL;
    nats_JSON           *jcfg       = NULL;
    nats_JSON           *jstate     = NULL;
    nats_JSON           *jcluster   = NULL;
    nats_JSON           *jmirror    = NULL;
    nats_JSON           **sources   = NULL;
    int                 sourcesLen  = 0;
    natsStatus          s;

    si = (natsJSStreamInfo*) NATS_CALLOC(1, sizeof(natsJSStreamInfo));
    if (si == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    // Get the config object
    s = nats_JSONGetObject(json, "config", &jcfg);
    IFOK(s, natsJS_unmarshalStreamConfig(&(si->Config), jcfg));
    IFOK(s, nats_JSONGetStr(json, "created", &(si->Created)));
    IFOK(s, nats_JSONGetObject(json, "state", &jstate));
    IFOK(s, _unmarshalStreamState(&(si->State), jstate));
    IFOK_INF(s, nats_JSONGetObject(json, "cluster", &jcluster));
    if (jcluster != NULL)
        s = _unmarshalClusterInfo(&(si->Cluster), jcluster);
    IFOK_INF(s, nats_JSONGetObject(json, "mirror", &jmirror));
    if (jmirror != NULL)
        s = _unmarshalStreamSourceInfo(&(si->Mirror), jmirror);
    IFOK_INF(s, nats_JSONGetArrayObject(json, "sources", &sources, &sourcesLen));
    if (sources != NULL)
    {
        int i;

        si->Sources = (natsJSStreamSourceInfo**) NATS_CALLOC(sourcesLen, sizeof(natsJSStreamSourceInfo*));
        if (si->Sources == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        for (i=0; (s == NATS_OK) && (i<sourcesLen); i++)
        {
            s = _unmarshalStreamSourceInfo(&(si->Sources[i]), sources[i]);
            if (s == NATS_OK)
                si->SourcesLen++;
        }
        // Free the array of JSON objects that was allocated by nats_JSONGetArrayObject.
        NATS_FREE(sources);
    }

    if (s == NATS_OK)
        *new_si = si;
    else
        natsJSStreamInfo_Destroy(si);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalStreamCreateResp(streamCreateResp *scr, natsMsg *resp)
{
    natsJSStreamInfo    *si   = NULL;
    nats_JSON           *json = NULL;
    natsStatus          s;

    memset(scr, 0, sizeof(streamCreateResp));

    s = natsJS_unmarshalResponse(&(scr->ar), &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (natsJS_apiResponseIsErr(&(scr->ar)))
        goto END;

    // At this point we need to unmarshal the stream info itself.
    s = _unmarshalStreamInfo(&si, json);

    if (s == NATS_OK)
        scr->si = si;
    else
        natsJSStreamInfo_Destroy(si);

END:
    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

static void
_freeStreamCreateRespContent(streamCreateResp *scr)
{
    if (scr == NULL)
        return;

    natsJS_freeApiRespContent(&(scr->ar));
    natsJSStreamInfo_Destroy(scr->si);
}

natsStatus
natsJSStreamConfig_Init(natsJSStreamConfig *cfg)
{
    if (cfg == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(cfg, 0, sizeof(natsJSStreamConfig));
    cfg->Retention      = natsJS_LimitsPolicy;
    cfg->MaxConsumers   = -1;
    cfg->MaxMsgs        = -1;
    cfg->MaxBytes       = -1;
    cfg->MaxMsgSize     = -1;
    cfg->Storage        = natsJS_FileStorage;
    cfg->Discard        = natsJS_DiscardOld;
    cfg->Replicas       = 1;
    return NATS_OK;
}

static natsStatus
_addUpdateOrGet(natsJSStreamInfo **new_si, jsStreamAction action, natsJS *js, natsJSStreamConfig *cfg, natsJSOptions *opts, natsJSErrCode *errCode)
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
    streamCreateResp    actResp;
    natsJSOptions       o;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if ((cfg == NULL) || nats_IsStringEmpty(cfg->Name))
        return nats_setError(NATS_INVALID_ARG, "%s", "stream name is required");

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
    s = natsJS_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, apiT, natsJS_lenWithoutTrailingDot(o.Prefix), o.Prefix, cfg->Name) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }

    if (action != jsStreamActionGet)
    {
        // Marshal the stream create/update request
        IFOK(s, natsJS_marshalStreamConfig(&buf, cfg));
        if (s == NATS_OK)
        {
            req = natsBuf_Data(buf);
            reqLen = natsBuf_Len(buf);
        }
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, req, reqLen, o.Wait));

    // If we got a response, check for error or return the stream info result.
    // Note: streamCreateResp and streamInfoResp used in update/get stream are the same.
    IFOK(s, _unmarshalStreamCreateResp(&actResp, resp));
    if (s == NATS_OK)
    {
        if (natsJS_apiResponseIsErr(&(actResp.ar)))
        {
            if (errCode != NULL)
                *errCode = (int) actResp.ar.Error.ErrCode;

            // Pick NATS_NOT_FOUND if this is for GetStreamInfo and the error is "stream not found".
            if ((action == jsStreamActionGet) && (actResp.ar.Error.ErrCode == JSStreamNotFoundErr))
                s = NATS_NOT_FOUND;
            else
                s = nats_setError(NATS_ERR, "%s", actResp.ar.Error.Description);
        }
        else if (new_si != NULL)
        {
            *new_si = actResp.si;
            actResp.si = NULL;
        }
        _freeStreamCreateRespContent(&actResp);
    }

    natsBuf_Destroy(buf);
    natsMsg_Destroy(resp);
    NATS_FREE(subj);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_AddStream(natsJSStreamInfo **new_si, natsJS *js, natsJSStreamConfig *cfg, natsJSOptions *opts, natsJSErrCode *errCode)
{
    natsStatus s = _addUpdateOrGet(new_si, jsStreamActionCreate, js, cfg, opts, errCode);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_UpdateStream(natsJSStreamInfo **new_si, natsJS *js, natsJSStreamConfig *cfg, natsJSOptions *opts, natsJSErrCode *errCode)
{
    natsStatus s = _addUpdateOrGet(new_si, jsStreamActionUpdate, js, cfg, opts, errCode);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_GetStreamInfo(natsJSStreamInfo **new_si, natsJS *js, const char *stream, natsJSOptions *opts, natsJSErrCode *errCode)
{
    natsStatus          s;
    natsJSStreamConfig  cfg;

    // Check for new_si here, the js and stream name will be checked in _addUpdateOrGet.
    if (new_si == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    natsJSStreamConfig_Init(&cfg);
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
_unmarshalStreamPurgeOrDelResp(streamPurgeOrDel *pdr, natsMsg *resp)
{
    nats_JSON   *json = NULL;
    natsStatus  s;

    memset(pdr, 0, sizeof(streamPurgeOrDel));

    s = natsJS_unmarshalResponse(&(pdr->ar), &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (!natsJS_apiResponseIsErr(&(pdr->ar)))
        IFOK_INF(s, nats_JSONGetBool(json, "success", &(pdr->Success)));

    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_marshalPurgeRequest(natsBuffer **new_buf, natsJSPurgeOptions *opts)
{
    natsStatus          s       = NATS_OK;
    natsBuffer          *buf    = NULL;
    bool                comma   = false;
    char                temp[64]= {'0'};

    if (nats_IsStringEmpty(opts->Subject) && (opts->Sequence <= 0) && opts->Keep <= 0)
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
    {
        snprintf(temp, sizeof(temp), "%" PRIu64, opts->Sequence);

        if (comma)
            s = natsBuf_AppendByte(buf, ',');

        IFOK(s, natsBuf_Append(buf, "\"seq\":", -1));
        IFOK(s, natsBuf_Append(buf, temp, -1));
        comma = true;
    }
    if ((s == NATS_OK) && (opts->Keep > 0))
    {
        snprintf(temp, sizeof(temp), "%" PRIu64, opts->Keep);

        if (comma)
            s = natsBuf_AppendByte(buf, ',');

        IFOK(s, natsBuf_Append(buf, "\"keep\":", -1));
        IFOK(s, natsBuf_Append(buf, temp, -1));
    }
    IFOK(s, natsBuf_AppendByte(buf, '}'));

    if (s == NATS_OK)
        *new_buf = buf;
    else
        natsBuf_Destroy(buf);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_purgeOrDelete(bool purge, natsJS *js, const char *stream, natsJSOptions *opts, natsJSErrCode *errCode)
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
    streamPurgeOrDel    pdResp;
    natsJSOptions       o;

    if (js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    if (nats_IsStringEmpty(stream))
        return nats_setError(NATS_INVALID_ARG, "%s", "stream name is required");

    s = natsJS_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, apiT, natsJS_lenWithoutTrailingDot(o.Prefix), o.Prefix, stream) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }
    if ((s == NATS_OK) && purge && (opts != NULL) && (opts->Purge != NULL))
    {
        s = _marshalPurgeRequest(&buf, opts->Purge);
        if ((s == NATS_OK) && (buf != NULL))
        {
            data = (const void*) natsBuf_Data(buf);
            dlen = natsBuf_Len(buf);
        }
    }

    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, js->nc, subj, data, dlen, o.Wait));

    IFOK(s, _unmarshalStreamPurgeOrDelResp(&pdResp, resp));
    if (s == NATS_OK)
    {
        if (natsJS_apiResponseIsErr(&(pdResp.ar)))
        {
            if (errCode != NULL)
                *errCode = (int) pdResp.ar.Error.ErrCode;
            s = nats_setError(NATS_ERR, "%s", pdResp.ar.Error.Description);
        }
        else if (!pdResp.Success)
        {
            s = nats_setError(NATS_ERR, "failed to %s stream %s",
                              (purge ? "purge" : "delete"), stream);
        }
        natsJS_freeApiRespContent(&(pdResp.ar));
    }

    natsBuf_Destroy(buf);
    natsMsg_Destroy(resp);
    NATS_FREE(subj);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_PurgeStream(natsJS *js, const char *stream, natsJSOptions *opts, natsJSErrCode *errCode)
{
    natsStatus s = _purgeOrDelete(true, js, stream, opts, errCode);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_DeleteStream(natsJS *js, const char *stream, natsJSOptions *opts, natsJSErrCode *errCode)
{
    natsStatus s = _purgeOrDelete(false, js, stream, opts, errCode);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
natsJS_unmarshalAccountInfo(natsJSAccountInfo **new_ai, nats_JSON *json)
{
    natsStatus          s;
    nats_JSON           *obj = NULL;
    natsJSAccountInfo   *ai  = NULL;

    ai = (natsJSAccountInfo*) NATS_CALLOC(1, sizeof(natsJSAccountInfo));
    if (ai == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = nats_JSONGetULong(json, "memory", &(ai->Memory));
    IFOK(s, nats_JSONGetULong(json, "storage", &(ai->Store)));
    IFOK(s, nats_JSONGetInt(json, "streams", &(ai->Streams)));
    IFOK(s, nats_JSONGetInt(json, "consumers", &(ai->Consumers)));
    IFOK_INF(s, nats_JSONGetStr(json, "domain", &(ai->Domain)));
    IFOK(s, nats_JSONGetObject(json, "api", &obj));
    IFOK(s, nats_JSONGetULong(obj, "total", &(ai->API.Total)));
    IFOK(s, nats_JSONGetULong(obj, "errors", &(ai->API.Errors)));
    IFOK(s, nats_JSONGetObject(json, "limits", &obj));
    IFOK(s, nats_JSONGetLong(obj, "max_memory", &(ai->Limits.MaxMemory)));
    IFOK(s, nats_JSONGetLong(obj, "max_storage", &(ai->Limits.MaxStore)));
    IFOK(s, nats_JSONGetInt(obj, "max_streams", &(ai->Limits.MaxStreams)));
    IFOK(s, nats_JSONGetInt(obj, "max_consumers", &(ai->Limits.MaxConsumers)));

    if (s == NATS_OK)
        *new_ai = ai;
    else
        natsJSAccountInfo_Destroy(ai);

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_unmarshalAccountInfoResp(accountInfoResp *air, natsMsg *resp)
{
    nats_JSON   *json = NULL;
    natsStatus  s;

    memset(air, 0, sizeof(accountInfoResp));

    s = natsJS_unmarshalResponse(&(air->ar), &json, resp);
    if (s != NATS_OK)
        return NATS_UPDATE_ERR_STACK(s);

    if (!natsJS_apiResponseIsErr(&(air->ar)))
        s = natsJS_unmarshalAccountInfo(&(air->ai), json);

    nats_JSONDestroy(json);
    return NATS_UPDATE_ERR_STACK(s);
}

static void
_freeAccountInfoRespContent(accountInfoResp *air)
{
    if (air == NULL)
        return;

    natsJS_freeApiRespContent(&(air->ar));
    natsJSAccountInfo_Destroy(air->ai);
}

natsStatus
natsJS_GetAccountInfo(natsJSAccountInfo **new_ai, natsJS *js, natsJSOptions *opts, natsJSErrCode *errCode)
{
    accountInfoResp air;
    natsMsg         *resp   = NULL;
    char            *subj   = NULL;
    natsConnection  *nc     = NULL;
    natsStatus      s       = NATS_OK;
    bool            freePfx = false;
    natsJSOptions   o;

    if (new_ai == NULL || js == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    s = natsJS_setOpts(&nc, &freePfx, js, opts, &o);
    if (s == NATS_OK)
    {
        if (nats_asprintf(&subj, jsApiAccountInfo, natsJS_lenWithoutTrailingDot(o.Prefix), o.Prefix) < 0)
            s = nats_setDefaultError(NATS_NO_MEMORY);

        if (freePfx)
            NATS_FREE((char*) o.Prefix);
    }
    // Send the request
    IFOK_JSR(s, natsConnection_Request(&resp, nc, subj, NULL, 0, o.Wait));

    // If we get a response, unmarshal the response
    IFOK(s, _unmarshalAccountInfoResp(&air, resp));

    if (s == NATS_OK)
    {
        if (natsJS_apiResponseIsErr(&(air.ar)))
        {
            if (errCode != NULL)
                *errCode = (int) air.ar.Error.ErrCode;
            s = nats_setError(NATS_ERR, "%s", air.ar.Error.Description);
        }
        else
        {
            *new_ai = air.ai;
            air.ai = NULL;
        }
        _freeAccountInfoRespContent(&air);
    }

    // Common cleanup that is done regardless of success or failure.
    NATS_FREE(subj);
    natsMsg_Destroy(resp);
    return NATS_UPDATE_ERR_STACK(s);
}

void
natsJSAccountInfo_Destroy(natsJSAccountInfo *ai)
{
    if (ai == NULL)
        return;

    NATS_FREE(ai->Domain);
    NATS_FREE(ai);
}

natsStatus
natsJSPlacement_Init(natsJSPlacement *placement)
{
    if (placement == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(placement, 0, sizeof(natsJSPlacement));
    return NATS_OK;
}

natsStatus
natsJSStreamSource_Init(natsJSStreamSource *source)
{
    if (source == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(source, 0, sizeof(natsJSStreamSource));
    return NATS_OK;

}

natsStatus
natsJSExternalStream_Init(natsJSExternalStream *external)
{
    if (external == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(external, 0, sizeof(natsJSExternalStream));
    return NATS_OK;
}

natsStatus
natsJSPurgeOptions_Init(natsJSPurgeOptions *po)
{
    if (po == NULL)
        return nats_setDefaultError(NATS_INVALID_ARG);

    memset(po, 0, sizeof(natsJSPurgeOptions));
    return NATS_OK;
}
