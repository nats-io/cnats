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

#ifndef NATS_H_
#define NATS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <stdio.h>

#include "status.h"
#include "version.h"

/** \def NATS_EXTERN
 *  \brief Needed for shared library.
 *
 *  Based on the platform this is compiled on, it will resolve to
 *  the appropriate instruction so that objects are properly exported
 *  when building the shared library.
 */
#if defined(_WIN32)
  #include <winsock2.h>
  #if defined(nats_EXPORTS)
    #define NATS_EXTERN __declspec(dllexport)
  #elif defined(nats_IMPORTS)
    #define NATS_EXTERN __declspec(dllimport)
  #else
    #define NATS_EXTERN
  #endif

  typedef SOCKET      natsSock;
#else
  #define NATS_EXTERN
  typedef int         natsSock;
#endif

/*! \mainpage %NATS C client.
 *
 * \section intro_sec Introduction
 *
 * The %NATS C Client is part of %NATS, an open-source cloud-native
 * messaging system, and is supported by [Synadia Communications Inc.](http://www.synadia.com).
 * This client, written in C, follows the go client closely, but
 * diverges in some places.
 *
 * \section install_sec Installation
 *
 * Instructions to build and install the %NATS C Client can be
 * found at the [NATS C Client GitHub page](https://github.com/nats-io/nats.c)
 *
 * \section faq_sec Frequently Asked Questions
 *
 * Some of the frequently asked questions can be found [here](https://github.com/nats-io/nats.c#faq)
 *
 * \section other_doc_section Other Documentation
 *
 * This documentation focuses on the %NATS C Client API; for additional
 * information, refer to the following:
 *
 * - [General Documentation for nats.io](http://nats.io/documentation)
 * - [NATS C Client found on GitHub](https://github.com/nats-io/nats.c)
 * - [The NATS Server (nats-server) found on GitHub](https://github.com/nats-io/nats-server)
 */

/** \brief The default `NATS Server` URL.
 *
 *  This is the default URL a `NATS Server`, running with default listen
 *  port, can be reached at.
 */
#define NATS_DEFAULT_URL "nats://localhost:4222"

//
// Types.
//
/** \defgroup typesGroup Types
 *
 *  NATS Types.
 *  @{
 */

/** \brief A connection to a `NATS Server`.
 *
 * A #natsConnection represents a bare connection to a `NATS Server`. It will
 * send and receive byte array payloads.
 */
typedef struct __natsConnection     natsConnection;

/** \brief Statistics of a #natsConnection
 *
 * Tracks various statistics received and sent on a connection,
 * including counts for messages and bytes.
 */
typedef struct __natsStatistics     natsStatistics;

/** \brief Interest on a given subject.
 *
 * A #natsSubscription represents interest in a given subject.
 */
typedef struct __natsSubscription   natsSubscription;

/** \brief A structure holding a subject, optional reply and payload.
 *
 * #natsMsg is a structure used by Subscribers and
 * #natsConnection_PublishMsg().
 */
typedef struct __natsMsg            natsMsg;

/** \brief Way to configure a #natsConnection.
 *
 * Options can be used to create a customized #natsConnection.
 */
typedef struct __natsOptions        natsOptions;

/** \brief Unique subject often used for point-to-point communication.
 *
 * This can be used as the reply for a request. Inboxes are meant to be
 * unique so that replies can be sent to a specific subscriber. That
 * being said, inboxes can be shared across multiple subscribers if
 * desired.
 */
typedef char                        natsInbox;


/**
 * The JetStream context. Use for JetStream assets management and communication.
 *
 * \warning A context MUST not be destroyed concurrently with #jsCtx API calls
 * (for instance #js_Publish or #js_PublishAsync, etc...). However, it
 * is safe to destroy the context while a #jsPubAckErrHandler callback is
 * running or while inside #js_PublishAsyncComplete.
 */
typedef struct __jsCtx              jsCtx;

/**
 * JetStream publish options.
 *
 * These are options that you can provide to JetStream publish APIs.
 *
 * The common usage will be to initialize a structure on the stack by
 * calling #jsPubOptions_Init. Note that strings are owned by
 * the application and need to be valid for the duration of the API
 * call this object is passed to.
 *
 * \note It is the user responsibility to free the strings if they
 * have been allocated.
 *
 * @see jsPubOptions_Init
 */
typedef struct jsPubOptions
{
        int64_t         MaxWait;                ///< Amount of time (in milliseconds) to wait for a publish response, default will the context's Wait value.
        const char      *MsgId;                 ///< Message ID used for de-duplication.
        const char      *ExpectStream;          ///< Expected stream to respond from the publish call.
        const char      *ExpectLastMsgId;       ///< Expected last message ID in the stream.
        uint64_t        ExpectLastSeq;          ///< Expected last message sequence in the stream.
        uint64_t        ExpectLastSubjectSeq;   ///< Expected last message sequence for the subject in the stream.

} jsPubOptions;

/**
 * Determines how messages in a set are retained.
 */
typedef enum
{
        js_LimitsPolicy = 0,    ///< Specifies that messages are retained until any given limit is reached, which could be one of MaxMsgs, MaxBytes, or MaxAge. This is the default.
        js_InterestPolicy,      ///< Specifies that when all known observables have acknowledged a message it can be removed.
        js_WorkQueuePolicy,     ///< Specifies that when the first worker or subscriber acknowledges the message it can be removed.

} jsRetentionPolicy;

/**
 * Determines how to proceed when limits of messages or bytes are reached.
 */
typedef enum
{
        js_DiscardOld = 0,  ///< Will remove older messages to return to the limits. This is the default.
        js_DiscardNew,      ///< Will fail to store new messages.

} jsDiscardPolicy;

/**
 * Determines how messages are stored for retention.
 */
typedef enum
{
        js_FileStorage = 0, ///< Specifies on disk storage. It's the default.
        js_MemoryStorage,   ///< Specifies in memory only.

} jsStorageType;

/**
 * Determines how the consumer should select the first message to deliver.
 */
typedef enum
{
        js_DeliverAll = 0,          ///< Starts from the very beginning of a stream. This is the default.
        js_DeliverLast,             ///< Starts with the last sequence received.
	js_DeliverNew,              ///< Starts with messages sent after the consumer is created.
        js_DeliverByStartSequence,  ///< Starts from a given sequence.
	js_DeliverByStartTime,      ///< Starts from a given UTC time (number of nanoseconds since epoch)

} jsDeliverPolicy;

/**
 * Determines how the consumer should acknowledge delivered messages.
 */
typedef enum
{
        js_AckNone = 0, ///< Requires no acks for delivered messages.
        js_AckAll,      ///< When acking a sequence number, this implicitly acks all sequences below this one as well.
        js_AckExplicit, ///< Requires ack or nack for all messages.

} jsAckPolicy;

/**
 * Determines how the consumer should replay messages it already has queued in the stream.
 */
typedef enum
{
        js_ReplayInstant = 0,   ///< Replays messages as fast as possible.
        js_ReplayOriginal,      ///< Maintains the same timing as the messages were received.

} jsReplayPolicy;


/**
 * Used to guide placement of streams in clustered JetStream.
 *
 * Initialize the object with #jsPlacement_Init.
 *
 * \note The strings are applications owned and will not be freed by the library.
 *
 * See #jsStreamConfig for information on how to configure a stream.
 *
 * @see jsPlacement_Init
 */
typedef struct jsPlacement
{
        const char      *Cluster;       //`json:"cluster"`
        const char      **Tags;         //`json:"tags,omitempty"`
        int             TagsLen;

} jsPlacement;

/**
 * Allows you to qualify access to a stream source in another account.
 *
 * Initialize the object with #jsExternalStream_Init.
 *
 * \note The strings are applications owned and will not be freed by the library.
 *
 * See #jsStreamConfig for information on how to configure a stream.
 */
typedef struct jsExternalStream
{
        const char      *APIPrefix;     //`json:"api"`
        const char      *DeliverPrefix; //`json:"deliver"`

} jsExternalStream;

/**
 * Dictates how streams can source from other streams.
 *
 * Initialize the object with #jsStreamSource_Init.
 *
 * \note The strings are applications owned and will not be freed by the library.
 *
 * \note The `OptStartTime` needs to be expressed as the number of nanoseconds
 * passed since 00:00:00 UTC Thursday, 1 January 1970.
 *
 * See #jsStreamConfig for information on how to configure a stream.
 */
typedef struct jsStreamSource
{
        const char              *Name;          //`json:"name"`
        uint64_t                OptStartSeq;    //`json:"opt_start_seq,omitempty"`
        int64_t                 OptStartTime;   //`json:"opt_start_time,omitempty"`
        const char              *FilterSubject; //`json:"filter_subject,omitempty"`
        jsExternalStream        *External;      //`json:"external,omitempty"`

} jsStreamSource;

/**
 * Configuration of a JetStream stream.
 *
 * There are sensible defaults for most. If no subjects are
 * given the name will be used as the only subject.
 *
 * In order to add/update a stream, a configuration needs to be set.
 * The typical usage would be to initialize all required objects on the stack
 * and configure them, then pass the pointer to the configuration to
 * #js_AddStream or #js_UpdateStream.
 *
 * \note The strings are applications owned and will not be freed by the library.
 *
 * @see jsStreamConfig_Init
 *
 * \code{.unparsed}
 * jsStreamConfig   sc;
 * jsPlacement      p;
 * jsStreamSource   m;
 * jsExternalStream esm;
 * jsStreamSource   s1;
 * jsStreamSource   s2;
 * jsExternalStream esmS2;
 * const char       *subjects[]     = {"foo", "bar"};
 * const char       *tags[]         = {"tag1", "tag2"};
 * jsStreamSource   *sources[]      = {&s1, &s2};
 *
 * jsStreamConfig_Init(&sc);
 *
 * jsPlacement_Init(&p);
 * p.Cluster = "MyCluster";
 * p.Tags = tags;
 * p.TagsLen = 2;
 *
 * jsStreamSource_Init(&m);
 * m.Name = "AStream";
 * m.OptStartSeq = 100;
 * m.FilterSubject = "foo";
 * jsExternalStream_Init(&esm);
 * esm.APIPrefix = "mirror.prefix.";
 * esm.DeliverPrefix = "deliver.prefix.";
 * m.External = &esm;
 *
 * jsStreamSource_Init(&s1);
 * s1.Name = "StreamOne";
 * s1.OptStartSeq = 10;
 * s1.FilterSubject = "stream.one";
 *
 * jsStreamSource_Init(&s2);
 * s2.Name = "StreamTwo";
 * s2.FilterSubject = "stream.two";
 * jsExternalStream_Init(&esmS2);
 * esmS2.APIPrefix = "mirror.prefix.";
 * esmS2.DeliverPrefix = "deliver.prefix.";
 * s2.External = &esmS2;
 *
 * sc.Name = "MyStream";
 * sc.Subjects = subjects;
 * sc.SubjectsLen = 2;
 * sc.Retention = js_InterestPolicy;
 * sc.Replicas = 3;
 * sc.Placement = &p;
 * sc.Mirror = &m;
 * sc.Sources = sources;
 * sc.SourcesLen = 2;
 *
 * s = js_AddStream(&si, js, &sc, NULL, &jerr);
 * \endcode
 */
typedef struct jsStreamConfig {
        const char              *Name;              //`json:"name"`
        const char              **Subjects;         //`json:"subjects,omitempty"`
        int                     SubjectsLen;
        jsRetentionPolicy       Retention;          //`json:"retention"`
        int                     MaxConsumers;       //`json:"max_consumers"`
        int64_t                 MaxMsgs;            //`json:"max_msgs"`
        int64_t                 MaxBytes;           //`json:"max_bytes"`
        int64_t                 MaxAge;             //`json:"max_age"`
        int64_t                 MaxMsgsPerSubject;  //`json:"max_msgs_per_subject"`
        int32_t                 MaxMsgSize;         //`json:"max_msg_size,omitempty"`
        jsDiscardPolicy         Discard;            //`json:"discard"`
        jsStorageType           Storage;            //`json:"storage"`
        int                     Replicas;           //`json:"num_replicas"`
        bool                    NoAck;              //`json:"no_ack,omitempty"`
        const char              *Template;          //`json:"template_owner,omitempty"`
        int64_t                 Duplicates;         //`json:"duplicate_window,omitempty"`
        jsPlacement             *Placement;         //`json:"placement,omitempty"`
        jsStreamSource          *Mirror;            //`json:"mirror,omitempty"`
        jsStreamSource          **Sources;          //`json:"sources,omitempty"`
        int                     SourcesLen;

} jsStreamConfig;

/**
 * Information about messages that have been lost
 */
typedef struct jsLostStreamData
{
        uint64_t                *Msgs;          //`json:"msgs"`
        int                     MsgsLen;
        uint64_t                Bytes;          //`json:"bytes"`

} jsLostStreamData;

/**
 * Information about the given stream
 *
 * \note `FirstTime` and `LastTime` are message timestamps expressed as the number
 * of nanoseconds passed since 00:00:00 UTC Thursday, 1 January 1970.
 */
typedef struct jsStreamState
{
        uint64_t                Msgs;           //`json:"messages"`
        uint64_t                Bytes;          //`json:"bytes"`
        uint64_t                FirstSeq;       //`json:"first_seq"`
        int64_t                 FirstTime;      //`json:"first_ts"`
        uint64_t                LastSeq;        //`json:"last_seq"`
        int64_t                 LastTime;       //`json:"last_ts"`
        uint64_t                NumDeleted;     //`json:"num_deleted,omitempty"`
        uint64_t                *Deleted;       //`json:"deleted,omitempty"`
        int                     DeletedLen;
        jsLostStreamData        *Lost;          //`json:"lost,omitempty"`
        int                     Consumers;      //`json:"consumer_count"`

} jsStreamState;

/**
 * Information about all the peers in the cluster that
 * are supporting the stream or consumer.
 */
typedef struct jsPeerInfo
{
        char        *Name;      //`json:"name"`
        bool        Current;    //`json:"current"`
        bool        Offline;    //`json:"offline,omitempty"`
        int64_t     Active;     //`json:"active"`
        uint64_t    Lag;        //`json:"lag,omitempty"`

} jsPeerInfo;

/**
 * Information about the underlying set of servers
 * that make up the stream or consumer.
 */
typedef struct jsClusterInfo
{
        char            *Name;      //`json:"name,omitempty"`
        char            *Leader;    //`json:"leader,omitempty"`
        jsPeerInfo      **Replicas; //`json:"replicas,omitempty"`
        int             ReplicasLen;

} jsClusterInfo;

/**
 * Information about an upstream stream source.
 */
typedef struct jsStreamSourceInfo
{
        char                    *Name;          //`json:"name"`
        jsExternalStream        *External;      //`json:"external,omitempty"`
        uint64_t                Lag;            //`json:"lag"`
        int64_t                 Active;         //`json:"active"`

} jsStreamSourceInfo;

/**
 * Configuration and current state for this stream.
 *
 * \note `Created` is the timestamp when the stream was created, expressed as
 * the number of nanoseconds passed since 00:00:00 UTC Thursday, 1 January 1970.
 */
typedef struct jsStreamInfo
{
        jsStreamConfig          *Config;    //`json:"config"`
        int64_t                 Created;    //`json:"created"`
        jsStreamState           State;      //`json:"state"`
        jsClusterInfo           *Cluster;   //`json:"cluster,omitempty"`
        jsStreamSourceInfo      *Mirror;    //`json:"mirror,omitempty"`
        jsStreamSourceInfo      **Sources;  //`json:"sources,omitempty"`
        int                     SourcesLen;

} jsStreamInfo;

/**
 * Configuration of a JetStream consumer.
 *
 * In order to add a consumer, a configuration needs to be set.
 * The typical usage would be to initialize all required objects on the stack
 * and configure them, then pass the pointer to the configuration to
 * #js_AddConsumer.
 *
 * \note `OptStartTime` needs to be expressed as the number of nanoseconds
 * passed since 00:00:00 UTC Thursday, 1 January 1970.
 *
 * \note The strings are applications owned and will not be freed by the library.
 *
 * \note `SampleFrequency` is a sampling value, represented as a string such as "50"
 * for 50%, that causes the server to produce advisories for consumer ack metrics.
 * If the percent sign is in the string, and since `%` is an escape character, it
 * needs to be doubled, such as `cfg.SampleFrequency = "50%%";`.
 *
 * @see jsConsumerConfig_Init
 *
 * \code{.unparsed}
 * jsConsumerInfo   *ci = NULL;
 * jsConsumerConfig cc;
 *
 * jsConsumerConfig_Init(&cc);
 * cc.Durable = "MY_DURABLE";
 * cc.DeliverSubject = "foo";
 * cc.DeliverPolicy = js_DeliverNew;
 *
 * s = js_AddConsumer(&ci, js, &cc, NULL, &jerr);
 * \endcode
 */
typedef struct jsConsumerConfig
{
        const char              *Durable;               //`json:"durable_name,omitempty"`
        const char              *DeliverSubject;        //`json:"deliver_subject,omitempty"`
        jsDeliverPolicy         DeliverPolicy;          //`json:"deliver_policy"`
        uint64_t                OptStartSeq;            //`json:"opt_start_seq,omitempty"`
        int64_t                 OptStartTime;           //`json:"opt_start_time,omitempty"`
        jsAckPolicy             AckPolicy;              //`json:"ack_policy"`
        int64_t                 AckWait;                //`json:"ack_wait,omitempty"`
        int                     MaxDeliver;             //`json:"max_deliver,omitempty"`
        const char              *FilterSubject;         //`json:"filter_subject,omitempty"`
        jsReplayPolicy          ReplayPolicy;           //`json:"replay_policy"`
        uint64_t                RateLimit;              //`json:"rate_limit_bps,omitempty"` // Bits per sec
        const char              *SampleFrequency;       //`json:"sample_freq,omitempty"`
        int                     MaxWaiting;             //`json:"max_waiting,omitempty"`
        int                     MaxAckPending;          //`json:"max_ack_pending,omitempty"`
        bool                    FlowControl;            //`json:"flow_control,omitempty"`
        int64_t                 Heartbeat;              //`json:"idle_heartbeat,omitempty"`

} jsConsumerConfig;

/**
 * Includes the consumer and stream sequence info from a JetStream consumer.
 */
typedef struct jsSequencePair
{
        uint64_t        Consumer;                       //`json:"consumer_seq"`
        uint64_t        Stream;                         //`json:"stream_seq"`

} jsSequencePair;

/**
 * Configuration and current state for this consumer.
 *
 * \note `Created` is the timestamp when the consumer was created, expressed as the number
 * of nanoseconds passed since 00:00:00 UTC Thursday, 1 January 1970.
 */
typedef struct jsConsumerInfo
{
        char                    *Stream;                //`json:"stream_name"`
        char                    *Name;                  //`json:"name"`
        int64_t                 Created;                //`json:"created"`
        jsConsumerConfig        *Config;                //`json:"config"`
        jsSequencePair          Delivered;              //`json:"delivered"`
        jsSequencePair          AckFloor;               //`json:"ack_floor"`
        int                     NumAckPending;          //`json:"num_ack_pending"`
        int                     NumRedelivered;         //`json:"num_redelivered"`
        int                     NumWaiting;             //`json:"num_waiting"`
        uint64_t                NumPending;             //`json:"num_pending"`
        jsClusterInfo           *Cluster;               //`json:"cluster,omitempty"`

} jsConsumerInfo;

/**
 * Reports on API calls to JetStream for this account.
 */
typedef struct jsAPIStats
{
        uint64_t Total;  //`json:"total"`
        uint64_t Errors; //`json:"errors"`

} jsAPIStats;

/**
 * Includes the JetStream limits of the current account.
 */
typedef struct  jsAccountLimits
{
        int64_t MaxMemory;      //`json:"max_memory"`
        int64_t MaxStore;       //`json:"max_storage"`
        int     MaxStreams;     //`json:"max_streams"`
        int     MaxConsumers;   //`json:"max_consumers"`

} jsAccountLimits;

/**
 * Information about the JetStream usage from the current account.
 */
typedef struct jsAccountInfo
{
        uint64_t                Memory;         //`json:"memory"`
        uint64_t                Store;          //`json:"storage"`
        int                     Streams;        //`json:"streams"`
        int                     Consumers;      //`json:"consumers"`
        char                    *Domain;        //`json:"domain,omitempty"`
        jsAPIStats              API;            //`json:"api"`
        jsAccountLimits         Limits;         //`json:"limits"`

} jsAccountInfo;

/**
 * Ack received after successfully publishing a message.
 */
typedef struct jsPubAck
{
        char            *Stream;        //`json:"stream"`
        uint64_t        Sequence;       //`json:"seq"`
        bool            Duplicate;      //`json:"duplicate,omitempty"`

} jsPubAck;

/**
 * Publish acknowledgment failure that will be passed to the optional
 * #jsPubAckErrHandler callback.
 */
typedef struct jsPubAckErr
{
        natsMsg         *Msg;
        natsStatus      Err;
        jsErrCode       ErrCode;
        const char      *ErrText;

} jsPubAckErr;

#ifndef BUILD_IN_DOXYGEN
// Forward declaration
typedef void (*jsPubAckErrHandler)(jsCtx *js, jsPubAckErr *pae, void *closure);
#endif

/**
 * JetStream context options.
 *
 * Initialize the object with #jsOptions_Init.
 */
typedef struct jsOptions
{
        const char              *Prefix;                        ///< JetStream prefix, default is "$JS.API"
        const char              *Domain;                        ///< Domain changes the domain part of JetSteam API prefix.
        int64_t                 Wait;                           ///< Amount of time (in milliseconds) to wait for various JetStream API requests, default is 5000 ms (5 seconds).

        /**
         * Publish Async options
         */
        struct jsOptionsPublishAsync
        {
                int                     MaxPending;             ///< Maximum outstanding asynchronous publishes that can be inflight at one time.
                jsPubAckErrHandler      ErrHandler;             ///< Callback invoked when error encountered publishing a given message.
                void                    *ErrHandlerClosure;     ///< Closure (or user data) passed to #jsPubAckErrHandler callback.
                int64_t                 StallWait;              ///< Amount of time (in milliseconds) to wait in a PublishAsync call when there is MaxPending inflight messages, default is 200 ms.

        } PublishAsync;

        /**
         * Advanced stream options
         *
         * * `Purge` for advanced purge options.
         * * `Info` for advanced information retrieval options.
         */
        struct jsOptionsStream
        {
                /**
                 * Advanced stream purge options
                 *
                 * * `Subject` will filter the purge request to only messages that match the subject, which can have wildcards.<br>
                 * * `Sequence` will purge up to but not including this sequence and can be combined with subject filtering.<br>
                 * * `Keep` will specify how many messages to keep and can be combined with subject filtering.<br>
                 *
                 * \note `Sequence` and `Keep` are mutually exclusive, so both can not be set at the same time.
                 */
                struct jsOptionsStreamPurge
                {
                        const char      *Subject;       ///< This is the subject to match against messages for the purge command.
                        uint64_t        Sequence;       ///< Purge up to but not including sequence.
                        uint64_t        Keep;           ///< Number of messages to keep.

                } Purge;                                ///< Optional stream purge options.

                /**
                 * Advance stream information retrieval options
                 */
                struct jsOptionsStreamInfo
                {
                        bool            DeletedDetails; ///< Get the list of deleted message sequences.

                } Info;                                 ///< Optional stream information retrieval options.

        } Stream;                                       ///< Optional stream options.

} jsOptions;

#if defined(NATS_HAS_STREAMING)
/** \brief A connection to a `NATS Streaming Server`.
 *
 * A #stanConnection represents a connection to a `NATS Streaming Server`.
 */
typedef struct __stanConnection     stanConnection;

/** \brief Interest on a given channel.
 *
 * A #stanSubscription represents interest in a given channel.
 */
typedef struct __stanSubscription   stanSubscription;

/** \brief The Streaming message.
 *
 * #stanMsg is the object passed to the subscriptions' message callbacks.
 */
typedef struct __stanMsg            stanMsg;

/** \brief Way to configure a #stanConnection.
 *
 * Options can be used to create a customized #stanConnection.
 */
typedef struct __stanConnOptions    stanConnOptions;

/** \brief Way to configure a #stanSubscription.
 *
 * Options can be used to create a customized #stanSubscription.
 */
typedef struct __stanSubOptions     stanSubOptions;
#endif

/** @} */ // end of typesGroup

//
// Callbacks.
//

/** \defgroup callbacksGroup Callbacks
 *
 *  NATS Callbacks.
 *  @{
 */

/** \brief Callback used to deliver messages to the application.
 *
 * This is the callback that one provides when creating an asynchronous
 * subscription. The library will invoke this callback for each message
 * arriving through the subscription's connection.
 *
 * \warning If this callback is setup for a subject that is used as the reply
 * subject to #natsConnection_PublishRequest calls (and its variants), it
 * is possible to get an empty message with a header "Status" with value
 * "503" that is sent by the server when there were no responders on the
 * request's subject. Use #natsMsg_IsNoResponders to know if that is the case.
 *
 * @see natsConnection_Subscribe()
 * @see natsConnection_QueueSubscribe()
 * @see natsMsg_IsNoResponders()
 */
typedef void (*natsMsgHandler)(
        natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure);

/** \brief Callback used to notify the user of asynchronous connection events.
 *
 * This callback is used for asynchronous events such as disconnected
 * and closed connections.
 *
 * @see natsOptions_SetClosedCB()
 * @see natsOptions_SetDisconnectedCB()
 * @see natsOptions_SetReconnectedCB()
 *
 * \warning Such callback is invoked from a dedicated thread and the state
 *          of the connection that triggered the event may have changed since
 *          that event was generated.
 */
typedef void (*natsConnectionHandler)(
        natsConnection  *nc, void *closure);

/** \brief Callback used to notify the user of errors encountered while processing
 *         inbound messages.
 *
 * This callback is used to process asynchronous errors encountered while processing
 * inbound messages, such as #NATS_SLOW_CONSUMER.
 */
typedef void (*natsErrHandler)(
        natsConnection *nc, natsSubscription *subscription, natsStatus err,
        void *closure);

/** \brief Attach this connection to the external event loop.
 *
 * After a connection has (re)connected, this callback is invoked. It should
 * perform the necessary work to start polling the given socket for READ events.
 *
 * @param userData location where the adapter implementation will store the
 * object it created and that will later be passed to all other callbacks. If
 * `*userData` is not `NULL`, this means that this is a reconnect event.
 * @param loop the event loop (as a generic void*) this connection is being
 * attached to.
 * @param nc the connection being attached to the event loop.
 * @param socket the socket to poll for read/write events.
 */
typedef natsStatus (*natsEvLoop_Attach)(
        void            **userData,
        void            *loop,
        natsConnection  *nc,
        natsSock        socket);

/** \brief Read event needs to be added or removed.
 *
 * The `NATS` library will invoke this callback to indicate if the event
 * loop should start (`add is `true`) or stop (`add` is `false`) polling
 * for read events on the socket.
 *
 * @param userData the pointer to an user object created in #natsEvLoop_Attach.
 * @param add `true` if the event library should start polling, `false` otherwise.
 */
typedef natsStatus (*natsEvLoop_ReadAddRemove)(
        void            *userData,
        bool            add);

/** \brief Write event needs to be added or removed.
 *
 * The `NATS` library will invoke this callback to indicate if the event
 * loop should start (`add is `true`) or stop (`add` is `false`) polling
 * for write events on the socket.
 *
 * @param userData the pointer to an user object created in #natsEvLoop_Attach.
 * @param add `true` if the event library should start polling, `false` otherwise.
 */
typedef natsStatus (*natsEvLoop_WriteAddRemove)(
        void            *userData,
        bool            add);

/** \brief Detach from the event loop.
 *
 * The `NATS` library will invoke this callback to indicate that the connection
 * no longer needs to be attached to the event loop. User can cleanup some state.
 *
 * @param userData the pointer to an user object created in #natsEvLoop_Attach.
 */
typedef natsStatus (*natsEvLoop_Detach)(
        void            *userData);

/** \brief Callback used to fetch and return account signed user JWT.
 *
 * This handler is invoked when connecting and reconnecting. It should
 * return the user JWT that will be sent to the server.
 *
 * The user JWT is returned as a string that is allocated by the user and is
 * freed by the library after the handler is invoked.
 *
 * If the user is unable to return the JWT, a status other than `NATS_OK` should
 * be returned (we recommend `NATS_ERR`). A custom error message can be returned
 * through `customErrTxt`. The user must allocate the memory for this error
 * message and the library will free it after the invocation of the handler.
 *
 * \warning There may be repeated invocations of this handler for a given connection
 * so it is crucial to always return a copy of the user JWT maintained by the
 * application, since again, the library will free the memory pointed by `userJWT`
 * after each invocation of this handler.
 *
 * @see natsOptions_SetUserCredentialsCallbacks()
 * @see natsOptions_SetUserCredentialsFromFiles()
 */
typedef natsStatus (*natsUserJWTHandler)(
        char            **userJWT,
        char            **customErrTxt,
        void            *closure);


/** \brief Callback used to sign a nonce sent by the server.
 *
 * This handler is invoked when connecting and reconnecting. It should
 * sign the given `nonce` and return a raw signature through `signature` and
 * specify how many characters the signature has using `signatureLength`.
 *
 * The memory pointed by `signature` must be allocated by the user and
 * will be freed by the library after each invocation of this handler.
 *
 * If the user is unable to sign, a status other than `NATS_OK` (we recommend
 * `NATS_ERR`) should be returned. A custom error message can be returned
 * through `customErrTxt`. The user must allocate the memory for this error
 * message and the library will free it after the invocation of this handler.
 *
 * The library will base64 encode this raw signature and send that to the server.
 *
 * \warning There may be repeated invocations of this handler for a given connection
 * so it is crucial to always return a copy of the signature, since again,
 * the library will free the memory pointed by `signature` after each invocation
 * of this handler.
 *
 * @see natsOptions_SetUserCredentialsCallbacks()
 * @see natsOptions_SetUserCredentialsFromFiles()
 * @see natsOptions_SetNKey()
 */
typedef natsStatus (*natsSignatureHandler)(
        char            **customErrTxt,
        unsigned char   **signature,
        int             *signatureLength,
        const char      *nonce,
        void            *closure);

/** \brief Callback used to build a token on connections and reconnections.
 *
 * This is the function that one provides to build a different token at each reconnect.
 *
 * @see natsOptions_SetTokenHandler()
 *
 * \warning Such callback is invoked synchronously from the connection thread.
 */
typedef const char* (*natsTokenHandler)(void *closure);


/** \brief Callback used to notify that an object lifecycle is complete.
 *
 * Currently used for asynchronous #natsSubscription objects. When set, this callback will
 * be invoked after the subscription is closed and the message handler has returned.
 *
 * @see natsSubscription_SetOnCompleteCB()
 */
typedef void (*natsOnCompleteCB)(void *closure);

/** \brief Callback used to specify how long to wait between reconnects.
 *
 * This callback is used to get from the user the desired delay the library
 * should pause before attempting to reconnect again. Note that this is invoked
 * after the library tried the whole list of URLs and failed to reconnect.
 *
 * \note This callback is invoked from the connection reconnect thread and waits
 * for user input. It should not block and instead quickly return the desired
 * reconnect delay.
 * The state of the connection is disconnected when this callback is invoked.
 * Not much can be done with the passed connection, but user can call
 * #natsConnection_Close() if desired. This will abort the reconnect attempts
 * and close the connection.
 *
 * @param nc the pointer to the #natsConnection invoking this handler.
 * @param attempts the number of times the library tried the whole list of server URLs.
 * @param closure an optional pointer to a user defined object that was specified when
 * registering the callback.
 * @return the number of milliseconds to wait before trying to reconnect.
 */
typedef int64_t (*natsCustomReconnectDelayHandler)(natsConnection *nc, int attempts, void *closure);

#ifdef BUILD_IN_DOXYGEN
/** \brief Callback used to process asynchronous publish errors from JetStream.
 *
 * Callback used to process asynchronous publish errors from JetStream #js_PublishAsync
 * and #js_PublishMsgAsync. The provided #jsPubAckErr gives the user
 * access to the encountered error along with the original message sent to the server
 * for possible restransmitting.
 *
 * \note If the message is resent, the library will not destroy the original
 * message and once again take ownership of it. To resend the message, do the
 * following so that the library knows no to destroy the message (since the
 * call will clear the `Msg` field from the #jsPubAckErr object.
 *
 * \code{.unparsed}
 * void myPAECallback(jsCtx *js, jsPubAckErr *pae, void* closure)
 * {
 *      ...
 *      // Resend the message
 *      js_PublishMsgAsync(js, &(pae->Msg), NULL);
 * }
 * \endcode
 *
 * \warning The #jsPubAckErr object and its content will be invalid as
 * soon as the callback returns.
 *
 * \warning Unlike a NATS message callback, the user does not have to destroy
 * the original NATS message (present in the #jsPubAckErr object), the
 * library will do it.
 *
 * @param js the pointer to the #jsCtx object.
 * @param pae the pointer to the #jsPubAckErr object.
 * @param closure an optional pointer to a user defined object that was specified when
 * registering the callback.
 */
typedef void (*jsPubAckErrHandler)(jsCtx *js, jsPubAckErr *pae, void *closure);
#endif

#if defined(NATS_HAS_STREAMING)
/** \brief Callback used to notify of an asynchronous publish result.
 *
 * This is used for asynchronous publishing to provide status of the acknowledgment.
 * The function will be passed the GUID and any error state. No error means the
 * message was successfully received by NATS Streaming.
 *
 * @see stanConnection_PublishAsync()
 */
typedef void (*stanPubAckHandler)(const char *guid, const char *error, void *closure);

/** \brief Callback used to deliver messages to the application.
 *
 * This is the callback that one provides when creating an asynchronous
 * subscription. The library will invoke this callback for each message
 * arriving through the subscription's connection.
 *
 * @see stanConnection_Subscribe()
 * @see stanConnection_QueueSubscribe()
 */
typedef void (*stanMsgHandler)(
        stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg, void *closure);

/** \brief Callback used to notify the user of the permanent loss of the connection.
 *
 * This callback is used to notify the user that the connection to the Streaming
 * server is permanently lost.
 *
 */
typedef void (*stanConnectionLostHandler)(
        stanConnection *sc, const char* errorTxt, void *closure);
#endif

/** @} */ // end of callbacksGroup

//
// Functions.
//
/** \defgroup funcGroup Functions
 *
 *  NATS Functions.
 *  @{
 */

/** \defgroup libraryGroup Library
 *
 *  Library and helper functions.
 * @{
 */

/** \brief Initializes the library.
 *
 * This initializes the library.
 *
 * It is invoked automatically when creating a connection, using a default
 * spin count. However, you can call this explicitly before creating the very
 * first connection in order for your chosen spin count to take effect.
 *
 * \warning You must not call #nats_Open and #nats_Close concurrently.
 *
 * @param lockSpinCount The number of times the library will spin trying to
 * lock a mutex object.
 */
NATS_EXTERN natsStatus
nats_Open(int64_t lockSpinCount);


/** \brief Returns the Library's version.
 *
 * Returns the version of the library your application is linked with.
 */
NATS_EXTERN const char*
nats_GetVersion(void);

/** \brief Returns the Library's version as a number.
 *
 * The version is returned as an hexadecimal number. For instance, if the
 * string version is "1.2.3", the value returned will be:
 *
 * > 0x010203
 */
NATS_EXTERN uint32_t
nats_GetVersionNumber(void);

#ifdef BUILD_IN_DOXYGEN
/** \brief Check that the header is compatible with the library.
 *
 * The version of the header you used to compile your application may be
 * incompatible with the library the application is linked with.
 *
 * This function will check that the two are compatibles. If they are not,
 * a message is printed and the application will exit.
 *
 * @return `true` if the header and library are compatibles, otherwise the
 * application exits.
 *
 * @see nats_GetVersion
 * @see nats_GetVersionNumber
 */
NATS_EXTERN bool nats_CheckCompatibility(void);
#else

#define nats_CheckCompatibility() nats_CheckCompatibilityImpl(NATS_VERSION_REQUIRED_NUMBER, \
                                                              NATS_VERSION_NUMBER, \
                                                              NATS_VERSION_STRING)

NATS_EXTERN bool
nats_CheckCompatibilityImpl(uint32_t reqVerNumber, uint32_t verNumber, const char *verString);

#endif

/** \brief Gives the current time in milliseconds.
 *
 * Gives the current time in milliseconds.
 */
NATS_EXTERN int64_t
nats_Now(void);

/** \brief Gives the current time in nanoseconds.
 *
 * Gives the current time in nanoseconds. When such granularity is not
 * available, the time returned is still expressed in nanoseconds.
 */
NATS_EXTERN int64_t
nats_NowInNanoSeconds(void);

/** \brief Sleeps for a given number of milliseconds.
 *
 * Causes the current thread to be suspended for at least the number of
 * milliseconds.
 *
 * @param sleepTime the number of milliseconds.
 */
NATS_EXTERN void
nats_Sleep(int64_t sleepTime);

/** \brief Returns the calling thread's last known error.
 *
 * Returns the calling thread's last known error. This can be useful when
 * #natsConnection_Connect fails. Since no connection object is returned,
 * you would not be able to call #natsConnection_GetLastError.
 *
 * @param status if not `NULL`, this function will store the last error status
 * in there.
 * @return the thread local error string.
 *
 * \warning Do not free the string returned by this function.
 */
NATS_EXTERN const char*
nats_GetLastError(natsStatus *status);

/** \brief Returns the calling thread's last known error stack.
 *
 * Copies the calling thread's last known error stack into the provided buffer.
 * If the buffer is not big enough, #NATS_INSUFFICIENT_BUFFER is returned.
 *
 * @param buffer the buffer into the stack is copied.
 * @param bufLen the size of the buffer
 */
NATS_EXTERN natsStatus
nats_GetLastErrorStack(char *buffer, size_t bufLen);

/** \brief Prints the calling thread's last known error stack into the file.
 *
 * This call prints the calling thread's last known error stack into the file `file`.
 * It first prints the error status and the error string, then the stack.
 *
 * Here is an example for a call:
 *
 * \code{.unparsed}
 * Error: 29 - SSL Error - (conn.c:565): SSL handshake error: sslv3 alert bad certificate
 * Stack: (library version: 1.2.3-beta)
 *   01 - _makeTLSConn
 *   02 - _checkForSecure
 *   03 - _processExpectedInfo
 *   04 - _processConnInit
 *   05 - _connect
 *   06 - natsConnection_Connect
 * \endcode
 *
 * @param file the file the stack is printed to.
 */
NATS_EXTERN void
nats_PrintLastErrorStack(FILE *file);

/** \brief Sets the maximum size of the global message delivery thread pool.
 *
 * Normally, each asynchronous subscriber that is created has its own
 * message delivery thread. The advantage is that it reduces lock
 * contentions, therefore improving performance.<br>
 * However, if an application creates many subscribers, this is not scaling
 * well since the process would use too many threads.
 *
 * The library has a thread pool that can perform message delivery. If
 * a connection is created with the proper option set
 * (#natsOptions_UseGlobalMessageDelivery), then this thread pool
 * will be responsible for delivering the messages. The thread pool is
 * lazily initialized, that is, no thread is used as long as no subscriber
 * (requiring global message delivery) is created.
 *
 * Each subscriber will be attached to a given worker on the pool so that
 * message delivery order is guaranteed.
 *
 * This call allows you to set the maximum size of the pool.
 *
 * \note At this time, a pool does not shrink, but the caller will not get
 * an error when calling this function with a size smaller than the current
 * size.
 *
 * @see natsOptions_UseGlobalMessageDelivery()
 * @see \ref envVariablesGroup
 *
 * @param max the maximum size of the pool.
 */
NATS_EXTERN natsStatus
nats_SetMessageDeliveryPoolSize(int max);

/** \brief Release thread-local memory possibly allocated by the library.
 *
 * This needs to be called on user-created threads where NATS calls are
 * performed. This does not need to be called in threads created by
 * the library. For instance, do not call this function in the
 * message handler that you specify when creating a subscription.
 *
 * Also, you do not need to call this in an user thread (or the main)
 * if you are calling nats_Close() there.
 */
NATS_EXTERN void
nats_ReleaseThreadMemory(void);

/** \brief Signs a given text using the provided private key.
 *
 * The key is the encoded string representation of the private key, or seed.
 * This is what you get when generating an NKey using NATS tooling.
 *
 * The input is a string, generally the nonce sent by the server when
 * accepting a connection.
 *
 * This function signs the input and returns the signature through the
 * output arguments. This call allocates memory necessary to hold
 * the signature. If this is used as part of the signature callback
 * passed to #natsOptions_SetNKey(), then the memory will be automatically
 * freed by the library after the signature has been inserted in the CONNECT
 * protocol.
 * If this function is used outside of this context, it is the user responsibility
 * to free the allocated memory when no longer needed.
 *
 * @see natsOptions_SetNKey()
 *
 * @param encodedSeed the string encoded private key, also known as seed.
 * @param input the input to be signed.
 * @param signature the memory location of allocated memory containing the signed input.
 * @param signatureLength the size of the allocated signature.
 */
NATS_EXTERN natsStatus
nats_Sign(const char    *encodedSeed,
          const char    *input,
          unsigned char **signature,
          int           *signatureLength);

/** \brief Tear down the library.
 *
 * Releases memory used by the library.
 *
 * For this to take effect, all NATS objects that you have created
 * must first be destroyed.
 *
 * This call does not block and it is possible that the library
 * is not unloaded right away if there are still internal threads referencing
 * it, so calling #nats_Open() right away may fail. If you want to ensure
 * that the library is fully unloaded, call #nats_CloseAndWait() instead.
 *
 * \note There are still a small number of thread local keys and a mutex
 * that are not freed until the application exit (in which case a final cleanup
 * is executed).
 *
 * \warning You must not call #nats_Open and #nats_Close concurrently.
 *
 * @see nats_CloseAndWait()
 */
NATS_EXTERN void
nats_Close(void);

/** \brief Tear down the library and wait for all resources to be released.
 *
 * Similar to #nats_Close() except that this call will make sure that all
 * references to the library are decremented before returning (up to the
 * given timeout). Internal threads (such as subscriptions dispatchers,
 * etc..) hold a reference to the library. Only when all references have
 * been released that this call will return. It means that you must call
 * all the "destroy" calls before calling this function, otherwise it will
 * block forever (or up to given timeout).
 *
 * For instance, this code would "deadlock":
 * \code{.unparsed}
 * natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
 * nats_CloseWait(0);
 * natsConnection_Destroy(nc);
 * \endcode
 * But this would work as expected:
 * \code{.unparsed}
 * natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
 * natsConnection_Destroy(nc);
 * nats_CloseWait(0);
 * \endcode
 * The library and other objects (such as connections, subscriptions, etc)
 * use internal threads. After the destroy call, it is possible or even likely
 * that some threads are still running, holding references to the library.
 * Unlike #nats_Close(), which will simply ensure that the library is ultimately
 * releasing memory, the #nats_CloseAndWait() API will ensure that all those internal
 * threads have unrolled and that the memory used by the library is released before
 * returning.
 *
 * \note If a timeout is specified, the call may return #NATS_TIMEOUT but the
 * library is still being tear down and memory will be released. The error is just
 * to notify you that the operation did not complete in the allotted time. Calling
 * #nats_Open() in this case (or any implicit opening of the library) may result
 * in an error since the library may still be in the process of being closed.
 *
 * \warning Due to the blocking nature it is illegal to call this from any
 * NATS thread (such as message or connection callbacks). If trying to do so,
 * a #NATS_ILLEGAL_STATE error will be returned.
 *
 * @see nats_Close()
 *
 * @param timeout the maximum time to wait for the library to be closed. If
 * negative or 0, waits for as long as needed.
 */
NATS_EXTERN natsStatus
nats_CloseAndWait(int64_t timeout);

/** @} */ // end of libraryGroup

/** \defgroup statusGroup Status
 *
 * Functions related to #natsStatus.
 * @{
 */

/** \brief Get the text corresponding to a #natsStatus.
 *
 * Returns the static string corresponding to the given status.
 *
 * \warning The returned string is a static string, do not attempt to free
 * it.
 *
 * @param s status to get the text representation from.
 */
NATS_EXTERN const char*
natsStatus_GetText(natsStatus s);

/** @} */ // end of statusGroup

/** \defgroup statsGroup Statistics
 *
 *  Statistics Functions.
 *  @{
 */

/** \brief Creates a #natsStatistics object.
 *
 * Creates a statistics object that can be passed to #natsConnection_GetStats().
 *
 * \note The object needs to be destroyed when no longer needed.
 *
 * @see #natsStatistics_Destroy()
 *
 * @param newStats the location where to store the pointer to the newly created
 * #natsStatistics object.
 */
NATS_EXTERN natsStatus
natsStatistics_Create(natsStatistics **newStats);

/** \brief Extracts the various statistics values.
 *
 * Gets the counts out of the statistics object.
 *
 * \note You can pass `NULL` to any of the count your are not interested in
 * getting.
 *
 * @see natsConnection_GetStats()
 *
 * @param stats the pointer to the #natsStatistics object to get the values from.
 * @param inMsgs total number of inbound messages.
 * @param inBytes total size (in bytes) of inbound messages.
 * @param outMsgs total number of outbound messages.
 * @param outBytes total size (in bytes) of outbound messages.
 * @param reconnects total number of times the client has reconnected.
 */
NATS_EXTERN natsStatus
natsStatistics_GetCounts(const natsStatistics *stats,
                         uint64_t *inMsgs, uint64_t *inBytes,
                         uint64_t *outMsgs, uint64_t *outBytes,
                         uint64_t *reconnects);

/** \brief Destroys the #natsStatistics object.
 *
 * Destroys the statistics object, freeing up memory.
 *
 * @param stats the pointer to the #natsStatistics object to destroy.
 */
NATS_EXTERN void
natsStatistics_Destroy(natsStatistics *stats);

/** @} */ // end of statsGroup

/** \defgroup optsGroup Options
 *
 *  NATS Options.
 *  @{
 */

/** \brief Creates a #natsOptions object.
 *
 * Creates a #natsOptions object. This object is used when one wants to set
 * specific options prior to connecting to the `NATS Server`.
 *
 * After making the appropriate natsOptions_Set calls, this object is passed
 * to the #natsConnection_Connect() call, which will clone this object. After
 * natsConnection_Connect() returns, modifications to the options object
 * will not affect the connection.
 *
 * \note The object needs to be destroyed when no longer needed.*
 *
 * @see natsConnection_Connect()
 * @see natsOptions_Destroy()
 *
 * @param newOpts the location where store the pointer to the newly created
 * #natsOptions object.
 */
NATS_EXTERN natsStatus
natsOptions_Create(natsOptions **newOpts);

/** \brief Sets the URL to connect to.
 *
 * Sets the URL of the `NATS Server` the client should try to connect to.
 * The URL can contain optional user name and password.
 *
 * Some valid URLS:
 *
 * - nats://localhost:4222
 * - nats://user\@localhost:4222
 * - nats://user:password\@localhost:4222
 *
 * @see natsOptions_SetServers
 * @see natsOptions_SetUserInfo
 * @see natsOptions_SetToken
 *
 * @param opts the pointer to the #natsOptions object.
 * @param url the string representing the URL the connection should use
 * to connect to the server.
 *
 */
/*
 * The above is for doxygen. The proper syntax for username/password
 * is without the '\' character:
 *
 * nats://localhost:4222
 * nats://user@localhost:4222
 * nats://user:password@localhost:4222
 */
NATS_EXTERN natsStatus
natsOptions_SetURL(natsOptions *opts, const char *url);

/** \brief Set the list of servers to try to (re)connect to.
 *
 * This specifies a list of servers to try to connect (or reconnect) to.
 * Note that if you call #natsOptions_SetURL() too, the actual list will contain
 * the one from #natsOptions_SetURL() and the ones specified in this call.
 *
 * @see natsOptions_SetURL
 * @see natsOptions_SetUserInfo
 * @see natsOptions_SetToken
 *
 * @param opts the pointer to the #natsOptions object.
 * @param servers the array of strings representing the server URLs.
 * @param serversCount the size of the array.
 */
NATS_EXTERN natsStatus
natsOptions_SetServers(natsOptions *opts, const char** servers, int serversCount);

/** \brief Sets the user name/password to use when not specified in the URL.
 *
 * Credentials are usually provided through the URL in the form:
 * <c>nats://foo:bar\@localhost:4222</c>.<br>
 * Until now, you could specify URLs in two ways, with #natsOptions_SetServers
 * or #natsConnection_ConnectTo. The client library would connect (or reconnect)
 * only to this given list of URLs, so if any of the server in the list required
 * authentication, you were responsible for providing the appropriate credentials
 * in the URLs.<br>
 * <br>
 * However, with cluster auto-discovery, the client library asynchronously receives
 * URLs of servers in the cluster. These URLs do not contain any embedded credentials.
 * <br>
 * You need to use this function (or #natsOptions_SetToken) to instruct the client
 * library to use those credentials when connecting to a server that requires
 * authentication and for which there is no embedded credentials in the URL.
 *
 * @see natsOptions_SetToken
 * @see natsOptions_SetURL
 * @see natsOptions_SetServers
 *
 * @param opts the pointer to the #natsOptions object.
 * @param user the user name to send to the server during connect.
 * @param password the password to send to the server during connect.
 */
NATS_EXTERN natsStatus
natsOptions_SetUserInfo(natsOptions *opts, const char *user, const char *password);

/** \brief Sets the token to use when not specified in the URL.
 *
 * Tokens are usually provided through the URL in the form:
 * <c>nats://mytoken\@localhost:4222</c>.<br>
 * Until now, you could specify URLs in two ways, with #natsOptions_SetServers
 * or #natsConnection_ConnectTo. The client library would connect (or reconnect)
 * only to this given list of URLs, so if any of the server in the list required
 * authentication, you were responsible for providing the appropriate token
 * in the URLs.<br>
 * <br>
 * However, with cluster auto-discovery, the client library asynchronously receives
 * URLs of servers in the cluster. These URLs do not contain any embedded tokens.
 * <br>
 * You need to use this function (or #natsOptions_SetUserInfo) to instruct the client
 * library to use this token when connecting to a server that requires
 * authentication and for which there is no embedded token in the URL.
 *
 * @see natsOptions_SetUserInfo
 * @see natsOptions_SetURL
 * @see natsOptions_SetServers
 *
 * @param opts the pointer to the #natsOptions object.
 * @param token the token to send to the server during connect.
 */
NATS_EXTERN natsStatus
natsOptions_SetToken(natsOptions *opts, const char *token);

/** \brief Sets the tokenCb to use whenever a token is needed.
 *
 * For use cases where setting a static token through the URL<br>
 * or through #natsOptions_SetToken is not desirable.<br>
 * <br>
 * This function can be used to generate a token whenever the client needs one.<br>
 * Some example of use cases: expiring token, credential rotation, ...
 *
 * @see natsOptions_SetToken
 *
 * @param opts the pointer to the #natsOptions object.
 * @param tokenCb the tokenCb to use to generate a token to the server during connect.
 * @param closure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 */
NATS_EXTERN natsStatus
natsOptions_SetTokenHandler(natsOptions *opts, natsTokenHandler tokenCb,
                            void *closure);

/** \brief Indicate if the servers list should be randomized.
 *
 * If 'noRandomize' is true, then the list of server URLs is used in the order
 * provided by #natsOptions_SetURL() + #natsOptions_SetServers(). Otherwise, the
 * list is formed in a random order.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param noRandomize if `true`, the list will be used as-is.
 */
NATS_EXTERN natsStatus
natsOptions_SetNoRandomize(natsOptions *opts, bool noRandomize);

/** \brief Sets the (re)connect process timeout.
 *
 * This timeout, expressed in milliseconds, is used to interrupt a (re)connect
 * attempt to a `NATS Server`. This timeout is used both for the low level TCP
 * connect call, and for timing out the response from the server to the client's
 * initial `PING` protocol.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param timeout the time, in milliseconds, allowed for an individual connect
 * (or reconnect) to complete.
 *
 */
NATS_EXTERN natsStatus
natsOptions_SetTimeout(natsOptions *opts, int64_t timeout);

/** \brief Sets the name.
 *
 * This name is sent as part of the `CONNECT` protocol. There is no default name.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param name the name to set.
 */
NATS_EXTERN natsStatus
natsOptions_SetName(natsOptions *opts, const char *name);

/** \brief Sets the secure mode.
 *
 * Indicates to the server if the client wants a secure (SSL/TLS) connection.
 *
 * The default is `false`.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param secure `true` for a secure connection, `false` otherwise.
 */
NATS_EXTERN natsStatus
natsOptions_SetSecure(natsOptions *opts, bool secure);

/** \brief Loads the trusted CA certificates from a file.
 *
 * Loads the trusted CA certificates from a file.
 *
 * Note that the certificates are added to a SSL context for this #natsOptions
 * object at the time of this call, so possible errors while loading the
 * certificates will be reported now instead of when a connection is created.
 * You can get extra information by calling #nats_GetLastError.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param fileName the file containing the CA certificates.
 *
 */
NATS_EXTERN natsStatus
natsOptions_LoadCATrustedCertificates(natsOptions *opts, const char *fileName);

/** \brief Sets the trusted CA certificates from memory.
 *
 * Similar to #natsOptions_LoadCATrustedCertificates expect that instead
 * of loading from file, this loads from the given memory location.
 *
 * If more than one certificate need to be provided, they need to be
 * concatenated. For instance:
 *
 * \code{.unparsed}
 * const char *certs =
 *    "-----BEGIN CERTIFICATE-----\n"
 *    "MIIGjzCCBHegAwIBAgIJAKT2W9SKY7o4MA0GCSqGSIb3DQEBCwUAMIGLMQswCQYD\n"
 *    (...)
 *    "-----END CERTIFICATE-----\n"
 *    "-----BEGIN CERTIFICATE-----\n"
 *    "MIIXyz...\n"
 *    (...)
 *    "-----END CERTIFICATE-----\n"
 * \endcode
 *
 * @see natsOptions_LoadCATrustedCertificates
 *
 * @param opts the pointer to the #natsOptions object.
 * @param certificates the string containing the concatenated CA certificates.
 */
NATS_EXTERN natsStatus
natsOptions_SetCATrustedCertificates(natsOptions *opts, const char *certificates);

/** \brief Loads the certificate chain from a file, using the given key.
 *
 * The certificates must be in PEM format and must be sorted starting with
 * the subject's certificate, followed by intermediate CA certificates if
 * applicable, and ending at the highest level (root) CA.
 *
 * The private key file format supported is also PEM.
 *
 * See #natsOptions_LoadCATrustedCertificates regarding error reports.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param certsFileName the file containing the client certificates.
 * @param keyFileName the file containing the client private key.
 */
NATS_EXTERN natsStatus
natsOptions_LoadCertificatesChain(natsOptions *opts,
                                  const char *certsFileName,
                                  const char *keyFileName);

/** \brief Sets the client certificate and key.
 *
 * Similar to #natsOptions_LoadCertificatesChain expect that instead
 * of loading from file, this loads from the given memory locations.
 *
 * @see natsOptions_LoadCertificatesChain()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param cert the memory location containing the client certificates.
 * @param key the memory location containing the client private key.
 */
NATS_EXTERN natsStatus
natsOptions_SetCertificatesChain(natsOptions *opts,
                                 const char *cert,
                                 const char *key);

/** \brief Sets the list of available ciphers.
 *
 * \note This function does not impact TLSv1.3 ciphersuites.
 *
 * Sets the list of available ciphers.
 * Check https://www.openssl.org/docs/man1.1.1/man1/ciphers.html for the
 * proper syntax. Here is an example:
 *
 * > "-ALL:HIGH"
 *
 * See #natsOptions_LoadCATrustedCertificates regarding error reports.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param ciphers the ciphers suite.
 */
NATS_EXTERN natsStatus
natsOptions_SetCiphers(natsOptions *opts, const char *ciphers);

/** \brief Sets the list of available ciphers for TLSv1.3.
 *
 * Sets the list of available ciphers.
 * Check https://www.openssl.org/docs/man1.1.1/man3/SSL_CTX_set_ciphersuites.html for the
 * proper syntax. Here is an example:
 *
 * > "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256"
 *
 * See #natsOptions_LoadCATrustedCertificates regarding error reports.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param ciphers the ciphers suite.
 */
NATS_EXTERN natsStatus
natsOptions_SetCipherSuites(natsOptions *opts, const char *ciphers);

/** \brief Sets the server certificate's expected hostname.
 *
 * If set, the library will check that the hostname in the server
 * certificate matches the given `hostname`. This will occur when a connection
 * is created, not at the time of this call.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param hostname the expected server certificate hostname.
 */
NATS_EXTERN natsStatus
natsOptions_SetExpectedHostname(natsOptions *opts, const char *hostname);

/** \brief Switch server certificate verification.
 *
 * By default, the server certificate is verified. You can disable the verification
 * by passing <c>true</c> to this function.
 *
 * \warning This is fine for tests but use with caution since this is not secure.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param skip set it to <c>true</c> to disable - or skip - server certificate verification.
 */
NATS_EXTERN natsStatus
natsOptions_SkipServerVerification(natsOptions *opts, bool skip);

/** \brief Sets the verbose mode.
 *
 * Sets the verbose mode. If `true`, sends are echoed by the server with
 * an `OK` protocol message.
 *
 * The default is `false`.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param verbose `true` for a verbose protocol, `false` otherwise.
 */
NATS_EXTERN natsStatus
natsOptions_SetVerbose(natsOptions *opts, bool verbose);

/** \brief Sets the pedantic mode.
 *
 * Sets the pedantic mode. If `true` some extra checks will be performed
 * by the server.
 *
 * The default is `false`
 *
 * @param opts the pointer to the #natsOptions object.
 * @param pedantic `true` for a pedantic protocol, `false` otherwise.
 */
NATS_EXTERN natsStatus
natsOptions_SetPedantic(natsOptions *opts, bool pedantic);

/** \brief Sets the ping interval.
 *
 * Interval, expressed in milliseconds, in which the client sends `PING`
 * protocols to the `NATS Server`.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param interval the interval, in milliseconds, at which the connection
 * will send `PING` protocols to the server.
 */
NATS_EXTERN natsStatus
natsOptions_SetPingInterval(natsOptions *opts, int64_t interval);

/** \brief Sets the limit of outstanding `PING`s without corresponding `PONG`s.
 *
 * Specifies the maximum number of `PING`s without corresponding `PONG`s (which
 * should be received from the server) before closing the connection with
 * the #NATS_STALE_CONNECTION status. If reconnection is allowed, the client
 * library will try to reconnect.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param maxPingsOut the maximum number of `PING`s without `PONG`s
 * (positive number).
 */
NATS_EXTERN natsStatus
natsOptions_SetMaxPingsOut(natsOptions *opts, int maxPingsOut);

/** \brief Sets the size of the internal read/write buffers.
 *
 * Sets the size, in bytes, of the internal read/write buffers used for
 * reading/writing data from a socket.
 * If not specified, or the value is 0, the library will use a default value,
 * currently set to 32KB.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param ioBufSize the size, in bytes, of the internal buffer for read/write
 * operations.
 */
NATS_EXTERN natsStatus
natsOptions_SetIOBufSize(natsOptions *opts, int ioBufSize);

/** \brief Indicates if the connection will be allowed to reconnect.
 *
 * Specifies whether or not the client library should try to reconnect when
 * losing the connection to the `NATS Server`.
 *
 * The default is `true`.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param allow `true` if the connection is allowed to reconnect, `false`
 * otherwise.
 */
NATS_EXTERN natsStatus
natsOptions_SetAllowReconnect(natsOptions *opts, bool allow);

/** \brief Sets the maximum number of reconnect attempts.
 *
 * Specifies the maximum number of reconnect attempts.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param maxReconnect the maximum number of reconnects (positive number).
 */
NATS_EXTERN natsStatus
natsOptions_SetMaxReconnect(natsOptions *opts, int maxReconnect);

/** \brief Sets the time between reconnect attempts.
 *
 * Specifies how long to wait between two reconnect attempts from the same
 * server. This means that if you have a list with S1,S2 and are currently
 * connected to S1, and get disconnected, the library will immediately
 * attempt to connect to S2. If this fails, it will go back to S1, and this
 * time will wait for `reconnectWait` milliseconds since the last attempt
 * to connect to S1.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param reconnectWait the time, in milliseconds, to wait between attempts
 * to reconnect to the same server.
 */
NATS_EXTERN natsStatus
natsOptions_SetReconnectWait(natsOptions *opts, int64_t reconnectWait);

/** \brief Set the upper bound of a random delay added to reconnect wait.
 *
 * After a disconnect, the library will try to reconnect to any server URLs
 * in its list (the URLs are either provided by the user or discovered through
 * gossip protocol).
 *
 * After the library failed to reconnect to every server in the list, it
 * will wait for `reconnectWait` as specified with #natsOptions_SetReconnectWait().
 * This option adds some random jitter to the reconnect wait delay.
 *
 * This will help minimize the thundering herd phenomenon. For instance, suppose
 * a server has 1000 connections, all were created at different times, but
 * have the same reconnect wait option. If this server suddenly stops, then all
 * connections will detect the failure and initiate a reconnect at the same time.
 * The issue is even greater when those connections are TLS because of the added
 * cost of the TLS handshake.
 *
 * @see natsOptions_SetReconnectWait()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param jitter the jitter in milliseconds for non TLS connections.
 * @param jitterTLS the jitter in milliseconds for TLS connections.
 */
NATS_EXTERN natsStatus
natsOptions_SetReconnectJitter(natsOptions *opts, int64_t jitter, int64_t jitterTLS);

/** \brief Sets the handler to invoke when the library needs to wait before the next reconnect attempts.
 *
 * This callback is invoked after the library tried every URL in the server list
 * and failed to reconnect. It passes to the user the current number of attempts.
 * This function shall return the amount of time the library will sleep before attempting
 * to reconnect again.
 *
 * It is strongly recommended that this value contains some jitter to prevent all
 * connections to attempt reconnecting at the same time.
 *
 * \note When using this approach, the reconnect wait as specified by #natsOptions_SetReconnectWait()
 * is ignored.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param cb the custom reconnect delay handler to invoke.
 * @param closure a pointer to an user defined object (can be `NULL`). See
 * the #natsCustomReconnectDelayHandler prototype.
 */
NATS_EXTERN natsStatus
natsOptions_SetCustomReconnectDelay(natsOptions *opts,
                                    natsCustomReconnectDelayHandler cb,
                                    void *closure);

/** \brief Sets the size of the backing buffer used during reconnect.
 *
 * Sets the size, in bytes, of the backing buffer holding published data
 * while the library is reconnecting. Once this buffer has been exhausted,
 * publish operations will return the #NATS_INSUFFICIENT_BUFFER error.
 * If not specified, or the value is 0, the library will use a default value,
 * currently set to 8MB.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param reconnectBufSize the size, in bytes, of the backing buffer for
 * write operations during a reconnect.
 */
NATS_EXTERN natsStatus
natsOptions_SetReconnectBufSize(natsOptions *opts, int reconnectBufSize);

/** \brief Sets the maximum number of pending messages per subscription.
 *
 * Specifies the maximum number of inbound messages that can be buffered in the
 * library, for each subscription, before inbound messages are dropped and
 * #NATS_SLOW_CONSUMER status is reported to the #natsErrHandler callback (if
 * one has been set).
 *
 * @see natsOptions_SetErrorHandler()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param maxPending the number of messages allowed to be buffered by the
 * library before triggering a slow consumer scenario.
 */
NATS_EXTERN natsStatus
natsOptions_SetMaxPendingMsgs(natsOptions *opts, int maxPending);

/** \brief Sets the error handler for asynchronous events.
 *
 * Specifies the callback to invoke when an asynchronous error
 * occurs. This is used by applications having only asynchronous
 * subscriptions that would not know otherwise that a problem with the
 * connection occurred.
 *
 * @see natsErrHandler
 *
 * @param opts the pointer to the #natsOptions object.
 * @param errHandler the error handler callback.
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetErrorHandler(natsOptions *opts, natsErrHandler errHandler,
                            void *closure);

/** \brief Sets the callback to be invoked when a connection to a server
 *         is permanently lost.
 *
 * Specifies the callback to invoke when a connection is terminally closed,
 * that is, after all reconnect attempts have failed (when reconnection is
 * allowed).
 *
 * @param opts the pointer to the #natsOptions object.
 * @param closedCb the callback to be invoked when the connection is closed.
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetClosedCB(natsOptions *opts, natsConnectionHandler closedCb,
                        void *closure);

/** \brief Sets the callback to be invoked when the connection to a server is
 *         lost.
 *
 * Specifies the callback to invoke when a connection to the `NATS Server`
 * is lost.
 *
 * \warning Invocation of this callback is asynchronous, which means that
 * the state of the connection may have changed when this callback is
 * invoked.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param disconnectedCb the callback to be invoked when a connection to
 * a server is lost
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetDisconnectedCB(natsOptions *opts,
                              natsConnectionHandler disconnectedCb,
                              void *closure);

/** \brief Sets the callback to be invoked when the connection has reconnected.
 *
 * Specifies the callback to invoke when the client library has successfully
 * reconnected to a `NATS Server`.
 *
 * \warning Invocation of this callback is asynchronous, which means that
 * the state of the connection may have changed when this callback is
 * invoked.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param reconnectedCb the callback to be invoked when the connection to
 * a server has been re-established.
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetReconnectedCB(natsOptions *opts,
                             natsConnectionHandler reconnectedCb,
                             void *closure);

/** \brief Sets the callback to be invoked when new servers are discovered.
 *
 * Specifies the callback to invoke when the client library has been notified
 * of one or more new `NATS Servers`.
 *
 * \warning Invocation of this callback is asynchronous, which means that
 * the state may have changed when this callback is invoked.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param discoveredServersCb the callback to be invoked when new servers
 * have been discovered.
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetDiscoveredServersCB(natsOptions *opts,
                                   natsConnectionHandler discoveredServersCb,
                                   void *closure);

/** \brief Sets the callback to be invoked when server enters lame duck mode.
 *
 * Specifies the callback to invoke when the server notifies
 * the connection that it entered lame duck mode, that is, going to
 * gradually disconnect all its connections before shuting down. This is
 * often used in deployments when upgrading NATS Servers.
 *
 * \warning Invocation of this callback is asynchronous, which means that
 * the state may have changed when this callback is invoked.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param lameDuckCb the callback to be invoked when server enters
 * lame duck mode.
 * @param closure a pointer to an user object that will be passed to
 * the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetLameDuckModeCB(natsOptions *opts,
                              natsConnectionHandler lameDuckCb,
                              void *closure);

/** \brief Sets the external event loop and associated callbacks.
 *
 * If you want to use an external event loop, the `NATS` library will not
 * create a thread to read data from the socket, and will not directly write
 * data to the socket. Instead, the library will invoke those callbacks
 * for various events.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param loop the `void*` pointer to the external event loop.
 * @param attachCb the callback invoked after the connection is connected,
 * or reconnected.
 * @param readCb the callback invoked when the event library should start or
 * stop polling for read events.
 * @param writeCb the callback invoked when the event library should start or
 * stop polling for write events.
 * @param detachCb the callback invoked when a connection is closed.
 */
NATS_EXTERN natsStatus
natsOptions_SetEventLoop(natsOptions *opts,
                         void *loop,
                         natsEvLoop_Attach          attachCb,
                         natsEvLoop_ReadAddRemove   readCb,
                         natsEvLoop_WriteAddRemove  writeCb,
                         natsEvLoop_Detach          detachCb);

/** \brief Switch on/off the use of a central message delivery thread pool.
 *
 * Normally, each asynchronous subscriber that is created has its own
 * message delivery thread. The advantage is that it reduces lock
 * contentions, therefore improving performance.<br>
 * However, if an application creates many subscribers, this is not scaling
 * well since the process would use too many threads.
 *
 * When a connection is created from a `nats_Options` that has enabled
 * global message delivery, asynchronous subscribers from this connection
 * will use a shared thread pool responsible for message delivery.
 *
 * \note The message order per subscription is still guaranteed.
 *
 * @see nats_SetMessageDeliveryPoolSize()
 * @see \ref envVariablesGroup
 *
 * @param opts the pointer to the #natsOptions object.
 * @param global if `true`, uses the global message delivery thread pool,
 * otherwise, each asynchronous subscriber will create their own message
 * delivery thread.
 */
NATS_EXTERN natsStatus
natsOptions_UseGlobalMessageDelivery(natsOptions *opts, bool global);

/** \brief Dictates the order in which host name are resolved during connect.
 *
 * The library would previously favor IPv6 addresses during the connect process.
 * <br>
 * You can now change the order, or even exclude a family of addresses, using
 * this option. Here is the list of possible values:
 * <br>
 * Value | Meaning
 * ------|--------
 * 46 | try IPv4 first, if it fails try IPv6
 * 64 | try IPv6 first, if it fails try IPv4
 * 4 | use only IPv4
 * 6 | use only IPv6
 * 0 | any family, no specific order
 *
 * \note If this option is not set, or you specify `0` for the order, the
 * library will use the first IP (based on the DNS configuration) for which
 * a successful connection can be made.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param order a string representing the order for the IP resolution.
 */
NATS_EXTERN natsStatus
natsOptions_IPResolutionOrder(natsOptions *opts, int order);

/** \brief Sets if Publish calls should send data right away.
 *
 * For throughput performance, the client library tries by default to buffer
 * as much data as possible before sending it over TCP.
 *
 * Setting this option to `true` will make Publish calls send the
 * data right away, reducing latency, but also throughput.
 *
 * A good use-case would be a connection used to solely send replies.
 * Imagine, a requestor sending a request, waiting for the reply before
 * sending the next request.<br>
 * The replier application will send only one reply at a time (since
 * it will not receive the next request until the requestor receives
 * the reply).<br>
 * In such case, it makes sense for the reply to be sent right away.
 *
 * The alternative would be to call #natsConnection_Flush(),
 * but this call requires a round-trip with the server, which is less
 * efficient than using this option.
 *
 * Note that the Request() call already automatically sends the request
 * as fast as possible, there is no need to set an option for that.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param sendAsap a boolean indicating if published data should be
 * sent right away or be buffered.
 */
NATS_EXTERN natsStatus
natsOptions_SetSendAsap(natsOptions *opts, bool sendAsap);

/** \brief Switches the use of old style requests.
 *
 * Setting `useOldStyle` to `true` forces the request calls to use the original
 * behavior, which is to create a new inbox, a new subscription on that inbox
 * and set auto-unsubscribe to 1.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param useOldStyle a boolean indicating if old request style should be used.
 */
NATS_EXTERN natsStatus
natsOptions_UseOldRequestStyle(natsOptions *opts, bool useOldStyle);

/** \brief Fails pending requests on disconnect event.
 *
 * If this option is enabled, all pending #natsConnection_Request() family
 * calls will fail with the #NATS_CONNECTION_DISCONNECTED status.
 *
 * \note This does not apply to requests from connections that use the
 * old style requests.
 *
 * @see natsOptions_UseOldRequestStyle
 *
 * @param opts the pointer to the #natsOptions object.
 * @param failRequests a boolean indicating if pending requests should fail
 * when a disconnect event occurs.
 */
NATS_EXTERN natsStatus
natsOptions_SetFailRequestsOnDisconnect(natsOptions *opts, bool failRequests);

/** \brief Sets if connection receives its own messages.
 *
 * This configures whether the server will echo back messages
 * that are sent on this connection if there is also matching subscriptions.
 *
 * Set this to `true` to prevent the server from sending back messages
 * produced by this connection. The default is false, that is, messages
 * originating from this connection will be sent by the server if the
 * connection has matching subscriptions.
 *
 * \note This is supported on servers >= version 1.2.0. Calling
 * #natsConnection_Connect() with the option set to `true` to server below
 * this version will return the `NATS_NO_SERVER_SUPPORT` error.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param noEcho a boolean indicating if sent messages can be delivered back
 * to this connection or not.
 */
NATS_EXTERN natsStatus
natsOptions_SetNoEcho(natsOptions *opts, bool noEcho);

/** \brief Indicates if initial connect failure should be retried or not.
 *
 * By default, #natsConnection_Connect() attempts to connect to a server
 * specified in provided list of servers. If it cannot connect and the list has been
 * fully tried, the function returns an error.
 *
 * This option is used to changed this default behavior.
 *
 * If `retry` is set to `true` and connection cannot be established right away, the
 * library will attempt to connect based on the reconnect attempts
 * and delay settings.
 *
 * \note The connect retry logic uses reconnect settings even if #natsOptions_SetAllowReconnect()
 * has been set to false. In other words, a failed connect may be retried even though
 * a reconnect will not be allowed should the connection to the server be lost
 * after initial connect.
 *
 * The behavior will then depend on the value of the `connectedCb` parameter:
 *
 * * If `NULL`, then the call blocks until it can connect
 * or exhausts the reconnect attempts.
 *
 * * If not `NULL`, and no connection can be immediately
 * established, the #natsConnection_Connect() calls returns #NATS_NOT_YET_CONNECTED
 * to indicate that no connection is currently established, but will
 * try asynchronously to connect using the reconnect attempts/delay settings. If
 * the connection is later established, the specified callback will be
 * invoked. If no connection can be made and the retry attempts are exhausted,
 * the callback registered with #natsOptions_SetClosedCB(), if any, will be
 * invoked.
 *
 * \note If #natsConnection_Connect() returns `NATS_OK` (that is, a connection to
 * a `NATS Server` was established in that call), then the `connectedCb` callback
 * will not be invoked.
 *
 * If `retry` is set to false, #natsConnection_Connect() behaves as originally
 * designed, that is, returns an error and no connection object if failing to connect
 * to any server in the list.
 *
 * \note The `connectedCb` parameter is ignored and set to `NULL` in the options object
 * when `retry` is set to `false`.
 *
 * @see natsOptions_SetMaxReconnect()
 * @see natsOptions_SetReconnectWait()
 * @see natsOptions_SetClosedCB()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param retry a boolean indicating if a failed connect should be retried.
 * @param connectedCb if `retry` is true and this is not `NULL`, then the
 * connect may be asynchronous and this callback will be invoked if the connect
 * succeeds.
 * @param closure a pointer to an user object that will be passed to the callback. `closure` can be `NULL`.
 */
NATS_EXTERN natsStatus
natsOptions_SetRetryOnFailedConnect(natsOptions *opts, bool retry,
        natsConnectionHandler connectedCb, void* closure);

/** \brief Sets the callbacks to fetch user JWT and sign server's nonce.
 *
 * Any time the library creates a TCP connection to the server, the server
 * in response sends an `INFO` protocol. That `INFO` protocol, for NATS Server
 * at v2.0.0+, may include a `nonce` for the client to sign.
 *
 * If this option is set, the library will invoke the two handlers to fetch
 * the user JWT and sign the server's nonce.
 *
 * This is an option that will be used only by users that are able to
 * sign using Ed25519 (public-key signature system). Most users will probably
 * prefer the user of #natsOptions_SetUserCredentialsFromFiles().
 *
 * \note natsOptions_SetUserCredentialsCallbacks() and natsOptions_SetNKey()
 * are mutually exclusive. Calling this function will remove the NKey and
 * replace the signature handler, that was set with natsOptions_SetNKey(),
 * with this one.
 *
 * @see natsUserJWTHandler
 * @see natsSignatureHandler
 * @see natsOptions_SetUserCredentialsFromFiles()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param ujwtCB the callback to invoke to fetch the user JWT.
 * @param ujwtClosure the closure that will be passed to the `ujwtCB` callback.
 * @param sigCB the callback to invoke to sign the server nonce.
 * @param sigClosure the closure that will be passed to the `sigCB` callback.
 */
NATS_EXTERN natsStatus
natsOptions_SetUserCredentialsCallbacks(natsOptions *opts,
                                        natsUserJWTHandler      ujwtCB,
                                        void                    *ujwtClosure,
                                        natsSignatureHandler    sigCB,
                                        void                    *sigClosure);

/** \brief Sets the file(s) to use to fetch user JWT and seed required to sign nonce.
 *
 * This is a convenient option that specifies the files(s) to use to fetch
 * the user JWT and the user seed to be used to sign the server's nonce.
 *
 * The `userOrChainedFile` contains the user JWT token and possibly the user
 * NKey seed. Note the format of this file:
 *
 * \code{.unparsed}
 * -----BEGIN NATS USER JWT-----
 * ...an user JWT token...
 * ------END NATS USER JWT------
 *
 * ************************* IMPORTANT *************************
 * NKEY Seed printed below can be used to sign and prove identity.
 * NKEYs are sensitive and should be treated as secrets.
 *
 * -----BEGIN USER NKEY SEED-----
 * SU...
 * ------END USER NKEY SEED------
 * \endcode
 *
 * The `---BEGIN NATS USER JWT---` header is used to detect where the user
 * JWT is in this file.
 *
 * If the file does not contain the user NKey seed, then the `seedFile` file
 * name must be specified and must contain the user NKey seed.
 *
 * \note natsOptions_SetUserCredentialsFromFiles() and natsOptions_SetNKey()
 * are mutually exclusive. Calling this function will remove the NKey and
 * replace the signature handler, that was set with natsOptions_SetNKey(),
 * with an internal one that will handle the signature.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param userOrChainedFile the name of the file containing the user JWT and
 * possibly the user NKey seed.
 * @param seedFile the name of the file containing the user NKey seed.
 */
NATS_EXTERN natsStatus
natsOptions_SetUserCredentialsFromFiles(natsOptions *opts,
                                        const char *userOrChainedFile,
                                        const char *seedFile);

/** \brief Sets the NKey public key and signature callback.
 *
 * Any time the library creates a TCP connection to the server, the server
 * in response sends an `INFO` protocol. That `INFO` protocol, for NATS Server
 * at v2.0.0+, may include a `nonce` for the client to sign.
 *
 * If this option is set, the library will add the NKey public key `pubKey`
 * to the `CONNECT` protocol along with the server's nonce signature resulting
 * from the invocation of the signature handler `sigCB`.
 *
 * \note natsOptions_SetNKey() and natsOptions_SetUserCredentialsCallbacks()
 * or natsOptions_SetUserCredentialsFromFiles() are mutually exclusive.
 * Calling this function will remove the user JWT callback and replace the
 * signature handler, that was set with one of the user credentials options,
 * with this one.
 *
 * @see natsSignatureHandler
 *
 * @param opts the pointer to the #natsOptions object.
 * @param pubKey the user NKey public key.
 * @param sigCB the callback to invoke to sign the server nonce.
 * @param sigClosure the closure that will be passed to the `sigCB` callback.
 */
NATS_EXTERN natsStatus
natsOptions_SetNKey(natsOptions             *opts,
                    const char              *pubKey,
                    natsSignatureHandler    sigCB,
                    void                    *sigClosure);

/** \brief Sets the NKey public key and its seed file.
 *
 * Any time the library creates a TCP connection to the server, the server
 * in response sends an `INFO` protocol. That `INFO` protocol, for NATS Server
 * at v2.0.0+, may include a `nonce` for the client to sign.
 *
 * If this option is set, the library will add the NKey public key `pubKey`
 * to the `CONNECT` protocol along with the server's nonce signature signed
 * using the private key from the provided seed file. The library takes care
 * of clearing the memory holding the private key read from the file as soon
 * as it is no longer needed.
 *
 * \note natsOptions_SetNKeyFromSeed() and natsOptions_SetUserCredentialsCallbacks()
 * or natsOptions_SetUserCredentialsFromFiles() are mutually exclusive.
 * Calling this function will remove the user JWT callback and replace the
 * signature handler, that was set with one of the user credentials options,
 * with this one.
 *
 * @see natsSignatureHandler
 *
 * @param opts the pointer to the #natsOptions object.
 * @param pubKey the user NKey public key.
 * @param seedFile the name of the file containing the user NKey seed.
 */
NATS_EXTERN natsStatus
natsOptions_SetNKeyFromSeed(natsOptions *opts,
                            const char  *pubKey,
                            const char  *seedFile);

/** \brief Sets the write deadline.
 *
 * If this is set, the socket is set to non-blocking mode and
 * write will have a deadline set. If the deadline is reached,
 * the write call will return an error which will translate
 * to publish calls, or any library call trying to send data
 * to the server, to possibly fail.
 *
 * @param opts the pointer to the #natsOptions object.
 * @param deadline the write deadline expressed in milliseconds.
 * If set to 0, it means that there is no deadline and socket
 * is in blocking mode.
 */
NATS_EXTERN natsStatus
natsOptions_SetWriteDeadline(natsOptions *opts, int64_t deadline);

/** \brief Enable/Disable the "no responders" feature.
 *
 * By default, when a connection to a NATS Server v2.2.0+ is made,
 * the library signals to the server that it supports the "no responders"
 * feature, which means that if a request is made, and there are
 * no subscriptions on the request subject (no responders), then
 * the server sends back an empty message with the header "Status"
 * and value "503". The request APIs capture this message and
 * instead return a #NATS_NO_RESPONDERS status, instead of waiting
 * for the timeout to occur and return #NATS_TIMEOUT.
 *
 * In case where users set up their own asynchronous subscription
 * on the reply subject and publish the request with #natsConnection_PublishRequest
 * and the like, then the message callback may be invoked with this
 * "no responders" message, which can be checked with #natsMsg_IsNoResponders.
 *
 * However, if users don't want to have to deal with that, it is
 * possible to instruct the server to disable this feature for
 * a given connection. If that is the case, requests will behave
 * as with pre-v2.2.0 servers, in that the request will timeout
 * when there are no responders.
 *
 * \note This function is to disable the feature that is normally
 * enabled by default. Passing `false` means that it would enable
 * it again if you had previously disable the option. However, the
 * feature may still be disabled when connecting to a server that
 * does not support it.
 *
 * @see natsMsg_IsNoResponders()
 *
 * @param opts the pointer to the #natsOptions object.
 * @param disabled the boolean to indicate if the feature should be
 * disabled or not.
 */
NATS_EXTERN natsStatus
natsOptions_DisableNoResponders(natsOptions *opts, bool disabled);

/** \brief Destroys a #natsOptions object.
 *
 * Destroys the natsOptions object, freeing used memory. See the note in
 * the natsOptions_Create() call.
 *
 * @param opts the pointer to the #natsOptions object to destroy.
 */
NATS_EXTERN void
natsOptions_Destroy(natsOptions *opts);

/** @} */ // end of optsGroup

#if defined(NATS_HAS_STREAMING)
/** \defgroup stanConnOptsGroup Streaming Connection Options
 *
 *   NATS Streaming Connection Options.
 *   @{
 */

/** \brief Creates a #stanConnOptions object.
 *
 * Creates a #stanConnOptions object. This object is used when one wants to set
 * specific options prior to connecting to the `NATS Streaning Server`.
 *
 * After making the appropriate `stanConnOptions_SetXXX()` calls, this object is passed
 * to the #stanConnection_Connect() call, which will clone this object. After
 * #stanConnection_Connect() returns, modifications to the options object
 * will not affect the connection.
 *
 * The default options set in this call are:
 * url: `nats://localhost:4222`
 * connection wait: 2 seconds
 * publish ack wait: 30 seconds
 * discovery prefix: `_STAN.discovery`
 * maximum publish acks inflight and percentage: 16384, 50%
 * ping interval: 5 seconds
 * max ping out without response: 3
 *
 * \note The object needs to be destroyed when no longer needed.
 *
 * @see stanConnection_Connect()
 * @see stanConnOptions_Destroy()
 *
 * @param newOpts the location where store the pointer to the newly created
 * #stanConnOptions object.
 */
NATS_EXTERN natsStatus
stanConnOptions_Create(stanConnOptions **newOpts);

/** \brief Sets the URL to connect to.
 *
 * Sets the URL of the `NATS Streaming Server` the client should try to connect to.
 * The URL can contain optional user name and password. You can provide a comma
 * separated list of URLs too.
 *
 * Some valid URLS:
 *
 * - nats://localhost:4222
 * - nats://user\@localhost:4222
 * - nats://user:password\@localhost:4222
 * - nats://host1:4222,nats://host2:4222,nats://host3:4222
 *
 * \note This option takes precedence over #natsOptions_SetURL when NATS options
 * are passed with #stanConnOptions_SetNATSOptions.
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param url the string representing the URL the connection should use
 * to connect to the server.
 *
 */
NATS_EXTERN natsStatus
stanConnOptions_SetURL(stanConnOptions *opts, const char *url);

/** \brief Sets the NATS Options to use to create the connection.
 *
 * The Streaming client connects to the NATS Streaming Server through
 * a regular NATS Connection (#natsConnection). To configure this connection
 * create a #natsOptions and configure it as needed, then call this function.
 *
 * This function clones the passed options, so after this call, any
 * changes to the given #natsOptions will not affect the #stanConnOptions.
 *
 * \note If both #natsOptions_SetURL and #stanConnOptions_SetURL are used
 * the URL(s) set in the later take precedence.
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param nOpts the pointer to the #natsOptions object to use to create
 * the connection to the server.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetNATSOptions(stanConnOptions *opts, natsOptions *nOpts);

/** \brief Sets the timeout for establishing a connection.
 *
 * Value expressed in milliseconds.
 *
 * Default is 2000 milliseconds (2 seconds).
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param wait how long to wait for a response from the streaming server.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetConnectionWait(stanConnOptions *opts, int64_t wait);

/** \brief Sets the timeout for waiting for an ACK for a published message.
 *
 * Value expressed in milliseconds.
 *
 * Default is 30000 milliseconds (30 seconds).
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param wait how long to wait for a response from the streaming server.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetPubAckWait(stanConnOptions *opts, int64_t wait);

/** \brief Sets the subject prefix the library sends the connect request to.
 *
 * Default is `_STAN.discovery`
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param prefix the subject prefix the library sends the connect request to.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetDiscoveryPrefix(stanConnOptions *opts, const char *prefix);


/** \brief Sets the maximum number of published messages without outstanding ACKs from the server.
 *
 * A connection will block #stanConnection_Publish() or #stanConnection_PublishAsync calls
 * if the number of outstanding published messages has reached this number.
 *
 * When the connection receives ACKs, the number of outstanding messages will decrease.
 * If the number falls between `maxPubAcksInflight * percentage`, then the blocked publish
 * calls will be released.
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param maxPubAcksInflight the maximum number of published messages without ACKs from the server.
 * @param percentage the percentage (expressed as a float between ]0.0 and 1.0]) of the maxPubAcksInflight
 * number below which a blocked publish call is released.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetMaxPubAcksInflight(stanConnOptions *opts, int maxPubAcksInflight, float percentage);

/** \brief Sets the ping interval and max out values.
 *
 * Value expressed in number of seconds.
 *
 * Default is 5 seconds and 3 missed PONGs.
 *
 * The interval needs to be at least 1 and represents the number of seconds.
 * The maxOut needs to be at least 2, since the count of sent PINGs increase
 * whenever a PING is sent and reset to 0 when a response is received.
 * Setting to 1 would cause the library to close the connection right away.
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param interval the number of seconds between each PING.
 * @param maxOut the maximum number of PINGs without receiving a PONG back.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetPings(stanConnOptions *opts, int interval, int maxOut);

/** \brief Sets the connection lost handler.
 *
 * This callback will be invoked should the client permanently lose
 * contact with the server (or another client replaces it while being
 * disconnected). The callback will not be invoked on normal #stanConnection_Close().
 *
 * @param opts the pointer to the #stanConnOptions object.
 * @param handler the handler to be invoked when the connection to the streaming server is lost.
 * @param closure the closure the library will pass to the callback.
 */
NATS_EXTERN natsStatus
stanConnOptions_SetConnectionLostHandler(stanConnOptions *opts, stanConnectionLostHandler handler, void *closure);

/** \brief Destroys a #stanConnOptions object.
 *
 * Destroys the #stanConnOptions object, freeing used memory. See the note in
 * the #stanConnOptions_Create() call.
 *
 * @param opts the pointer to the #stanConnOptions object to destroy.
 */
NATS_EXTERN void
stanConnOptions_Destroy(stanConnOptions *opts);

/** @} */ // end of stanConnOptsGroup

/** \defgroup stanSubOptsGroup Streaming Subscription Options
 *
 *   NATS Streaming Subscription Options.
 *   @{
 */

/** \brief Creates a #stanSubOptions object.
 *
 * Creates a #stanSubOptions object. This object is used when one wants to set
 * specific options prior to create a subscription.
 *
 * After making the appropriate `stanSubOptions_SetXXX()` calls, this object is passed
 * to the #stanConnection_Subscribe() or #stanConnection_QueueSubscribe() call, which
 * will clone this object. It means that modifications to the options object will not
 * affect the created subscription.
 *
 * The default options set in this call are:
 * ackWait: 30000 milliseconds (30 seconds)
 * maxIinflight: 1024
 * start position: new only
 *
 * \note The object needs to be destroyed when no longer needed.
 *
 * @see stanConnection_Subscribe()
 * @see stanConnection_QueueSubscribe()
 * @see stanSubOptions_Destroy()
 *
 * @param newOpts the location where store the pointer to the newly created
 * #stanSubOptions object.
 */
NATS_EXTERN natsStatus
stanSubOptions_Create(stanSubOptions **newOpts);

/** \brief Sets the Durable Name for this subscription.
 *
 * If a durable name is set, this subscription will be durable. It means that
 * if the application stops and re-create the same subscription with the
 * same connection client ID and durable name (or simply durable name for
 * queue subscriptions), then the server will resume (re)delivery of messages
 * from the last known position in the steam for that durable.
 *
 * It means that the start position, if provided, is used only when the durable
 * subscription is first created, then is ignored by the server for the rest
 * of the durable subscription lifetime.
 *
 * \note Durable names should be alphanumeric and not contain the character `:`.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param durableName the string representing the name of the durable subscription.
 *
 */
NATS_EXTERN natsStatus
stanSubOptions_SetDurableName(stanSubOptions *opts, const char *durableName);

/** \brief Sets the timeout for waiting for an ACK from the cluster's point of view for delivered messages.
 *
 * Value expressed in milliseconds.
 *
 * Default is 30000 milliseconds (30 seconds).
 *
 * If the server does not receive an acknowledgment from the subscription for
 * a delivered message after this amount of time, the server will re-deliver
 * the unacknowledged message.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param wait how long the server will wait for an acknowledgment from the subscription.
 */
NATS_EXTERN natsStatus
stanSubOptions_SetAckWait(stanSubOptions *opts, int64_t wait);

/** \brief Sets the the maximum number of messages the cluster will send without an ACK.
 *
 * Default is 1024.
 *
 * If a subscription receives messages but does not acknowledge them, the server will
 * stop sending new messages when it reaches this number. Unacknowledged messages are
 * re-delivered regardless of that setting.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param maxInflight the maximum number of messages the subscription will receive without sending back ACKs.
 */
NATS_EXTERN natsStatus
stanSubOptions_SetMaxInflight(stanSubOptions *opts, int maxInflight);

/** \brief Sets the desired start position based on the given sequence number.
 *
 * This allows the subscription to start at a specific sequence number in
 * the channel's message log.
 *
 * If the sequence is smaller than the first available message in the
 * message log (old messages dropped due to channel limits),
 * the subscription will be created with the first available sequence.
 *
 * Conversely, if the given sequence is higher than the currently
 * last sequence number, the subscription will receive only new published messages.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param seq the starting sequence.
 */
NATS_EXTERN natsStatus
stanSubOptions_StartAtSequence(stanSubOptions *opts, uint64_t seq);

/** \brief Sets the desired start position based on the given time.
 *
 * When the subscription is created, the server will send messages starting
 * with the message which timestamp is at least the given time. The time
 * is expressed in number of milliseconds since the EPOCH, as given by
 * nats_Now() for instance.
 *
 * If the time is in the past and the most recent message's timestamp is older
 * than the given time, or if the time is in the future, the subscription
 * will receive only new messages.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param time the start time, expressed in milliseconds since the EPOCH.
 */
NATS_EXTERN natsStatus
stanSubOptions_StartAtTime(stanSubOptions *opts, int64_t time);

/** \brief Sets the desired start position based on the given delta.
 *
 * When the subscription is created, the server will send messages starting
 * with the message which timestamp is at least now minus `delta`. In other words,
 * this means start receiving messages that were sent n milliseconds ago.
 *
 * The delta is expressed in milliseconds.
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param delta he historical time delta (from now) from which to start receiving messages.
 */
NATS_EXTERN natsStatus
stanSubOptions_StartAtTimeDelta(stanSubOptions *opts, int64_t delta);

/** \brief The subscription should start with the last message in the channel.
 *
 * When the subscription is created, the server will start sending messages
 * starting with the last message currently in the channel message's log.
 * The subscription will then receive any new published message.
 *
 * @param opts the pointer to the #stanSubOptions object.
 */
NATS_EXTERN natsStatus
stanSubOptions_StartWithLastReceived(stanSubOptions *opts);

/** \brief The subscription should start with the first message in the channel.
 *
 * When the subscription is created, the server will start sending messages
 * starting with the first message currently in the channel message's log.
 *
 * @param opts the pointer to the #stanSubOptions object.
 */
NATS_EXTERN natsStatus
stanSubOptions_DeliverAllAvailable(stanSubOptions *opts);

/** \brief Sets the subscription's acknowledgment mode.
 *
 * By default, a subscription will automatically send a message acknowledgment
 * after the #stanMsgHandler callback returns.
 *
 * In order to control when the acknowledgment is sent, set the acknowledgment
 * mode to `manual` and call #stanSubscription_AckMsg().
 *
 * @see #stanSubscription_AckMsg()
 *
 * @param opts the pointer to the #stanSubOptions object.
 * @param manual a boolean indicating if the subscription should auto-acknowledge
 * or if the user will.
 */
NATS_EXTERN natsStatus
stanSubOptions_SetManualAckMode(stanSubOptions *opts, bool manual);

/** \brief Destroys a #stanSubOptions object.
 *
 * Destroys the #stanSubOptions object, freeing used memory. See the note in
 * the #stanSubOptions_Create() call.
 *
 * @param opts the pointer to the #stanSubOptions object to destroy.
 */
NATS_EXTERN void
stanSubOptions_Destroy(stanSubOptions *opts);

/** @} */ // end of stanSubOptsGroup
#endif

/** \defgroup inboxGroup Inboxes
 *
 *   NATS Inboxes.
 *   @{
 */

/** \brief Creates an inbox.
 *
 * Returns an inbox string which can be used for directed replies from
 * subscribers. These are guaranteed to be unique, but can be shared
 * and subscribed to by others.
 *
 * \note The inbox needs to be destroyed when no longer needed.
 *
 * @see #natsInbox_Destroy()
 *
 * @param newInbox the location where to store a pointer to the newly
 * created #natsInbox.
 */
NATS_EXTERN natsStatus
natsInbox_Create(natsInbox **newInbox);

/** \brief Destroys the inbox.
 *
 * Destroys the inbox.
 *
 * @param inbox the pointer to the #natsInbox object to destroy.
 */
NATS_EXTERN void
natsInbox_Destroy(natsInbox *inbox);

/** @} */ // end of inboxGroup

/** \defgroup msgGroup Message
 *
 *  NATS Message.
 *  @{
 */

/** \brief Creates a #natsMsg object.
 *
 * Creates a #natsMsg object. This is used by the subscription related calls
 * and by #natsConnection_PublishMsg().
 *
 * \note Messages need to be destroyed with #natsMsg_Destroy() when no
 * longer needed.
 *
 * @see natsMsg_Destroy()
 *
 * @param newMsg the location where to store the pointer to the newly created
 * #natsMsg object.
 * @param subj the subject this message will be sent to. Cannot be `NULL`.
 * @param reply the optional reply for this message.
 * @param data the optional message payload.
 * @param dataLen the size of the payload.
 */
NATS_EXTERN natsStatus
natsMsg_Create(natsMsg **newMsg, const char *subj, const char *reply,
               const char *data, int dataLen);

/** \brief Returns the subject set in this message.
 *
 * Returns the subject set on that message.
 *
 * \warning The string belongs to the message and must not be freed.
 * Copy it if needed.
 * @param msg the pointer to the #natsMsg object.
 */
NATS_EXTERN const char*
natsMsg_GetSubject(const natsMsg *msg);

/** \brief Returns the reply set in this message.
 *
 * Returns the reply, possibly `NULL`.
 *
 * \warning The string belongs to the message and must not be freed.
 * Copy it if needed.
 *
 * @param msg the pointer to the #natsMsg object.
 */
NATS_EXTERN const char*
natsMsg_GetReply(const natsMsg *msg);

/** \brief Returns the message payload.
 *
 * Returns the message payload, possibly `NULL`.
 *
 * Note that although the data sent and received from the server is not `NULL`
 * terminated, the NATS C Client does add a `NULL` byte to the received payload.
 * If you expect the received data to be a "string", then this conveniently
 * allows you to call #natsMsg_GetData() without having to copy the returned
 * data to a buffer to add the `NULL` byte at the end.
 *
 * \warning The string belongs to the message and must not be freed.
 * Copy it if needed.
 *
 * @param msg the pointer to the #natsMsg object.
 */
NATS_EXTERN const char*
natsMsg_GetData(const natsMsg *msg);

/** \brief Returns the message length.
 *
 * Returns the message's payload length, possibly 0.
 *
 * @param msg the pointer to the #natsMsg object.
 */
NATS_EXTERN int
natsMsg_GetDataLength(const natsMsg *msg);

/** \brief Set the header entries associated with `key` to the single element `value`.
 *
 * It will replace any existing value associated with `key`.
 *
 * \warning Prior to v3.0.0, the `key` was stored in its canonical form, this is no
 * longer the case. Header keys are now case sensitive.
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param key the key under which the `value` will be stored. It can't ne `NULL` or empty.
 * @param value the string to store under the given `key`. The value can be `NULL` or empty string.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Set(natsMsg *msg, const char *key, const char *value);

/** \brief Add `value` to the header associated with `key`.
 *
 * It will append to any existing values associated with `key`.
 *
 * \warning Prior to v3.0.0, the `key` was stored in its canonical form, this is no
 * longer the case. Header keys are now case sensitive.
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param key the key under which the `value` will be stored. It can't ne `NULL` or empty.
 * @param value the string to add to the values associated with the given `key`. The value can be `NULL` or empty string.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Add(natsMsg *msg, const char *key, const char *value);

/** \brief Get the header entry associated with `key`.
 *
 * If more than one entry for the `key` is available, the first is returned.
 * The returned value is owned by the library and MUST not be freed or altered.
 *
 * \warning Prior to v3.0.0, the `key` was stored in its canonical form, this is no
 * longer the case. Header keys are now case sensitive.
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param key the key for which the value is requested.
 * @param value the memory location where the library will store the pointer to the first
 * value (if more than one is found) associated with the `key`.
 * @return NATS_NOT_FOUND if `key` is not present in the headers.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Get(natsMsg *msg, const char *key, const char **value);

/** \brief Get all header values associated with `key`.
 *
 * The returned strings are own by the library and MUST not be freed or altered.
 * However, the returned array `values` MUST be freed by the user.
 *
 * \code{.c}
 * const char* *values = NULL;
 * int         count   = 0;
 *
 * s = natsMsgHeader_Values(msg, "My-Key", &values, &count);
 * if (s == NATS_OK)
 * {
 *      // do something with the values
 *
 *      // then free the array of pointers.
 *      free((void*) values);
 * }
 * \endcode
 *
 * \warning Prior to v3.0.0, the `key` was stored in its canonical form, this is no
 * longer the case. Header keys are now case sensitive.
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param key the key for which the values are requested.
 * @param values the memory location where the library will store the pointer to the array
 * of values.
 * @param count the memory location where the library will store the number of values returned.
 * @return NATS_NOT_FOUND if `key` is not present in the headers.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Values(natsMsg *msg, const char *key, const char* **values, int *count);

/** \brief Get all header keys.
 *
 * The returned strings are own by the library and MUST not be freed or altered.
 * However, the returned array `keys` MUST be freed by the user.
 *
 * \code{.c}
 * const char* *keys = NULL;
 * int         count   = 0;
 *
 * s = natsMsgHeader_Keys(msg, &keys, &count);
 * if (s == NATS_OK)
 * {
 *      // do something with the keys
 *
 *      // then free the array of pointers.
 *      free((void*) keys);
 * }
 * \endcode
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param keys the memory location where the library will store the pointer to the array
 * of keys.
 * @param count the memory location where the library will store the number of keys returned.
 * @return NATS_NOT_FOUND if no key is present.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Keys(natsMsg *msg, const char* **keys, int *count);

/** \brief Delete the value(s) associated with `key`.
 *
 * \warning Prior to v3.0.0, the `key` was stored in its canonical form, this is no
 * longer the case. Header keys are now case sensitive.
 *
 * \warning Headers are not thread-safe, that is, you must not set/add/get values or
 * delete keys for the same message from different threads. The internal structure
 * of `natsMsg` may possible be altered during this call.
 *
 * @param msg the pointer to the #natsMsg object.
 * @param key the key to delete from the headers map.
 * @return NATS_NOT_FOUND if `key` is not present in the headers.
 */
NATS_EXTERN natsStatus
natsMsgHeader_Delete(natsMsg *msg, const char *key);

/** \brief Indicates if this message is a "no responders" message from the server.
 *
 * Starting with the NATS Server v2.2.0+ and the C client v2.2.0+ releases, which
 * introduced support for message headers and the "no responders" feature, if a
 * request is received by the server and there are no subscriptions on the
 * request's subject, the server sends a message with no payload but with a header
 * "Status" with value "503".
 *
 * The call #natsConnection_Request() and its variants intercept this special
 * message and instead of returning it to the user, they return #NATS_NO_RESPONDERS.
 *
 * If a synchronous subscription is created on a subject used as a reply subject
 * to a #natsConnection_PublishRequest call (and its variants), #natsSubscription_NextMsg
 * also detects this message and returns #NATS_NO_RESPONDERS (but it was not from
 * release v2.2.0 to v2.4.1).
 *
 * For asynchronous subscriptions, the user may want to know that the request
 * failed because there are no responders. For that reason, the message is passed
 * to the message callback, and this function can be used to detect that this
 * is a "no responders" message from the server and act accordingly.
 *
 * @param msg the pointer to the #natsMsg object.
 * @return `true` if this message is a "no responders" message from the server,
 * that is, has no payload and the "Status" header with "503" as the value.
 */
NATS_EXTERN bool
natsMsg_IsNoResponders(natsMsg *msg);

/** \brief Destroys the message object.
 *
 * Destroys the message, freeing memory.
 *
 * @param msg the pointer to the #natsMsg object to destroy.
 */
NATS_EXTERN void
natsMsg_Destroy(natsMsg *msg);

/** @} */ // end of msgGroup

#if defined(NATS_HAS_STREAMING)
/** \defgroup stanMsgGroup Streaming Message
 *
 *  NATS Streaming Message.
 *  @{
 */

/** \brief Returns the message's sequence number.
 *
 * Returns the message's sequence number (as assigned by the cluster).
 *
 * @param msg the pointer to the #stanMsg object.
 */
NATS_EXTERN uint64_t
stanMsg_GetSequence(const stanMsg *msg);

/** \brief Returns the message's timestamp.
 *
 * Returns the message's timestamp (as assigned by the cluster).
 *
 * @param msg the pointer to the #stanMsg object.
 */
NATS_EXTERN int64_t
stanMsg_GetTimestamp(const stanMsg *msg);

/** \brief Returns the message's redelivered flag.
 *
 * Returns the message's redelivered flag. This can help detect if this
 * message is a possible duplicate (due to redelivery and at-least-once
 * semantic).
 *
 * @param msg the pointer to the #stanMsg object.
 */
NATS_EXTERN bool
stanMsg_IsRedelivered(const stanMsg *msg);

/** \brief Returns the message payload.
 *
 * Returns the message payload, possibly `NULL`.
 *
 * Note that although the data sent and received from the server is not `NULL`
 * terminated, the NATS C Client does add a `NULL` byte to the received payload.
 * If you expect the received data to be a "string", then this conveniently
 * allows you to call #stanMsg_GetData() without having to copy the returned
 * data to a buffer to add the `NULL` byte at the end.
 *
 * \warning The string belongs to the message and must not be freed.
 * Copy it if needed.
 *
 * @param msg the pointer to the #stanMsg object.
 */
NATS_EXTERN const char*
stanMsg_GetData(const stanMsg *msg);

/** \brief Returns the message length.
 *
 * Returns the message's payload length, possibly 0.
 *
 * @param msg the pointer to the #stanMsg object.
 */
NATS_EXTERN int
stanMsg_GetDataLength(const stanMsg *msg);

/** \brief Destroys the message object.
 *
 * Destroys the message, freeing memory.
 *
 * @param msg the pointer to the #stanMsg object to destroy.
 */
NATS_EXTERN void
stanMsg_Destroy(stanMsg *msg);

/** @} */ // end of stanMsgGroup
#endif

/** \defgroup connGroup Connection
 *
 * NATS Connection
 * @{
 */

/** \defgroup connMgtGroup Management
 *
 *  Functions related to connection management.
 *  @{
 */

/** \brief Connects to a `NATS Server` using the provided options.
 *
 * Attempts to connect to a `NATS Server` with multiple options.
 *
 * This call is cloning the #natsOptions object. Once this call returns,
 * changes made to the `options` will not have an effect to this
 * connection. The `options` can however be changed prior to be
 * passed to another #natsConnection_Connect() call if desired.
 *
 * @see #natsOptions
 * @see #natsConnection_Destroy()
 *
 * @param nc the location where to store the pointer to the newly created
 * #natsConnection object.
 * @param options the options to use for this connection. If `NULL`
 * this call is equivalent to #natsConnection_ConnectTo() with #NATS_DEFAULT_URL.
 *
 */
NATS_EXTERN natsStatus
natsConnection_Connect(natsConnection **nc, natsOptions *options);

/** \brief Process a read event when using external event loop.
 *
 * When using an external event loop, and the callback indicating that
 * the socket is ready for reading, this call will read data from the
 * socket and process it.
 *
 * @param nc the pointer to the #natsConnection object.
 *
 * \warning This API is reserved for external event loop adapters.
 */
NATS_EXTERN void
natsConnection_ProcessReadEvent(natsConnection *nc);

/** \brief Process a write event when using external event loop.
 *
 * When using an external event loop, and the callback indicating that
 * the socket is ready for writing, this call will write data to the
 * socket.
 *
 * @param nc the pointer to the #natsConnection object.
 *
 * \warning This API is reserved for external event loop adapters.
 */
NATS_EXTERN void
natsConnection_ProcessWriteEvent(natsConnection *nc);

/** \brief Connects to a `NATS Server` using any of the URL from the given list.
 *
 * Attempts to connect to a `NATS Server`.
 *
 * This call supports multiple comma separated URLs. If more than one is
 * specified, it behaves as if you were using a #natsOptions object and
 * called #natsOptions_SetServers() with the equivalent array of URLs.
 * The list is randomized before the connect sequence starts.
 *
 * @see #natsConnection_Destroy()
 * @see #natsOptions_SetServers()
 *
 * @param nc the location where to store the pointer to the newly created
 * #natsConnection object.
 * @param urls the URL to connect to, or the list of URLs to chose from.
 * If `NULL` this call is equivalent to #natsConnection_ConnectTo()
 * with #NATS_DEFAULT_URL
 */
NATS_EXTERN natsStatus
natsConnection_ConnectTo(natsConnection **nc, const char *urls);

/** \brief Test if connection has been closed.
 *
 * Tests if connection has been closed.
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN bool
natsConnection_IsClosed(natsConnection *nc);

/** \brief Test if connection is reconnecting.
 *
 * Tests if connection is reconnecting.
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN bool
natsConnection_IsReconnecting(natsConnection *nc);

/** \brief Test if connection is draining.
 *
 * Tests if connection is draining.
 *
 * @param nc the pointer to the #natsConnection object.
 */
bool
natsConnection_IsDraining(natsConnection *nc);

/** \brief Returns the current state of the connection.
 *
 * Returns the current state of the connection.
 *
 * @see #natsConnStatus
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN natsConnStatus
natsConnection_Status(natsConnection *nc);

/** \brief Returns the number of bytes to be sent to the server.
 *
 * When calling any of the publish functions, data is not necessarily
 * immediately sent to the server. Some buffering occurs, allowing
 * for better performance. This function indicates if there is any
 * data not yet transmitted to the server.
 *
 * @param nc the pointer to the #natsConnection object.
 * @return the number of bytes to be sent to the server, or -1 if the
 * connection is closed.
 */
NATS_EXTERN int
natsConnection_Buffered(natsConnection *nc);

/** \brief Flushes the connection.
 *
 * Performs a round trip to the server and return when it receives the
 * internal reply.
 *
 * Note that if this call occurs when the connection to the server is
 * lost, the `PING` will not be echoed even if the library can connect
 * to a new (or the same) server. Therefore, in such situation, this
 * call will fail with the status #NATS_CONNECTION_DISCONNECTED.
 *
 * If the connection is closed while this call is in progress, then the
 * status #NATS_CONN_STATUS_CLOSED would be returned instead.
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN natsStatus
natsConnection_Flush(natsConnection *nc);

/** \brief Flushes the connection with a given timeout.
 *
 * Performs a round trip to the server and return when it receives the
 * internal reply, or if the call times-out (timeout is expressed in
 * milliseconds).
 *
 * See possible failure case described in #natsConnection_Flush().
 *
 * @param nc the pointer to the #natsConnection object.
 * @param timeout in milliseconds, is the time allowed for the flush
 * to complete before #NATS_TIMEOUT error is returned.
 */
NATS_EXTERN natsStatus
natsConnection_FlushTimeout(natsConnection *nc, int64_t timeout);

/** \brief Returns the maximum message payload.
 *
 * Returns the maximum message payload accepted by the server. The
 * information is gathered from the `NATS Server` when the connection is
 * first established.
 *
 * @param nc the pointer to the #natsConnection object.
 * @return the maximum message payload.
 */
NATS_EXTERN int64_t
natsConnection_GetMaxPayload(natsConnection *nc);

/** \brief Gets the connection statistics.
 *
 * Copies in the provided statistics structure, a snapshot of the statistics for
 * this connection.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param stats the pointer to a #natsStatistics object in which statistics
 * will be copied.
 */
NATS_EXTERN natsStatus
natsConnection_GetStats(natsConnection *nc, natsStatistics *stats);

/** \brief Gets the URL of the currently connected server.
 *
 * Copies in the given buffer, the connected server's Url. If the buffer is
 * too small, an error is returned.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param buffer the buffer in which the URL is copied.
 * @param bufferSize the size of the buffer.
 */
NATS_EXTERN natsStatus
natsConnection_GetConnectedUrl(natsConnection *nc, char *buffer, size_t bufferSize);

/** \brief Gets the server Id.
 *
 * Copies in the given buffer, the connected server's Id. If the buffer is
 * too small, an error is returned.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param buffer the buffer in which the server id is copied.
 * @param bufferSize the size of the buffer.
 */
NATS_EXTERN natsStatus
natsConnection_GetConnectedServerId(natsConnection *nc, char *buffer, size_t bufferSize);

/** \brief Returns the list of server URLs known to this connection.
 *
 * Returns the list of known servers, including additional servers
 * discovered after a connection has been established (with servers
 * version 0.9.2 and above).
 *
 * No credential information is included in any of the server URLs
 * returned by this call.<br>
 * If you want to use any of these URLs to connect to a server that
 * requires authentication, you will need to use #natsOptions_SetUserInfo
 * or #natsOptions_SetToken.
 *
 * \note The user is responsible for freeing the memory of the returned array.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param servers the location where to store the pointer to the array
 * of server URLs.
 * @param count the location where to store the number of elements of the
 * returned array.
 */
NATS_EXTERN natsStatus
natsConnection_GetServers(natsConnection *nc, char ***servers, int *count);

/** \brief Returns the list of discovered server URLs.
 *
 * Unlike #natsConnection_GetServers, this function only returns
 * the list of servers that have been discovered after the a connection
 * has been established (with servers version 0.9.2 and above).
 *
 * No credential information is included in any of the server URLs
 * returned by this call.<br>
 * If you want to use any of these URLs to connect to a server that
 * requires authentication, you will need to use #natsOptions_SetUserInfo
 * or #natsOptions_SetToken.
 *
 * \note The user is responsible for freeing the memory of the returned array.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param servers the location where to store the pointer to the array
 * of server URLs.
 * @param count the location where to store the number of elements of the
 * returned array.
 */
NATS_EXTERN natsStatus
natsConnection_GetDiscoveredServers(natsConnection *nc, char ***servers, int *count);

/** \brief Gets the last connection error.
 *
 * Returns the last known error as a 'natsStatus' and the location to the
 * null-terminated error string.
 *
 * \warning The returned string is owned by the connection object and
 * must not be freed.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param lastError the location where the pointer to the connection's last
 * error string is copied.
 */
NATS_EXTERN natsStatus
natsConnection_GetLastError(natsConnection *nc, const char **lastError);

/** \brief Gets the current client ID assigned by the server.
 *
 * Returns the client ID assigned by the server to which the client is
 * currently connected to.
 *
 * \note The value may change if the client reconnects.
 *
 * This function returns #NATS_NO_SERVER_SUPPORT if the server's version
 * is less than 1.2.0.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param cid the location where to store the client ID.
 */
NATS_EXTERN natsStatus
natsConnection_GetClientID(natsConnection *nc, uint64_t *cid);

/** \brief Drains the connection with default timeout.
 *
 * Drain will put a connection into a drain state. All subscriptions will
 * immediately be put into a drain state. Upon completion, the publishers
 * will be drained and can not publish any additional messages. Upon draining
 * of the publishers, the connection will be closed. Use the #natsOptions_SetClosedCB()
 * option to know when the connection has moved from draining to closed.
 *
 * This call uses a default drain timeout of 30 seconds.
 *
 * \warning This function does not block waiting for the draining operation
 * to complete.
 *
 * @see natsOptions_SetClosedCB
 * @see natsConnection_DrainTimeout
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN natsStatus
natsConnection_Drain(natsConnection *nc);

/** \brief Drains the connection with given timeout.
 *
 * Identical to #natsConnection_Drain but the timeout can be specified here.
 *
 * The value is expressed in milliseconds. Zero or negative value means
 * that the operation will not timeout.
 *
 * \warning This function does not block waiting for the draining operation
 * to complete.
 *
 * @see natsOptions_SetClosedCB
 * @see natsConnection_Drain
 *
 * @param nc the pointer to the #natsConnection object.
 * @param timeout the allowed time for a drain operation to complete, expressed
 * in milliseconds.
 */
NATS_EXTERN natsStatus
natsConnection_DrainTimeout(natsConnection *nc, int64_t timeout);

/** \brief Signs any 'message' using the connection's user credentials.
 *
 * The connection must have been created with the #natsOptions_SetUserCredentialsFromFiles.
 *
 * This call will sign the given message with the private key extracted through
 * the user credentials file(s).
 *
 * @param nc the pointer to the #natsConnection object.
 * @param message the byte array to sign.
 * @param messageLen the length of the given byte array.
 * @param sig an array of 64 bytes in which the signature will be copied.
 */
NATS_EXTERN natsStatus
natsConnection_Sign(natsConnection *nc,
                    const unsigned char *message, int messageLen,
                    unsigned char sig[64]);

/** \brief Returns the client's IP address as reported by the server.
 *
 * When a connection is created to the server, the server identifies
 * the connection's remote IP address and return it back to the client.
 *
 * \note The user is responsible to free memory allocated to store
 * the client IP address.
 *
 * \note This is supported on servers >= version 2.1.6. Calling
 * #natsConnection_GetClientIP() with server below this version will
 * return the #NATS_NO_SERVER_SUPPORT error.
 *
 * @see natsConnection_GetLocalIPAndPort to get the local IP and port instead.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param ip the memory location where to store the client's IP string.
 * The user is responsible from freeing this memory.
 */
NATS_EXTERN natsStatus
natsConnection_GetClientIP(natsConnection *nc, char **ip);

/** \brief Returns the round trip time between this client and the server.
 *
 * The value returned is in nanoseconds.
 *
 * \note If the library is currently trying to reconnect, this call will
 * return #NATS_CONNECTION_DISCONNECTED.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param rtt the memory location where to store the rtt.
 */
NATS_EXTERN natsStatus
natsConnection_GetRTT(natsConnection *nc, int64_t *rtt);

/** \brief Returns if the connection to current server supports headers.
 *
 * Returns NATS_OK if the server this client is currently connected to
 * supports headers, NATS_NO_SERVER_SUPPORT otherwise.
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN natsStatus
natsConnection_HasHeaderSupport(natsConnection *nc);

/** \brief Returns the connection local IP and port.
 *
 * Unlike #natsConnection_GetClientIP, this function returns the
 * connection's local IP and port.
 *
 * \note The user is responsible for freeing the memory allocated
 * for the returned IP string.
 *
 * @param nc the pointer of the #natsConnection object.
 * @param ip the memory location where to store the local IP string.
 * The user is responsible from freeing this memory.
 * @param port the memory location where to store the local port.
 *
 * @return #NATS_OK on success.
 * @return #NATS_CONNECTION_DISCONNECTED if disconnected.
 * @return #NATS_CONNECTION_CLOSED is connection is closed.
 * @return #NATS_SYS_ERROR if a system error getting the IP and port occurred.
 * @return #NATS_NO_MEMORY if the library was unable to allocate memory for the returned IP string.
 */
natsStatus
natsConnection_GetLocalIPAndPort(natsConnection *nc, char **ip, int *port);

/** \brief Closes the connection.
 *
 * Closes the connection to the server. This call will release all blocking
 * calls, such as #natsConnection_Flush() and #natsSubscription_NextMsg().
 * The connection object is still usable until the call to
 * #natsConnection_Destroy().
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN void
natsConnection_Close(natsConnection *nc);

/** \brief Destroys the connection object.
 *
 * Destroys the connection object, freeing up memory.
 * If not already done, this call first closes the connection to the server.
 *
 * @param nc the pointer to the #natsConnection object.
 */
NATS_EXTERN void
natsConnection_Destroy(natsConnection *nc);

/** @} */ // end of connMgtGroup

/** \defgroup connPubGroup Publishing
 *
 *  Publishing functions
 *  @{
 */

/** \brief Publishes data on a subject.
 *
 * Publishes the data argument to the given subject. The data argument is left
 * untouched and needs to be correctly interpreted on the receiver.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the data is sent to.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 */
NATS_EXTERN natsStatus
natsConnection_Publish(natsConnection *nc, const char *subj,
                       const void *data, int dataLen);

/** \brief Publishes a string on a subject.
 *
 * Convenient function to publish a string. This call is equivalent to:
 *
 * \code{.c}
 * const char* myString = "hello";
 *
 * natsConnection_Publish(nc, subj, (const void*) myString, (int) strlen(myString));
 * \endcode
 *
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the data is sent to.
 * @param str the string to be sent.
 */
NATS_EXTERN natsStatus
natsConnection_PublishString(natsConnection *nc, const char *subj,
                             const char *str);

/** \brief Publishes a message on a subject.
 *
 * Publishes the #natsMsg, which includes the subject, an optional reply and
 * optional data.
 *
 * @see #natsMsg_Create()
 *
 * @param nc the pointer to the #natsConnection object.
 * @param msg the pointer to the #natsMsg object to send.
 */
NATS_EXTERN natsStatus
natsConnection_PublishMsg(natsConnection *nc, natsMsg *msg);

/** \brief Publishes data on a subject expecting replies on the given reply.
 *
 * Publishes the data argument to the given subject expecting a response on
 * the reply subject. Use #natsConnection_Request() for automatically waiting
 * for a response inline.
 *
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the request is sent to.
 * @param reply the reply on which resonses are expected.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 */
NATS_EXTERN natsStatus
natsConnection_PublishRequest(natsConnection *nc, const char *subj,
                              const char *reply, const void *data, int dataLen);

/** \brief Publishes a string on a subject expecting replies on the given reply.
 *
 * Convenient function to publish a request as a string. This call is
 * equivalent to:
 *
 * \code{.c}
 * const char* myString = "hello";
 *
 * natsPublishRequest(nc, subj, reply, (const void*) myString, (int) strlen(myString));
 * \endcode
 *
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the request is sent to.
 * @param reply the reply on which resonses are expected.
 * @param str the string to send.
 */
NATS_EXTERN natsStatus
natsConnection_PublishRequestString(natsConnection *nc, const char *subj,
                                    const char *reply, const char *str);

/** \brief Sends a request and waits for a reply.
 *
 * Sends a request payload and delivers the first response message,
 * or an error, including a timeout if no message was received properly.
 *
 * \warning If connected to a NATS Server v2.2.0+ with no responder running
 * when the request is received, this call will return a #NATS_NO_RESPONDERS error.
 *
 * @param replyMsg the location where to store the pointer to the received
 * #natsMsg reply.
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the request is sent to.
 * @param data the data of the request, can be `NULL`.
 * @param dataLen the length of the data to send.
 * @param timeout in milliseconds, before this call returns #NATS_TIMEOUT
 * if no response is received in this alloted time.
 */
NATS_EXTERN natsStatus
natsConnection_Request(natsMsg **replyMsg, natsConnection *nc, const char *subj,
                       const void *data, int dataLen, int64_t timeout);

/** \brief Sends a request (as a string) and waits for a reply.
 *
 * Convenient function to send a request as a string. This call is
 * equivalent to:
 *
 * \code{.c}
 * const char* myString = "hello";
 *
 * natsConnection_Request(replyMsg, nc, subj, (const void*) myString, (int) strlen(myString));
 * \endcode
 *
 * \warning See warning about no responders in #natsConnection_Request().
 *
 * @param replyMsg the location where to store the pointer to the received
 * #natsMsg reply.
 * @param nc the pointer to the #natsConnection object.
 * @param subj the subject the request is sent to.
 * @param str the string to send.
 * @param timeout in milliseconds, before this call returns #NATS_TIMEOUT
 * if no response is received in this alloted time.
 */
NATS_EXTERN natsStatus
natsConnection_RequestString(natsMsg **replyMsg, natsConnection *nc,
                             const char *subj, const char *str,
                             int64_t timeout);

/** \brief Sends a request based on the given `requestMsg` and waits for a reply.
 *
 * Similar to #natsConnection_Request but uses `requestMsg` to extract subject,
 * and payload to send.
 *
 * \warning See warning about no responders in #natsConnection_Request().
 *
 * @param replyMsg the location where to store the pointer to the received
 * #natsMsg reply.
 * @param nc the pointer to the #natsConnection object.
 * @param requestMsg the message used for the request.
 * @param timeout in milliseconds, before this call returns #NATS_TIMEOUT
 * if no response is received in this alloted time.
 */
NATS_EXTERN natsStatus
natsConnection_RequestMsg(natsMsg **replyMsg, natsConnection *nc,
                          natsMsg *requestMsg, int64_t timeout);

/** @} */ // end of connPubGroup

/** \defgroup connSubGroup Subscribing
 *
 *  Subscribing functions.
 *  @{
 */

/** \brief Creates an asynchronous subscription.
 *
 * Expresses interest in the given subject. The subject can have wildcards
 * (see \ref wildcardsGroup). Messages will be delivered to the associated
 * #natsMsgHandler.
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 * @param cb the #natsMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 */
NATS_EXTERN natsStatus
natsConnection_Subscribe(natsSubscription **sub, natsConnection *nc,
                         const char *subject, natsMsgHandler cb,
                         void *cbClosure);

/** \brief Creates an asynchronous subscription with a timeout.
 *
 * Expresses interest in the given subject. The subject can have wildcards
 * (see \ref wildcardsGroup). Messages will be delivered to the associated
 * #natsMsgHandler.
 *
 * If no message is received by the given timeout (in milliseconds), the
 * message handler is invoked with a `NULL` message.<br>
 * You can then destroy the subscription in the callback, or simply
 * return, in which case, the message handler will fire again when a
 * message is received or the subscription times-out again.
 *
 * \note Receiving a message reset the timeout. Until all pending messages
 * are processed, no timeout will occur. The timeout starts when the
 * message handler for the last pending message returns.
 *
 * \warning If you re-use message handler code between subscriptions with
 * and without timeouts, keep in mind that the message passed in the
 * message handler may be `NULL`.
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 * @param timeout the interval (in milliseconds) after which, if no message
 * is received, the message handler is invoked with a `NULL` message.
 * @param cb the #natsMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 */
NATS_EXTERN natsStatus
natsConnection_SubscribeTimeout(natsSubscription **sub, natsConnection *nc,
                                const char *subject, int64_t timeout,
                                natsMsgHandler cb, void *cbClosure);

/** \brief Creates a synchronous subcription.
 *
 * Similar to #natsConnection_Subscribe, but creates a synchronous subscription
 * that can be polled via #natsSubscription_NextMsg().
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 */
NATS_EXTERN natsStatus
natsConnection_SubscribeSync(natsSubscription **sub, natsConnection *nc,
                             const char *subject);

/** \brief Creates an asynchronous queue subscriber.
 *
 * Creates an asynchronous queue subscriber on the given subject.
 * All subscribers with the same queue name will form the queue group and
 * only one member of the group will be selected to receive any given
 * message asynchronously.
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 * @param queueGroup the name of the group.
 * @param cb the #natsMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 *
 */
NATS_EXTERN natsStatus
natsConnection_QueueSubscribe(natsSubscription **sub, natsConnection *nc,
                              const char *subject, const char *queueGroup,
                              natsMsgHandler cb, void *cbClosure);

/** \brief Creates an asynchronous queue subscriber with a timeout.
 *
 * Creates an asynchronous queue subscriber on the given subject.
 * All subscribers with the same queue name will form the queue group and
 * only one member of the group will be selected to receive any given
 * message asynchronously.
 *
 * If no message is received by the given timeout (in milliseconds), the
 * message handler is invoked with a `NULL` message.<br>
 * You can then destroy the subscription in the callback, or simply
 * return, in which case, the message handler will fire again when a
 * message is received or the subscription times-out again.
 *
 * \note Receiving a message reset the timeout. Until all pending messages
 * are processed, no timeout will occur. The timeout starts when the
 * message handler for the last pending message returns.
 *
 * \warning If you re-use message handler code between subscriptions with
 * and without timeouts, keep in mind that the message passed in the
 * message handler may be `NULL`.
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 * @param queueGroup the name of the group.
 * @param timeout the interval (in milliseconds) after which, if no message
 * is received, the message handler is invoked with a `NULL` message.
 * @param cb the #natsMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 */
NATS_EXTERN natsStatus
natsConnection_QueueSubscribeTimeout(natsSubscription **sub, natsConnection *nc,
                   const char *subject, const char *queueGroup,
                   int64_t timeout, natsMsgHandler cb, void *cbClosure);

/** \brief Creates a synchronous queue subscriber.
 *
 * Similar to #natsConnection_QueueSubscribe, but creates a synchronous
 * subscription that can be polled via #natsSubscription_NextMsg().
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param nc the pointer to the #natsConnection object.
 * @param subject the subject this subscription is created for.
 * @param queueGroup the name of the group.
 */
NATS_EXTERN natsStatus
natsConnection_QueueSubscribeSync(natsSubscription **sub, natsConnection *nc,
                                  const char *subject, const char *queueGroup);

/** @} */ // end of connSubGroup

/** @} */ // end of connGroup

/** \defgroup subGroup Subscription
 *
 *  NATS Subscriptions.
 *  @{
 */

/** \brief Enables the No Delivery Delay mode.
 *
 * By default, messages that arrive are not immediately delivered. This
 * generally improves performance. However, in case of request-reply,
 * this delay has a negative impact. In such case, call this function
 * to have the subscriber be notified immediately each time a message
 * arrives.
 *
 * @param sub the pointer to the #natsSubscription object.
 *
 * \deprecated No longer needed. Will be removed in the future.
 */
NATS_EXTERN natsStatus
natsSubscription_NoDeliveryDelay(natsSubscription *sub);

/** \brief Returns the next available message.
 *
 * Return the next message available to a synchronous subscriber or block until
 * one is available.
 * A timeout (expressed in milliseconds) can be used to return when no message
 * has been delivered. If the value is zero, then this call will not wait and
 * return the next message that was pending in the client, and #NATS_TIMEOUT
 * otherwise.
 *
 * \note If you create a subscription for a subject used as the reply subject
 * of a #natsConnection_PublishRequest call (or any of its variant), and there
 * are no responders for the request subject, NATS Servers v2.2.0+ will return
 * an empty message with a header "Status" and value "503". The library v2.2.0
 * until v2.4.1 would return this message to the user, which was wrong.<br>
 * The library now returns no message and #NATS_NO_RESPONDERS status.
 *
 * @param nextMsg the location where to store the pointer to the next available
 * message.
 * @param sub the pointer to the #natsSubscription object.
 * @param timeout time, in milliseconds, after which this call will return
 * #NATS_TIMEOUT if no message is available.
 */
NATS_EXTERN natsStatus
natsSubscription_NextMsg(natsMsg **nextMsg, natsSubscription *sub,
                         int64_t timeout);

/** \brief Unsubscribes.
 *
 * Removes interest on the subject. Asynchronous subscription may still have
 * a callback in progress, in that case, the subscription will still be valid
 * until the callback returns.
 *
 * @param sub the pointer to the #natsSubscription object.
 */
NATS_EXTERN natsStatus
natsSubscription_Unsubscribe(natsSubscription *sub);

/** \brief Auto-Unsubscribes.
 *
 * This call issues an automatic #natsSubscription_Unsubscribe that is
 * processed by the server when 'max' messages have been received.
 * This can be useful when sending a request to an unknown number
 * of subscribers.
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param max the maximum number of message you want this subscription
 * to receive.
 */
NATS_EXTERN natsStatus
natsSubscription_AutoUnsubscribe(natsSubscription *sub, int max);

/** \brief Gets the number of pending messages.
 *
 * Returns the number of queued messages in the client for this subscription.
 *
 * \deprecated Use #natsSubscription_GetPending instead.
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param queuedMsgs the location where to store the number of queued messages.
 */
NATS_EXTERN natsStatus
natsSubscription_QueuedMsgs(natsSubscription *sub, uint64_t *queuedMsgs);

/** \brief Sets the limit for pending messages and bytes.
 *
 * Specifies the maximum number and size of incoming messages that can be
 * buffered in the library for this subscription, before new incoming messages are
 * dropped and #NATS_SLOW_CONSUMER status is reported to the #natsErrHandler
 * callback (if one has been set).
 *
 * If no limit is set at the subscription level, the limit set by #natsOptions_SetMaxPendingMsgs
 * before creating the connection will be used.
 *
 * \note If no option is set, there is still a default of `65536` messages and
 * `65536 * 1024` bytes.
 *
 * @see natsOptions_SetMaxPendingMsgs
 * @see natsSubscription_GetPendingLimits
 *
 * @param sub he pointer to the #natsSubscription object.
 * @param msgLimit the limit in number of messages that the subscription can hold.
 * @param bytesLimit the limit in bytes that the subscription can hold.
 */
NATS_EXTERN natsStatus
natsSubscription_SetPendingLimits(natsSubscription *sub, int msgLimit, int bytesLimit);

/** \brief Returns the current limit for pending messages and bytes.
 *
 * Regardless if limits have been explicitly set with #natsSubscription_SetPendingLimits,
 * this call will store in the provided memory locations, the limits set for
 * this subscription.
 *
 * \note It is possible for `msgLimit` and/or `bytesLimits` to be `NULL`, in which
 * case the corresponding value is obviously not stored, but the function will
 * not return an error.
 *
 * @see natsOptions_SetMaxPendingMsgs
 * @see natsSubscription_SetPendingLimits
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param msgLimit if not `NULL`, the memory location where to store the maximum
 * number of pending messages for this subscription.
 * @param bytesLimit if not `NULL`, the memory location where to store the maximum
 * size of pending messages for this subscription.
 */
NATS_EXTERN natsStatus
natsSubscription_GetPendingLimits(natsSubscription *sub, int *msgLimit, int *bytesLimit);

/** \brief Returns the number of pending messages and bytes.
 *
 * Returns the total number and size of pending messages on this subscription.
 *
 * \note It is possible for `msgs` and `bytes` to be NULL, in which case the
 * corresponding values are obviously not stored, but the function will not return
 * an error.
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param msgs if not `NULL`, the memory location where to store the number of
 * pending messages.
 * @param bytes if not `NULL`, the memory location where to store the total size of
 * pending messages.
 */
NATS_EXTERN natsStatus
natsSubscription_GetPending(natsSubscription *sub, int *msgs, int *bytes);

/** \brief Returns the number of delivered messages.
 *
 * Returns the number of delivered messages for this subscription.
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param msgs the memory location where to store the number of
 * delivered messages.
 */
NATS_EXTERN natsStatus
natsSubscription_GetDelivered(natsSubscription *sub, int64_t *msgs);

/** \brief Returns the number of dropped messages.
 *
 * Returns the number of known dropped messages for this subscription. This happens
 * when a consumer is not keeping up and the library starts to drop messages
 * when the maximum number (and/or size) of pending messages has been reached.
 *
 * \note If the server declares the connection a slow consumer, this number may
 * not be valid.
 *
 * @see natsOptions_SetMaxPendingMsgs
 * @see natsSubscription_SetPendingLimits
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param msgs the memory location where to store the number of dropped messages.
 */
NATS_EXTERN natsStatus
natsSubscription_GetDropped(natsSubscription *sub, int64_t *msgs);

/** \brief Returns the maximum number of pending messages and bytes.
 *
 * Returns the maximum of pending messages and bytes seen so far.
 *
 * \note `msgs` and/or `bytes` can be NULL.
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param msgs if not `NULL`, the memory location where to store the maximum
 * number of pending messages seen so far.
 * @param bytes if not `NULL`, the memory location where to store the maximum
 * number of bytes pending seen so far.
 */
NATS_EXTERN natsStatus
natsSubscription_GetMaxPending(natsSubscription *sub, int *msgs, int *bytes);

/** \brief Clears the statistics regarding the maximum pending values.
 *
 * Clears the statistics regarding the maximum pending values.
 *
 * @param sub the pointer to the #natsSubscription object.
 */
NATS_EXTERN natsStatus
natsSubscription_ClearMaxPending(natsSubscription *sub);

/** \brief Get various statistics from this subscription.
 *
 * This is a convenient function to get several subscription's statistics
 * in one call.
 *
 * \note Any or all of the statistics pointers can be `NULL`.
 *
 * @see natsSubscription_GetPending
 * @see natsSubscription_GetMaxPending
 * @see natsSubscription_GetDelivered
 * @see natsSubscription_GetDropped
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param pendingMsgs if not `NULL`, memory location where to store the
 * number of pending messages.
 * @param pendingBytes if not `NULL`, memory location where to store the
 * total size of pending messages.
 * @param maxPendingMsgs if not `NULL`, memory location where to store the
 * maximum number of pending messages seen so far.
 * @param maxPendingBytes if not `NULL`, memory location where to store the
 * maximum total size of pending messages seen so far.
 * @param deliveredMsgs if not `NULL`, memory location where to store the
 * number of delivered messages.
 * @param droppedMsgs if not `NULL`, memory location where to store the
 * number of dropped messages.
 */
NATS_EXTERN natsStatus
natsSubscription_GetStats(natsSubscription *sub,
                          int     *pendingMsgs,
                          int     *pendingBytes,
                          int     *maxPendingMsgs,
                          int     *maxPendingBytes,
                          int64_t *deliveredMsgs,
                          int64_t *droppedMsgs);

/** \brief Checks the validity of the subscription.
 *
 * Returns a boolean indicating whether the subscription is still active.
 * This will return false if the subscription has already been closed,
 * or auto unsubscribed.
 *
 * @param sub the pointer to the #natsSubscription object.
 */
NATS_EXTERN bool
natsSubscription_IsValid(natsSubscription *sub);

/** \brief Drains the subscription with a default timeout.
 *
 * Drain will remove interest but continue to invoke callbacks until all messages
 * have been processed.
 *
 * This call uses a default drain timeout of 30 seconds. See #natsSubscription_DrainTimeout
 * for details on behavior when timeout elapses.
 *
 * \warning This function does not block waiting for the operation
 * to complete. To synchronously wait, see #natsSubscription_WaitForDrainCompletion
 *
 * @see natsSubscription_DrainTimeout
 * @see natsSubscription_WaitForDrainCompletion
 * @see natsSubscription_DrainCompletionStatus
 *
 * @param sub the pointer to the #natsSubscription object.
 */
NATS_EXTERN natsStatus
natsSubscription_Drain(natsSubscription *sub);

/** \brief Drains the subscription with the specified timeout.
 *
 * Drain will remove interest but continue to invoke callbacks until all messages
 * have been processed, or the specified timeout has elapsed. In that case, the
 * subscription will be forcibly closed and remaining pending messages (if any)
 * will not be processed.
 *
 * The timeout is expressed in milliseconds. Zero or negative value means that
 * the call will not timeout, but see below for more details.
 *
 * When this call returns, the UNSUBSCRIBE protocol for this subscription has
 * been enqueued to the outgoing connection buffer, but not sent to the server,
 * ensuring that this call does not block.
 *
 * The library then asynchronously ensures that this protocol is sent and waits
 * for a confirmation from the server. After that, it is guaranteed that no
 * new message for this subscription will be received and the library can proceed
 * with the rest of the draining.
 *
 * However, should the "flush" of the protocol fail, the library will ensure that
 * no new message is added to the subscription (in the event the server did not
 * receive the UNSUBSCRIBE protocol and still attempts to deliver messages), and
 * will proceed with the draining of the pending messages. Users can check the
 * status of the draining after it has completed by calling #natsSubscription_DrainCompletionStatus.
 *
 * If no timeout is specified (that is, value is zero or negative), a timeout
 * will be used for the "flush" of the protocol. Again, even in case of failure,
 * the draining will proceed.
 *
 * If a timeout is specified, the complete process: "flush" of the protocol
 * and draining of messages, must happen before the timeout elapses otherwise the
 * subscription will be forcibly closed, and not all message callbacks may be invoked.
 *
 * Regardless of the presence of a timeout or not, should the subscription or connection
 * be closed while draining occurs, the draining process will stop. The
 * #natsSubscription_WaitForDrainCompletion call will not report an error. To
 * know if an error occurred, the user can call #natsSubscription_DrainCompletionStatus
 * after ensuring that the drain has completed.
 *
 * \warning This function does not block waiting for the operation
 * to complete. To synchronously wait, see #natsSubscription_WaitForDrainCompletion
 *
 * @see natsSubscription_Drain
 * @see natsSubscription_WaitForDrainCompletion
 * @see natsSubscription_DrainCompletionStatus
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param timeout how long to wait for the operation to complete, expressed in
 * milliseconds. If the timeout elapses the subscription will be closed.
 */
NATS_EXTERN natsStatus
natsSubscription_DrainTimeout(natsSubscription *sub, int64_t timeout);

/** \brief Blocks until the drain operation completes.
 *
 * This function blocks until the subscription is fully drained.
 * Returns no error if the subscription is drained or closed, otherwise
 * returns the error if the subscription was not in drained mode (#NATS_ILLEGAL_STATE)
 * or if this subscription was not drained or closed prior to the specified
 * timeout (#NATS_TIMEOUT).
 *
 * The timeout is expressed in milliseconds. Zero or negative value
 * means that the call will not timeout.
 *
 * Note that if this call times-out, it does not mean that the drain stops.
 * The drain will continue until its own timeout elapses.
 *
 * @see natsSubscription_Drain
 * @see natsSubscription_DrainTimeout
 * @see natsSubscription_DrainCompletionStatus
 *
 * @param sub the pointer to the #natsSubscription object.
 * @param timeout how long to wait for the operation to complete, expressed in
 * milliseconds.
 */
NATS_EXTERN natsStatus
natsSubscription_WaitForDrainCompletion(natsSubscription *sub, int64_t timeout);

/** \brief Returns the status of the drain after completion.
 *
 * Once the drain has completed, users can use this function to know if the
 * drain completed successfully or not.
 *
 * Possible return values (the list is not exhaustive):
 *
 * <c>NATS_OK</c> the library sent the UNSUBSCRIBE protocol and finished processing
 * all messages that were pending.<br>
 * <c>NATS_ILLEGAL_STATE</c> this call was made for a subscription that had not
 * started draining or the draining is still in progress.<br>
 * <c>NATS_INVALID_SUBSCRIPTION</c> the subscription was closed while draining,
 * which means that some messages may not have been processed.<br>
 * <c>NATS_CONNECTION_CLOSED</c> the connection was closed while draining, which
 * means that some messages may not have been processed.
 *
 * \note This call does not wait for the drain completion (see #natsSubscription_WaitForDrainCompletion
 * for that).
 *
 * @see natsSubscription_Drain
 * @see natsSubscription_DrainTimeout
 * @see natsSubscription_WaitForDrainCompletion
 *
 * @param sub the pointer to the #natsSubscription object.
 */
NATS_EXTERN natsStatus
natsSubscription_DrainCompletionStatus(natsSubscription *sub);

/** \brief Sets a completion callback.
 *
 * In order to make sure that an asynchronous subscription's message handler is no longer invoked once the
 * subscription is closed (#natsSubscription_Unsubscribe), the subscription should be closed from the
 * message handler itslef.
 *
 * If the application closes the subscription from a different thread and immediately frees resources
 * needed in the message handler, there is a risk of a crash since the subscription's message handler
 * may still be invoked one last time or already in the process of executing.
 *
 * To address this, the user can set a callback that will be invoked after the subscription is closed and the
 * message handler has returned. This applies to asynchronous subscriptions using their own dispatcher or using
 * the library's delivery thread pool.
 *
 * \note You don't need to call this function if you are not freeing resources needed in the message handler or if
 * you always close the subscription from the message handler itself.
 *
 * \note If you plan on calling this function, you should do so before calling #natsSubscription_AutoUnsubscribe, since
 * there is a risk that the subscription be removed as soon as #natsSubscription_AutoUnsubscribe returns.
 *
 * Calling this function on a synchronous or closed subscription will return #NATS_INVALID_SUBSCRIPTION.
 *
 * @see natsOnCompleteCB
 *
 * @param sub the pointer to the #natsSubscription object
 * @param cb the callback to invoke when the last message of a closed subscription has been dispatched
 * @param closure the pointer to a user defined object (possibly `NULL`) that will be passed to the callback
 */
NATS_EXTERN natsStatus
natsSubscription_SetOnCompleteCB(natsSubscription *sub, natsOnCompleteCB cb, void *closure);

/** \brief Destroys the subscription.
 *
 * Destroys the subscription object, freeing up memory.
 * If not already done, this call will removes interest on the subject.
 *
 * @param sub the pointer to the #natsSubscription object to destroy.
 */
NATS_EXTERN void
natsSubscription_Destroy(natsSubscription *sub);

/** @} */ // end of subGroup

#if defined(NATS_HAS_STREAMING)
/** \defgroup stanConnGroup Streaming Connection
 *
 * NATS Streaming Connection
 * @{
 */

/** \defgroup stanConnMgtGroup Management
 *
 *  Functions related to connection management.
 *  @{
 */

/** \brief Connects to a `NATS Streaming Server` using the provided options.
 *
 * Attempts to connect to a `NATS Streaming Server`.
 *
 * This call is cloning the #stanConnOptions object, if given. Once this call returns,
 * changes made to the `options` will not have an effect to this connection.
 * The `options` can however be changed prior to be passed to another
 * #stanConnection_Connect() call if desired.
 *
 * \note The Streaming connection does not honor the NATS Connection option
 * #natsOptions_SetRetryOnFailedConnect(). If you pass NATS Options with
 * this option enabled, no error is returned, but if the connection cannot
 * be established "right away", the connect call will return an error.
 *
 * \warning If connecting to a NATS Server v2.2.0+ and there is no Streaming server
 * listening on the connect request subject, this call will return #NATS_NO_RESPONDERS,
 * not #NATS_TIMEOUT.
 *
 * @see #stanConnOptions
 * @see #stanConnection_Destroy()
 *
 * @param sc the location where to store the pointer to the newly created
 * #natsConnection object.
 * @param clusterID the name of the cluster this connection is for.
 * @param clientID the client ID for this connection. Only one connection
 * with this ID will be accepted by the server. Use only a-zA-Z0-9_- characters.
 * @param options the options to use for this connection (can be `NULL`).
 */
NATS_EXTERN natsStatus
stanConnection_Connect(stanConnection **sc, const char *clusterID, const char *clientID,
                       stanConnOptions *options);

/** \brief Returns the underlying NATS Connection.
 *
 * This can be used if the application needs to do non streaming messaging
 * but does not want to create a separate NATS Connection.
 *
 * Obtain a NATS connection from a NATS streaming connection. The NATS
 * connection can be used to perform regular NATS operations, but it is owned
 * and managed by the NATS streaming connection. It cannot be closed,
 * which will happen when the NATS streaming connection is closed.
 *
 * \note For each call to this function, the user must call
 * #stanConnection_ReleaseNATSConnection() when access to the NATS Connection
 * is no longer needed.
 *
 * \warning The returned connection cannot be closed, drained nor destroyed.
 * Calling corresponding functions will have no effect or return #NATS_ILLEGAL_STATE.
 *
 * @see stanConnection_ReleaseNATSConnection()
 *
 * @param sc the pointer to the #stanConnection object.
 * @param nc the location where to store the pointer of the #natsConnection object.
 */
NATS_EXTERN natsStatus
stanConnection_GetNATSConnection(stanConnection *sc, natsConnection **nc);

/** \brief Releases the NATS Connection.
 *
 * This should be paired with the #stanConnection_GetNATSConnection() call.
 * That is, after getting a reference to the underlying NATS Connection and
 * once that connection is no longer needed, calling this function will
 * allow resources to be properly released when the streaming connection is destroyed.
 *
 * You would normally call #stanConnection_GetNATSConnection() and this function
 * only once.
 *
 * After the last #stanConnection_ReleaseNATSConnection() call is made, you
 * must no longer use the NATS Connection because if #stanConnection_Destroy()
 * is called, that could make the pointer to the NATS Connection invalid.
 *
 * \note If the streaming connection is closed/destroyed before the last
 * call to #stanConnection_ReleaseNATSConnection, the pointer to the NATS
 * connection will still be valid, although all calls will fail since the
 * connection is now closed. Calling this function will release the streaming
 * object allowing memory to be freed.
 *
 * @see stanConnection_GetNATSConnection
 *
 * @param sc the pointer to the #stanConnection object.
 */
NATS_EXTERN void
stanConnection_ReleaseNATSConnection(stanConnection *sc);

/** \brief Closes the connection.
 *
 * Closes the connection to the server. This call will release all blocking
 * calls. The connection object is still usable until the call to
 * #stanConnection_Destroy().
 *
 * \warning See warning about connecting to a NATS Server v2.2.0+ in #stanConnection_Connect().
 *
 * @param sc the pointer to the #stanConnection object.
 */
NATS_EXTERN natsStatus
stanConnection_Close(stanConnection *sc);

/** \brief Destroys the connection object.
 *
 * Destroys the connection object, freeing up memory.
 * If not already done, this call first closes the connection to the server.
 *
 * @param sc the pointer to the #stanConnection object.
 */
NATS_EXTERN natsStatus
stanConnection_Destroy(stanConnection *sc);

/** @} */ // end of stanConnMgtGroup

/** \defgroup stanConnPubGroup Publishing
 *
 *  Publishing functions
 *  @{
 */

/** \brief Publishes data on a channel.
 *
 * Publishes the data argument to the given channel. The data argument is left
 * untouched and needs to be correctly interpreted on the receiver.
 *
 * @param sc the pointer to the #stanConnection object.
 * @param channel the channel name the data is sent to.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 */
NATS_EXTERN natsStatus
stanConnection_Publish(stanConnection *sc, const char *channel,
                       const void *data, int dataLen);

/** \brief Asynchronously publishes data on a channel.
 *
 * Publishes the data argument to the given channel. The data argument is left
 * untouched and needs to be correctly interpreted on the receiver.
 *
 * This function does not wait for an acknowledgment back from the server.
 * Instead, the library will invoke the provided callback when that acknowledgment
 * comes.
 *
 * In order to correlate the acknowledgment with the published message, you
 * can use the `ahClosure` since this will be passed to the #stanPubAckHandler
 * on every invocation. In other words, you should use a unique closure for
 * each published message.
 *
 * @param sc the pointer to the #natsConnection object.
 * @param channel the channel name the data is sent to.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 * @param ah the publish acknowledgment callback. If `NULL` the user will not
 * be notified of the publish result.
 * @param ahClosure the closure the library will pass to the `ah` callback if
 * one has been set.
 */
NATS_EXTERN natsStatus
stanConnection_PublishAsync(stanConnection *sc, const char *channel,
                            const void *data, int dataLen,
                            stanPubAckHandler ah, void *ahClosure);

/** @} */ // end of stanConnPubGroup

/** \defgroup stanConnSubGroup Subscribing
 *
 *  Subscribing functions.
 *  @{
 */

/** \brief Creates a subscription.
 *
 * Expresses interest in the given subject. The subject can NOT have wildcards.
 * Messages will be delivered to the associated #stanMsgHandler.
 *
 * \warning See warning about connecting to a NATS Server v2.2.0+ in #stanConnection_Connect().
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param sc the pointer to the #natsConnection object.
 * @param channel the channel this subscription is created for.
 * @param cb the #stanMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #stanMsgHandler prototype.
 * @param options the optional to further configure the subscription.
 */
NATS_EXTERN natsStatus
stanConnection_Subscribe(stanSubscription **sub, stanConnection *sc,
                         const char *channel, stanMsgHandler cb,
                         void *cbClosure, stanSubOptions *options);

/** \brief Creates a queue subscription.
 *
 * Creates a queue subscriber on the given channel.
 * All subscribers with the same queue name will form the queue group and
 * only one member of the group will be selected to receive any given
 * message asynchronously.
 *
 * \warning See warning about connecting to a NATS Server v2.2.0+ in #stanConnection_Connect().
 *
 * @param sub the location where to store the pointer to the newly created
 * #natsSubscription object.
 * @param sc the pointer to the #natsConnection object.
 * @param channel the channel name this subscription is created for.
 * @param queueGroup the name of the group.
 * @param cb the #natsMsgHandler callback.
 * @param cbClosure a pointer to an user defined object (can be `NULL`). See
 * the #natsMsgHandler prototype.
 * @param options the optional options to further configure this queue subscription.
 */
NATS_EXTERN natsStatus
stanConnection_QueueSubscribe(stanSubscription **sub, stanConnection *sc,
                              const char *channel, const char *queueGroup,
                              stanMsgHandler cb, void *cbClosure, stanSubOptions *options);

/** @} */ // end of stanConnSubGroup

/** @} */ // end of stanConnGroup

/** \defgroup stanSubGroup Streaming Subscription
 *
 *  NATS Streaming Subscriptions.
 *  @{
 */

/** \brief Sets a completion callback.
 *
 * In order to make sure that an asynchronous subscription's message handler is no longer invoked once the
 * subscription is closed (or unsubscribed) (#stanSubscription_Close, #stanSubscription_Unsubscribe), the
 * subscription should be closed from the message handler itslef.
 *
 * If the application closes the subscription from a different thread and immediately frees resources
 * needed in the message handler, there is a risk of a crash since the subscription's message handler
 * may still be invoked one last time or already in the process of executing.
 *
 * To address this, the user can set a callback that will be invoked after the subscription is closed and the
 * message handler has returned.
 *
 * \note You don't need to call this function if you are not freeing resources needed in the message handler or if
 * you always close the subscription from the message handler itself.
 *
 * @see natsOnCompleteCB
 *
 * @param sub the pointer to the #stanSubscription object
 * @param cb the callback to invoke when the last message of a closed subscription has been dispatched
 * @param closure the pointer to a user defined object (possibly `NULL`) that will be passed to the callback
 */
NATS_EXTERN natsStatus
stanSubscription_SetOnCompleteCB(stanSubscription *sub, natsOnCompleteCB cb, void *closure);

/** \brief Acknowledge a message.
 *
 * If the subscription is created with manual acknowledgment mode (see #stanSubOptions_SetManualAckMode)
 * then it is the user responsibility to acknowledge received messages when
 * appropriate.
 *
 * @param sub the pointer to the #stanSubscription object.
 * @param msg the message to acknowledge.
 */
NATS_EXTERN natsStatus
stanSubscription_AckMsg(stanSubscription *sub, stanMsg *msg);

/** \brief Permanently remove a subscription.
 *
 * Removes interest on the channel. The subscription may still have
 * a callback in progress, in that case, the subscription will still be valid
 * until the callback returns.
 *
 * For non-durable subscriptions, #stanSubscription_Unsubscribe and #stanSubscription_Close
 * have the same effect.
 *
 * For durable subscriptions, calling this function causes the server
 * to remove the durable subscription (instead of simply suspending it).
 * It means that once this call is made, calling #stanConnection_Subscribe() with
 * the same durable name creates a brand new durable subscription, instead
 * of simply resuming delivery.
 *
 * \warning See warning about connecting to a NATS Server v2.2.0+ in #stanConnection_Connect().
 *
 * @param sub the pointer to the #stanSubscription object.
 */
NATS_EXTERN natsStatus
stanSubscription_Unsubscribe(stanSubscription *sub);

/** \brief Closes the subscription.
 *
 * Similar to #stanSubscription_Unsubscribe() except that durable interest
 * is not removed in the server. The durable subscription can therefore be
 * resumed.
 *
 * \warning See warning about connecting to a NATS Server v2.2.0+ in #stanConnection_Connect().
 *
 * @param sub the pointer to the #stanSubscription object.
 */
NATS_EXTERN natsStatus
stanSubscription_Close(stanSubscription *sub);

/** \brief Destroys the subscription.
 *
 * Destroys the subscription object, freeing up memory.
 * If not already done, this call will removes interest on the subject.
 *
 * @param sub the pointer to the #stanSubscription object to destroy.
 */
NATS_EXTERN void
stanSubscription_Destroy(stanSubscription *sub);

/** @} */ // end of stanSubGroup
#endif

/** \defgroup jsGroup JetStream
 *
 *  JetStream.
 *  @{
 */

/** \brief Initializes a streaming context options structure.
 *
 * Use this before setting specific stream context options and passing it
 * to JetStream APIs.
 *
 * @param opts the pointer to the #jsOptions to initialize.
 */
NATS_EXTERN natsStatus
jsOptions_Init(jsOptions *opts);

/** \brief Returns a new JetStream context.
 *
 * A JetStream context is used for messaging and assets management.
 *
 * Since the underlying NATS connection is used for communication, the NATS connection
 * should stay valid while using the JetStream context. That is, do not close/destroy
 * the NATS connection before destroying the JetStream context.
 *
 * \note When done, the context should be destroyed to release memory.
 *
 * @param js the location where to store the pointer to the newly created #jsCtx object.
 * @param nc the pointer to the #natsConnection object from which to get the JetStream context.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 */
NATS_EXTERN natsStatus
natsConnection_JetStream(jsCtx **js, natsConnection *nc, jsOptions *opts);

/** \brief Destroys the JetStream context.
 *
 * Releases memory used by the context object.
 *
 * @param js the pointer to the #jsCtx object to destroy.
 */
NATS_EXTERN void
jsCtx_Destroy(jsCtx *js);

/** \defgroup jsAssetsGroup JetStream Assets Management
 *
 *  JetStream Assets Management
 *  @{
 */

/** \brief Initializes a streaming configuration structure.
 *
 * Use this before setting specific stream configuration options and passing this
 * configuration to some of the stream management APIs.
 *
 * @param cfg the pointer to the #jsStreamConfig to initialize.
 */
NATS_EXTERN natsStatus
jsStreamConfig_Init(jsStreamConfig *cfg);

/** \brief Initializes a placement configuration structure.
 *
 * Use this before setting specific stream placement options.
 *
 * @param placement the pointer to the #jsPlacement to initialize.
 */
NATS_EXTERN natsStatus
jsPlacement_Init(jsPlacement *placement);

/** \brief Initializes a stream source configuration structure.
 *
 * Use this before setting specific stream source options.
 *
 * @param source the pointer to the #jsStreamSource to initialize.
 */
NATS_EXTERN natsStatus
jsStreamSource_Init(jsStreamSource *source);

/** \brief Initializes an external stream configuration structure.
 *
 * Use this before setting specific external stream options.
 *
 * @param external the pointer to the #jsExternalStream to initialize.
 */
NATS_EXTERN natsStatus
jsExternalStream_Init(jsExternalStream *external);

/** \brief Creates a stream.
 *
 * Creates a stream based on the provided configuration (that cannot be `NULL`).
 * The name is mandatory and cannot contain <c>.</c> characters.
 *
 * \note If you do not need a #jsStreamInfo to be returned, you can pass `NULL`,
 * otherwise, on success you are responsible for freeing this object.
 *
 * @see jsStreamConfig_Init
 * @see jsStreamInfo_Destroy
 *
 * @param si the location where to store the pointer to the new #jsStreamInfo object in
 * response to the creation request, or `NULL` if the stream information is not needed.
 * @param js the pointer to the #jsCtx context.
 * @param cfg the pointer to the #jsStreamConfig.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_AddStream(jsStreamInfo **si, jsCtx *js, jsStreamConfig *cfg, jsOptions *opts, jsErrCode *errCode);

/** \brief Updates a stream.
 *
 * Updates a stream based on the provided configuration (that cannot be `NULL`).
 * The name is mandatory and cannot contain <c>.</c> characters.
 *
 * \note If you do not need a #jsStreamInfo to be returned, you can pass `NULL`,
 * otherwise, on success you are responsible for freeing this object.
 *
 * @see jsStreamConfig_Init
 * @see jsStreamInfo_Destroy
 *
 * @param si the location where to store the pointer to the new #jsStreamInfo object in
 * response to the creation request, or `NULL` if the stream information is not needed.
 * @param js the pointer to the #jsCtx context.
 * @param cfg the pointer to the #jsStreamConfig.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_UpdateStream(jsStreamInfo **si, jsCtx *js, jsStreamConfig *cfg, jsOptions *opts, jsErrCode *errCode);

/** \brief Purges a stream.
 *
 * Purges the stream named <c>stream</c>.
 *
 * For more advanced purge options, you can specify them through #jsOptions.
 *
 * \code{.unparsed}
 * jsOptions        o;
 *
 * jsOptions_Init(&o);
 * o.Stream.Purge.Subject  = "foo";
 * o.Stream.Purge.Sequence = 4;
 *
 * js_PurgeStream(js, "MY_STREAM", &o, &jerr);
 * \endcode
 *
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream to purge.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_PurgeStream(jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode);

/** \brief Deletes a stream.
 *
 * Deletes the stream named <c>stream</c>.
 *
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream to delete.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_DeleteStream(jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode);

/** \brief Retreives information from a stream.
 *
 * Returns information about the stream named <c>stream</c>.
 *
 * \note You need to free the returned object.
 *
 * To get some detailed information about deleted messages, set this option:
 *
 * \code{.unparsed}
 * jsOptions                o;
 *
 * jsOptions_Init(&o);
 * o.Stream.Info.DeletedDetails = true;
 * js_GetStreamInfo(&si, js, "MY_STREAM", &o, &jerr);
 * \endcode
 *
 * @see jsStreamInfo_Destroy
 *
 * @param si the location where to store the pointer to the new #jsStreamInfo object in
 * response to the creation request.
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream which information is retrieved.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_GetStreamInfo(jsStreamInfo **si, jsCtx *js, const char *stream, jsOptions *opts, jsErrCode *errCode);

/** \brief Destroys the stream information object.
 *
 * Releases memory allocated for this stream information object.
 *
 * @param si the pointer to the #jsStreamInfo object.
 */
NATS_EXTERN void
jsStreamInfo_Destroy(jsStreamInfo *si);

/** \brief Initializes a consumer configuration structure.
 *
 * Use this before adding a consumer.
 *
 * @see #jsConsumerConfig
 *
 * @param cc the pointer to the #jsConsumerConfig to initialize.
 */
NATS_EXTERN natsStatus
jsConsumerConfig_Init(jsConsumerConfig *cc);

/** \brief Adds a JetStream consumer.
 *
 * Adds a consumer based on the provided configuration (that cannot be `NULL`).
 *
 * \note If you do not need a #jsConsumerInfo to be returned, you can pass `NULL`,
 * otherwise, on success you are responsible for freeing this object.
 *
 * @see jsConsumerConfig_Init
 * @see jsConsumerInfo_Destroy
 *
 * @param ci the location where to store the pointer to the new #jsConsumerInfo object in
 * response to the creation request, or `NULL` if the consumer information is not needed.
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream.
 * @param cfg the pointer to the #jsConsumerConfig.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_AddConsumer(jsConsumerInfo **ci, jsCtx *js,
                   const char *stream, jsConsumerConfig *cfg,
                   jsOptions *opts, jsErrCode *errCode);

/** \brief Retrieves information about a consumer.
 *
 * \note The returned object should be destroyed using #jsConsumerInfo_Destroy in order
 * to free allocated memory.
 *
 * @param ci the location where to store the pointer to the new #jsConsumerInfo object.
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream.
 * @param consumer the name of the consumer.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_GetConsumerInfo(jsConsumerInfo **ci, jsCtx *js,
                       const char *stream, const char *consumer,
                       jsOptions *opts, jsErrCode *errCode);

/** \brief Deletes a consumer.
 *
 * Deletes the consumer named <c>consumer</c> from stream named <c>stream</c>.
 *
 * @param js the pointer to the #jsCtx context.
 * @param stream the name of the stream.
 * @param consumer the name of the consumer.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_DeleteConsumer(jsCtx *js, const char *stream, const char *consumer,
                      jsOptions *opts, jsErrCode *errCode);

/** \brief Destroys the consumer information object.
 *
 * Releases memory allocated for this consumer information object.
 *
 * @param ci the pointer to the #jsConsumerInfo object.
 */
NATS_EXTERN void
jsConsumerInfo_Destroy(jsConsumerInfo *ci);

/** \brief Retrieves information about the JetStream usage from an account.
 *
 * Retrieves information about the JetStream usage from an account.
 *
 * \note The returned object should be destroyed using #jsAccountInfo_Destroy in order
 * to free allocated memory.
 *
 * @param js the pointer to the #jsCtx context.
 * @param opts the pointer to the #jsOptions object, possibly `NULL`.
 * @param ai the location where to store the pointer to the new #jsAccountInfo object in
 * response to the account information request.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_GetAccountInfo(jsAccountInfo **ai, jsCtx *js, jsOptions *opts, jsErrCode *errCode);

/** \brief Destroys the account information object.
 *
 * Releases memory allocated for this account information object.
 *
 * @param ai the pointer to the #jsAccountInfo object.
 */
NATS_EXTERN void
jsAccountInfo_Destroy(jsAccountInfo *ai);

/** @} */ // end of jsAssetsGroup

/** \defgroup jsPubGroup Publishing
 *
 *  Publishing functions
 *  @{
 */

/** \brief Initializes a publish options structure.
 *
 * Use this before setting specific publish options and passing this
 * configuration to the JetStream publish APIs.
 *
 * @param opts the pointer to the #jsPubOptions to initialize.
 */
NATS_EXTERN natsStatus
jsPubOptions_Init(jsPubOptions *opts);

/** \brief Publishes data on a subject to JetStream.
 *
 * Publishes the data to the given subject to JetStream.
 *
 * See #js_PublishMsg for details.
 *
 * @param pubAck the location where to store the pub acknowledgment, or `NULL` if not needed.
 * @param js the pointer to the #jsCtx object.
 * @param subj the subject the data is sent to.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 * @param opts the publish options, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_Publish(jsPubAck **pubAck, jsCtx *js, const char *subj, const void *data, int dataLen,
           jsPubOptions *opts, jsErrCode *errCode);

/** \brief Publishes a message to JetStream.
 *
 * Publishes the given message to JetStream.
 *
 * \note If you are not interested in inspecting the publish acknowledgment, you can
 * pass `NULL`, but keep in mind that the publish acknowledgment is still sent by
 * the server.
 * \note The returned #jsPubAck object needs to be destroyed with #jsPubAck_Destroy
 * when no longer needed.
 *
 * @see jsPubAck_Destroy
 *
 * @param pubAck the location where to store the pub acknowledgment, or `NULL` if not needed.
 * @param js the pointer to the #jsCtx object.
 * @param msg the pointer to the #natsMsg object to send.
 * @param opts the publish options, possibly `NULL`.
 * @param errCode the location where to store the JetStream specific error code, or `NULL`
 * if not needed.
 */
NATS_EXTERN natsStatus
js_PublishMsg(jsPubAck **pubAck, jsCtx *js, natsMsg *msg,
              jsPubOptions *opts, jsErrCode *errCode);

/** \brief Destroys the publish acknowledgment object.
 *
 * Releases memory allocated for this publish acknowledgment object.
 *
 * @param pubAck the #jsPubAck object to destroy.
 */
NATS_EXTERN void
jsPubAck_Destroy(jsPubAck *pubAck);

/** \brief Publishes data to JetStream but does not wait for a #jsPubAck.
 *
 * See #js_PublishMsgAsync for details.
 *
 * @param js the pointer to the #jsCtx object.
 * @param subj the subject the data is sent to.
 * @param data the data to be sent, can be `NULL`.
 * @param dataLen the length of the data to be sent.
 * @param opts the publish options, possibly `NULL`.
 */
NATS_EXTERN natsStatus
js_PublishAsync(jsCtx *js, const char *subj, const void *data, int dataLen,
                jsPubOptions *opts);

/** \brief Publishes a message to JetStream but does not wait for a #jsPubAck.
 *
 * Publishes a message asynchronously to JetStream. User can call #js_PublishAsyncComplete
 * to be notified when all publish acknowledgments for the pending publish calls
 * have been received.
 *
 * \warning The message should not be destroyed by the caller. The library takes
 * ownership and will either destroy it or pass it to the optional error callback.
 *
 * @see js_PublishAsyncComplete
 * @see jsPubAckErrHandler
 *
 * @param js the pointer to the #jsCtx object.
 * @param msg the memory location where the pointer to the #natsMsg object to send is located.
 * If the library takes ownership of the message, this location will be cleared so a following
 * call to #natsMsg_Destroy would have no effect.
 * @param opts the publish options, possibly `NULL`.
 */
NATS_EXTERN natsStatus
js_PublishMsgAsync(jsCtx *js, natsMsg **msg, jsPubOptions *opts);

/** \brief Wait for all outstanding messages to be acknowledged.
 *
 * This call will block until the library has received acknowledgment for
 * all outstanding published messages.
 *
 * To limit the wait, user can pass a #jsPubOptions with a `MaxWait` set to the
 * maximum number of milliseconds that the call should block.
 *
 * @param js the pointer to the #jsCtx object.
 * @param opts the publish options, possibly `NULL`.
 */
NATS_EXTERN natsStatus
js_PublishAsyncComplete(jsCtx *js, jsPubOptions *opts);

/** @} */ // end of jsPubGroup

/** @} */ // end of jsGroup

/** @} */ // end of funcGroup

/**  \defgroup wildcardsGroup Wildcards
 *  @{
 *  Use of wildcards. There are two type of wildcards: `*` for partial,
 *  and `>` for full.
 *
 *  A subscription on subject `foo.*` would receive messages sent to:
 *  - `foo.bar`
 *  - `foo.baz`
 *
 *  but not on:
 *
 *  - `foo.bar.baz` (too many elements)
 *  - `bar.foo`. (does not start with `foo`).
 *
 *  A subscription on subject `foo.>` would receive messages sent to:
 *  - `foo.bar`
 *  - `foo.baz`
 *  - `foo.bar.baz`
 *
 *  but not on:
 *  - `foo` (only one element, needs at least two)
 *  - `bar.baz` (does not start with `foo`).
 ** @} */

/**  \defgroup envVariablesGroup Environment Variables
 *  @{
 *  You will find here the environment variables that change the default behavior
 *  of the NATS C Client library.
 * <br><br>
 * Name | Effect
 * -----|:-----:
 * <b>`NATS_DEFAULT_TO_LIB_MSG_DELIVERY`</b> | On #nats_Open, the library looks for this environment variable. If set (to any value), the library will default to using a global thread pool to perform message delivery. See #natsOptions_UseGlobalMessageDelivery and #nats_SetMessageDeliveryPoolSize.
 *
 ** @} */


#ifdef __cplusplus
}
#endif

#endif /* NATS_H_ */
