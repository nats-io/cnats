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

#ifndef STATUS_H_
#define STATUS_H_

#ifdef __cplusplus
extern "C" {
#endif


/// The connection state
typedef enum
{
#if defined(NATS_CONN_STATUS_NO_PREFIX)
    // This is deprecated and applications referencing connection
    // status should be updated to use the values prefixed with NATS_CONN_STATUS_.

    DISCONNECTED = 0, ///< The connection has been disconnected
    CONNECTING,       ///< The connection is in the process or connecting
    CONNECTED,        ///< The connection is connected
    CLOSED,           ///< The connection is closed
    RECONNECTING,     ///< The connection is in the process or reconnecting
    DRAINING_SUBS,    ///< The connection is draining subscriptions
    DRAINING_PUBS,    ///< The connection is draining publishers
#else
    NATS_CONN_STATUS_DISCONNECTED = 0, ///< The connection has been disconnected
    NATS_CONN_STATUS_CONNECTING,       ///< The connection is in the process or connecting
    NATS_CONN_STATUS_CONNECTED,        ///< The connection is connected
    NATS_CONN_STATUS_CLOSED,           ///< The connection is closed
    NATS_CONN_STATUS_RECONNECTING,     ///< The connection is in the process or reconnecting
    NATS_CONN_STATUS_DRAINING_SUBS,    ///< The connection is draining subscriptions
    NATS_CONN_STATUS_DRAINING_PUBS,    ///< The connection is draining publishers
#endif

} natsConnStatus;

/// Status returned by most of the APIs
typedef enum
{
    NATS_OK         = 0,                ///< Success

    NATS_ERR,                           ///< Generic error
    NATS_PROTOCOL_ERROR,                ///< Error when parsing a protocol message,
                                        ///  or not getting the expected message.
    NATS_IO_ERROR,                      ///< IO Error (network communication).
    NATS_LINE_TOO_LONG,                 ///< The protocol message read from the socket
                                        ///  does not fit in the read buffer.

    NATS_CONNECTION_CLOSED,             ///< Operation on this connection failed because
                                        ///  the connection is closed.
    NATS_NO_SERVER,                     ///< Unable to connect, the server could not be
                                        ///  reached or is not running.
    NATS_STALE_CONNECTION,              ///< The server closed our connection because it
                                        ///  did not receive PINGs at the expected interval.
    NATS_SECURE_CONNECTION_WANTED,      ///< The client is configured to use TLS, but the
                                        ///  server is not.
    NATS_SECURE_CONNECTION_REQUIRED,    ///< The server expects a TLS connection.
    NATS_CONNECTION_DISCONNECTED,       ///< The connection was disconnected. Depending on
                                        ///  the configuration, the connection may reconnect.

    NATS_CONNECTION_AUTH_FAILED,        ///< The connection failed due to authentication error.
    NATS_NOT_PERMITTED,                 ///< The action is not permitted.
    NATS_NOT_FOUND,                     ///< An action could not complete because something
                                        ///  was not found. So far, this is an internal error.

    NATS_ADDRESS_MISSING,               ///< Incorrect URL. For instance no host specified in
                                        ///  the URL.

    NATS_INVALID_SUBJECT,               ///< Invalid subject, for instance NULL or empty string.
    NATS_INVALID_ARG,                   ///< An invalid argument is passed to a function. For
                                        ///  instance passing NULL to an API that does not
                                        ///  accept this value.
    NATS_INVALID_SUBSCRIPTION,          ///< The call to a subscription function fails because
                                        ///  the subscription has previously been closed.
    NATS_INVALID_TIMEOUT,               ///< Timeout must be positive numbers.

    NATS_ILLEGAL_STATE,                 ///< An unexpected state, for instance calling
                                        ///  #natsSubscription_NextMsg() on an asynchronous
                                        ///  subscriber.

    NATS_SLOW_CONSUMER,                 ///< The maximum number of messages waiting to be
                                        ///  delivered has been reached. Messages are dropped.

    NATS_MAX_PAYLOAD,                   ///< Attempt to send a payload larger than the maximum
                                        ///  allowed by the NATS Server.
    NATS_MAX_DELIVERED_MSGS,            ///< Attempt to receive more messages than allowed, for
                                        ///  instance because of #natsSubscription_AutoUnsubscribe().

    NATS_INSUFFICIENT_BUFFER,           ///< A buffer is not large enough to accommodate the data.

    NATS_NO_MEMORY,                     ///< An operation could not complete because of insufficient
                                        ///  memory.

    NATS_SYS_ERROR,                     ///< Some system function returned an error.

    NATS_TIMEOUT,                       ///< An operation timed-out. For instance
                                        ///  #natsSubscription_NextMsg().

    NATS_FAILED_TO_INITIALIZE,          ///< The library failed to initialize.
    NATS_NOT_INITIALIZED,               ///< The library is not yet initialized.

    NATS_SSL_ERROR,                     ///< An SSL error occurred when trying to establish a
                                        ///  connection.

    NATS_NO_SERVER_SUPPORT,             ///< The server does not support this action.

    NATS_NOT_YET_CONNECTED,             ///< A connection could not be immediately established and
                                        ///  #natsOptions_SetRetryOnFailedConnect() specified
                                        ///  a connected callback. The connect is retried asynchronously.

    NATS_DRAINING,                      ///< A connection and/or subscription entered the draining mode.
                                        ///  Some operations will fail when in that mode.

    NATS_INVALID_QUEUE_NAME,            ///< An invalid queue name was passed when creating a queue subscription.

    NATS_NO_RESPONDERS,                 ///< No responders were running when the server received the request.

} natsStatus;

typedef enum {

    JSAccountResourcesExceededErr = 10002,              ///< Resource limits exceeded for account
    JSBadRequestErr = 10003,                            ///< Bad request
    JSClusterIncompleteErr = 10004,                     ///< Incomplete results
    JSClusterNoPeersErr = 10005,                        ///< No suitable peers for placement
    JSClusterNotActiveErr = 10006,                      ///< JetStream not in clustered mode
    JSClusterNotAssignedErr = 10007,                    ///< JetStream cluster not assigned to this server
    JSClusterNotAvailErr = 10008,                       ///< JetStream system temporarily unavailable
    JSClusterNotLeaderErr = 10009,                      ///< JetStream cluster can not handle request
    JSClusterRequiredErr = 10010,                       ///< JetStream clustering support required
    JSClusterTagsErr = 10011,                           ///< Tags placement not supported for operation
    JSConsumerCreateErr = 10012,                        ///< General consumer creation failure string
    JSConsumerNameExistErr = 10013,                     ///< Consumer name already in use
    JSConsumerNotFoundErr = 10014,                      ///< Consumer not found
    JSSnapshotDeliverSubjectInvalidErr = 10015,         ///< Deliver subject not valid
    JSConsumerDurableNameNotInSubjectErr = 10016,       ///< Consumer expected to be durable but no durable name set in subject
    JSConsumerDurableNameNotMatchSubjectErr = 10017,    ///< Consumer name in subject does not match durable name in request
    JSConsumerDurableNameNotSetErr = 10018,             ///< Consumer expected to be durable but a durable name was not set
    JSConsumerEphemeralWithDurableInSubjectErr = 10019, ///< Consumer expected to be ephemeral but detected a durable name set in subject
    JSConsumerEphemeralWithDurableNameErr = 10020,      ///< Consumer expected to be ephemeral but a durable name was set in request
    JSStreamExternalApiOverlapErr = 10021,              ///< Stream external api prefix must not overlap
    JSStreamExternalDelPrefixOverlapsErr = 10022,       ///< Stream external delivery prefix overlaps with stream subject
    JSInsufficientResourcesErr = 10023,                 ///< Insufficient resources
    JSStreamInvalidExternalDeliverySubjErr = 10024,     ///< Stream external delivery prefix must not contain wildcards
    JSInvalidJSONErr = 10025,                           ///< Invalid JSON
    JSMaximumConsumersLimitErr = 10026,                 ///< Maximum consumers exceeds account limit
    JSMaximumStreamsLimitErr = 10027,                   ///< Maximum number of streams reached
    JSMemoryResourcesExceededErr = 10028,               ///< Insufficient memory resources available
    JSMirrorConsumerSetupFailedErr = 10029,             ///< Generic mirror consumer setup failure
    JSMirrorMaxMessageSizeTooBigErr = 10030,            ///< Stream mirror must have max message size >= source
    JSMirrorWithSourcesErr = 10031,                     ///< Stream mirrors can not also contain other sources
    JSMirrorWithStartSeqAndTimeErr = 10032,             ///< Stream mirrors can not have both start seq and start time configured
    JSMirrorWithSubjectFiltersErr = 10033,              ///< Stream mirrors can not contain filtered subjects
    JSMirrorWithSubjectsErr = 10034,                    ///< Stream mirrors can not also contain subjects
    JSNoAccountErr = 10035,                             ///< Account not found
    JSClusterUnSupportFeatureErr = 10036,               ///< Not currently supported in clustered mode
    JSNoMessageFoundErr = 10037,                        ///< No message found
    JSNotEmptyRequestErr = 10038,                       ///< Expected an empty request payload
    JSNotEnabledForAccountErr = 10039,                  ///< JetStream not enabled for account
    JSClusterPeerNotMemberErr = 10040,                  ///< Peer not a member
    JSRaftGeneralErr = 10041,                           ///< General RAFT error
    JSRestoreSubscribeFailedErr = 10042,                ///< JetStream unable to subscribe to restore snapshot
    JSSequenceNotFoundErr = 10043,                      ///< Sequence not found
    JSClusterServerNotMemberErr = 10044,                ///< Server is not a member of the cluster
    JSSourceConsumerSetupFailedErr = 10045,             ///< General source consumer setup failure
    JSSourceMaxMessageSizeTooBigErr = 10046,            ///< Stream source must have max message size >= target
    JSStorageResourcesExceededErr = 10047,              ///< Insufficient storage resources available
    JSStreamAssignmentErr = 10048,                      ///< Generic stream assignment error
    JSStreamCreateErr = 10049,                          ///< Generic stream creation error
    JSStreamDeleteErr = 10050,                          ///< General stream deletion error
    JSStreamGeneralError = 10051,                       ///< General stream failure
    JSStreamInvalidConfig = 10052,                      ///< Stream configuration validation error
    JSStreamLimitsErr = 10053,                          ///< General stream limits exceeded error
    JSStreamMessageExceedsMaximumErr = 10054,           ///< Message size exceeds maximum allowed
    JSStreamMirrorNotUpdatableErr = 10055,              ///< Mirror configuration can not be updated
    JSStreamMismatchErr = 10056,                        ///< Stream name in subject does not match request
    JSStreamMsgDeleteFailed = 10057,                    ///< Generic message deletion failure error
    JSStreamNameExistErr = 10058,                       ///< Stream name already in use
    JSStreamNotFoundErr = 10059,                        ///< Stream not found
    JSStreamNotMatchErr = 10060,                        ///< Expected stream does not match
    JSStreamReplicasNotUpdatableErr = 10061,            ///< Replicas configuration can not be updated
    JSStreamRestoreErr = 10062,                         ///< Restore failed
    JSStreamSequenceNotMatchErr = 10063,                ///< Expected stream sequence does not match
    JSStreamSnapshotErr = 10064,                        ///< Snapshot failed
    JSStreamSubjectOverlapErr = 10065,                  ///< Subjects overlap with an existing stream
    JSStreamTemplateCreateErr = 10066,                  ///< Generic template creation failed
    JSStreamTemplateDeleteErr = 10067,                  ///< Generic stream template deletion failed error
    JSStreamTemplateNotFoundErr = 10068,                ///< Template not found
    JSStreamUpdateErr = 10069,                          ///< Generic stream update error
    JSStreamWrongLastMsgIDErr = 10070,                  ///< Wrong last msg ID
    JSStreamWrongLastSequenceErr = 10071,               ///< Wrong last sequence
    JSTempStorageFailedErr = 10072,                     ///< JetStream unable to open temp storage for restore
    JSTemplateNameNotMatchSubjectErr = 10073,           ///< Template name in subject does not match request
    JSStreamReplicasNotSupportedErr = 10074,            ///< Replicas > 1 not supported in non-clustered mode
    JSPeerRemapErr = 10075,                             ///< Peer remap failed
    JSNotEnabledErr = 10076,                            ///< JetStream not enabled
    JSStreamStoreFailedErr = 10077,                     ///< Generic error when storing a message failed
    JSConsumerConfigRequiredErr = 10078,                ///< Consumer config required
    JSConsumerDeliverToWildcardsErr = 10079,            ///< Consumer deliver subject has wildcards
    JSConsumerPushMaxWaitingErr = 10080,                ///< Consumer in push mode can not set max waiting
    JSConsumerDeliverCycleErr = 10081,                  ///< Consumer deliver subject forms a cycle
    JSConsumerMaxPendingAckPolicyRequiredErr = 10082,   ///< Consumer requires ack policy for max ack pending
    JSConsumerSmallHeartbeatErr = 10083,                ///< Consumer idle heartbeat needs to be >= 100ms
    JSConsumerPullRequiresAckErr = 10084,               ///< Consumer in pull mode requires explicit ack policy
    JSConsumerPullNotDurableErr = 10085,                ///< Consumer in pull mode requires a durable name
    JSConsumerPullWithRateLimitErr = 10086,             ///< Consumer in pull mode can not have rate limit set
    JSConsumerMaxWaitingNegativeErr = 10087,            ///< Consumer max waiting needs to be positive
    JSConsumerHBRequiresPushErr = 10088,                ///< Consumer idle heartbeat requires a push based consumer
    JSConsumerFCRequiresPushErr = 10089,                ///< Consumer flow control requires a push based consumer
    JSConsumerDirectRequiresPushErr = 10090,            ///< Consumer direct requires a push based consumer
    JSConsumerDirectRequiresEphemeralErr = 10091,       ///< Consumer direct requires an ephemeral consumer
    JSConsumerOnMappedErr = 10092,                      ///< Consumer direct on a mapped consumer
    JSConsumerFilterNotSubsetErr = 10093,               ///< Consumer filter subject is not a valid subset of the interest subjects
    JSConsumerInvalidPolicyErr = 10094,                 ///< Generic delivery policy error
    JSConsumerInvalidSamplingErr = 10095,               ///< Failed to parse consumer sampling configuration
    JSStreamInvalidErr = 10096,                         ///< Stream not valid
    JSStreamMaximumConsumersReachedErr = 10097,         ///< Maximum consumers limit reached
    JSConsumerWQRequiresExplicitAckErr = 10098,         ///< Workqueue stream requires explicit ack
    JSConsumerWQMultipleUnfilteredErr = 10099,          ///< Multiple non-filtered consumers not allowed on workqueue stream
    JSConsumerWQConsumerNotUniqueErr = 10100,           ///< Filtered consumer not unique on workqueue stream
    JSConsumerWQConsumerNotDeliverAllErr = 10101,       ///< Consumer must be deliver all on workqueue stream
    JSConsumerNameTooLongErr = 10102,                   ///< Consumer name is too long
    JSConsumerBadDurableNameErr = 10103,                ///< Durable name can not contain '.', '*', '>'
    JSConsumerStoreFailedErr = 10104,                   ///< Error creating store for consumer
    JSConsumerExistingActiveErr = 10105,                ///< Consumer already exists and is still active
    JSConsumerReplacementWithDifferentNameErr = 10106,  ///< Consumer replacement durable config not the same

} jsErrCode;

#ifdef __cplusplus
}
#endif

#endif /* STATUS_H_ */
