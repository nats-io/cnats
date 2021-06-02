# NATS & NATS Streaming - C Client
A C client for the [NATS messaging system](https://nats.io).

Go [here](http://nats-io.github.io/nats.c) for the online documentation,
and check the [frequently asked questions](https://github.com/nats-io/nats.c#faq).

This NATS Client implementation is heavily based on the [NATS GO Client](https://github.com/nats-io/nats.go). There is support for Mac OS/X, Linux and Windows (although we don't have specific platform support matrix).

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.com/nats-io/nats.c.svg?branch=master)](https://travis-ci.com/github/nats-io/nats.c)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nats.c/badge.svg?branch=master)](https://coveralls.io/github/nats-io/nats.c?branch=master)
[![Release](https://img.shields.io/badge/release-v2.5.0-blue.svg?style=flat)](https://github.com/nats-io/nats.c/releases/tag/v2.5.0)
[![Documentation](https://img.shields.io/badge/doc-Doxygen-brightgreen.svg?style=flat)](http://nats-io.github.io/nats.c)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/nats-io/nats.c.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/nats-io/nats.c/alerts/)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/nats-io/nats.c.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/nats-io/nats.c/context:cpp)

# Table of Contents

- [Building](#building)
	* [TLS Support](#tls-support)
    * [Building with Streaming](#building-with-streaming)
    * [Building with Libsodium](#building-with-libsodium)
    * [Testing](#testing)
- [Documentation](#documentation)
- [NATS Client](#nats-client)
    * [Important Changes](#important-changes)
    * [JetStream](#jetstream)
        * [JetStream Basic Usage](#jetstream-basic-usage)
        * [JetStream Basic Management](#jetstream-basic-usage)
	* [Getting Started](#getting-started)
	* [Basic Usage](#basic-usage)
    * [Headers](#headers)
	* [Wildcard Subscriptions](#wildcard-subscriptions)
	* [Queue Groups](#queue-groups)
	* [TLS](#tls)
	* [New Authentication (Nkeys and User Credentials)](#new-authentication-nkeys-and-user-credentials)
	* [Advanced Usage](#advanced-usage)
	* [Clustered Usage](#clustered-usage)
	* [Using an Event Loop Library](#using-an-event-loop-library)
	* [FAQ](#faq)
- [NATS Streaming Client](#nats-streaming-client)
	* [Streaming Basic Usage](#streaming-basic-usage)
		* [Streaming Subscriptions](#streaming-subscriptions)
		* [Streaming Durable Subscriptions](#streaming-durable-subscriptions)
		* [Streaming Queue Groups](#streaming-queue-groups)
			* [Creating a Queue Group](#creating-a-queue-group)
			* [Start Position](#start-position)
			* [Leaving the Group](#leaving-the-group)
			* [Closing a Queue Group](#closing-a-queue-group)
		* [Streaming Durable Queue Groups](#streaming-durable-queue-groups)
			* [Creating a Durable Queue Group](#creating-a-durable-queue-group)
			* [Start Position of the Durable Queue Group](#start-position-of-the-durable-queue-group)
			* [Leaving the Durable Queue Group](#leaving-the-durable-queue-group)
			* [Closing the Durable Queue Group](#closing-the-durable-queue-group)
		* [Streaming Wildcard Subscriptions](#streaming-wildcard-subscriptions)
	* [Streaming Advanced Usage](#streaming-advanced-usage)
		* [Connection Status](#connection-status)
		* [Asynchronous Publishing](#asynchronous-publishing)
		* [Message Acknowledgments and Redelivery](#message-acknowledgments-and-redelivery)
		* [Rate limiting/matching](#rate-limitingmatching)
			* [Publisher Rate Limiting](#publisher-rate-limiting)
			* [Subscriber Rate Limiting](#subscriber-rate-limiting)
- [License](#license)

## Building

First, download the source code:
```
git clone git@github.com:nats-io/nats.c.git .
```

To build the library, use [CMake](https://cmake.org/download/). Note that by default the NATS Streaming API will be built and included in the NATS library.
See below if you do not want to build the Streaming related APIs.

Make sure that CMake is added to your path. If building on Windows, open a command shell from the Visual Studio Tools menu, and select the appropriate command shell (x64 or x86 for 64 or 32 bit builds respectively). You will also probably need to run this with administrator privileges.

Create a `build` directory (any name would work) from the root source tree, and `cd` into it. Then issue this command for the first time:

```
cmake ..
```

In some architectures, you may experience a compilation error for `mutex.c.o` because there is no support
for the assembler instruction that we use to yield when spinning trying to acquire a lock.

You may get this sort of build error:
```
/tmp/cc1Yp7sD.s: Assembler messages:
/tmp/cc1Yp7sD.s:302: Error: selected processor does not support ARM mode `yield'
src/CMakeFiles/nats_static.dir/build.make:542: recipe for target 'src/CMakeFiles/nats_static.dir/unix/mutex.c.o' failed
```
If that's the case, you can solve this by enabling the `NATS_BUILD_NO_SPIN` flag (or use `-DNATS_NO_SPIN` if you compile without CMake):
```
cmake .. -DNATS_BUILD_NO_SPIN=ON
```

If you had previously built the library, you may need to do a `make clean`, or simply delete and re-create the build directory before executing the cmake command.

To build on Windows, you would need to select the build generator. For instance, to select `nmake`, you would run:

```
cmake .. -G "NMake Makefiles"
```

Running `cmake -h` would give you the list of possible options and all the generator names.

Alternatively, you can run the GUI version. From that same *build* command shell, start the GUI:

```
c:\program files (x86)\CMake\bin\cmake-gui.exe
```

If you started with an empty build directory, you would need to select the source and build directory, then click `Configure`. Here, you will be able to select from the drop-down box the name of the build generator. When done, click `Generate`. Then you can go back to your command shell, or Visual Studio and build.

To modify some of the build options, you need to edit the cache and rebuild.

```
make edit_cache
```

Note that if you build on Windows and have selected "NMake Makefiles", replace all following references to `make` with `nmake`.

Editing the cache allows you to select the build type (Debug, Release, etc), the architecture (64 or 32bit), and so on.

The default target will build everything, that is, the static and shared NATS libraries and also the examples and the test program. Each are located in their respective directories under your build directory: `src`, `examples` and `test`.

```
make install
```
Will copy both the static and shared libraries in the folder `install/lib` and the public headers in `install/include`.

## TLS Support

By default, the library is built with TLS support. You can disable this from the cmake gui `make edit_cache` and switch the `NATS_BUILD_WITH_TLS` option to `OFF`, or pass the option directly to the `cmake` command:

```
cmake .. -DNATS_BUILD_WITH_TLS=OFF
```

Starting `2.0.0`, when building with TLS/SSL support, the server certificate's expected hostname is always verified. It means that the hostname provided in the URL(s) or through the option `natsOptions_SetExpectedHostname()` will be used to check the hostname present in the certificate. Prior to `2.0.0`, the hostname would be verified *only* if the option `natsOptions_SetExpectedHostname()` was invoked.

Although we recommend leaving the new default behavior, you can restore the previous behavior by building the library with this option off:

```
cmake .. -DNATS_BUILD_TLS_FORCE_HOST_VERIFY=OFF
```

The NATS C client is built using APIs from the [OpenSSL](https://github.com/openssl/openssl) library. By default we use `1.0.2` APIs. You can compile the NATS C client with OpenSSL API version `1.1+`. To do that, you need to enable the `NATS_BUILD_TLS_USE_OPENSSL_1_1_API` option:

```
cmake .. -DNATS_BUILD_TLS_USE_OPENSSL_1_1_API=ON
```

Since the NATS C client dynamically links to the OpenSSL library, you need to make sure that you are then running your application against an OpenSSL 1.1+ library.

Note that the option `NATS_BUILD_WITH_TLS_CLIENT_METHOD` is deprecated. Its purpose was to make the NATS C client use a method that was introduced in OpenSSL `1.1+`. The new option `NATS_BUILD_TLS_USE_OPENSSL_1_1_API` is more generic and replaces `NATS_BUILD_WITH_TLS_CLIENT_METHOD`. If you are using scripts to automate your build process that makes use of `NATS_BUILD_WITH_TLS_CLIENT_METHOD`, they will still work and using this deprecated option will have the same effect than setting `NATS_BUILD_TLS_USE_OPENSSL_1_1_API` to `ON`.

## Building with Streaming

When building the library with Streaming support, the NATS library uses the [libprotobuf-c](https://github.com/protobuf-c/protobuf-c) library.
When cmake runs for the first time (or after removing `CMakeCache.txt` and calling `cmake ..` again), it is looking for the libprotobuf-c library. If it does not find it, a message is printed and the build process fails.
CMake searches for the library in directories where libraries are usually found. However, if you want to specify a specific directory where the library is located, you need to do this:
```
cmake .. -DNATS_PROTOBUF_DIR=<my libprotobuf-c directory>
```
The static library will be used by default. If you want to change that, or if the library has not the expected name, you need to do this:
```
# Use the library named mylibproto.so located at /my/location
cmake .. -DNATS_PROTOBUF_LIBRARY=/my/location/mylibproto.so
```
The two could be combined if the include header is located in a different directory
```
# Use the library named mylibproto.so located at /my/location and the directory protobuf-c/ containing protobuf-c.h located at /my/other/location
cmake .. -DNATS_PROTOBUF_LIBRARY=/my/location/mylibproto.so -DNATS_PROTOBUF_DIR=/my/other/location
```

If you don't want to build the NATS Streaming APIs to be included in the NATS library:
```
cmake .. -DNATS_BUILD_STREAMING=OFF
```

## Building with Libsodium

When using the new NATS 2.0 security features, the library needs to sign some "nonce" sent by the server during a connect or reconnect.
We use [Ed25519](https://ed25519.cr.yp.to/) public-key signature. The library comes with some code to perform the signature.
In most case, it will be fine, but if performance is an issue (especially if you plan to use the `natsConnection_Sign()` function a lot), you will have the option to build with the [Libsodium](https://github.com/jedisct1/libsodium) library.

Follow instructions on how to install the libsodium library [here](https://download.libsodium.org/doc/).

On macOS, you could use `brew`:
```
brew install libsodium
```
On Linux, you could use `apt-get`
```
apt-get install libsodium-dev
```
Once installed, you can rebuild the NATS C client by first enabling the use of the libsodium library:
```
cmake .. -DNATS_BUILD_USE_SODIUM=ON
```
If you have the libsodium library installed in a non standard location that CMake cannot find, you can specify the location of this directory:
```
cmake .. -DNATS_BUILD_USE_SODIUM=ON -DNATS_SODIUM_DIR=/my/path/to/libsodium
```

## Testing

On platforms where `valgrind` is available, you can run the tests with memory checks.
Here is an example:

```
make test ARGS="-T memcheck"
```

Or, you can invoke directly the `ctest` program:

```
ctest -T memcheck -V -I 1,4
```
The above command would run the tests with `valgrind` (`-T memcheck`), with verbose output (`-V`), and run the tests from 1 to 4 (`-I 1,4`).

If you add a test to `test/test.c`, you need to add it into the `allTests` array. Each entry contains a name, and the test function. You can add it anywhere into this array.
Build you changes:

```
make
[ 44%] Built target nats
[ 88%] Built target nats_static
[ 90%] Built target nats-publisher
[ 92%] Built target nats-queuegroup
[ 94%] Built target nats-replier
[ 96%] Built target nats-requestor
[ 98%] Built target nats-subscriber
Scanning dependencies of target testsuite
[100%] Building C object test/CMakeFiles/testsuite.dir/test.c.o
Linking C executable testsuite
[100%] Built target testsuite
```

Now regenerate the list by invoking the test suite without any argument:

```
./test/testsuite
Number of tests: 77
```

This list the number of tests added to the file `list.txt`. Move this file to the source's test directory.

```
mv list.txt ../test/
```

Then, refresh the build:

```
cmake ..
-- Configuring done
-- Generating done
-- Build files have been written to: /home/ivan/nats.c/build
```

You can use the following environment variables to influence the testsuite behavior.

When running with memory check, timing changes and overall performance is slower. The following variable allows the testsuite to adjust some of values used during the test:

```
export NATS_TEST_VALGRIND=yes
```

On Windows, it would be `set` instead of `export`.

When running the tests in verbose mode, the following environment variable allows you to see the server output from within the test itself. Without this option, the server output is silenced:

```
export NATS_TEST_KEEP_SERVER_OUTPUT=yes
```

If you want to change the default server executable name (`nats-server.exe`) or specify a specific location, use this environment variable:

```
set NATS_TEST_SERVER_EXE=c:\test\nats-server.exe
```


# Documentation

The public API has been documented using [Doxygen](http://www.stack.nl/~dimitri/doxygen/).

To generate the documentation, go to the `doc` directory and type the following command:

```
doxygen DoxyFile.NATS.Client
```

If you toggle the build of the Streaming APIs, and the documentation is no longer matching
what is being built, you can update the documentation by switching the `NATS_UPDATE_DOC` build flag and rebuild the documentation.

From the build directory:
```
cmake .. -DNATS_UPDATE_DOC=ON
make
cd <nats.c root dir>/doc
doxygen DoxyFile.NATS.Client
```

The generated documentation will be located in the `html` directory. To see the documentation, point your browser to the file `index.html` in that directory.

Go [here](http://nats-io.github.io/nats.c) for the online documentation.

The source code is also quite documented.

# NATS Client

## Important Changes

This section lists important changes such as deprecation notices, etc...

### Version `2.0.0`

This version introduces the security concepts used by NATS Server `2.0.0` and therefore aligns with the server version. There have been new APIs introduced, but the most important change is the new default behavior with TLS connections:

* When establishing a secure connection, the server certificate's hostname is now always verified, regardless if the user has invoked `natsOptions_SetExpectedHostname()`. This may break applications that were for instance using an IP to connect to a server that had only the hostname in the certificate. This can be solved by changing your application to use the hostname in the URL or make use of `natsOptions_SetExpectedHostname()`. If this is not possible, you can restore the old behavior by building the library with the new behavior disabled. See #tls-support for more information.

* This repository used to include precompiled libraries of [libprotobuf-c](https://github.com/protobuf-c/protobuf-c) for macOS, Linux and Windows along with the header files (in the `/pbuf` directory).
We have now removed this directory and require that the user installs the libprotobuf-c library separately. See the [building instructions](#building) to specify the library location if CMake cannot find it directly.

### Version `1.8.0`

* The `natsConnStatus` enum values have been prefixed with `NATS_CONN_STATUS_`. If your application is
not using referencing any original value, such as `CONNECTED` or `CLOSED`, etc.. then there is nothing
that you need to do. If you do, you have two options:
	* Replace all references from the orignal values to the new value (adding the prefix)
	* Compile the library with the option to use the original values (no prefix). Edit the CMake cache
and turn on the option `NATS_BUILD_NO_PREFIX_CONNSTS`. This can be done this way from the build directory:
`cmake .. -DNATS_BUILD_NO_PREFIX_CONNSTS=ON`

## Getting Started

The `examples/getstarted` directory has a set of simple examples that are fully functional, yet very simple.
The goal is to demonstrate how easy to use the API is.

A more complex set of examples are in `examples/` directory and can also be used to benchmark the client library.

## Basic Usage

Note that for simplicity, error checking is not performed here.
```c
natsConnection      *nc  = NULL;
natsSubscription    *sub = NULL;
natsMsg             *msg = NULL;

// Connects to the default NATS Server running locally
natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);

// Connects to a server with username and password
natsConnection_ConnectTo(&nc, "nats://ivan:secret@localhost:4222");

// Connects to a server with token authentication
natsConnection_ConnectTo(&nc, "nats://myTopSecretAuthenticationToken@localhost:4222");

// Simple publisher, sending the given string to subject "foo"
natsConnection_PublishString(nc, "foo", "hello world");

// Publish binary data. Content is not interpreted as a string.
char data[] = {1, 2, 0, 4, 5};
natsConnection_Publish(nc, "foo", (const void*) data, 5);

// Simple asynchronous subscriber on subject foo, invoking message
// handler 'onMsg' when messages are received, and not providing a closure.
natsConnection_Subscribe(&sub, nc, "foo", onMsg, NULL);

// Simple synchronous subscriber
natsConnection_SubscribeSync(&sub, nc, "foo");

// Using a synchronous subscriber, gets the first message available, waiting
// up to 1000 milliseconds (1 second)
natsSubscription_NextMsg(&msg, sub, 1000);

// Destroy any message received (asynchronously or synchronously) or created
// by your application. Note that if 'msg' is NULL, the call has no effect.
natsMsg_Destroy(msg);

// Unsubscribing
natsSubscription_Unsubscribe(sub);

// Destroying the subscription (this will release the object, which may
// result in freeing the memory). After this call, the object must no
// longer be used.
natsSubscription_Destroy(sub);

// Publish requests to the given reply subject:
natsConnection_PublishRequestString(nc, "foo", "bar", "help!");

// Sends a request (internally creates an inbox) and Auto-Unsubscribe the
// internal subscriber, which means that the subscriber is unsubscribed
// when receiving the first response from potentially many repliers.
// This call will wait for the reply for up to 1000 milliseconds (1 second).
natsConnection_RequestString(&reply, nc, "foo", "help", 1000);

// Closing a connection (but not releasing the connection object)
natsConnection_Close(nc);

// When done with the object, free the memory. Note that this call
// closes the connection first, in other words, you could have simply
// this call instead of natsConnection_Close() followed by the destroy
// call.
natsConnection_Destroy(nc);

// Message handler
void
onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    // Prints the message, using the message getters:
    printf("Received msg: %s - %.*s\n",
        natsMsg_GetSubject(msg),
        natsMsg_GetDataLength(msg),
        natsMsg_GetData(msg));

    // Don't forget to destroy the message!
    natsMsg_Destroy(msg);
}
```

## JetStream

Support for JetStream starts with the version `v3.0.0` of the library and NATS Server `v2.2.0+`, although getting JetStream
specific error codes requires the server at version `v2.3.0+`.

Look at examples named `js-xxx.c` in the `examples` directory for examples on how to use the API.
The new objects and APIs are full documented in the online [documentation](http://nats-io.github.io/nats.c/group__js_group.html).

### JetStream Basic Usage

```c
// Connect to NATS
natsConnection_Connect(&conn, opts);

// Initialize and set some JetStream options
natsJSOptions_Init(&jsOpts);
jsOpts.PublishAsyncMaxPending = 256;

// Create JetStream Context
natsJS_NewContext(&js, conn, &jsOpts);

// Simple Stream Publisher
natsJS_Publish(&pa, js, "ORDERS.scratch", (const void*) "hello", 5, NULL, &jerr);

// Simple Async Stream Publisher
for (i=0; i < 500; i++)
{
    natsJS_PublishAsync(js, "ORDERS.scratch", (const void*) "hello", 5, NULL);
}

// Wait for up to 5 seconds to receive all publish acknowledgments.
natsJSPubOptions_Init(&jsPubOpts);
jsPubOpts.MaxWait = 5000;
natsJS_PublishAsyncComplete(js, &jsPubOpts);
```

### JetStream Basic Management

```c
natsJSStreamConfig  cfg;
const char          *subjects = {"ORDERS.*"};

// Connect to NATS
natsConnection_Connect(&conn, opts);

// Create JetStream Context
natsJS_NewContext(&js, conn, NULL);

// Initialize the configuration structure.
natsJSStreamConfig_Init(&cfg);
// Provide a name
cfg.Name = "ORDERS";
// Array of subjects and its size
cfg.Subjects = subjects;
cfg.SubjectsLen = 1;

// Create a Stream
natsJS_AddStream(NULL, js, &cfg, NULL, &jerr);

// Update a Stream
cfg.MaxBytes = 8;
natsJS_UpdateStream(NULL, js, &cfg, NULL, &jerr);

// Delete a Stream
natsJS_DeleteStream(js, "ORDERS", NULL, &jerr);
```

## Headers

Headers are available when connecting to servers at version 2.2.0+.

They closely resemble http headers. They are a map of key/value pairs, the value being an array of strings.

Headers allow users to add meta information about a message without interfering with the message payload.

Note that if an application attempts to send a message with a header when connected to a server that does not understand them, the publish call will return the error `NATS_NO_SERVER_SUPPORT`.

There is an API to know if the server currently connected to supports headers:
```c
natsStatus s = natsConnection_HasHeaderSupport(conn);
if (s == NATS_NO_SERVER_SUPPORT)
    // deal with server not supporting this feature.
```

If the server understands headers but is about to deliver the message to a client that doesn't, the headers are stripped off so that the older clients can still receive the messsage.
It is important to have all client and servers to a version that support headers if applications rely on headers.

For more details on the headers API, please get the example: `examples/getstarted/headers.c`.

## Wildcard Subscriptions

The `*` wildcard matches any token, at any level of the subject:

```c
natsConnection_Subscribe(&sub, nc, "foo.*.baz", onMsg, NULL);
```
This subscriber would receive messages sent to:

* foo.bar.baz
* foo.a.baz
* etc...

It would not, however, receive messages on:

* foo.baz
* foo.baz.bar
* etc...

The `>` wildcard matches any length of the fail of a subject, and can only be the last token.

```c
natsConnection_Subscribe(&sub, nc, "foo.>", onMsg, NULL);
```
This subscriber would receive any message sent to:

* foo.bar
* foo.bar.baz
* foo.foo.bar.bax.22
* etc...

However, it would not receive messages sent on:

* foo
* bar.foo.baz
* etc...

Publishing on this subject would cause the two above subscriber to receive the message:
```c
natsConnection_PublishString(nc, "foo.bar.baz", "got it?");
```

## Queue Groups

All subscriptions with the same queue name will form a queue group. Each message will be delivered to only one subscriber per queue group, using queue sematics. You can have as many queue groups as you wish. Normal subscribers will continue to work as expected.

```c
natsConnection_QueueSubscribe(&sub, nc, "foo", "job_workers", onMsg, NULL);
```

## TLS

(Note that the library needs to be built with TLS support - which is by default - for these APIs to work. See the Build chapter on how to build with or without TLS for more details).

An SSL/TLS connection is configured through the use of `natsOptions`. Depending on the level of security you desire, it can be as simple as setting the secure boolean to true on the `natsOptions_SetSecure()` call.

Even with full security (client verifying server certificate, and server requiring client certificates), the setup involves only a few calls.

```c
// Here is the minimum to create a TLS/SSL connection:

// Create an options object.
natsOptions_Create(&opts);

// Set the secure flag.
natsOptions_SetSecure(opts, true);

// You may not need this, but suppose that the server certificate
// is self-signed and you would normally provide the root CA, but
// don't want to. You can disable the server certificate verification
// like this:
natsOptions_SkipServerVerification(opts, true);

// Connect now...
natsConnection_Connect(&nc, opts);

// That's it! On success you will have a secure connection with the server!

(...)

// This example shows what it takes to have a full SSL configuration,
// including server expected's hostname, root CA, client certificates
// and specific ciphers to use.

// Create an options object.
natsOptions_Create(&opts);

// Set the secure flag.
natsOptions_SetSecure(opts, true);

// For a server with a trusted chain built into the client host,
// simply designate the server name that is expected. Without this
// call, the server certificate is still verified, but not the
// hostname.
natsOptions_SetExpectedHostname(opts, "localhost");

// Instead, if you are using a self-signed cert and need to load in the CA.
natsOptions_LoadCATrustedCertificates(opts, caCertFileName);

// If the server requires client certificates, provide them along with the
// private key, all in one call.
natsOptions_LoadCertificatesChain(opts, certChainFileName, privateKeyFileName);

// You can also specify preferred ciphers if you want.
natsOptions_SetCiphers(opts, "-ALL:HIGH");

// Then simply pass the options object to the connect call:
natsConnection_Connect(&nc, opts);

// That's it! On success you will have a secure connection with the server!
```

## New Authentication (Nkeys and User Credentials)

This requires server with version >= 2.0.0

NATS servers have a new security and authentication mechanism to authenticate with user credentials and Nkeys. The simplest form is to use the helper option `natsOptions_SetUserCredentialsFromFiles()`.

```c
// Retrieve both user JWT and NKey seed from single file `user.creds`.
s = natsOptions_SetUserCredentialsFromFiles(opts, "user.creds", NULL);
if (s == NATS_OK)
    s = natsConnection_Connect(&nc, opts);
```

With this option, the library will load the user JWT and NKey seed from a single file. Note that the library wipes the buffers used to read the files.

If you prefer to store the JWT and seed in two distinct files, use this form instead:

```c
// Retrieve the user JWT from the file `user.jwt` and the seed from the file `user.nk`.
s = natsOptions_SetUserCredentialsFromFiles(opts, "user.jwt", "user.nk");
if (s == NATS_OK)
    s = natsConnection_Connect(&nc, opts);
```

You can also set the callback handlers and manage challenge signing directly.

```c
/*
 * myUserJWTCb is a callback that is supposed to return the user JWT.
 * An optional closure can be specified.
 * mySignatureCB is a callback that is presented with a nonce and is
 * responsible for returning the signature for this nonce.
 * An optional closure can be specified.
 */
s = natsOptions_SetUserCredentialsCallbacks(opts, myUserJWTCb, NULL, mySignatureCb, NULL);
if (s == NATS_OK)
    s = natsConnection_Connect(&nc, opts);
```

For NKey authentication, it is possible to specify the public NKey and the file containing the corresponding NKey seed. On connect, the library will load this file to look for the NKey seed and use it to sign the nonce sent by the server. The library takes care of clearing the memory where the seed is copied as soon
as the nonce is signed.

```c
s = natsOptions_SetNKeyFromSeed(opts, "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4", "seed.nk");
if (s == NATS_OK)
    s = natsConnection_Connect(&nc, opts);
```
The "seed.nk" file contains the NKey seed (private key). Here is an example:
```
$ more seed.nk
SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY
```

Finally, it is possible to specify the public NKey and the signature callback. The public key will be sent to the server and the provided callback is responsible for signing the server's nonce. When the server receives the signed nonce, it can check that it was signed poperly using the provided public key.

```c
/*
 * myPublicKey is the user's public key, which will be sent to the server.
 * mySignatureCB is a callback that is presented with a nonce and is
 * responsible for returning the signature for this nonce.
 * An optional closure can be specified.
 */
s = natsOptions_SetNKey(opts, myPublicKey, mySignatureCb, NULL);
if (s == NATS_OK)
    s = natsConnection_Connect(&nc, opts);
```
The signature callback can use any crypto library to sign the nonce, but also the provided `nats_Sign()` function.
```c
natsStatus
mySignatureCb(
    char            **customErrTxt,
    unsigned char   **signature,
    int             *signatureLength,
    const char      *nonce,
    void            *closure)
{
    // This approach requires to provide the seed (private key).
    // Hardcoding it in the application (like in this example) may not be really safe.
    return nats_Sign(
        "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY",
        nonce, signature, signatureLength);
}
```

You can sign any content and get the signature in return. The connection must have been created with the `natsOptions_SetUserCredentialsFromFiles()` option for that to work.
```c
    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
        s = natsOptions_SetUserCredentialsFromFiles(opts, "user.creds", NULL);
    if (s == NATS_OK)
        s = natsConnection_Connect(&nc, opts);

    // Sign some arbitrary content
    const unsigned char *content   = (const unsigned char*) "hello";
    int                 contentLen = 5;
    unsigned char       sig[64];

    s = natsConnection_Sign(nc, content, contentLen, sig);
    if (s == NATS_OK)
    {
        // Do something with signature...
    }
```

## Advanced Usage

Flushing a connection ensures that any data buffered is flushed (sent to) the NATS Server.

```c
// Flush connection to server, returns when all messages have been processed.
natsConnection_Flush(nc);
printf("All clear!\n");

// Same as above but with a timeout value, expressed in milliseconds.
s = natsConnection_FlushTimeout(nc, 1000);
if (s == NATS_OK)
    printf("All clear!\n");
else if (s == NATS_TIMEOUT)
    printf("Flushed timed out!\n");
else
    printf("Error during flush: %d - %s\n", s, natsStatus_GetText(s));
```

Auto-unsubscribe allows a subscription to be automatically removed when the subscriber has received a given number of messages. This is used internally by the `natsConnection_Request()` call.

```c
// Auto-unsubscribe after 100 messages received
natsConnection_Subscribe(&sub, nc, "foo", onMsg, NULL);
natsSubscription_AutoUnsubscribe(sub, 100);
```

Subscriptions can be drained. This ensures that the interest is removed from the server but that all messages that were internally queued are processed.

```c
// This call does not block.
natsSubscription_Drain(sub);

// If you want to wait for the drain to complete, call this
// and specify a timeout. Zero or negative to wait for ever.
natsSubscription_WaitForDrainCompletion(sub, 0);
```

Connections can be drained. This process will first put all registered subscriptions in drain mode and prevent any new subscription from being created. When all subscriptions are drained, the publish calls are drained (by the mean of a connection flush) and new publish calls will fail at this point. Then the connection is closed.

```c
// Use default timeout of 30 seconds.
// But this call does not block. Use natsOptions_SetClosedCB() to be notified
// that the connection is closed.
natsConnection_Drain(nc);

// To specify a timeout for the operation to complete, after which the connection
// is forcefully closed. Here is an exampel of a timeout of 10 seconds (10,000 ms).
natsConnection_DrainTimeout(nc, 10000);
```

You can have multiple connections in your application, mixing subscribers and publishers.
```c
// Create a connection 'nc1' to host1
natsConnection_ConnectTo(&nc1, "nats://host1:4222");

// Create a connection 'nc2' to host2
natsConnection_ConnectTo(&nc2, "nats://host2:4222");

// Create a subscription on 'foo' from connection 'nc1'
natsConnection_Subscribe(&sub, nc1, "foo", onMsg, NULL);

// Uses connection 'nc2' to publish a message on subject 'foo'. The subscriber
// created previously will receive it through connection 'nc1'.
natsConnection_PublishString(nc2, "foo", "hello");
```

The use of `natsOptions` allows you to specify options used by the `natsConnection_Connect()` call. Note that the `natsOptions` object that is  passed to this call is cloned, whhich means that any modification done to the options object will not have any effect on the connected connection.

```c
natsOptions *opts = NULL;

// Create an options object
natsOptions_Create(&opts);

// Set some properties, starting with the URL to connect to:
natsOptions_SetURL(opts, "nats://host1:4222");

// Set a callback for asynchronous errors. This is useful when having an asynchronous
// subscriber, which would otherwise have no other way of reporting an error.
natsOptions_SetErrorHandler(opts, asyncCb, NULL);

// Connect using those options:
natsConnection_Connect(&nc, opts);

// Destroy the options object to free memory. The object was cloned by the connection,
// so the options can be safely destroyed.
natsOptions_Destroy(opts);
```

As we have seen, all callbacks have a `void *closure` parameter. This is useful when the callback needs to perform some work and need a reference to some object. When setting up the callback, you can specify a pointer to that object.
```c
// Our object definition
typedef struct __Errors
{
    int count;

} Errors;

(...)

int
main(int argc, char **argv)
{
    // Declare an 'Errors' object on the stack.
    Errors asyncErrors;

    // Initialize this object
    memset(&asyncErrors, 0, sizeof(asyncErrors);

    // Create a natsOptions object.
    (...)

    // Set the error callback, and pass the address of our Errors object.
    natsOptions_SetErrorHandler(opts, asyncCb, (void*) &asyncErrors);

    // Create the connection and subscriber.
    (...)

    // Say that we are done subscribing, we could check the number of errors:
    if (asyncErrors.count > 1000)
    {
        printf("That's a lot of errors!\n");
    }

    (...)
}
```

The callback would use the closure this way:
```c
static void
asyncCb(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    Errors *errors = (Errors*) closure;

    printf("Async error: %d - %s\n", err, natsStatus_GetText(err));

    errors->count++;
}
```
This is the same for all other callbacks used in the C NATS library.

The library can automaticall reconnect to a NATS Server if the connection breaks.
However, the initial connect itself would fail if no server is available at the
time of the connect. An option has been added to make the connect behaves as
the reconnect, using the reconnect attempts and wait:
```c
    s = natsOptions_SetMaxReconnect(opts, 5);
    if (s == NATS_OK)
        s = natsOptions_SetReconnectWait(opts, 1000);

    // Instruct the library to block the connect call for that
    // long until it can get a connection or fails.
    if (s == NATS_OK)
        s = natsOptions_SetRetryOnFailedConnect(opts, true, NULL, NULL);

    // If the server is not running, this will block for about 5 seconds.
    s = natsConnection_Connect(&conn, opts);
```

You can make the connect asynchronous (if it can't connect immediately) by
passing a connection handler:
```c
    s = natsOptions_SetMaxReconnect(opts, 5);
    if (s == NATS_OK)
        s = natsOptions_SetReconnectWait(opts, 1000);
    if (s == NATS_OK)
        s = natsOptions_SetRetryOnFailedConnect(opts, true, connectedCB, NULL);

    // Start the connect. If no server is running, it should return NATS_NOT_YET_CONNECTED.
    s = natsConnection_Connect(&conn, opts);
    printf("natsConnection_Connect call returned: %s\n", natsStatus_GetText(s));

    // Connection can be used to create subscription and publish messages (as
    // long as the reconnect buffer is not full).
```
Check the example `examples/connect.c` for more use cases.

You can also specify a write deadline which means that when the library is trying to
send bytes to the NATS Server, if the connection if unhealthy but as not been reported
as closed, calls will fail with a `NATS_TIMEOUT` error. The socket will be closed and
the library will attempt to reconnect (unless disabled). Note that this could also
happen in the event the server is not consuming fast enough.
```c
    // Sets a write deadline of 2 seconds (value is in milliseconds).
    s = natsOptions_SetWriteDeadline(opts, 2000);
```

## Clustered Usage

```c
static char *servers[] = { "nats://localhost:1222",
                           "nats://localhost:1223",
                           "nats://localhost:1224"};

// Setup options to include all servers in the cluster.
// We first created an options object, and pass the list of servers, specifying
// the number of servers on that list.
natsOptions_SetServers(opts, servers, 3);

// We could also set the amount to sleep between each reconnect attempt (expressed in
// milliseconds), and the number of reconnect attempts.
natsOptions_SetMaxReconnect(opts, 5);
natsOptions_SetReconnectWait(opts, 2000);

// We could also disable the randomization of the server pool
natsOptions_SetNoRandomize(opts, true);

// Setup a callback to be notified on disconnects...
natsOptions_SetDisconnectedCB(opts, disconnectedCb, NULL);

// And on reconncet
natsOptions_SetReconnectedCB(opts, reconnectedCb, NULL);

// This callback could be used to see who we are connected to on reconnect
static void
reconnectedCb(natsConnection *nc, void *closure)
{
    // Define a buffer to receive the url
    char buffer[64];

    buffer[0] = '\0';

    natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer));
    printf("Got reconnected to: %s\n", buffer);
}
```

## Using an Event Loop Library

For each connection, the `NATS` library creates a thread reading data from the socket. Publishing data results in the data being appended to a buffer, which is 'flushed' from a timer callback or in place when the buffer reaches a certain size. Flushing means that we write to the socket (and the socket is in blocking-mode).

If you have multiple connections running in your process, the number of threads will increase (because each connection uses a thread for receiving data from the socket). If this becomes an issue, or if you are already using an event notification library, you can instruct the `NATS` library to use that event library instead of using a thread to do the reads, and directly writing to the socket when data is published.

This works by setting the event loop and various callbacks through the `natsOptions_SetEventLoop()` API. Depending of the event loop you are using, you then have extra API calls to make. The API is in the `adapters` directory and is documented.

We provide adapters for two event notification libraries: [libevent](https://github.com/libevent/libevent), and [libuv](https://github.com/libuv/libuv).

```c
// Create an event loop object
uv_loop_t *uvLoop = uv_default_loop();

// Set it into an options object
natsOptions_SetEventLoop(opts,
                         (void*) uvLoop,
                         natsLibuv_Attach,
                         natsLibuv_Read,
                         natsLibuv_Write,
                         natsLibuv_Detach);

// Connect (as usual)
natsConnection_Connect(&conn, opts);

// Subscribe (as usual)
natsConnection_Subscribe(&sub, conn, subj, onMsg, NULL);

// Run the event loop
uv_run(uvLoop, UV_RUN_DEFAULT);
```

The callback `onMsg` that you have registered will be triggered as usual when data becomes available.

Where it becomes tricky is when publishing data. Indeed, publishing is merely putting data in a buffer, and it is the event library that will notify a callback that write to the socket should be performed. For that, the event loop needs to be 'running'.

So if you publish from the thread where the event loop is running, you need to 'run' the loop after each (or a number) of publish calls in order for data to actually be sent out. Alternatively, you can publish from a different thread than the thread running the event loop.

The above is important to keep in mind regarding calls that are doing request-reply. They should not be made from the thread running the event loop. Here is an example of such calls:

```
natsConnection_Request()
natsConnection_Flush()
natsConnection_FlushTimeout()
...
```

Indeed, since these calls publish data and wait for a 'response', if you execute then in the event loop thread (or while the loop is not 'running'), then data will not be sent out. Calls will fail to get a response and timeout.

For `natsConnection_Request()`, use the `natsConnection_PublishRequest()` instead, and have a subscriber for the response registered.

For others, asynchronous version of these calls should be made available.

See examples in the `examples` directory for complete usage.

## FAQ

Here are some of the frequently asked questions:

<b>Where do I start?</b>

There are several resources that will help you get started using the NATS C Client library.
<br>
The `examples/getstarted` directory contains very basic programs that use
the library for simple functions such as sending a message or setting up a subscription.
<br>
The `examples` directory itself contains more elaborated examples that include error
handling and more advanced APIs. You will also find examples to that show the use
of the NATS C Client library and external event loops.

<b>What about support for platform XYZ?</b>

We support platforms that are available to us for development and testing. This is currently
limited to Linux, macOS and Windows. Even then, there may be OS versions that you may have
problem building with and we will gladly accept PRs to fix the build process as long as it
does not break the ones we support!

<b>How do I build?</b>

We use cmake since it allows cross-platforms builds. This works for us. You are free to
create your own makefile or Windows solution. If you want to use cmake, follow these
[instructions](https://github.com/nats-io/nats.c#build).

<b>I have found a bug in your library, what do I do?</b>

Please report an issue [here](https://github.com/nats-io/nats.c/issues/new). Give us as much
as possible information on how you can reproduce this. If you have a fix for it, you can
also open a PR.

<b>Is the library thread-safe?</b>

All calls use internal locking where needed. As a user, you would need to do your own locking
if you were to share the same callback with different subscribers (since the callback would
be invoked from different threads for each subscriber).<br>
Note that this is true for any kind of callback that exist in the NATS C library, such as
connection or error handlers, etc.. if you specify the same callback you take the risk that
the code in that callback may be executed from different internal threads.

<b>What is the threading model of the library?</b>

The library uses some threads to handle internal timers or dispatch asynchronous errors
for instance. Here is a list of threads created as a result of the user creating NATS
objects:

- Each connection has a thread to read data from the socket. If you use an external
event loop, this thread is not created.

- Each connection has a thread responsible for flushing its outgoing buffer. If you create
the connection with the `natsOptions_SetSendAsap()` option, this thread is not created since
any outgoing data is flushed right away.

- Each asynchronous subscription has a thread used to dispatch messages to the user callback.
If you use `nats_SetMessageDeliveryPoolSize()`, a global thread pool (of the
size given as a parameter to that function) is used instead of a per-async-subscription thread.

<b>How do I send binary data?</b>

NATS is a text protocol with message payload being a byte array. The server never interprets
the content of a message.

The natsConnection_Publish() API accepts a pointer to memory and the user provides how many
bytes from that location need to be sent. The natsConnection_PublishString() is added for
convenience if you want to send strings, but it is really equivalent to calling natsConnection_Publish()
with `strlen` for the number of bytes.

<b>Is the data sent in place or from a different thread?</b>

For throughput reasons (and to mimic the Go client this library is based on), the client library
uses a buffer for all socket writes. This buffer is flushed in the thread where the publish occurs
if the buffer is full. The buffer size can be configured with natsOptions_SetIOBufSize(). You can query how
much data is in that buffer using the natsConnection_Buffered() function.

When a publish call does not fill the buffer, the call returns without any data actually sent
to the server. A dedicated thread (the flusher thread) will flush this buffer automatically. This
helps with throughput since the number of system calls are reduced and the number of bytes
sent at once is higher, however, it can add latency for request/reply situations where one
wants to send one message at a time. To that effect, natsConnection_Request() call does flush
the buffer in place and does not rely on the flusher thread.

The option natsOptions_SetSendAsap() can be used to force all publish calls from the connection
created with this option to flush the socket buffer at every call and not add delay by relying
on the flusher thread.

<b>The publish call did not return an error, is the message guaranteed to be sent to a subscriber?</b>

No! It is not even guaranteed that the server got that message. As described above, the message
could simply be in connection's buffer, even if each publish call is flushing the socket buffer,
after that, the call returns. There is no feedback from the server that it has actually processed
that message. The server could have crashed after reading from the socket.

Regardless if the server has gotten the message or not, there is a total decoupling between
publishing and subscribing. If the publisher needs to know that its message has bee received
and processed by the subscribing application, request/reply pattern should be used. Check
natsConnection_Request() and natsConnection_PublishRequest() for more details.

<b>Do I need to call natsConnection_Flush() everywhere?</b>

This function is not merely flushing the socket buffer, instead it sends a `PING` protocol
message to the server and gets a `PONG` back in a synchronous way.

As previously described, if you want to flush the socket buffer to reduce latency in all
publish calls, you should create the connection with the "send asap" option.

The natsConnection_Flush() API is often used to ensure that the server has processed one of the
protocol messages.

For instance, creating a subscription is asynchronous. When the call returns, you may
get an error if the connection was previously closed, but you would not get an error if your
connection user has no permission to create this subscription for instance.

Instead, the server sends an error message that is asynchronously received by the client library.
Calling natsConnection_Flush() on the same connection that created the subscription ensures
that the server has processed the subscription and if there was an error has sent that error back
before the `PONG`. It is then possible to check the natsConnection_GetLastError()
to figure out if the subscription was successfully registered or not.

<b>How is data and protocols received from the server?</b>

When you create a connection, a library thread is created to read protocols (including messages)
from the socket. You do not have to do anything in that regard. When data is read from the socket
it will be turned into protocols or messages and distributed to appropriate callbacks.

<b>Lot of things are asynchronous, how do I know if there is an error?</b>

You should set error callbacks to be notified when asynchronous errors occur. These can be
set through the use of natsOptions. Check natsOptions_SetErrorHandler() for instance. If you
register an error callback for a connection, should an error occurs, your registered error handler
will be invoked.

<b>Are messages from an asynchronous subscription dispatched in parallel?</b>

When you create an asynchronous subscription, the library will create a thread that is responsible
to dispatch messages for that subscription. Messages for a given subscription are dispatched
sequentially. That is, your callback is invoked with one message at a time, and only after the
callback returns that the library invokes it again with the next pending message.

So there is a thread per connection draining data from the socket, and then messages are passed
to the matching subscription thread that is then responsible to dispatch them.

If you plan to have many subscriptions and to reduce the number of threads used by the library,
you should look at using a library level thread pool that will take care of messages dispatching.
See nats_SetMessageDeliveryPoolSize(). A subscription will make use of the library pool if the
connection it was created from had the natsOptions_UseGlobalMessageDelivery() option set to true.

Even when using the global pool, messages from a given subscription are always dispatched
sequentially and from the same thread.

<b>What is the SLOW CONSUMER error that I see in the server?</b>

The server when sending data to a client (or route) connection sets a write deadline. That is,
if the socket write blocks for that amount of time, it will simply close the connection.

This error occurs when the client (or other server) is not reading data fast enough from the
socket. As we have seen earlier, a client connection creates a thread whose job is to read
data from the socket, parse it and move protocol or messages to appropriate handlers.

So this is not really symptomatic of a message handler  processing messages too slowly,
instead it is probably the result of resources issues (not enough CPU cycles to read from the
socket, or not reading fast enough) or internal locking contention that prevents the thread
reading from the socket to read data fast enough because it is blocked trying to acquire some
lock.

<b>What is the SLOW CONSUMER error in the client then?</b>

This error, in contrast with the error reported by the server, has to do with the disptaching
of messages to the user callback. As explained, messages are moved from the connection's
thread reading from the socket to the subscription's thread responsible for dispatching.

The subscription's internal queue as a default limited size. When the connection's thread
cannot add a message to that queue because it is full, it will drop the message and if
an error handler has been set, a message will be posted there.

For instance, having a message handler that takes too much time processing a message is
likely to cause a slow consumer client error if the incoming message rate is high and/or
the subscription pending limits are not big enough.

The natsSubscription_SetPendingLimits() API can be used to set the subscription's internal
queue limits. Values of `-1` for count and/or size means that the corresponding metric will
not be checked. Setting both to `-1` mean that the client library will queued all incoming
messages, regardless at which speed they are dispatched, which could cause your application
to use lots of memory.

<b>What happens if the server is restarted or there is a disconnect?</b>

By default, the library will try to reconnect. Reconnection options can be set to either
disable reconnect logic entirely, or set the number of attempts and delay between attempts.
See natsOptions_SetAllowReconnect(), natsOptions_SetMaxReconnect(), etc... for more information.

If you have not set natsOptions_SetNoRadomize() to true, then the list of given URLs are randomized
when the connection is created. When a disconnect occurs, the next URL is tried. If that fails,
the library moves to the next. When all have been tried, the loop restart from the first until
they have all been tried the max reconnect attempts defined through options. The library
will pause for as long as defined in the options when trying to reconnect to a server it was
previously connected to.

If you connect to a server that has the connect URL advertise enabled (default for servers 0.9.2+),
when servers are added to the cluster of NATS Servers, the server will send URLs of this
new server to its clients. This augments the "pool" or URLs that the connection may have been
created with and allows the library to reconnect to servers that have been added to the cluster.

While the library is disconnected from the server, all publish or new subscription calls are
buffered in memory. This buffer as a default size but can be configured. When this buffer is
full, further publish or new subscription calls will report failures.<br>
When the client library has reconnected to the server, the pending buffer will
be flushed to the server.

If your application needs to know if a disconnect occurs, or when the library has reconnected,
you should again set some callbacks to be notified of such events. Use natsOptions_SetDisconnectedCB(),
natsOptions_SetReconnectedCB() and natsOptions_SetClosedCB(). Note that the later is a final
event for a connection. When this callback is invoked, the connection is no longer valid, that
is, you will no longer receive or be able to send data with this connection.

<b>Do I need to free NATS objects?</b>

All objects that you create and that have a `_Destroy()` API should indeed be destroyed
if you want to not leak memory.One that is important and often missed is `natsMsg_Destroy()` that
needs to be called in the message handler once you are done processing the message. The
message has been created by the library and given to you in the message handler, but you are
responsible for calling `natsMsg_Destroy()`.

<b>What is nats_ReleaseThreadMemory() doing?</b>

The NATS C library may store objects using local thread storage. Threads that are created and
handled by the library are automatically cleaned-up, however, if the user creates a thread
and invokes some NATS APIs, there is a possibility that the library stored something in that
thread local storage. When the thread exits, the user should call this function so that
the library can destroy objects that it may have stored.

# NATS Streaming Client

## Streaming Basic Usage

Note that error checking is being ignored for clarity. Check the `examples/stan` directory
for complete usage.

```
// Connect without options
stanConnection_Connect(&sc, cluster, clientID, NULL);

// Simple Synchronous Publisher.
// This does not return until an ack has been received from NATS Streaming
stanConnection_Publish(sc, "foo", (const void*) "hello", 5);

// Simple Async Subscriber
stanConnection_PublishAsync(sc, "foo", (const void*) "hello", 5, _pubAckHandler, NULL);

// Simple Subscription without options (default to non durable, receive new only)
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, NULL);

// Unsubscribe (note that for non durable subscriptions, Unsubscribe() and Close() are the same
stanSubscription_Unsubscribe(sub);

// Close connection
stanConnection_Close(sc);
```

### Streaming Subscriptions

NATS Streaming subscriptions are similar to NATS subscriptions, but clients may start their subscription at an earlier point in the message stream, allowing them to receive messages that were published before this client registered interest.

The options are described with examples below:

```
// Create a Subscription Options:
stanSubOptions *subOpts = NULL;

stanSubOptions_Create(&subOpts);

// Subscribe starting with most recently published value
stanSubOptions_StartWithLastReceived(subOpts);

// OR: Receive all stored messages
stanSubOptions_DeliverAllAvailable(subOpts);

// OR: Receive messages starting at a specific sequence number
stanSubOptions_StartAtSequence(subOpts, 22);

// OR: Start at messages that were stored 30 seconds ago. Value is expressed in milliseconds.
stanSubOptions_StartAtTimeDelta(subOpts, 30000);

// Create the subscription with options
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, subOpts);
```

### Streaming Durable Subscriptions

Replay of messages offers great flexibility for clients wishing to begin processing at some earlier point in the data stream.
However, some clients just need to pick up where they left off from an earlier session, without having to manually track their position in the stream of messages.
Durable subscriptions allow clients to assign a durable name to a subscription when it is created.
Doing this causes the NATS Streaming server to track the last acknowledged message for that clientID + durable name, so that only messages since the last acknowledged message will be delivered to the client.

```
stanConnection_Connect(&sc, "test-cluster", "client-123", NULL);

// Create subscription options
stanSubOptions_Create(&subOpts);

// Set a durable name
stanSubOptions_SetDurableName(subOpts, "my-durable");

// Subscribe
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, subOpts);
...
// client receives message sequence 1-40
...
// client disconnects for an hour
...
// client reconnects with same clientID "client-123"
stanConnection_Connect(&sc, "test-cluster", "client-123", NULL);

// client re-subscribes to "foo" with same durable name "my-durable"
stanSubOptions_Create(&subOpts);
stanSubOptions_SetDurableName(subOpts, "my-durable");
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, subOpts);
...
// client receives messages 41-current
```

### Streaming Queue Groups

All subscriptions with the same queue name (regardless of the connection
they originate from) will form a queue group.
Each message will be delivered to only one subscriber per queue group,
using queuing semantics. You can have as many queue groups as you wish.

Normal subscribers will continue to work as expected.

#### Creating a Queue Group

A queue group is automatically created when the first queue subscriber is
created. If the group already exists, the member is added to the group.

```
stanConnection_Connect(&sc, "test-cluster", "clientid", NULL);

// Create a queue subscriber on "foo" for group "bar"
stanConnection_QueueSubscribe(&qsub1, "foo", "bar", onMsg, NULL, NULL);

// Add a second member
stanConnection_QueueSubscribe(&qsub2, "foo", "bar", onMsg, NULL, NULL);

// Notice that you can have a regular subscriber on that subject
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, NULL);

// A message on "foo" will be received by sub and qsub1 or qsub2.
```

#### Start Position

Note that once a queue group is formed, a member's start position is ignored
when added to the group. It will start receive messages from the last
position in the group.

Suppose the channel `foo` exists and there are `500` messages stored, the group
`bar` is already created, there are two members and the last
message sequence sent is `100`. A new member is added. Note its start position:

```
stanSubOptions_Create(&subOpts);
stanSubOptions_StartAtSequence(subOpts, 200);

stanConnection_QueueSubscribe(&qsub, "foo", "bar", onMsg, NULL, subOpts);
```

This will not produce an error, but the start position will be ignored. Assuming
this member would be the one receiving the next message, it would receive message
sequence `101`.

#### Leaving the Group

There are two ways of leaving the group: closing the subscriber's connection or
calling `Unsubscribe`:

```
// Have qsub leave the queue group
stanSubscription_Unsubscribe(qsub);
```

If the leaving member had un-acknowledged messages, those messages are reassigned
to the remaining members.

#### Closing a Queue Group

There is no special API for that. Once all members have left (either calling `Unsubscribe`,
or their connections are closed), the group is removed from the server.

The next call to `QueueSubscribe` with the same group name will create a brand new group,
that is, the start position will take effect and delivery will start from there.

### Streaming Durable Queue Groups

As described above, for non durable queue subscribers, when the last member leaves the group,
that group is removed. A durable queue group allows you to have all members leave but still
maintain state. When a member re-joins, it starts at the last position in that group.

#### Creating a Durable Queue Group

A durable queue group is created in a similar manner as that of a standard queue group,
except the `DurableName` option must be used to specify durability.

```
stanSubOptions_Create(&subOpts);
stanSubOptions_SetDurableName(subOpts, "dur");

stanConnection_QueueSubscribe(&qsub, "foo", "bar", onMsg, NULL, subOpts);
```
A group called `dur:bar` (the concatenation of durable name and group name) is created in
the server. This means two things:

- The character `:` is not allowed for a queue subscriber's durable name.
- Durable and non-durable queue groups with the same name can coexist.

```
// Non durable queue subscriber on group "bar"
stanConnection_QueueSubscribe(&qsub, "foo", "bar", onMsg, NULL, NULL);

// Durable queue subscriber on group "bar"
stanSubOptions_Create(&subOpts);
stanSubOptions_SetDurableName(subOpts, "mydurablegroup");
stanConnection_QueueSubscribe(&qsub, "foo", "bar", onMsg, NULL, subOpts);

// The same message produced on "foo" would be received by both queue subscribers.
```

#### Start Position of the Durable Queue Group

The rules for non-durable queue subscribers apply to durable subscribers.

#### Leaving the Durable Queue Group

As for non-durable queue subscribers, if a member's connection is closed, or if
`Unsubscribe` its called, the member leaves the group. Any unacknowledged message
is transferred to remaining members. See *Closing the Group* for important difference
with non-durable queue subscribers.

#### Closing the Durable Queue Group

The *last* member calling `Unsubscribe` will close (that is destroy) the
group. So if you want to maintain durability of the group, you should not be
calling `Unsubscribe`.

So unlike for non-durable queue subscribers, it is possible to maintain a queue group
with no member in the server. When a new member re-joins the durable queue group,
it will resume from where the group left of, actually first receiving all unacknowledged
messages that may have been left when the last member previously left.


### Streaming Wildcard Subscriptions

NATS Streaming subscriptions **do not** support wildcards.


## Streaming Advanced Usage

### Connection Status

The fact that the NATS Streaming server and clients are not directly connected poses a challenge when it comes to know if a client is still valid.
When a client disconnects, the streaming server is not notified, hence the importance of calling `stanConnection_Close()`. The server sends heartbeats
to the client's private inbox and if it misses a certain number of responses, it will consider the client's connection lost and remove it
from its state.

Why do we need PINGs? Picture the case where a client connects to a NATS Server which has a route to a NATS Streaming server (either connecting
to a standalone NATS Server or the server it embeds). If the connection between the streaming server and the client's NATS Server is broken,
the client's NATS connection would still be ok, yet, no communication with the streaming server is possible.

Starting with NATS Streaming Server `0.10.2`, the client library will send PINGs at regular intervals (default is 5 seconds)
and will close the streaming connection after a certain number of PINGs have been sent without any response (default is 3). When that
happens, a callback - if one is registered - will be invoked to notify the user that the connection is permanently lost, and the reason
for the failure.

Here is how you would specify your own PING values and the callback:

```
stanConnOptions_Create(&connOpts);

// Send PINGs every 10 seconds, and fail after 5 PINGs without any response.
stanConnOptions_SetPings(connOpts, 10, 5);

// Add a callback to be notified if the STAN connection is lost for good.
stanConnOptions_SetConnectionLostHandler(connOpts, connectionLostCB, NULL);

// Here is an example of the `connectionLostCB`:
connectionLostCB(stanConnection *sc, const char *errTxt, void *closure)
{
    printf("Connection lost: %s\n", errTxt);
}
```

Note that the only way to be notified in your application is to set the callback. If the callback is not set, PINGs are still sent and the connection
will be closed if needed, but the application won't know if it has only subscriptions. A default callback is used to simply
print to standard error the clusterID, the clientID and the error that caused the connection to be lost:
```
Connection permanently lost: clusterID=test-cluster clientID=client error=connection lost due to PING failure
```

When the connection is lost, your application would have to re-create it and all subscriptions if any.

The library creates its own NATS connection and sets set the reconnect attempts to "infinite". It should therefore be possible
for the library to always reconnect, but this does not mean that the streaming connection will not be closed, even if you set
a very high threshold for the PINGs max out value. Keep in mind that while the client is disconnected, the server is sending
heartbeats to the clients too, and when not getting any response, it will remove that client from its state. When the
communication is restored, the PINGs sent to the server will allow to detect this condition and report to the client that
the connection is now closed.

Also, while a client is "disconnected" from the server, another application with connectivity to the streaming server may
connect and uses the same client ID. The server, when detecting the duplicate client ID, will try to contact the first client
to know if it should reject the connect request of the second client. Since the communication between the server and the
first client is broken, the server will not get a response and therefore will replace the first client with the second one.

Prior to NATS Streaming Server `0.10.2`, if the communication between the first client and server were to be restored,
and the application would send messages, the server would accept those because the published messages client ID would be
valid, although the client is not. With a server `0.10.2+`, additional information is sent with each message to allow
the server to reject messages from a client that has been replaced by another client.

### Asynchronous Publishing

The basic publish API (`stanConnection_Publish()`) is synchronous; it does not return control to the caller until the
NATS Streaming server has acknowledged receipt of the message. To accomplish this, a unique identifier (GUID) is generated for
the message on creation, and the client library waits for a publish acknowledgement from the server with a matching GUID before
it returns control to the caller, possibly with an error indicating that the operation was not successful due to some server
problem or authorization error.

Advanced users may wish to process these publish acknowledgements manually to achieve higher publish throughput by not
waiting on individual acknowledgements during the publish operation. An asynchronous publish API is provided for this purpose:

```
static void
_pubAckHandler(const char *guid, const char *error, void *closure)
{
    // Note: this callback can be invoked by different threads
    if (error != NULL)
        printf("pub ack for guid:%s error:%s\n", guid, error);
    else
        printf("Received ack for msg id %s\n", guid);
}

(...)

s = stanConnection_PublishAsync(sc, "foo", (const void*) "hello", 5, _pubAckHandler, NULL);
if (s != NULL)
{
    printf("Error on publish: %d - %s\n", s, natsStatus_GetText(s));
    nats_PrintLastErrorStack(stderr);
}
```
If you want to correlate the published message with the guid in the acknowledgment handler, you should pass
a unique closure as the last argument of the `stanConnection_PublishAsync()` call. Check the `examples/stan/pub-async.c`
file for an example on how to do so.

### Message Acknowledgments and Redelivery

NATS Streaming offers At-Least-Once delivery semantics, meaning that once a message has been delivered to an eligible subscriber,
if an acknowledgement is not received within the configured timeout interval, NATS Streaming will attempt redelivery of the message.
This timeout interval is specified by the subscription option `stanSubOptions_SetAckWait()`, which defaults to 30 seconds.

By default, messages are automatically acknowledged by the NATS Streaming client library after the subscriber's message handler
is invoked. However, there may be cases in which the subscribing client wishes to accelerate or defer acknowledgement of the message.
To do this, the client must set manual acknowledgement mode on the subscription, and invoke `stanSubscription_AckMsg()`. ex:

```
// Subscribe with manual ack mode, and set AckWait to 60 seconds
stanSubOptions_Create(&subOpts);
stanSubOptions_SetManualAckMode(subOpts, true);
stanSubOptions_SetAckWait(subOpts, 60000);
stanConnection_Subscribe(&sub, sc, "foo", onMsg, NULL, subOpts);

// In the callback
void
onMsg(stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg, void *closure)
{
	// ack message before performing I/O intensive operation
    stanSubscription_AckMsg(sub, msg);

	printf("Received a message on %s: %.*s\n",
        channel,
        stanMsg_GetDataLength(msg),
        stanMsg_GetData(msg));
}
```

## Rate limiting/matching

A classic problem of publish-subscribe messaging is matching the rate of message producers with the rate of message consumers.
Message producers can often outpace the speed of the subscribers that are consuming their messages.
This mismatch is commonly called a "fast producer/slow consumer" problem, and may result in dramatic resource utilization spikes
in the underlying messaging system as it tries to buffer messages until the slow consumer(s) can catch up.

### Publisher rate limiting

NATS Streaming provides a connection option called `stanConnOptions_SetMaxPubAcksInflight()` that effectively limits the number
of unacknowledged messages that a publisher may have in-flight at any given time. When this maximum is reached, further publish
calls will block until the number of unacknowledged messages falls below the specified limit. ex:

```
stanConnOptions_Create(&connOpts);
stanConnOptions_SetMaxPubAcksInflight(connOpts, 25, 1.0);
stanConnection_Connect(&sc, cluster, clientID, connOpts);

(...)
static void
_pubAckHandler(const char *guid, const char *error, void *closure)
{
    // process the ack
    ...
}

(...)

for (i = 1; i < 1000; i++)
{
    // If the server is unable to keep up with the publisher, the number of outstanding acks will eventually
    // reach the max and this call will block
    stanConnection_PublishAsync(sc, "foo", (const void*) "hello", 5, _pubAckHandler, NULL);
}
```

Note the last parameter of `stanConnOptions_SetMaxPubAcksInflight()`. This is a float indicating the percentage
of outstanding ACKs to fall below before being allowed to send more messages. For instance, if the maximum is
1000 and percentage is 50% (0.5), the it means that if the publish calls were to be blocked because the
library has sent 1000 messages and not received an ACK yet from the server, the publish calls would be unblocked
only when the library has received 500 ACKs from the server. This prevents the connection from being blocked and
unblocked for each message when the limit has been reached.

### Subscriber rate limiting

Rate limiting may also be accomplished on the subscriber side, on a per-subscription basis, using a subscription
option called `stanSubOptions_SetMaxInflight()`. This option specifies the maximum number of outstanding acknowledgements
(messages that have been delivered but not acknowledged) that NATS Streaming will allow for a given subscription.
When this limit is reached, NATS Streaming will suspend delivery of messages to this subscription until the number
of unacknowledged messages falls below the specified limit. ex:

```
// Subscribe with manual ack mode and a max in-flight limit of 25
stanSubOptions_Create(&subOpts);
stanSubOptions_SetManualAckMode(subOpts, true);
stanSubOptions_SetMaxInflight(subOpts, 25);
stanConnection_Subscribe(&sub, "foo", onMsg, NULL, subOpts);

// In the callback
void
onMsg(stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg, void *closure)
{
    printf("Received a message\n");
    ...
    // Does not ack, or takes a very long time to ack
    ...
    // Message delivery will suspend when the number of unacknowledged messages reaches 25
}
```
However, the server will redeliver messages for which it did not receive an acknowledgment for more than the
value passed in `stanSubOptions_SetAckWait()` (or 30 seconds by default).

# License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
