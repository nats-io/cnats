// Copyright 2016-2021 The NATS Authors
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

#ifndef NUID_H_
#define NUID_H_

#include "status.h"

#define NUID_BUFFER_LEN (12 + 10)

// Seed sequential random with math/random and current time and generate crypto prefix.
natsStatus
natsNUID_init(void);

// Generate the next NUID string from the global locked NUID instance.
natsStatus
natsNUID_Next(char *buffer, int bufferLen);

int64_t
nats_Rand64(void);

void
natsNUID_free(void);

#endif /* NUID_H_ */
