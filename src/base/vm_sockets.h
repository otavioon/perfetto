/*
 * Copyright (C) 2023 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_BASE_VM_SOCKETS_H_
#define SRC_BASE_VM_SOCKETS_H_

#include <sys/socket.h>

#ifdef AF_VSOCK
// Use system vm_socket.h if avaialbe.
#include <linux/vm_sockets.h>
#else
// Fallback and use the stripped copy from the UAPI vm_sockets.h.

#include <stdint.h>  // For uint8_t.

#define AF_VSOCK 40

struct sockaddr_vm {
  sa_family_t svm_family;
  unsigned short svm_reserved1;
  unsigned int svm_port;
  unsigned int svm_cid;
  uint8_t svm_flags;
  unsigned char svm_zero[sizeof(struct sockaddr) - sizeof(sa_family_t) -
                         sizeof(unsigned short) - sizeof(unsigned int) -
                         sizeof(unsigned int) - sizeof(uint8_t)];
};

#endif

#endif  // SRC_BASE_VM_SOCKETS_H_
