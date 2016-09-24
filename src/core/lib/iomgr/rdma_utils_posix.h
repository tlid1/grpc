/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef GRPC_CORE_LIB_IOMGR_RDMA_VERBS_UTILS_POSIX_H
#define GRPC_CORE_LIB_IOMGR_RDMA_VERBS_UTILS_POSIX_H

#include <sys/socket.h>
#include <unistd.h>

#include "src/core/lib/iomgr/error.h"
#include "src/core/lib/iomgr/ev_posix.h"

#include <grpc/support/sync.h>
#include "src/core/lib/iomgr/endpoint.h"
void die(const char *reason);
#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define INIT_RECV_BUFFER_SIZE (sizeof(rdma_memory_region))
#define RDMA_MSG_CONTENT_SIZE 2048
#define RDMA_POST_RECV_NUM 16
typedef struct{
  int msg_info;
  size_t msg_len;
  char msg_content[RDMA_MSG_CONTENT_SIZE];
} rdma_message;
typedef struct{
  int msg_info;
  size_t msg_len;
  char msg_content[2];
} rdma_smessage;//In order to keep the same structure of rdma_message
typedef struct{
  rdma_message msg;
  rdma_smessage sms;
} rdma_memory_region;
typedef struct connect_context connect_context;
struct connect_context {
    grpc_fd *sendfdobj;/*int pollfd;*/
    grpc_fd *recvfdobj;/*int pollfd;*/
    int sendfd;
    int recvfd;

    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    struct ibv_mr *recv_buffer_mr;
    struct ibv_mr *send_buffer_mr;
    char *recv_buffer_region;
    char *send_buffer_region;

    struct ibv_pd *pd;
    struct ibv_pd *ctl_pd;

    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_comp_channel *recv_comp_channel;
    struct ibv_comp_channel *send_comp_channel;

    grpc_endpoint *ep;
    grpc_closure *closure;
    gpr_refcount refcount;
    //buffer_size;
};

void rdma_ctx_ref(connect_context *ctx);
void rdma_ctx_unref(grpc_exec_ctx *exec_ctx, connect_context *ctx);

#endif /* GRPC_CORE_LIB_IOMGR_RMDA_VERBS_UTILS_POSIX_H */
