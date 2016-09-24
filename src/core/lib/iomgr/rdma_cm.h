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

#ifndef GRPC_CORE_LIB_IOMGR_RDMA_CM_H
#define GRPC_CORE_LIB_IOMGR_RDMA_CM_H
/*
   Low level TCP "bottom half" implementation, for use by transports built on
   top of a TCP connection.

   Note that this file does not (yet) include APIs for creating the socket in
   the first place.

   All calls passing slice transfer ownership of a slice refcount unless
   otherwise specified.
*/

#include "src/core/lib/iomgr/endpoint.h"
#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/rdma_utils_posix.h"
//#include ???.h connect_context's decl !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

//#define GRPC_TCP_DEFAULT_READ_SLICE_SIZE 2048

extern int grpc_tcp_trace;

/* Create a tcp endpoint given a file desciptor and a read slice size.
   Takes ownership of fd. */
grpc_endpoint *grpc_rdma_create(connect_context* context,
                               const char *peer_string);

#define MSGINFO_MESSAGE 0
#define SENDCONTEXT_DATA 0
#define SENDCONTEXT_SMS 1
/* Return the tcp endpoint's fd, or -1 if this is not available. Does not
   release the fd.
   Requires: ep must be a tcp endpoint.
 */
int grpc_rdma_fd(grpc_endpoint *ep);

/* Destroy the tcp endpoint without closing its fd. *fd will be set and done
 * will be called when the endpoint is destroyed.
 * Requires: ep must be a tcp endpoint and fd must not be NULL. */
void grpc_rdma_destroy_and_release_fd(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep,
                                      int* fd_in,int *fd_out, grpc_closure *done);
/* Since RDMA has no fd, we need to sentence it to death when the peer disconnected.
 * When an endpoint was sentenced to death, all the call_backs will be returned with an error.*/
void grpc_rdma_sentence_death(grpc_exec_ctx *ctx,grpc_endpoint *ep);

#endif /* GRPC_CORE_LIB_IOMGR_TCP_POSIX_H */
