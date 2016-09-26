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

#include <grpc/support/port_platform.h>

#ifdef GPR_POSIX_SOCKET

#include "src/core/lib/iomgr/tcp_client.h"

#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/time.h>

#include "src/core/lib/iomgr/ev_posix.h"
#include "src/core/lib/iomgr/iomgr_posix.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/iomgr/tcp_posix.h"
#include "src/core/lib/iomgr/timer.h"
#include "src/core/lib/iomgr/unix_sockets_posix.h"
#include "src/core/lib/support/string.h"

#include <rdma/rdma_cma.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "src/core/lib/iomgr/rdma_utils_posix.h"
#include "src/core/lib/iomgr/rdma_cm.h"


extern int grpc_tcp_trace;

typedef struct {
  gpr_mu mu;
  grpc_fd *connectfd;
  struct rdma_event_channel *ec;
  gpr_timespec deadline;
  grpc_timer alarm;
  int refs;
  grpc_closure write_closure;
  grpc_pollset_set *interested_parties;
  char *addr_str;
  grpc_endpoint **ep;
  grpc_closure *closure;
  grpc_fd *sendfdobj;
  grpc_fd *recvfdobj;
  int time_out;
  int connected;
  int cb_called;
} async_connect;

//static grpc_error *prepare_socket(const struct sockaddr *addr, int fd) {
//  grpc_error *err = GRPC_ERROR_NONE;
//
//  GPR_ASSERT(fd >= 0);
//
//  err = grpc_set_socket_nonblocking(fd, 1);
//  if (err != GRPC_ERROR_NONE) goto error;
//  err = grpc_set_socket_cloexec(fd, 1);
//  if (err != GRPC_ERROR_NONE) goto error;
//  if (!grpc_is_unix_socket(addr)) {
//    err = grpc_set_socket_low_latency(fd, 1);
//    if (err != GRPC_ERROR_NONE) goto error;
//  }
//  err = grpc_set_socket_no_sigpipe_if_possible(fd);
//  if (err != GRPC_ERROR_NONE) goto error;
//  goto done;
//
//error:
//  if (fd >= 0) {
//    close(fd);
//  }
//done:
//  return err;
//}

static void tc_on_alarm(grpc_exec_ctx *exec_ctx, void *acp, grpc_error *error) {
  gpr_log(GPR_DEBUG,"CLIENT FUNC:tc_on_alarm CALLED");
  async_connect *ac = acp;
  int done;
  /*
     if (grpc_tcp_trace) {
     const char *str = grpc_error_string(error);
     gpr_log(GPR_DEBUG, "CLIENT_CONNECT: %s: on_alarm: error=%s", ac->addr_str,
     str);
     grpc_error_free_string(str);
     }
     */
  gpr_mu_lock(&ac->mu);
  done = (--ac->refs == 0);
  if (ac->connected == 0) {
    if (ac->connectfd != NULL) {
      grpc_fd_shutdown(exec_ctx, ac->connectfd);
    }
    ac->time_out = 1;
  }
  gpr_mu_unlock(&ac->mu);

  if (done) {
    if (ac->addr_str != NULL) {
      gpr_free(ac->addr_str);
      ac->addr_str = NULL;
    }
    if (ac != NULL) {
      gpr_mu_destroy(&ac->mu);
      gpr_free(ac->addr_str);
      gpr_free(ac);
      ac = NULL;
    } 
  } else {
    //ac->write_closure.cb(exec_ctx, ac->write_closure.cb_arg, GRPC_ERROR_NONE);
  }
}

#define TIMEOUT_IN_MS 500
static int on_addr_resolved(struct rdma_cm_id *id) {
  struct ibv_qp_init_attr qp_attr;

  struct ibv_pd *pd;
  struct ibv_cq *recv_cq, *send_cq;
  struct ibv_comp_channel *recv_comp_channel, *send_comp_channel;

  struct connect_context *context;
  struct ibv_mr *recv_buffer_mr;
  struct ibv_mr *send_buffer_mr;
  char *recv_buffer_region;
  char *send_buffer_region;

  struct ibv_recv_wr wr[RDMA_POST_RECV_NUM], *bad_wr = NULL;
  struct ibv_sge sge[RDMA_POST_RECV_NUM];

  uintptr_t uintptr_addr;
  int i;

  //build context 
  if ((pd = ibv_alloc_pd(id->verbs)) == NULL) {
    gpr_log(GPR_ERROR, "Client: ibv_alloc_pd() failed: %s",strerror(errno));
    return -1;
  } 

  if ((send_comp_channel = ibv_create_comp_channel(id->verbs)) == NULL) { //FIXME
    gpr_log(GPR_ERROR, "Client: ibv_create_comp_channel() failed: %s",strerror(errno));
    return -1;
  }
  if ((recv_comp_channel = ibv_create_comp_channel(id->verbs)) == NULL) { //FIXME
    gpr_log(GPR_ERROR, "Client: ibv_create_comp_channel() failed: %s",strerror(errno));
    return -1;
  }

  if ((send_cq = ibv_create_cq(id->verbs, 10, NULL, send_comp_channel, 0)) ==NULL) { //FIXME 
    gpr_log(GPR_ERROR, "Client: ibv_create_cq() failed: %s",strerror(errno));
    return -1;
  }
  if ((recv_cq = ibv_create_cq(id->verbs, 10, NULL, recv_comp_channel, 0)) ==NULL) { //FIXME 
    gpr_log(GPR_ERROR, "Client: ibv_create_cq() failed: %s",strerror(errno));
    return -1;
  }

  if ((ibv_req_notify_cq(send_cq, 0)) != 0) {
    gpr_log(GPR_ERROR, "Client: ibv_req_notify_cq() failed: %s",strerror(errno));
    return -1;
  }
  if ((ibv_req_notify_cq(recv_cq, 0)) != 0) {
    gpr_log(GPR_ERROR, "Client: ibv_req_notify_cq() failed: %s",strerror(errno));
    return -1;
  }

  int flags;
  flags = fcntl(send_comp_channel->fd, F_GETFL);
  fcntl(send_comp_channel->fd, F_SETFL, flags | O_NONBLOCK); //set non-blocking
  flags = fcntl(recv_comp_channel->fd, F_GETFL);
  fcntl(recv_comp_channel->fd, F_SETFL, flags | O_NONBLOCK); //set non-blocking


  //build_qp_attr(&qp_attr);
  memset(&qp_attr, 0, sizeof(qp_attr)); 
  qp_attr.send_cq = send_cq;
  qp_attr.recv_cq = recv_cq;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = 128; //TODO
  qp_attr.cap.max_recv_wr = 128;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;

  if ((rdma_create_qp(id, pd, &qp_attr)) != 0) {
    gpr_log(GPR_ERROR, "Client: rdma_create_qp() failed: %s",strerror(errno));
    return -1;
  }

  //register_memory(conn);
  recv_buffer_region = gpr_malloc(INIT_RECV_BUFFER_SIZE * RDMA_POST_RECV_NUM);
  //memset(recv_buffer_region,0,INIT_RECV_BUFFER_SIZE * RDMA_POST_RECV_NUM);
  send_buffer_region = gpr_malloc(INIT_RECV_BUFFER_SIZE);
  

  if((recv_buffer_mr = ibv_reg_mr(
          pd,
          recv_buffer_region,
          INIT_RECV_BUFFER_SIZE * RDMA_POST_RECV_NUM,
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
      == NULL) {
    gpr_log(GPR_ERROR, "Client: ibv_reg_mr() failed: %s",strerror(errno));
    return -1;
  }


  if((send_buffer_mr = ibv_reg_mr(
          pd,
          send_buffer_region,
          INIT_RECV_BUFFER_SIZE,
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
      == NULL) {
    gpr_log(GPR_ERROR, "Client: ibv_reg_mr() failed: %s",strerror(errno));
    return -1;
  }


  //build id->context
  context = (struct connect_context *)gpr_malloc(sizeof(struct connect_context));
  memset(context, 0 ,sizeof(struct connect_context));
  gpr_ref_init(&context->refcount, 1);
  context->sendfd = send_comp_channel->fd;
  context->recvfd = recv_comp_channel->fd;
  context->qp = id->qp;
  context->recv_buffer_region = recv_buffer_region;
  context->send_buffer_region = send_buffer_region;
  context->recv_buffer_mr = recv_buffer_mr;
  context->send_buffer_mr = send_buffer_mr;
  context->id = id;

  context->sendfdobj = NULL;
  context->recvfdobj = NULL;
  context->pd = pd;
  context->send_comp_channel = send_comp_channel;
  context->send_cq = send_cq;
  context->recv_comp_channel = recv_comp_channel;
  context->recv_cq = send_cq;
  id->context = context;


  //post_receives(context);
  for (i=0; i<RDMA_POST_RECV_NUM; i++) {
    memset(&(wr[i]),0,sizeof(wr[i]));
    uintptr_addr = (uintptr_t)context->recv_buffer_region +  (uintptr_t)i * INIT_RECV_BUFFER_SIZE;
    wr[i].wr_id = uintptr_addr;
    wr[i].next = &(wr[i+1]);
    if ( i == RDMA_POST_RECV_NUM -1) {
      wr[i].next = NULL;
    }
    wr[i].sg_list = &(sge[i]);
    wr[i].num_sge = 1;

    sge[i].addr = uintptr_addr;
    sge[i].length = INIT_RECV_BUFFER_SIZE;
    sge[i].lkey = recv_buffer_mr->lkey;

  }

  if ((ibv_post_recv(context->qp, &(wr[0]), &bad_wr)) != 0){
    gpr_log(GPR_ERROR, "Client: ibv_post_recv() failed: %s",strerror(errno));
    return -1;
  }

  //post_receives(context);
  //uintptr_addr = (uintptr_t)context->recv_buffer_region;
  //wr.wr_id = uintptr_addr;
  //wr.next = NULL;
  //wr.sg_list = &sge;
  //wr.num_sge = 1;

  //sge.addr = (uintptr_t)context->recv_buffer_region;
  //sge.length = INIT_RECV_BUFFER_SIZE;
  //sge.lkey = context->recv_buffer_mr->lkey;

  //if ((ibv_post_recv(context->qp, &wr, &bad_wr)) != 0){
  //     gpr_log(GPR_ERROR, "Client: ibv_post_recv() failed: %s",strerror(errno));
  //     return -1;
  //}

  if ((rdma_resolve_route(id, TIMEOUT_IN_MS)) != 0){
    gpr_log(GPR_ERROR, "Client: rdma_resolve_route() failed: %s",strerror(errno));
    return -1;
  }
  return 0; 
}

static int on_disconnect(grpc_exec_ctx *exec_ctx, struct rdma_cm_id *id) {
  struct connect_context *context = (struct connect_context *)id->context;
  context->closure=NULL;
  if (context->refcount.count >= 2) {
    grpc_rdma_sentence_death(exec_ctx, context->ep);
  }
  //ibv_dereg_mr(context->recv_buffer_mr); 
  //ibv_dereg_mr(context->send_buffer_mr); 
  //ibv_dealloc_pd(context->pd);
  //ibv_destroy_cq(context->recv_cq);
  //ibv_destroy_cq(context->send_cq);
  ////ibv_destroy_comp_channel(context->comp_channel);
  //gpr_free(context->recv_buffer_region);
  //gpr_free(context->send_buffer_region);
  //gpr_free(context);
  //rdma_destroy_id(id);
  rdma_ctx_unref(exec_ctx, context);

  return 0;
}

static void grpc_rdma_client_on_event(grpc_exec_ctx *exec_ctx, 
    void *arg,
    grpc_error *error) {
  async_connect *ac = arg;
  grpc_endpoint **ep = ac->ep;
  struct rdma_event_channel *ec = ac->ec;
  grpc_closure *closure = ac->closure;
  struct connect_context *context = NULL;

  struct rdma_cm_event event_handle;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *id = NULL;

  int timeout;
  int done;

  char *name = NULL;
  struct rdma_conn_param cm_params;
  int poll_count = 0;

  GRPC_ERROR_REF(error);

  if (error != GRPC_ERROR_NONE ) {
    goto error;
  }
  
  gpr_mu_lock(&ac->mu);
  timeout = ac->time_out ;
  gpr_mu_unlock(&ac->mu);

  if (timeout == 1) {
    gpr_log(GPR_ERROR, "Client: Timeout occurred");
    error =
      grpc_error_set_str(error, GRPC_ERROR_STR_OS_ERROR, "Timeout occurred");
    goto error;
  }

  while (rdma_get_cm_event(ec, &event) != 0) {
    if (errno == EAGAIN && ++poll_count < 100){
      usleep(50000);
    } else{
      gpr_log(GPR_ERROR, "Client: get_cm_event failed:%d",errno);
      goto error;
    }
  }
  memcpy(&event_handle, event, sizeof(*event));
  id = event_handle.id;
  rdma_ack_cm_event(event);
  switch (event_handle.event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      gpr_log(GPR_DEBUG, "Client: on_event RDMA_CM_EVENT_ADDR_RESOLVED");
      if (on_addr_resolved(id) != 0) {
        error = grpc_error_set_str(error, 
            GRPC_ERROR_STR_DESCRIPTION, "Failed on on_addr_resolved");
        goto error;
      }
      break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      gpr_log(GPR_DEBUG, "Client: on_event RDMA_CM_EVENT_ROUTE_RESOLVED");
      memset(&cm_params, 0, sizeof(cm_params));
      if ((rdma_connect(id, &cm_params)) != 0) {
        error = grpc_error_set_str(error,
            GRPC_ERROR_STR_DESCRIPTION, "Failed on rdma_connect");
        goto error;
      }
      break;
    case RDMA_CM_EVENT_ESTABLISHED:
      gpr_log(GPR_DEBUG, "Client: on_event RDMA_CM_EVENT_ESTABLISHED");
      gpr_mu_lock(&ac->mu);
      ac->connected = 1;
      gpr_mu_unlock(&ac->mu);
      //grpc_timer_cancel(exec_ctx, &ac->alarm);

      gpr_asprintf(&name, "rdma-client:%s", ac->addr_str); 
      context = (struct connect_context *)id->context;
      context->sendfdobj = grpc_fd_create(context->sendfd, name);
      context->recvfdobj = grpc_fd_create(context->recvfd, name);
      context->closure = &ac->write_closure;
      ac->sendfdobj = context->sendfdobj;
      ac->recvfdobj = context->recvfdobj;
      *ep = grpc_rdma_create(id->context, ac->addr_str);
      context->ep = *ep;

      grpc_exec_ctx_sched(exec_ctx, closure, error, NULL);
      ac->cb_called = 1;

      gpr_free(name);
      break;
    case RDMA_CM_EVENT_DISCONNECTED:
      gpr_log(GPR_DEBUG, "Client: on_event RDMA_CM_EVENT_DISCONNECTED");
      on_disconnect(exec_ctx, id);
      goto done;
      break;
    default:
      gpr_log(GPR_ERROR, "Client: on_event Unknow RDMA_CM_EVENT:%d", event_handle.event);
      error = grpc_error_set_str(error, GRPC_ERROR_STR_OS_ERROR, "Get Unknow RDMA_CN_EVENT");
      goto error;
      break;
  }

  grpc_fd_notify_on_read(exec_ctx, ac->connectfd, &ac->write_closure);

  return;

error:
  //grpc_timer_cancel(exec_ctx, &ac->alarm);
  error = grpc_error_set_str(error, 
      GRPC_ERROR_STR_DESCRIPTION, "Failed to connect to remote host");
  error = grpc_error_set_str(error, 
                  GRPC_ERROR_STR_TARGET_ADDRESS, ac->addr_str);

  grpc_exec_ctx_sched(exec_ctx, closure, error, NULL);
  ac->cb_called = 1;

done:
  //if (id != NULL) {
  //  if (id->verbs->device)
  //  {}//FIXME//rdma_disconnect(id);
  //}
  if (ac->connectfd != NULL) {
    //grpc_pollset_set_del_fd(exec_ctx, ac->interested_parties, ac->connectfd);
    grpc_fd_orphan(exec_ctx, ac->connectfd, NULL, NULL, "tcp_client_orphan");
    ac->connectfd = NULL;
  }

  gpr_mu_lock(&ac->mu);
  done = (--ac->refs == 0);
  gpr_mu_unlock(&ac->mu);
  if (done) {
    if (ac->addr_str != NULL) {
      gpr_free(ac->addr_str);
      ac->addr_str = NULL;
    }
    if (ac != NULL) {
      gpr_mu_destroy(&ac->mu);
      gpr_free(ac);
      ac = NULL;
    }
  }
  return;
}


#define TIMEOUT_IN_MS 500
static void rdma_client_connect_impl(grpc_exec_ctx *exec_ctx,
    grpc_closure *closure, grpc_endpoint **ep,
    grpc_pollset_set *interested_parties,
    const struct sockaddr *addr,
    size_t addr_len, gpr_timespec deadline) {
  int connectfd;
  async_connect *ac =  gpr_malloc(sizeof(async_connect));
  //struct sockaddr_in6 addr6_v4mapped;
  //struct sockaddr_in addr4_copy;
  grpc_fd *fdobj;
  char *name;
  char *addr_str;
  //FIXME//grpc_error *error;

  *ep = NULL;


  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;

  //int port = grpc_sockaddr_get_port(addr);
  //struct sockaddr_in addrx;
  //memset(&addrx,0,sizeof(struct sockaddr_in));
  //addrx.sin_family=AF_INET;
  //grpc_sockaddr_set_port((const struct sockaddr *)&addrx,port);

  //test_print_addr(addr); //FIXME
  /* Use dualstack sockets where available. */
  //if (grpc_sockaddr_to_v4mapped(addr, &addr6_v4mapped)) {
  //  addr = (const struct sockaddr *)&addr6_v4mapped;
  //  addr_len = sizeof(addr6_v4mapped);
  //}

  //error = grpc_create_dualstack_socket(addr, SOCK_STREAM, 0, &dsmode, &fd);
  if ((ec = rdma_create_event_channel()) == NULL) {
    grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_create_event_channel"), NULL);
    return;
  }
  if (rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP) != 0) {
    grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_create_id"), NULL);
    rdma_destroy_event_channel(ec);
    return;
  }

  gpr_log(GPR_DEBUG, "rdma_client_connect_impl");
  grpc_rdma_util_print_addr(addr);

  if (rdma_resolve_addr(conn, NULL, (struct sockaddr *)addr, gpr_time_to_millis(deadline)) != 0) {
    grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_resolve_addr"), NULL);
    rdma_destroy_id(conn); 
    rdma_destroy_event_channel(ec);
    gpr_log(GPR_ERROR, "Client: rdma_resolve_addr Timeout occurred");
    return;
  }

  int flags;
  flags = fcntl(ec->fd, F_GETFL);
  if (fcntl(ec->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    gpr_log(GPR_ERROR, "Client: fcntl set non-blocking failed");
  }
  connectfd = ec->fd;

  //if (error != GRPC_ERROR_NONE) {
  //  grpc_exec_ctx_sched(exec_ctx, closure, error, NULL);
  //  return;
  //}

  //if (dsmode == GRPC_DSMODE_IPV4) {
  //  /* If we got an AF_INET socket, map the address back to IPv4. */
  //  GPR_ASSERT(grpc_sockaddr_is_v4mapped(addr, &addr4_copy));
  //  addr = (struct sockaddr *)&addr4_copy;
  //  addr_len = sizeof(addr4_copy);
  //}

  //if ((error = prepare_socket(addr, fd)) != GRPC_ERROR_NONE) { 
  //  grpc_exec_ctx_sched(exec_ctx, closure, error, NULL);
  //  return;
  //}

  //do {
  //  GPR_ASSERT(addr_len < ~(socklen_t)0);
  //  err = connect(fd, addr, (socklen_t)addr_len); 
  //} while (err < 0 && errno == EINTR);
  addr_str = grpc_sockaddr_to_uri(addr);
  //gpr_asprintf(&name, "tcp-client:%s", addr_str); 
  gpr_asprintf(&name, "rdma-client-connection:%s", addr_str); 
  fdobj = grpc_fd_create(connectfd, name); 

  //if (err >= 0) {
  //  *ep = grpc_tcp_create(fdobj, GRPC_TCP_DEFAULT_READ_SLICE_SIZE, addr_str);
  //  grpc_exec_ctx_sched(exec_ctx, closure, GRPC_ERROR_NONE, NULL);
  //  goto done;
  //}

  //*ep =  grpc_rdma_create(fdobj, GRPC_RDMA_DEFAULT_READ_BUFFER_SIZE, addr_str); //FIXME
  //grpc_exec_ctx_sched(exec_ctx, closure, GRPC_ERROR_NONE, NULL);
  //goto done;

  //if (errno != EWOULDBLOCK && errno != EINPROGRESS) {
  //  grpc_fd_orphan(exec_ctx, fdobj, NULL, NULL, "tcp_client_connect_error");
  //  grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "connect"),
  //                      NULL);
  //  goto done;
  //}

  grpc_pollset_set_add_fd(exec_ctx, interested_parties, fdobj); //FIXME

  memset(ac, 0, sizeof(async_connect));
  ac->closure = closure;
  ac->ec = ec;
  ac->ep = ep;
  ac->connectfd = fdobj;
  ac->interested_parties = interested_parties;
  ac->addr_str = addr_str;
  addr_str = NULL;
  gpr_mu_init(&ac->mu);
  ac->refs = 2;
  ac->write_closure.cb = grpc_rdma_client_on_event;
  ac->write_closure.cb_arg = ac;
  ac->time_out=0;
  ac->connected=0;
  ac->cb_called=0;
  ac->sendfdobj =NULL;
  ac->recvfdobj =NULL;
  //if (grpc_tcp_trace) {
  //  gpr_log(GPR_DEBUG, "CLIENT_CONNECT: %s: asynchronously connecting",
  //          ac->addr_str);
  //}
  gpr_mu_lock(&ac->mu);
  grpc_timer_init(exec_ctx, &ac->alarm,
      gpr_convert_clock_type(deadline, GPR_CLOCK_MONOTONIC),
      tc_on_alarm, ac, gpr_now(GPR_CLOCK_MONOTONIC));
  /* lkx810 : Use grpc_fd_notify_on_write() to get rdma ec event 
   * until get event RDMA_CM_EVENT_ESTABLISHED */
  grpc_fd_notify_on_read(exec_ctx, ac->connectfd, &ac->write_closure);
  gpr_mu_unlock(&ac->mu);
  gpr_free(name);
}

// overridden by api_fuzzer.c
void (*grpc_rdma_client_connect_impl)(
    grpc_exec_ctx *exec_ctx, grpc_closure *closure, grpc_endpoint **ep,
    grpc_pollset_set *interested_parties, const struct sockaddr *addr,
    size_t addr_len, gpr_timespec deadline) = rdma_client_connect_impl;

void grpc_rdma_client_connect(grpc_exec_ctx *exec_ctx, grpc_closure *closure,
    grpc_endpoint **ep,
    grpc_pollset_set *interested_parties,
    const struct sockaddr *addr, size_t addr_len,
    gpr_timespec deadline) {
  grpc_rdma_client_connect_impl(exec_ctx, closure, ep, interested_parties, addr,
      addr_len, deadline);
}

#endif
