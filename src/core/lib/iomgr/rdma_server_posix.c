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

/* FIXME: "posix" files shouldn't be depending on _GNU_SOURCE */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <grpc/support/port_platform.h>

#ifdef GPR_POSIX_SOCKET

#include "src/core/lib/iomgr/tcp_server.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>
#include <grpc/support/useful.h>

#include "src/core/lib/iomgr/resolve_address.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/iomgr/socket_utils_posix.h"
#include "src/core/lib/iomgr/tcp_posix.h"
#include "src/core/lib/iomgr/unix_sockets_posix.h"
#include "src/core/lib/support/string.h"


#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "src/core/lib/iomgr/rdma_utils_posix.h"
#include "src/core/lib/iomgr/rdma_server.h"

#include "src/core/lib/iomgr/rdma_cm.h"

#define MIN_SAFE_ACCEPT_QUEUE_SIZE 100

void test_print_addr(const struct sockaddr *addr){
  GPR_ASSERT( ((struct sockaddr*)addr)->sa_family == AF_INET ||  ((struct sockaddr*)addr)->sa_family == AF_INET6 );
  char *addr_str;
  grpc_sockaddr_to_string(&addr_str, (struct sockaddr *)addr, 1);
  if ( ((struct sockaddr*)addr)->sa_family == AF_INET ) {
	gpr_log(GPR_DEBUG,"ADDR: to:AF_INET addr:%s port:%d",addr_str,grpc_sockaddr_get_port((struct sockaddr *)addr));
  }
  else if ( ((struct sockaddr*)addr)->sa_family == AF_INET6 ) {
	gpr_log(GPR_DEBUG,"ADDR: AF_INET6 addr:%s port:%d",addr_str,grpc_sockaddr_get_port((struct sockaddr *)addr));
  }

  gpr_free(addr_str);
}

//static gpr_once s_init_max_accept_queue_size;
//static int s_max_accept_queue_size;

typedef struct grpc_rdma_server grpc_rdma_server;
/* one listening port */
typedef struct grpc_rdma_listener grpc_rdma_listener;
struct grpc_rdma_listener {
  //int fd;
  struct rdma_event_channel *ec;
  grpc_fd *emfd;
  struct rdma_cm_id *listener;
  grpc_fd *sendfdobj;
  grpc_fd *recvfdobj;

  grpc_rdma_server *server;
  union {
    uint8_t untyped[GRPC_MAX_SOCKADDR_SIZE];
    struct sockaddr sockaddr;
  } addr;
  size_t addr_len;
  int port;
  unsigned port_index;
  unsigned fd_index;
  grpc_closure read_closure;
  grpc_closure destroyed_closure;
  struct grpc_rdma_listener *next;
  /* When we add a listener, more than one can be created, mainly because of
     IPv6. A sibling will still be in the normal list, but will be flagged
     as such. Any action, such as ref or unref, will affect all of the
     siblings in the list. */
  struct grpc_rdma_listener *sibling;
  int is_sibling;
};

/* the overall server */
struct grpc_rdma_server {
  gpr_refcount refs;
  /* Called whenever accept() succeeds on a server port. */
  grpc_rdma_server_cb on_accept_cb;
  void *on_accept_cb_arg;

  gpr_mu mu;

  /* active port count: how many ports are actually still listening */
  size_t active_ports; //
  size_t active_listeners;
  /* destroyed port count: how many ports are completely destroyed */
  size_t destroyed_ports; //
  size_t destroyed_listeners;

  /* is this server shutting down? */
  bool shutdown;

  /* linked list of server ports */
  grpc_rdma_listener *head;
  grpc_rdma_listener *tail;
  unsigned nports; //
  unsigned nlisteners; //

  /* List of closures passed to shutdown_starting_add(). */
  grpc_closure_list shutdown_starting;

  /* shutdown callback */
  grpc_closure *shutdown_complete;

  /* all pollsets interested in new connections */
  grpc_pollset **pollsets;
  /* number of pollsets in the pollsets array */
  size_t pollset_count;

  /* next pollset to assign a channel to */
  gpr_atm next_pollset_to_assign;
};

static gpr_once check_init = GPR_ONCE_INIT;
//static bool has_so_reuseport;

static void init(void) {
    //TODO
}

grpc_error *grpc_rdma_server_create(grpc_closure *shutdown_complete,
                                   grpc_rdma_server **server) {
  gpr_once_init(&check_init, init);
  grpc_rdma_server *s = gpr_malloc(sizeof(grpc_rdma_server));

  gpr_ref_init(&s->refs, 1);
  gpr_mu_init(&s->mu);
  s->active_ports = 0;
  s->destroyed_ports = 0;
  s->shutdown = false;
  s->shutdown_starting.head = NULL;
  s->shutdown_starting.tail = NULL;
  s->shutdown_complete = shutdown_complete;
  s->on_accept_cb = NULL;
  s->on_accept_cb_arg = NULL;
  s->head = NULL;
  s->tail = NULL;
  s->nports = 0;
  gpr_atm_no_barrier_store(&s->next_pollset_to_assign, 0);
  *server = s;
  return GRPC_ERROR_NONE;
}

static void finish_shutdown(grpc_exec_ctx *exec_ctx, grpc_rdma_server *s) {
  if (s->shutdown_complete != NULL) {
    grpc_exec_ctx_sched(exec_ctx, s->shutdown_complete, GRPC_ERROR_NONE, NULL);
  }

  gpr_mu_destroy(&s->mu);

  while (s->head) {
    grpc_rdma_listener *sp = s->head;
    s->head = sp->next;
    gpr_free(sp);
  }

  gpr_free(s);
}

static void destroyed_listener(grpc_exec_ctx *exec_ctx, void *server,
                           grpc_error *error) {
  grpc_rdma_server *s = server;
  gpr_mu_lock(&s->mu);
  s->destroyed_ports++;
  if (s->destroyed_ports == s->nports) {
    gpr_mu_unlock(&s->mu);
    finish_shutdown(exec_ctx, s);
  } else {
    GPR_ASSERT(s->destroyed_ports < s->nports);
    gpr_mu_unlock(&s->mu);
  }
}

/* called when all listening endpoints have been shutdown, so no further
   events will be received on them - at this point it's safe to destroy
   things */
static void deactivated_all_ports(grpc_exec_ctx *exec_ctx, grpc_rdma_server *s) {
  /* delete ALL the things */
  gpr_mu_lock(&s->mu);

  if (!s->shutdown) {
    gpr_mu_unlock(&s->mu);
    return;
  }

  if (s->head) {
    grpc_rdma_listener *sp;
    for (sp = s->head; sp; sp = sp->next) {
      grpc_unlink_if_unix_domain_socket(&sp->addr.sockaddr);
      sp->destroyed_closure.cb = destroyed_listener;
      sp->destroyed_closure.cb_arg = s;
      grpc_fd_orphan(exec_ctx, sp->emfd, &sp->destroyed_closure, NULL,
                     "rdma_listener_shutdown");
    }
    gpr_mu_unlock(&s->mu);
  } else {
    gpr_mu_unlock(&s->mu);
    finish_shutdown(exec_ctx, s);
  }
}

static void rdma_server_destroy(grpc_exec_ctx *exec_ctx, grpc_rdma_server *s) {
  gpr_mu_lock(&s->mu);

  GPR_ASSERT(!s->shutdown);
  s->shutdown = true;

  /* shutdown all fd's */
  if (s->active_ports) {
    grpc_rdma_listener *sp;
    for (sp = s->head; sp; sp = sp->next) {
      grpc_fd_shutdown(exec_ctx, sp->emfd);
    }
    gpr_mu_unlock(&s->mu);
  } else {
    gpr_mu_unlock(&s->mu);
    deactivated_all_ports(exec_ctx, s);
  }
}

///* get max listen queue size on linux */
//static void init_max_accept_queue_size(void) {
//  int n = SOMAXCONN;
//  char buf[64];
//  FILE *fp = fopen("/proc/sys/net/core/somaxconn", "r");
//  if (fp == NULL) {
//    /* 2.4 kernel. */
//    s_max_accept_queue_size = SOMAXCONN;
//    return;
//  }
//  if (fgets(buf, sizeof buf, fp)) {
//    char *end;
//    long i = strtol(buf, &end, 10);
//    if (i > 0 && i <= INT_MAX && end && *end == 0) {
//      n = (int)i;
//    }
//  }
//  fclose(fp);
//  s_max_accept_queue_size = n;
//
//  if (s_max_accept_queue_size < MIN_SAFE_ACCEPT_QUEUE_SIZE) {
//    gpr_log(GPR_INFO,
//            "Suspiciously small accept queue (%d) will probably lead to "
//            "connection drops",
//            s_max_accept_queue_size);
//  }
//}
//
//static int get_max_accept_queue_size(void) {
//  gpr_once_init(&s_init_max_accept_queue_size, init_max_accept_queue_size);
//  return s_max_accept_queue_size;
//}

/* Prepare a recently-created socket for listening. */
//static grpc_error *prepare_listener(struct rdma_cm_id *listener, 
//                                    const struct sockaddr *addr,
//                                    size_t addr_len,
//                                    int *port) {
//  //struct sockaddr_storage sockname_temp;
//  socklen_t sockname_len;
//  grpc_error *err = GRPC_ERROR_NONE;
//
//  GPR_ASSERT(fd >= 0);
//
//  //TODO
//  err = grpc_set_socket_nonblocking(fd, 1);
//  if (err != GRPC_ERROR_NONE) goto error;
//  err = grpc_set_socket_cloexec(fd, 1);
//  if (err != GRPC_ERROR_NONE) goto error;
//  if (!grpc_is_unix_socket(addr)) {
//    err = grpc_set_socket_low_latency(fd, 1);
//    if (err != GRPC_ERROR_NONE) goto error;
//    err = grpc_set_socket_reuse_addr(fd, 1);
//    if (err != GRPC_ERROR_NONE) goto error;
//  }
//  err = grpc_set_socket_no_sigpipe_if_possible(fd);
//  if (err != GRPC_ERROR_NONE) goto error;
//
//
//  //
//  //GPR_ASSERT(addr_len < ~(socklen_t)0);
//  //if (bind(fd, addr, (socklen_t)addr_len) < 0) {
//  //  err = GRPC_OS_ERROR(errno, "bind");
//  //  goto error;
//  //}
//
//  //if (listen(fd, get_max_accept_queue_size()) < 0) {
//  //  err = GRPC_OS_ERROR(errno, "listen");
//  //  goto error;
//  //}
//
//  //TODO 
//  //sockname_len = sizeof(sockname_temp);
//  //if (getsockname(fd, (struct sockaddr *)&sockname_temp, &sockname_len) < 0) {
//  //  err = GRPC_OS_ERROR(errno, "getsockname");
//  //  goto error;
//  //}
//
//  //*port = grpc_sockaddr_get_port((struct sockaddr *)&sockname_temp);
//  //
//  *port = grpc_sockaddr_get_port((struct sockaddr *)&addr);
//  return GRPC_ERROR_NONE;
//
//error:
//  GPR_ASSERT(err != GRPC_ERROR_NONE);
//  if (fd >= 0) {
//    close(fd);
//  }
//  grpc_error *ret = grpc_error_set_int(
//      GRPC_ERROR_CREATE_REFERENCING("Unable to configure socket", &err, 1),
//      GRPC_ERROR_INT_FD, fd);
//  GRPC_ERROR_UNREF(err);
//  return ret;
//}

static int on_connect_request(struct rdma_cm_id *id) {
    struct ibv_qp_init_attr qp_attr;
    struct rdma_conn_param cm_params;

    struct ibv_pd *pd = NULL;
    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;
    struct ibv_comp_channel *send_comp_channel = NULL;
    struct ibv_comp_channel *recv_comp_channel = NULL;

    struct connect_context *context = NULL;
    //int pollfd;
    //struct ibv_qp *qp;
    struct ibv_mr *recv_buffer_mr = NULL;
    struct ibv_mr *send_buffer_mr = NULL;
    char *recv_buffer_region = NULL;
    char *send_buffer_region = NULL;

    struct ibv_recv_wr wr[RDMA_POST_RECV_NUM], *bad_wr = NULL;
    struct ibv_sge sge[RDMA_POST_RECV_NUM];
    int i;
    uintptr_t uintptr_addr;

    //build_context(id->verbs);
    if ((pd = ibv_alloc_pd(id->verbs)) == NULL) {
         gpr_log(GPR_ERROR, "CLIENT: ibv_alloc_pd() failed: %s",strerror(errno));
         return -1;
    }
    if ((send_comp_channel = ibv_create_comp_channel(id->verbs)) == NULL) { //FIXME
         gpr_log(GPR_ERROR, "CLIENT: ibv_create_comp_channel() failed: %s",strerror(errno));
         return -1;
    }
    if ((recv_comp_channel = ibv_create_comp_channel(id->verbs)) == NULL) { //FIXME
         gpr_log(GPR_ERROR, "CLIENT: ibv_create_comp_channel() failed: %s",strerror(errno));
         return -1;
    }
    if ((send_cq = ibv_create_cq(id->verbs, 10, NULL, send_comp_channel, 0)) ==NULL) { //FIXME
         gpr_log(GPR_ERROR, "CLIENT: ibv_create_cq() failed: %s",strerror(errno));
         return -1;
    }
    if ((recv_cq = ibv_create_cq(id->verbs, 10, NULL, recv_comp_channel, 0)) ==NULL) { //FIXME
         gpr_log(GPR_ERROR, "CLIENT: ibv_create_cq() failed: %s",strerror(errno));
         return -1;
    }
    if ((ibv_req_notify_cq(recv_cq, 0)) != 0) {
         gpr_log(GPR_ERROR, "CLIENT: ibv_req_notify_cq() failed: %s",strerror(errno));
         return -1;
    }
    if ((ibv_req_notify_cq(send_cq, 0)) != 0) {
         gpr_log(GPR_ERROR, "CLIENT: ibv_req_notify_cq() failed: %s",strerror(errno));
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
    qp_attr.cap.max_send_wr =  128; //TODO
    qp_attr.cap.max_recv_wr =  128;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    //id->qp=NULL;
    if ((rdma_create_qp(id, pd, &qp_attr)) != 0) {
         gpr_log(GPR_ERROR, "CLIENT: rdma_create_qp() failed: %s",strerror(errno));
         return -1;
    }
 
   //register_memory(context);
    recv_buffer_region = gpr_malloc(INIT_RECV_BUFFER_SIZE * RDMA_POST_RECV_NUM); 
    send_buffer_region = gpr_malloc(INIT_RECV_BUFFER_SIZE); 
    
    if((recv_buffer_mr = ibv_reg_mr(
                pd,
                recv_buffer_region,
                INIT_RECV_BUFFER_SIZE * RDMA_POST_RECV_NUM,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
      == NULL) {
         gpr_log(GPR_ERROR, "CLIENT: ibv_reg_mr() failed: %s",strerror(errno));
         return -1;
    }

    if((send_buffer_mr = ibv_reg_mr(
                pd,
                send_buffer_region,
                INIT_RECV_BUFFER_SIZE ,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
      == NULL) {
         gpr_log(GPR_ERROR, "CLIENT: ibv_reg_mr() failed: %s",strerror(errno));
         return -1;
    }

    //build id->context
    context = (struct connect_context *)gpr_malloc(sizeof(struct connect_context));
    memset(context, 0, sizeof(struct connect_context));
    gpr_ref_init(&context->refcount, 1);
    context->sendfd = send_comp_channel->fd;
    context->recvfd = recv_comp_channel->fd;
    context->qp = id->qp;
    context->recv_buffer_region = recv_buffer_region;
    context->recv_buffer_mr = recv_buffer_mr;
    context->send_buffer_region = send_buffer_region;
    context->send_buffer_mr = send_buffer_mr;
    context->id = id;
    
    context->pd = pd;
    context->recv_comp_channel = recv_comp_channel;
    context->send_comp_channel = send_comp_channel;
    context->recv_cq = recv_cq;
    context->send_cq = send_cq;
    
    context->sendfdobj = NULL;
    context->recvfdobj = NULL;
    id->context = context;

    //post_receives(context);
    for (i=0; i<RDMA_POST_RECV_NUM; i++) {
        memset(&(wr[i]),0,sizeof(wr[i]));
        uintptr_addr = (uintptr_t)context->recv_buffer_region + (uintptr_t)i * INIT_RECV_BUFFER_SIZE;
        wr[i].wr_id = uintptr_addr;
        wr[i].next = &(wr[i+1]);
        if ( i == RDMA_POST_RECV_NUM - 1) {
             wr[i].next = NULL;
        }
        wr[i].sg_list = &(sge[i]);
        wr[i].num_sge = 1;

        sge[i].addr = uintptr_addr;
        sge[i].length = INIT_RECV_BUFFER_SIZE;
        sge[i].lkey = recv_buffer_mr->lkey;

    }

    if ((ibv_post_recv(context->qp, &(wr[0]), &bad_wr)) != 0){
            gpr_log(GPR_ERROR, "CLIENT: ibv_post_recv() failed: %s",strerror(errno));
            return -1;
    }

    //memset(&wr,0,sizeof(wr));
    //uintptr_addr = (uintptr_t)context->recv_buffer_region;
    //wr.wr_id = uintptr_addr;
    //wr.next = NULL;
    //wr.sg_list = &sge;
    //wr.num_sge = 1;

    //sge.addr = uintptr_addr;
    //sge.length = INIT_RECV_BUFFER_SIZE;
    //sge.lkey = recv_buffer_mr->lkey;

    //if ((ibv_post_recv(context->qp, &wr, &bad_wr)) != 0){
    //        gpr_log(GPR_ERROR, "CLIENT: ibv_post_recv() failed: %s",strerror(errno));
    //        return -1;
    //}

    memset(&cm_params, 0, sizeof(cm_params));
    if ((rdma_accept(id, &cm_params)) != 0) {
	gpr_log(GPR_ERROR, "CLIENT: rdma_accept() failed: %s",strerror(errno));
	return -1;
    }


    return 0;
}


static int on_disconnect(grpc_exec_ctx *exec_ctx, struct rdma_cm_id *id) {
    struct connect_context *context = (struct connect_context *)id->context;
    if (context->refcount.count >= 2) {
        grpc_rdma_sentence_death(exec_ctx, context->ep);
    }
    //ibv_dereg_mr(context->recv_buffer_mr); 
    //ibv_dereg_mr(context->send_buffer_mr); 
    //ibv_dealloc_pd(context->pd);
    //ibv_destroy_cq(context->send_cq);
    //ibv_destroy_cq(context->recv_cq);
    ////rdma_destroy_qp(id);
    ////ibv_destroy_comp_channel(context->comp_channel);

    //gpr_free(context->recv_buffer_region);
    //gpr_free(context->send_buffer_region);
    //gpr_free(id->context);
    //rdma_destroy_id(id);
    rdma_ctx_unref(exec_ctx, context);

    
    return 0;
}

/* event manager callback when reads are ready */
//static void on_read(grpc_exec_ctx *exec_ctx, void *arg, grpc_error *err) {
static int on_connection(grpc_exec_ctx *exec_ctx, grpc_rdma_listener *sp,
                                     void *ctx) {
  struct connect_context *context = (struct connect_context *)ctx;

  grpc_rdma_server_acceptor acceptor = {sp->server, sp->port_index,
                                       sp->fd_index}; 
  grpc_pollset *read_notifier_pollset = NULL;
  struct sockaddr *addr;
  char *addr_str;
  char *name;

  //if (err != GRPC_ERROR_NONE) {
  //  goto error;
  //}
  read_notifier_pollset =
      sp->server->pollsets[(size_t)gpr_atm_no_barrier_fetch_add(
                               &sp->server->next_pollset_to_assign, 1) %
                           sp->server->pollset_count];
  /* loop until accept4 returns EAGAIN, and then re-arm notification */
  //for (;;) {
  //  struct sockaddr_storage addr;
  //  socklen_t addrlen = sizeof(addr);
  //  char *addr_str;
  //  char *name;
  //  /* Note: If we ever decide to return this address to the user, remember to
  //     strip off the ::ffff:0.0.0.0/96 prefix first. */
  //  int fd = grpc_accept4(sp->fd, (struct sockaddr *)&addr, &addrlen, 1, 1);
  //  if (fd < 0) {
  //    switch (errno) {
  //      case EINTR:
  //        continue;
  //      case EAGAIN:
  //        grpc_fd_notify_on_read(exec_ctx, sp->emfd, &sp->read_closure);
  //        return;
  //      default:
  //        gpr_log(GPR_ERROR, "Failed accept4: %s", strerror(errno));
  //        goto error;
  //    }
  //  }

  //grpc_set_socket_no_sigpipe_if_possible(fd);
    
    addr = rdma_get_peer_addr(context->id);

    addr_str = grpc_sockaddr_to_uri((struct sockaddr *)addr);
    gpr_asprintf(&name, "tcp-server-connection:%s", addr_str);
    /*
    if (grpc_tcp_trace) {
      gpr_log(GPR_DEBUG, "SERVER_CONNECT: incoming connection: %s", addr_str);
    }
*/


    if (read_notifier_pollset == NULL) {
      gpr_log(GPR_ERROR, "Read notifier pollset is not set on the fd");
      //goto error;
      return -1;
    }

    context->sendfdobj = grpc_fd_create(context->sendfd, name);
    context->recvfdobj = grpc_fd_create(context->recvfd, name);
    grpc_pollset_add_fd(exec_ctx, read_notifier_pollset, context->sendfdobj);
    grpc_pollset_add_fd(exec_ctx, read_notifier_pollset, context->recvfdobj);

    context->ep = grpc_rdma_create(context, addr_str);

    //grpc_rdma_create(fdobj, GRPC_TCP_DEFAULT_READ_SLICE_SIZE, addr_str), 
    sp->server->on_accept_cb(
        exec_ctx, sp->server->on_accept_cb_arg,
	context->ep,
        read_notifier_pollset, &acceptor);

    gpr_free(name);
    gpr_free(addr_str);
  //}

  //GPR_UNREACHABLE_CODE(return );

//error:
 return 0;
 }



static void grpc_rdma_server_on_event(grpc_exec_ctx *exec_ctx,
                                        void *arg,
                                        grpc_error *err) {
    grpc_rdma_listener *sp = arg;
    struct rdma_event_channel *ec = sp->ec;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *id = NULL;
    //static struct rdma_cm_event event_handle;
    int ret = 0;

    if (err != GRPC_ERROR_NONE ) {
	goto error;
    } 

    grpc_fd_notify_on_read(exec_ctx, sp->emfd, &sp->read_closure); //poll again

    if (rdma_get_cm_event(ec, &event) != 0 ) {
	    return;
    }

    id = event->id;
    switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
	    gpr_log(GPR_DEBUG, "Server: on_event RDMA_CM_EVENT_CONNECT_REQUEST");
            ret = on_connect_request(event->id);
            rdma_ack_cm_event(event);
            break;
        case RDMA_CM_EVENT_ESTABLISHED:
	    gpr_log(GPR_DEBUG, "Server: on_event RDMA_CM_EVENT_ESTABLISHED");
            ret = on_connection(exec_ctx, sp, event->id->context);
            rdma_ack_cm_event(event);
            break;
        case RDMA_CM_EVENT_DISCONNECTED:
	    gpr_log(GPR_DEBUG, "Server: on_event RDMA_CM_EVENT_DISCONNECTED");
            rdma_ack_cm_event(event);
            ret = on_disconnect(exec_ctx, id);
            break;
        default:
	    gpr_log(GPR_ERROR, "Server: on_event Unknow RDMA_CM_EVENT:%d", event->event);
            ret = -1;
            break;
    }

    if (ret != 0) {
        goto error;
    }
    return;
error:
    gpr_log(GPR_ERROR, "Server:Get error");
    gpr_mu_lock(&sp->server->mu);
    if (0 == --sp->server->active_ports) {
        gpr_mu_unlock(&sp->server->mu);
        deactivated_all_ports(exec_ctx, sp->server);
    } else {
        gpr_mu_unlock(&sp->server->mu);
    }
}

static grpc_error *add_listener_to_server(grpc_rdma_server *s,
                                         const struct sockaddr *addr,
                                         size_t addr_len, unsigned port_index,
                                         unsigned fd_index,
                                         grpc_rdma_listener **server_listener) {
  grpc_rdma_listener *sp = NULL;
  int port = -1;
  char *addr_str;
  char *name;
  int connectfd;
  int flags;

  grpc_error *err = GRPC_ERROR_NONE; 

  //grpc_error *err = prepare_listener(listener, addr, addr_len, &port);
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;

  if ((ec = rdma_create_event_channel()) == NULL) {
	  //grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_create_event_channel"), NULL);
	  return GRPC_OS_ERROR(errno, "rdma_create_event_channel");
  }
  if (rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP) != 0) {
	  //grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_create_id"), NULL);
	  rdma_destroy_event_channel(ec);
	  return GRPC_OS_ERROR(errno, "rdma_create_id");
  }

  if ((rdma_bind_addr(listener, (struct sockaddr *)addr)) != 0) {
	  //grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_bind_addr"), NULL);
	  rdma_destroy_id(listener);
	  rdma_destroy_event_channel(ec);
	  return GRPC_OS_ERROR(errno, "rdma_bind_addr");
  }

  if((rdma_listen(listener, 128)) != 0) { /* TODO get_max_accept_queue_size() == 128? */ 
          //grpc_exec_ctx_sched(exec_ctx, closure, GRPC_OS_ERROR(errno, "rdma_listen"), NULL);
          rdma_destroy_id(listener);
          rdma_destroy_event_channel(ec);
          return GRPC_OS_ERROR(errno, "rdma_listen");
  }
  port = ntohs(rdma_get_src_port(listener));

  flags = fcntl(ec->fd, F_GETFL);
  fcntl(ec->fd, F_SETFL, flags | O_NONBLOCK); //set non-blocking
  connectfd = ec->fd;

  if (err == GRPC_ERROR_NONE) {
    GPR_ASSERT(port > 0);
    grpc_sockaddr_to_string(&addr_str, (struct sockaddr *)addr, 1);
    gpr_asprintf(&name, "tcp-server-listener:%s", addr_str);
    gpr_mu_lock(&s->mu);
    s->nports++;
    GPR_ASSERT(!s->on_accept_cb && "must add ports before starting server");
    sp = gpr_malloc(sizeof(grpc_rdma_listener));
    sp->next = NULL;
    if (s->head == NULL) {
      s->head = sp;
    } else {
      s->tail->next = sp;
    }
    s->tail = sp;
    sp->server = s;
    //sp->fd = connectfd;
    sp->ec = ec;
    sp->listener = listener;
    sp->emfd = grpc_fd_create(connectfd, name);
    memcpy(sp->addr.untyped, addr, addr_len);
    sp->addr_len = addr_len;
    sp->port = port;
    sp->port_index = port_index;
    sp->fd_index = fd_index;
    sp->is_sibling = 0;
    sp->sibling = NULL;
    GPR_ASSERT(sp->emfd);
    gpr_mu_unlock(&s->mu);
    gpr_free(addr_str);
    gpr_free(name);
  }

  *server_listener = sp;
  return err;
}

grpc_error *grpc_rdma_server_add_port(grpc_rdma_server *s, const void *addr,
                                     size_t addr_len, int *out_port) {
  grpc_rdma_listener *sp;
  //grpc_rdma_listener *sp2 = NULL;
  //int fd;
  //grpc_dualstack_mode dsmode;
  //struct sockaddr_in6 addr6_v4mapped;
  //struct sockaddr_in wild4;
  //struct sockaddr_in6 wild6;
  //struct sockaddr_in addr4_copy;
  struct sockaddr *allocated_addr = NULL;
  //struct sockaddr_storage sockname_temp;
  struct sockaddr_storage *sockname_temp;
  //socklen_t sockname_len;
  int port;
  unsigned port_index = 0;
  unsigned fd_index = 0;
  grpc_error *errs[2] = {GRPC_ERROR_NONE, GRPC_ERROR_NONE};
  if (s->tail != NULL) {
    port_index = s->tail->port_index + 1;
  }

  grpc_unlink_if_unix_domain_socket((struct sockaddr *)addr); //FIXME

  /* Check if this is a wildcard port, and if so, try to keep the port the same
     as some previously created listener. */
  if (grpc_sockaddr_get_port(addr) == 0) {
      for (sp = s->head; sp; sp = sp->next) {
          //sockname_len = sizeof(sockname_temp);
          sockname_temp = (struct sockaddr_storage *)rdma_get_local_addr(sp->listener);
          //if (0 == getsockname(sp->fd, (struct sockaddr *)&sockname_temp, 
          //                     &sockname_len)) {
          //  port = grpc_sockaddr_get_port((struct sockaddr *)&sockname_temp);
          port = grpc_sockaddr_get_port((struct sockaddr *)&sockname_temp);
          if (port > 0) {
              allocated_addr = gpr_malloc(addr_len);
              memcpy(allocated_addr, addr, addr_len);
              grpc_sockaddr_set_port(allocated_addr, port);
              addr = allocated_addr;
              break;
              // }
          }
      }
  }

  sp = NULL;

  //if (grpc_sockaddr_to_v4mapped(addr, &addr6_v4mapped)) {
  //  addr = (const struct sockaddr *)&addr6_v4mapped;
  //  addr_len = sizeof(addr6_v4mapped);
  //}

  /* Treat :: or 0.0.0.0 as a family-agnostic wildcard. */
  //if (grpc_sockaddr_is_wildcard(addr, &port)) {
  //  grpc_sockaddr_make_wildcards(port, &wild4, &wild6);

  //  /* Try listening on IPv6 first. */
  //  addr = (struct sockaddr *)&wild6;
  //  addr_len = sizeof(wild6);
  //  //errs[0] = grpc_create_dualstack_socket(addr, SOCK_STREAM, 0, &dsmode, &fd);

  //  //if (errs[0] == GRPC_ERROR_NONE) {
  //  //  //errs[0] = add_socket_to_server(s, fd, addr, addr_len, port_index,
  //  //  //                               fd_index, &sp);
  //  
        errs[0] = add_listener_to_server(s, addr, addr_len,
                                        port_index, fd_index, &sp);
      if (errs[0] == GRPC_ERROR_NONE) {
          goto done;
      }
  //    //if (fd >= 0 && dsmode == GRPC_DSMODE_DUALSTACK) {
  //    //  goto done;
  //    //}

  //    //FIXME  : port
  //    //if (sp != NULL) {
  //    //  ++fd_index;
  //    //}
  //    ///* If we didn't get a dualstack socket, also listen on 0.0.0.0. */
  //    //if (port == 0 && sp != NULL) {
  //    //  grpc_sockaddr_set_port((struct sockaddr *)&wild4, sp->port);
  //    //}
  //    //
  //  //}
  //  addr = (struct sockaddr *)&wild4;
  //  addr_len = sizeof(wild4);
  //}

  ////struct rdma_cm_id *listener = NULL;
  ////errs[1] = grpc_create_rdma_listener_fd(addr, &dsmode, listener, &fd);
  ////if (errs[1] == GRPC_ERROR_NONE) {
  ////  if (dsmode == GRPC_DSMODE_IPV4 &&
  ////      grpc_sockaddr_is_v4mapped(addr, &addr4_copy)) {
  ////    addr = (struct sockaddr *)&addr4_copy;
  ////    addr_len = sizeof(addr4_copy);
  ////  }
  //  sp2 = sp;
  //  //errs[1] =
  //  //    add_socket_to_server(s, fd, addr, addr_len, port_index, fd_index, &sp);
  //  errs[1] = add_listener_to_server(s, addr, addr_len, 
  //                                      port_index, fd_index, &sp);
  //  if (sp2 != NULL && sp != NULL) {
  //    sp2->sibling = sp;
  //    sp->is_sibling = 1; //FIXME : sibling
  //  }
  ////}

done:
  gpr_free(allocated_addr);
  if (sp != NULL) {
    *out_port = sp->port;
    GRPC_ERROR_UNREF(errs[0]);
    GRPC_ERROR_UNREF(errs[1]);
    return GRPC_ERROR_NONE;
  } else {
    *out_port = -1;
    char *addr_str = grpc_sockaddr_to_uri(addr);
    grpc_error *err = grpc_error_set_str(
        GRPC_ERROR_CREATE_REFERENCING("Failed to add port to server", errs,
                                      GPR_ARRAY_SIZE(errs)),
        GRPC_ERROR_STR_TARGET_ADDRESS, addr_str);
    GRPC_ERROR_UNREF(errs[0]);
    GRPC_ERROR_UNREF(errs[1]);
    gpr_free(addr_str);
    return err;
  }
}

unsigned grpc_rdma_server_port_fd_count(grpc_rdma_server *s,
                                       unsigned port_index) {
  unsigned num_fds = 0;
  grpc_rdma_listener *sp;
  for (sp = s->head; sp && port_index != 0; sp = sp->next) {
    if (!sp->is_sibling) {
      --port_index;
    }
  }
  for (; sp; sp = sp->sibling, ++num_fds)
    ;
  return num_fds;
}

int grpc_rdma_server_port_fd(grpc_rdma_server *s, unsigned port_index,
                            unsigned fd_index) {
  grpc_rdma_listener *sp;
  for (sp = s->head; sp && port_index != 0; sp = sp->next) {
    if (!sp->is_sibling) {
      --port_index;
    }
  }
  for (; sp && fd_index != 0; sp = sp->sibling, --fd_index)
    ;
  if (sp) {
    return sp->ec->fd; //FIXME
  } else {
    return -1;
  }
}

struct sockaddr *grpc_rdma_server_port_addr(grpc_rdma_server *s, unsigned port_index,
                            unsigned fd_index) {
  grpc_rdma_listener *sp;
  for (sp = s->head; sp && port_index != 0; sp = sp->next) {
    if (!sp->is_sibling) {
      --port_index;
    }
  }
  for (; sp && fd_index != 0; sp = sp->sibling, --fd_index)
    ;
  if (sp) {
    return rdma_get_local_addr(sp->listener);
  } else {
    return NULL;
  }
}



void grpc_rdma_server_start(grpc_exec_ctx *exec_ctx, grpc_rdma_server *s,
                           grpc_pollset **pollsets, size_t pollset_count,
                           grpc_rdma_server_cb on_accept_cb,
                           void *on_accept_cb_arg) {
  size_t i;
  grpc_rdma_listener *sp;
  GPR_ASSERT(on_accept_cb);
  gpr_mu_lock(&s->mu);
  GPR_ASSERT(!s->on_accept_cb);
  GPR_ASSERT(s->active_ports == 0);
  s->on_accept_cb = on_accept_cb;
  s->on_accept_cb_arg = on_accept_cb_arg;
  s->pollsets = pollsets;
  s->pollset_count = pollset_count;
  sp = s->head;
  while (sp != NULL) {
     for (i = 0; i < pollset_count; i++) {
        grpc_pollset_add_fd(exec_ctx, pollsets[i], sp->emfd);
      }
      //sp->read_closure.cb = on_read;
      sp->read_closure.cb = grpc_rdma_server_on_event;
      sp->read_closure.cb_arg = sp;
      grpc_fd_notify_on_read(exec_ctx, sp->emfd, &sp->read_closure);

      s->active_ports++;
      sp = sp->next;
  }
  gpr_mu_unlock(&s->mu);
}

grpc_rdma_server *grpc_rdma_server_ref(grpc_rdma_server *s) {
  gpr_ref(&s->refs);
  return s;
}

void grpc_rdma_server_shutdown_starting_add(grpc_rdma_server *s,
                                           grpc_closure *shutdown_starting) {
  gpr_mu_lock(&s->mu);
  grpc_closure_list_append(&s->shutdown_starting, shutdown_starting,
                           GRPC_ERROR_NONE);
  gpr_mu_unlock(&s->mu);
}

void grpc_rdma_server_unref(grpc_exec_ctx *exec_ctx, grpc_rdma_server *s) {
  if (gpr_unref(&s->refs)) {
    /* Complete shutdown_starting work before destroying. */
    grpc_exec_ctx local_exec_ctx = GRPC_EXEC_CTX_INIT;
    gpr_mu_lock(&s->mu);
    grpc_exec_ctx_enqueue_list(&local_exec_ctx, &s->shutdown_starting, NULL);
    gpr_mu_unlock(&s->mu);
    if (exec_ctx == NULL) {
      grpc_exec_ctx_flush(&local_exec_ctx);
      rdma_server_destroy(&local_exec_ctx, s);
      grpc_exec_ctx_finish(&local_exec_ctx);
    } else {
      grpc_exec_ctx_finish(&local_exec_ctx);
      rdma_server_destroy(exec_ctx, s);
    }
  }
}

void grpc_rdma_server_shutdown_listeners(grpc_exec_ctx *exec_ctx,
                                        grpc_rdma_server *s) {
  gpr_mu_lock(&s->mu);
  /* shutdown all fd's */
  if (s->active_ports) {
    grpc_rdma_listener *sp;
    for (sp = s->head; sp; sp = sp->next) {
      grpc_fd_shutdown(exec_ctx, sp->emfd);
    }
  }
  gpr_mu_unlock(&s->mu);
}

#endif
