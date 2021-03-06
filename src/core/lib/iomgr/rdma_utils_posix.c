
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <rdma/rdma_cma.h>

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>
#include <grpc/support/useful.h>

#include "src/core/lib/iomgr/rdma_utils_posix.h" 
void rdma_ctx_free(grpc_exec_ctx *exec_ctx, connect_context *);
void grpc_rdma_util_print_addr(const struct sockaddr *addr);

void grpc_rdma_util_print_addr(const struct sockaddr *addr){
  GPR_ASSERT( ((struct sockaddr*)addr)->sa_family == AF_INET ||  ((struct sockaddr*)addr)->sa_family == AF_INET6 );
  char *addr_str;
  grpc_sockaddr_to_string(&addr_str, (struct sockaddr *)addr, 1);
  if ( ((struct sockaddr*)addr)->sa_family == AF_INET ) {
	gpr_log(GPR_DEBUG,"ADDR: AF_INET addr:%s port:%d",addr_str,grpc_sockaddr_get_port((struct sockaddr *)addr));
  }
  else if ( ((struct sockaddr*)addr)->sa_family == AF_INET6 ) {
	gpr_log(GPR_DEBUG,"ADDR: AF_INET6 addr:%s port:%d",addr_str,grpc_sockaddr_get_port((struct sockaddr *)addr));
  }
  gpr_free(addr_str);
}



void die(const char *reason) {
    fprintf(stderr, "%s\n", reason);
    fprintf(stderr, "%d", errno);
    exit(1);
}


void rdma_ctx_unref(grpc_exec_ctx *exec_ctx, struct connect_context *context) {
  if (gpr_unref(&context->refcount)) {
    rdma_ctx_free(exec_ctx, context);
  }
}

void rdma_ctx_ref(struct connect_context *context) { 
  gpr_ref(&context->refcount); 
}

void rdma_ctx_free(grpc_exec_ctx *exec_ctx, struct connect_context *context) {
  grpc_fd_orphan(exec_ctx,context->sendfdobj,NULL,NULL,"RDMACTX_FREE");
  grpc_fd_orphan(exec_ctx,context->recvfdobj,NULL,NULL,"RDMACTX_FREE");
  ibv_dereg_mr(context->recv_buffer_mr); 
  ibv_dereg_mr(context->send_buffer_mr); 
  ibv_dealloc_pd(context->pd);
  ibv_destroy_cq(context->send_cq);
  ibv_destroy_cq(context->recv_cq);
  //rdma_destroy_qp(id);
  //ibv_destroy_comp_channel(context->comp_channel);
  gpr_free(context->recv_buffer_region);
  gpr_free(context->send_buffer_region);

  rdma_destroy_id(context->id);
  gpr_free(context);
}
