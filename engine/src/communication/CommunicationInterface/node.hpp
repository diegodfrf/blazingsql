#pragma once

#include <string>

#ifdef CUDF_SUPPORT
#include <ucp/api/ucp.h>
#endif

namespace comm {

/// \brief A Node is the representation of a RAL component used in the transport
/// process.
class node {
public:
#ifdef CUDF_SUPPORT
  node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker);
#endif

  node(int idx, std::string id, std::string ip, int port);

  int index() const;
  std::string id() const;

#ifdef CUDF_SUPPORT
  ucp_ep_h get_ucp_endpoint() const;
  ucp_worker_h get_ucp_worker() const;
#endif

  int port() const;
  std::string ip() const;
protected:
  int _idx;
  std::string _id;
  std::string _ip;
  int _port;
  
#ifdef CUDF_SUPPORT
  ucp_ep_h _ucp_ep;
  ucp_worker_h _ucp_worker;
#endif
};

}  // namespace comm
