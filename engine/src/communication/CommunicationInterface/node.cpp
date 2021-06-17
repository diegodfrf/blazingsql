#include "node.hpp"

namespace comm{

#ifdef CUDF_SUPPORT
node::node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker)
	: _idx{idx}, _id{id}, _ucp_ep{ucp_ep}, _ucp_worker{ucp_worker} {}
#endif

node::node(int idx, std::string id, std::string ip, int port)
	: _idx{idx}, _id{id}, _ip{ip}, _port{port} {}

int node::index() const { return _idx; }

std::string node::id() const { return _id; };

#ifdef CUDF_SUPPORT
ucp_ep_h node::get_ucp_endpoint() const { return _ucp_ep; }
#endif
std::string node::ip() const { return _ip; }

int node::port() const { return _port; }

#ifdef CUDF_SUPPORT
ucp_worker_h node::get_ucp_worker() const { return _ucp_worker; }
#endif

} // namespace comm
