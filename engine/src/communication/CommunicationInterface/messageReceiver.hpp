#pragma once

#include "protocols.hpp"
#include <vector>
#include <map>
#include <tuple>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <transport/ColumnTransport.h>
#include "bmr/BufferProvider.h"
#include "cache_machine/CacheMachine.h"


namespace comm {

/**
 * A struct that lets us access the request that the end points ucx-py generates.
 */
struct ucx_request {
	//!!!!!!!!!!! do not modify this struct this has to match what is found in
	// https://github.com/rapidsai/ucx-py/blob/branch-0.15/ucp/_libs/ucx_api.pyx
	// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
	int completed; /**< Completion flag that we do not use. */
	int uid;	   /**< We store a map of request uid ==> buffer_transport to manage completion of send */
};


/**
 * A struct for managing the 64 bit tag that ucx uses
 * This allow us to make a value that is stored in 8 bytes
 */
struct blazing_ucp_tag {
	int  message_id;			   /**< The message id which is generated by a global atomic*/
	uint16_t worker_origin_id; /**< The id to make sure each tag is unique */
	uint16_t frame_id;		   /**< The id of the frame being sent. 0 for being_transmission*/
};


  /**
  * @brief A Class used for the reconstruction of a BlazingTable from
  * metadata and column data
  */
class message_receiver {
using ColumnTransport = blazingdb::transport::ColumnTransport;

public:

  /**
  * @brief Constructs a message_receiver.
  *
  * This is a place for a message to receive chunks. It calls the deserializer after the complete
  * message has been assembled
  *
  * @param column_transports This is metadata about how a column will be reconstructed used by the deserialzer
  * @param metadata This is information about how the message was routed and payloads that are used in
  *                 execution, planning, or physical optimizations. E.G. num rows in table, num partitions to be processed
  * @param output_cache The destination for the message being received. It is either a specific cache inbetween
  *                     two kernels or it is intended for the general input cache using a mesage_id
  */
  message_receiver(const std::map<std::string, comm::node>& nodes, const std::vector<char>& buffer, std::shared_ptr<ral::cache::CacheMachine> input_cache);
  virtual ~message_receiver(){}

  size_t buffer_size(u_int16_t index);
  void allocate_buffer(uint16_t index, cudaStream_t stream = 0);
  node get_sender_node();
  size_t num_buffers();
  void confirm_transmission();
  void * get_buffer(uint16_t index);
  bool is_finished();
  void finish(cudaStream_t stream = 0);
private:


  std::vector<ColumnTransport> _column_transports;
  std::vector<ral::memory::blazing_chunked_column_info> _chunked_column_infos;
  std::shared_ptr<ral::cache::CacheMachine> _output_cache;
  ral::cache::MetadataDictionary _metadata;
  std::vector<size_t> _buffer_sizes;
  std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> > _raw_buffers;
  std::map<std::string, comm::node> _nodes_info_map;
  std::atomic<int> _buffer_counter;
  std::mutex _finish_mutex;
  bool _finished_called = false;
  std::shared_ptr<ral::cache::CacheMachine> input_cache;
};

} // namespace comm
