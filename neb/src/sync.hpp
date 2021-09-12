#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <dory/ctrl/block.hpp>

#include "broadcastable.hpp"

namespace dory {
namespace neb {

using deliver_callback =
    std::function<void(uint64_t k, volatile const void *m, int proc_id)>;

namespace sync {
class NonEquivocatingBroadcast;
}
}  // namespace neb

using namespace neb;

class SyncNonEquivocatingBroadcast {
 public:
  SyncNonEquivocatingBroadcast(int self_id, std::vector<int> proc_ids,
                               neb::deliver_callback deliver_cb);
  ~SyncNonEquivocatingBroadcast();

  /**
   * @param uint64_t: message key
   * @param msg: message to broadcast
   */
  void broadcast(uint64_t k, Broadcastable &msg);

 private:
  std::unique_ptr<sync::NonEquivocatingBroadcast> impl;
  std::unique_ptr<ControlBlock> cb;
  dory::logger logger;
};
}  // namespace dory