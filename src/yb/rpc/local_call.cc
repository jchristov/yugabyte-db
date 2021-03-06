//
// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
//

#include "yb/rpc/local_call.h"

#include "yb/rpc/rpc_controller.h"
#include "yb/util/memory/memory.h"

namespace yb {
namespace rpc {

using std::shared_ptr;

LocalOutboundCall::LocalOutboundCall(
    const ConnectionId& conn_id, const RemoteMethod& remote_method,
    const shared_ptr<OutboundCallMetrics>& outbound_call_metrics,
    google::protobuf::Message* response_storage, RpcController* controller,
    ResponseCallback callback)
    : OutboundCall(
          conn_id, remote_method, outbound_call_metrics, response_storage, controller, callback) {
}

Status LocalOutboundCall::SetRequestParam(const google::protobuf::Message& req) {
  req_ = &req;
  return Status::OK();
}

void LocalOutboundCall::Serialize(std::deque<RefCntBuffer> *output) const {
  LOG(FATAL) << "local call should not require serialization";
}

const std::shared_ptr<LocalYBInboundCall>& LocalOutboundCall::CreateLocalInboundCall() {
  DCHECK(inbound_call_.get() == nullptr);
  const MonoDelta timeout = controller()->timeout();
  const MonoTime deadline = timeout.Initialized() ? start_ + timeout : MonoTime::Max();
  auto outbound_call = std::static_pointer_cast<LocalOutboundCall>(shared_from(this));
  inbound_call_ = std::make_shared<LocalYBInboundCall>(remote_method(), outbound_call, deadline);
  return inbound_call_;
}

Status LocalOutboundCall::GetSidecar(int idx, Slice* sidecar) const {
  if (idx < 0 || idx >= inbound_call_->sidecars().size()) {
    return STATUS(InvalidArgument, strings::Substitute(
        "Index $0 does not reference a valid sidecar", idx));
  }
  const RefCntBuffer& car = inbound_call_->sidecars()[idx];
  *sidecar = Slice(car.udata(), car.size());
  return Status::OK();
}

LocalYBInboundCall::LocalYBInboundCall(
    const RemoteMethod& remote_method, std::weak_ptr<LocalOutboundCall> outbound_call,
    const MonoTime& deadline)
    : YBInboundCall(remote_method), outbound_call_(outbound_call), deadline_(deadline) {
}

const Endpoint& LocalYBInboundCall::remote_address() const {
  static const Endpoint endpoint;
  return endpoint;
}

const Endpoint& LocalYBInboundCall::local_address() const {
  static const Endpoint endpoint;
  return endpoint;
}

void LocalYBInboundCall::Respond(const google::protobuf::MessageLite& response, bool is_success) {
  if (is_success) {
    outbound_call()->SetFinished();
  } else {
    outbound_call()->SetFailed(STATUS(RemoteError, "Local call error"),
                               new ErrorStatusPB(down_cast<const ErrorStatusPB&>(response)));
  }
}

Status LocalYBInboundCall::ParseParam(google::protobuf::Message* message) {
  LOG(FATAL) << "local call should not require parsing";
}

} // namespace rpc
} // namespace yb
