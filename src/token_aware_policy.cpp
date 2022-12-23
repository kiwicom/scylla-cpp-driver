/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "token_aware_policy.hpp"

#include "random.hpp"
#include "request_handler.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

// The number of replicas is bounded by replication factor per DC. In practice, the number
// of replicas is fairly small so a linear search should be extremely fast.
static inline bool contains(const CopyOnWriteHostVec& replicas, const Address& address) {
  for (HostVec::const_iterator i = replicas->begin(), end = replicas->end(); i != end; ++i) {
    if ((*i)->address() == address) {
      return true;
    }
  }
  return false;
}

void TokenAwarePolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                            const String& local_dc, const String& local_rack) {
  if (random != NULL) {
    if (shuffle_replicas_) {
      // Store random so that it can be used to shuffle replicas.
      random_ = random;
    } else {
      // Make sure that different instances of the token aware policy (e.g. different sessions)
      // don't use the same host order.
      index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
    }
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random, local_dc, local_rack);
}

static inline CopyOnWriteHostVec replicas_for_request(const String& keyspace,
                                                      const RoutableRequest* request,
                                                      const TokenMap* token_map) {
  if (token_map == NULL || keyspace.empty()) {
    return CopyOnWriteHostVec(NULL);
  }
  int64_t token_hint;
  bool has_token_hint;
  std::tie(token_hint, has_token_hint) = request->token_hint();
  if (has_token_hint) {
    return token_map->get_replicas_for_token(keyspace, token_hint);
  }
  String routing_key;
  if (!request->get_routing_key(&routing_key)) {
    return CopyOnWriteHostVec(NULL);
  }
  return token_map->get_replicas(keyspace, routing_key);
}

QueryPlan* TokenAwarePolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                            const TokenMap* token_map) {
  if (request_handler != NULL) {
    const RoutableRequest* request =
        static_cast<const RoutableRequest*>(request_handler->request());
    switch (request->opcode()) {
      {
        case CQL_OPCODE_QUERY:
        case CQL_OPCODE_EXECUTE:
        case CQL_OPCODE_BATCH:
	  CopyOnWriteHostVec replicas = replicas_for_request(keyspace, request, token_map);
          if (replicas && !replicas->empty()) {
            if (random_ != NULL) {
              random_shuffle(replicas->begin(), replicas->end(), random_);
            }
            return new TokenAwareQueryPlan(
                child_policy_.get(),
                child_policy_->new_query_plan(keyspace, request_handler, token_map), replicas,
                index_);
          }
          break;
      }

      default:
        break;
    }
  }
  return child_policy_->new_query_plan(keyspace, request_handler, token_map);
}

Host::Ptr TokenAwarePolicy::TokenAwareQueryPlan::compute_next() {
  while (remaining_local_ > 0) {
    --remaining_local_;
    const Host::Ptr& host((*replicas_)[index_++ % replicas_->size()]);
    if (child_policy_->is_host_up(host->address()) &&
        child_policy_->distance(host) == CASS_HOST_DISTANCE_LOCAL) {
      return host;
    }
  }

  while (remaining_remote_ > 0) {
    --remaining_remote_;
    const Host::Ptr& host((*replicas_)[index_++ % replicas_->size()]);
    if (child_policy_->is_host_up(host->address()) &&
        child_policy_->distance(host) == CASS_HOST_DISTANCE_REMOTE) {
      return host;
    }
  }

  while (remaining_remote2_ > 0) {
    --remaining_remote2_;
    const Host::Ptr& host((*replicas_)[index_++ % replicas_->size()]);
    if (child_policy_->is_host_up(host->address()) &&
        child_policy_->distance(host) == CASS_HOST_DISTANCE_REMOTE2) {
      return host;
    }
  }

  Host::Ptr host;
  while ((host = child_plan_->compute_next())) {
    if (!contains(replicas_, host->address()) ||
        child_policy_->distance(host) > CASS_HOST_DISTANCE_REMOTE2) {
      return host;
    }
  }
  return Host::Ptr();
}
