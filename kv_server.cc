#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <string>
#include <deque>

using namespace std;

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::ServerCompletionQueue;
using grpc::ServerAsyncResponseWriter;

using kvstore::DeleteReply;
using kvstore::DeleteRequest;
using kvstore::CommonReply;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutReply;
using kvstore::PutRequest;

unordered_map<string, string> cache;
int cache_size = 3;
deque<string> lruqueue;
unordered_map<string, int> lfumap;
string cache_type = "LRU";

int hashString(string s)
{
    int sum = 0;
    for (int i = 0; i < s.length(); i++)
    {
        sum += s[i];
    }
    return sum % 20;
}

string get_value_from_map(string key)
{

    if (cache_type.compare("LRU") == 0)
    {

        if (cache.find(key) == cache.end())
            return "";
        else
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lruqueue.push_front(key);
            return cache[key];
        }
    }

    if (cache_type.compare("LFU") == 0)
    {
        if (cache.find(key) == cache.end())
            return "";
        else
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lruqueue.push_front(key);
            lfumap[key] = lfumap[key] + 1;
            return cache[key];
        }
    }

    return "";
}

void put_value(string key, string value)
{
    if (cache_type.compare("LRU") == 0)
    {

        if (cache.find(key) == cache.end()) // if key is not present in cache
        {
            if (cache_size == lruqueue.size()) //  if cache size is full pop the last entry of cache
            {
                string last = lruqueue.back();
                lruqueue.pop_back();
                cache.erase(last);
            }
        }
        else // else if key is in lru queue remove it and add it again in the front
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;

            lruqueue.erase(iter);
            cache.erase(key);
        }

        lruqueue.push_front(key);
        cache[key] = value;
    }

    if (cache_type.compare("LFU") == 0)
    {

        if (cache.find(key) == cache.end()) // if key is not present in cache
        {
            if (cache_size == lruqueue.size()) //  if cache size is full pop the least frequent entry from cache
            {
                int minfreq = 9999;
                for (auto i : lfumap)
                {
                    if (i.second < minfreq)
                        minfreq = i.second;
                }
                auto iter = lruqueue.rbegin();
                for (; iter != lruqueue.rend(); ++iter)
                    if (lfumap[*iter] == minfreq)
                        break;
                deque<string>::iterator it = lruqueue.begin();
                while (*it != *iter)
                    it++;
                lruqueue.erase(it);
                cache.erase(*iter);
                lfumap.erase(*iter);
            }

            lruqueue.push_front(key);
            cache[key] = value;
            lfumap[key] = 0;
        }
        else // else if key is in cache increment frequency
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lruqueue.push_front(key);

            cache[key] = value;
            lfumap[key] += 1;
        }
    }
}

int delete_key(string key)
{
    if (cache_type.compare("LRU") == 0)
    {
        if (cache.find(key) == cache.end())
            return 0;
        else
        {
            cache.erase(key);
            deque<string>::iterator it = lruqueue.begin();
            while (*it != key)
                it++;
            lruqueue.erase(it);
            return 1; // success
        }
    }

    if (cache_type.compare("LFU") == 0)
    {
        if (cache.find(key) == cache.end())
            return 0;
        else
        {
            cache.erase(key);
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lfumap.erase(key);
            return 1; // success
        }
    }
    return 0;
}

class ServerImpl final
{

    enum CallType { GET, DEL, PUT };
    CallType type_;

    public:
  ~ServerImpl() {
    server_->Shutdown();
    cq_->Shutdown();
  }

  void Run() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    HandleRpcs();
  }

  private:
  class CallData {
    
   public:
    CallData(KVStore::AsyncService* service, ServerCompletionQueue* cq, CallType ctype)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), type_(ctype) {
      Proceed();
    }
    // CallData(KVStore::AsyncService* service, ServerCompletionQueue* cq, CallType ctype)
    //     : service_(service), cq_(cq), putresponder_(&ctx_), status_(CREATE), type_(ctype) {
    //   Proceed();
    // }

    void Proceed() {
      if (status_ == CREATE) {
        status_ = PROCESS;

        if(type_ == GET)
            service_->RequestGET(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
        if(type_ == PUT)
            service_->RequestPUT(&ctx_, &putrequest_, &responder_, cq_, cq_,
                                  this);

        if(type_ == DEL)
            service_->RequestDEL(&ctx_, &deleterequest_, &responder_, cq_, cq_,
                                  this);
      } else if (status_ == PROCESS) {

        if(type_ == GET) {
            new CallData(service_, cq_, GET);

            string key = request_.key();
            cout << "Key = " << key;
            string value = get_value_from_map(key);
            if (value.compare("") == 0)
            {
                reply_.set_status(400);
                reply_.set_errordescription("KEY NOT EXIST");
            }
            else
            {
                reply_.set_status(200);
                reply_.set_value(value);
                reply_.set_errordescription("RETRIEVED VALUE");
            }

            status_ = FINISH;
            responder_.Finish(reply_, Status::OK, this);
        }
        
        else if(type_ == PUT) {
            new CallData(service_, cq_, PUT);

            string key = putrequest_.key();
            string value = putrequest_.value();
            put_value(key, value);
            reply_.set_status(200);
            reply_.set_errordescription("PUT SUCCESFULL");

            status_ = FINISH;
            responder_.Finish(reply_, Status::OK, this);
        }

        else if(type_ == DEL) {
            new CallData(service_, cq_, DEL);

            string key = deleterequest_.key();
            int value = delete_key(key);
            if (value == 0)
            {
                reply_.set_status(400);
                reply_.set_errordescription("KEY NOT EXIST");
            }
            else
            {
                reply_.set_status(200);
                reply_.set_errordescription("KEY DELETED");
            }

            status_ = FINISH;
            responder_.Finish(reply_, Status::OK, this);
        }

        
      } else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
      }
    }

    private:
    KVStore::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;

    GetRequest request_;
    CommonReply reply_;

    PutRequest putrequest_;
    DeleteRequest deleterequest_;
    //PutReply putreply_;

    ServerAsyncResponseWriter<CommonReply> responder_;
    //ServerAsyncResponseWriter<PutReply> putresponder_;

    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
    CallType type_;
    
  };

  void HandleRpcs() {
    //ServerImpl::CallType typex;
    new CallData(&service_, cq_.get(), GET);
    new CallData(&service_, cq_.get(), PUT);
    new CallData(&service_, cq_.get(), DEL);
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  KVStore::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
  ServerImpl server;
  server.Run();

  return 0;
}
