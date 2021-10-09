#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <string>
 
using namespace std;
 
#include <grpcpp/grpcpp.h>
 
#include "kvstore.grpc.pb.h"
 
 
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
 
using kvstore::KVStore;
using kvstore::GetRequest;
using kvstore::GetReply;
using kvstore::PutRequest;
using kvstore::PutReply;
using kvstore::DeleteRequest;
using kvstore::DeleteReply;

unordered_map<string, string> cache;

string get_value_from_map(string key) {
  for (auto i: cache) {
    if (key.compare(i.first) == 0) {
      return i.second;
    }
  }
  return "";
}

void put_value(string key, string value) {
    cache[key] = value;
}
 
int delete_key(string key) {
  for (auto i: cache) {
    if (key.compare(i.first) == 0) {
cache.erase(key);
      return 1; // successfully deleted
    }
  }
  return 0; // key not found
    
}

class KVStoreServiceImpl final : public KVStore::Service {

  Status GET(ServerContext* context,
                   const GetRequest *request, GetReply *response) override  {
    string key = request->key();
    string value = get_value_from_map(key);
    if(value.compare("") == 0) {
        response->set_status(400);
        response->set_errordescription("KEY NOT EXIST");
    } else {
        response->set_status(200);
        response->set_value(value);
    }
    
    return Status::OK;
  }

Status PUT(ServerContext* context,
                   const PutRequest *request, PutReply *respone) override {
    string key = request->key();
    string value = request->value();
    put_value(request.key(), request.value());
    response.set_status(200);
    
    return Status::OK;
  }

Status DEL(ServerContext* context,
                   const DeleteRequest *request, DeleteReply*response) override  {
    string key = request->key();
    int value = delete_key(key);
    if(value == 0) {
        response->set_status(400);
        response->set_errordescription("KEY NOT EXIST");
    } else {
        response->set_status(200);
  
    }
    
    return Status::OK;
  }

};

void RunServer() {
 
    string address("0.0.0.0:5000");
    KVStoreServiceImpl service;
 
    ServerBuilder builder;
 
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
 
    unique_ptr<Server> server(builder.BuildAndStart());
    cout << "Server listening on port: " << address << endl;
 
    server->Wait();
}
 
int main(int argc, char** argv) {
  RunServer();
  
  return 0;
}




