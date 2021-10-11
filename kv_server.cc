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

using kvstore::DeleteReply;
using kvstore::DeleteRequest;
using kvstore::GetReply;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutReply;
using kvstore::PutRequest;

unordered_map<string, string> cache;
int cache_size=3;
deque<string> lruqueue;
string cache_type = "LRU";


int hashString(string s){
    int sum=0;
    for(int i=0;i<s.length();i++){
        sum+=s[i];
    }
    return sum%20;
}

string get_value_from_map(string key)
{

    if (cache_type.compare("LRU") == 0)
    {
        cout << "Key = " << key;
        for (auto i : cache)
        {
            if (key.compare(i.first) == 0)
            {
                // found
                deque<string>::iterator it = lruqueue.begin();
                while (*it != key)
                {
                    it++;
                }

                // update queue: update it to most recent used value
                lruqueue.erase(it);
                lruqueue.push_front(key);
                return i.second;
            }
        }
        return "";
    }

    return "";
}

void put_value(string key, string value)
{
    if (cache_type.compare("LRU") == 0)
    {

        // not present in cache
        if (cache.find(key) == cache.end())
        {
            // check if cache is full
            if (cache_size == lruqueue.size())
            {
                string last = lruqueue.back();
                lruqueue.pop_back();
                cache.erase(last);
            }
        }
        else
        {
            // present in cache, remove it from queue and map
            deque<string>::iterator it = lruqueue.begin();
            while (*it != key)
                it++;

            lruqueue.erase(it);
            cache.erase(key);
        }

        // update the cache
        lruqueue.push_front(key);
        cache[key] = value;
    }
}

int delete_key(string key)
{
    if (cache_type.compare("LRU") == 0)
    {
        for (auto i : cache)
        {
            if (key.compare(i.first) == 0)
            {
                cache.erase(key);
            deque<string>::iterator it = lruqueue.begin();
            while (*it != key)
                it++;
                lruqueue.erase(it);
            
                return 1; // success
            }
        }
        return 0; // key not found
    }

    return 0;
}

class KVStoreServiceImpl final : public KVStore::Service
{

    Status GET(ServerContext *context,
               const GetRequest *request, GetReply *response) override
    {
        string key = request->key();
        cout << "Key = " << key;
        string value = get_value_from_map(key);
        if (value.compare("") == 0)
        {
            response->set_status(400);
            response->set_errordescription("KEY NOT EXIST");
        }
        else
        {
            response->set_status(200);
            response->set_value(value);
        }

        return Status::OK;
    }

    Status PUT(ServerContext *context,
               const PutRequest *request, PutReply *response) override
    {
        string key = request->key();
        string value = request->value();
        put_value(key, value);
        response->set_status(200);

        return Status::OK;
    }

    Status DEL(ServerContext *context,
               const DeleteRequest *request, DeleteReply *response) override
    {
        string key = request->key();
        int value = delete_key(key);
        if (value == 0)
        {
            response->set_status(400);
            response->set_errordescription("KEY NOT EXIST");
        }
        else
        {
            response->set_status(200);
        }

        return Status::OK;
    }
};

void RunServer()
{

    string address("0.0.0.0:5000");
    KVStoreServiceImpl service;

    ServerBuilder builder;

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    unique_ptr<Server> server(builder.BuildAndStart());
    cout << "Server listening on port: " << address << endl;

    server->Wait();
}

int main(int argc, char **argv)
{
    RunServer();

    return 0;
}
