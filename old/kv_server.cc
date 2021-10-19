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
            response->set_errordescription("RETRIEVED VALUE");

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
        response->set_errordescription("PUT SUCCESFULL");
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
            response->set_errordescription("KEY DELETED");

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
