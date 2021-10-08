#include <string>
#include <cstdio>

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace std;

using kvstore::KVStore;
using kvstore::GetRequest;
using kvstore::GetReply;
using kvstore::PutRequest;
using kvstore::PutReply;
using kvstore::DeleteRequest;
using kvstore::DeleteReply;

class KVClient{
public:
	KVClient(shared_ptr<Channel> channel) : stub_(KVStore::NewStub(channel)){}

	void GET(string key) {
		GetRequest request;
		request.set_key(key);

		GetReply reply;
		ClientContext context;

		Status status = stub_->GET(&context, request, &reply);
		cout<<"Value = "<< reply.value() << "Status = " << reply.status() << " error description = " << reply.errordescription(); 

		if(status.ok()){
			cout << "Key get successfully";
		} else {
			cout << "error in getting key";
		}
	}

	void DEL(string key) {
		DeleteRequest request;
		request.set_key(key);

		DeleteReply reply;
		ClientContext context;

		Status status = stub_->DEL(&context, request, &reply);

		if(status.ok()){
			cout << "Key deleted";
		} else {
			cout << "Error in deleting key";
		}
	}

	void PUT(string key, string value) {
		PutRequest request;
		request.set_key(key);
		request.set_value(value);

		PutReply reply;
		ClientContext context;

		Status status = stub_->PUT(&context, request, &reply);

		if(status.ok()){
			cout << "Put successful";
		} else {
			cout << "Put failed";
		}
	}


	private:
	unique_ptr<KVStore::Stub> stub_;

};

void runGET(string key) {
	string address("0.0.0.0:5000");
	KVClient client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
				)
		       );

	client.GET(key);
}

void runPUT(string key,string value) {
	string address("0.0.0.0:5000");
	KVClient client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
				)
		       );

	client.PUT(key, value);
}

void runDEL(string key) {
	string address("0.0.0.0:5000");
	KVClient client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
				)
		       );

	client.DEL(key);
}







int main(int argc, char* argv[]){
	runGET("5");
	runPUT("3","1");
	runDEL("5");
	/*if(argc > 1){
		cout<<”Batch Mode : ”<<argv[1];
	}
	else{
		string cmd,key,value;
		cout<<”Interactive Mode :  ”;
		while(1){
			cout<< ”$>” ;
			cin >> cmd;			
			if(cmd.compare(“GET”)==0){
				cin>>key;
				runGET(key);	
			}
			else if(cmd.compare(“PUT”)==0){
				cin>>key;
				cin>>value;
				runPUT(key,value);	
			}
			else if(cmd.compare(“DEL”)==0){
				cin>>key;
				runDEL(key,value);	
			}

		}
	}*/
	return 0;
}


