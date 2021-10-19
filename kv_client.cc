#include <string>
#include <cstdio>
#include <fstream>
#include <sstream>

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace std;

using kvstore::KVStore;
using kvstore::GetRequest;
using kvstore::CommonReply;
using kvstore::PutRequest;
using kvstore::PutReply;
using kvstore::DeleteRequest;
using kvstore::DeleteReply;

class KVClient{
public:
	explicit KVClient(shared_ptr<Channel> channel) : stub_(KVStore::NewStub(channel)){}

	void GET(string key) {
		GetRequest request;
		request.set_key(key);

		CommonReply reply;
		ClientContext context;
		CompletionQueue cq;
		Status status;


		std::unique_ptr<ClientAsyncResponseReader<CommonReply> > rpc(
        stub_->PrepareAsyncGET(&context, request, &cq));

        rpc->StartCall();

        rpc->Finish(&reply, &status, (void*)1);

        void* got_tag;
    	bool ok = false;
    	GPR_ASSERT(cq.Next(&got_tag, &ok));
    	GPR_ASSERT(got_tag == (void*)1);
    	GPR_ASSERT(ok);

		//Status status = stub_->GET(&context, request, &reply);
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

		CommonReply reply;
		ClientContext context;
		CompletionQueue cq;
		Status status;


		std::unique_ptr<ClientAsyncResponseReader<CommonReply> > rpc(
        stub_->PrepareAsyncDEL(&context, request, &cq));

        rpc->StartCall();

        rpc->Finish(&reply, &status, (void*)1);

        void* got_tag;
    	bool ok = false;
    	GPR_ASSERT(cq.Next(&got_tag, &ok));
    	GPR_ASSERT(got_tag == (void*)1);
    	GPR_ASSERT(ok);

		//Status status = stub_->DEL(&context, request, &reply);

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

		CommonReply reply;
		ClientContext context;
		CompletionQueue cq;
		Status status;

		std::unique_ptr<ClientAsyncResponseReader<CommonReply> > rpc(
        stub_->PrepareAsyncPUT(&context, request, &cq));

        rpc->StartCall();

        rpc->Finish(&reply, &status, (void*)1);

        void* got_tag;
    	bool ok = false;
    	GPR_ASSERT(cq.Next(&got_tag, &ok));
    	GPR_ASSERT(got_tag == (void*)1);
    	GPR_ASSERT(ok);

		//Status status = stub_->PUT(&context, request, &reply);

		if(status.ok()){
			cout << "Put successful";
		} else {
			cout << "Put failed";
		}
	}


	private:
	unique_ptr<KVStore::Stub> stub_;

};


int main(int argc, char* argv[]){

	string address("0.0.0.0:50051");
	KVClient client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
				)
	);

	if(argc > 1){
		cout<<"Batch Mode : "<<argv[1];
		fstream newfile;
		newfile.open(argv[1],ios::in);
		if (newfile.is_open()){  
			string tp;
			while(getline(newfile, tp)){ 
				cout << tp << "\n"; 
				string a[3];
				istringstream ss(tp);
				string del;
				int i=0;
				while(getline(ss,del,' ')){
					a[i]=del.c_str();  
					i++;
				}
				if(a[0].compare("GET")==0){
					client.GET(a[1]);    
				}
				else if(a[0].compare("PUT")==0){
					client.PUT(a[1],a[2]);    
				}
				else if(a[0].compare("DEL")==0){
					client.DEL(a[1]);    
				}

			}
      			newfile.close();
		}
   
	}
	else{
		string cmd,key,value;
		cout<<"Interactive Mode : ";
		while(1){
			cout<< "$>" ;
			cin >> cmd;			
			if(cmd.compare("GET")==0){
				cin>>key;
				client.GET(key);	
			}
			else if(cmd.compare("PUT")==0){
				cin>>key;
				cin>>value;
				client.PUT(key,value);	
			}
			else if(cmd.compare("DEL")==0){
				cin>>key;
				client.DEL(key);	
			}
			else if(cmd.compare("EXIT")==0)
                		break;


		}
	}
	return 0;
}

