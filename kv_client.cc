#include <string>
#include <cstdio>
#include <fstream>
#include <sstream>

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace std;

using kvstore::KVStore;
using kvstore::GetRequest;
using kvstore::CommonReply;
using kvstore::PutRequest;
using kvstore::DeleteRequest;

class KVClient{
public:
	KVClient(shared_ptr<Channel> channel) : stub_(KVStore::NewStub(channel)){}

	void GET(string key) {
		GetRequest request;
		request.set_key(key);

		CommonReply reply;
		ClientContext context;


		Status status = stub_->GET(&context, request, &reply);

		if(reply.status() == 200) {
			cout<<"Value = "<< reply.value() << " Status = " << reply.status() << endl;
		} else if(reply.status() == 400) {
			cout<< "ERROR: "<< "Status = " << reply.status() << " error description = " << reply.errordescription() << endl;
		} 

		if(status.ok()){
			//cout << "Key get successfully" << endl;
		} else {
			cout << "Connection Error.." << endl;
		}
	}

	void DEL(string key) {
		DeleteRequest request;
		request.set_key(key);

		CommonReply reply;
		ClientContext context;

		Status status = stub_->DEL(&context, request, &reply);

		if(reply.status() == 200) {
			cout<< "Key = " << key <<" deleted successfully " << "Status = " << reply.status() << endl;
		} else if(reply.status() == 400) {
			cout<< "ERROR: "<< "Status = " << reply.status() << " error description = " << reply.errordescription() << endl;
		} 

		if(status.ok()){
			//cout << "Key get successfully" << endl;
		} else {
			cout << "Connection Error.." << endl;
		}
	}

	void PUT(string key, string value) {
		PutRequest request;
		request.set_key(key);
		request.set_value(value);

		CommonReply reply;
		ClientContext context;

		Status status = stub_->PUT(&context, request, &reply);

		if(reply.status() == 200) {
			cout<< "Key = " << key <<" Value = " << value << " put successfully " << "Status = " << reply.status() << endl;
		} else if(reply.status() == 400) {
			cout<< "ERROR in putting value"<< endl;
		}

		if(status.ok()){
			//cout << "Key get successfully" << endl;
		} else {
			cout << "Connection Error.." << endl;
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
				if(a[1].length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else if(a[0].compare("GET")==0){
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
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else {
					client.GET(key);
				}	
			}
			else if(cmd.compare("PUT")==0){
				cin>>key;
				cin>>value;
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else
					client.PUT(key,value);	
			}
			else if(cmd.compare("DEL")==0){
				cin>>key;
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else
					client.DEL(key);	
			}
			else if(cmd.compare("EXIT")==0)
                		break;


		}
	}
	return 0;
}