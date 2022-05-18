#include "google/cloud/storage/client.h"                                        
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "graph_query.h"

namespace gcs = ::google::cloud::storage;

int structurally_defined_query(gcs::Client* client) {
    // 1. Structural Filter
    trace_structure query_trace;
    query_trace.node_names.insert(std::make_pair(0, ASTERISK_SERVICE));
query_trace.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));
query_trace.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));
query_trace.edges.insert(std::make_pair(0, 1));
query_trace.edges.insert(std::make_pair(1, 2));


    // 2. Attribute Filter
    std::vector<query_condition> conditions;
    
                    	query_condition condition1;
                    	condition1.node_index = 0,
                    	condition1.type = int_value;
                    	get_value_func condition1_union;
                    	condition1_union.int_func = &opentelemetry::proto::trace::v1::Span::start_time_unix_nano;
                    	condition1.func = condition1_union;
                    	condition1.node_property_value = 5,
                    	condition1.comp = Lesser_than;
                    	conditions.push_back(condition1);
                

    int now = 3651500700; // this is so far in the future as to be meaningless
    auto client = gcs::Client();
    std::vector<std::string> total = get_traces_by_structure(query_trace, 0, now, conditions, &client);

    // 3. Aggregation
    
}

int no_structure_query(gcs::Client* client) {

}

int span_query(gcs::Client* client) {

}





int main(int argc, char* argv[]) {
    // 1. Create client
    auto client = gcs::Client();

    // 2. What kind of structure is this?
    std::string query_type = "";
    if (query_type == "structurally_defined") {
        structurally_defined_query(&client);
    } else if (query_type == "no_structure") {
        no_structure_query(&client);
    } else if (query_type == "span_structure") {
        span_query(&client);
    }
}
