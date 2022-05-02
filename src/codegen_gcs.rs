use super::codegen_common::assign_id_to_property;
use super::codegen_common::parse_udf;
use super::codegen_common::AggregationUdf;
use super::codegen_common::CodeStruct;
use super::codegen_common::ScalarOrAggregationUdf;
use super::codegen_common::ScalarUdf;
use super::ir::Aggregate;
use super::ir::AttributeFilter;
use super::ir::IrReturnEnum;
use super::ir::Property;
use super::ir::PropertyOrUDF;
use super::ir::StructuralFilter;
use super::ir::UdfCall;
use super::ir::VisitorResults;
use indexmap::IndexMap;
use indexmap::IndexSet;

/********************************/
// Code Generation
/********************************/

fn make_struct_filter_blocks(
    attr_filters: &[AttributeFilter],
    struct_filters: &[StructuralFilter],
    id_to_property: &IndexMap<String, u64>,
) -> Vec<String> {
    let mut target_blocks = Vec::new();
    for struct_filter in struct_filters {
        target_blocks.push("trace_structure query_trace;".to_string());
        for vertex in &struct_filter.vertices {
            // TODO(jessica): insert vertices
        }

        for edge in &struct_filter.edges {
            // TODO(jessica): insert edges
        }

        for property_filter in attr_filters {
            if property_filter.node != "trace" {
                let mut property_name_without_period = property_filter.property.clone();
                if property_name_without_period.starts_with('.') {
                    property_name_without_period.remove(0);
                }
                // TODO(jessica): insert property filters
            }
        }
    }
    target_blocks
}

fn make_attr_filter_blocks(
    attr_filters: &[AttributeFilter],
    id_to_property: &IndexMap<String, u64>,
) -> Vec<String> {
    let mut trace_lvl_prop_blocks = Vec::new();
    for attr_filter in attr_filters {
        if attr_filter.node == "trace" {
            let mut prop = attr_filter.property.clone();
            if prop.starts_with('.') {
                prop.remove(0);
            }
            // TODO(jessica): make this into real code
            let trace_filter_block = format!(
                "{prop_name} {value}",
                prop_name = id_to_property[&prop],
                value = attr_filter.value
            );
            trace_lvl_prop_blocks.push(trace_filter_block);
        }
    }
    trace_lvl_prop_blocks
}

fn make_storage_rpc_value_from_trace(
    property: &str,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    format!(
        "//TODO {prop}",
        prop = id_to_property[property]
    )
}

fn make_storage_rpc_value_from_target(
    entity: &str,
    property: &str,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    format!("//TODO {node_id} {property} {property_name}",
        node_id = entity,
        property = id_to_property[property],
        property_name = property
    )
}

fn make_return_block(
    entity_ref: &PropertyOrUDF,
    query_data: &VisitorResults,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    match entity_ref {
        PropertyOrUDF::Property(prop) => match prop.parent.as_str() {
            "trace" => make_storage_rpc_value_from_trace(
                &prop.to_dot_string(),
                id_to_property,
            ),
            _ => make_storage_rpc_value_from_target(
                &prop.parent,
                &prop.to_dot_string(),
                id_to_property,
            ),
        },
        PropertyOrUDF::UdfCall(call) => {
            // Because of quirky design we need to get the first arg
            if call.args.len() != 1 {
                panic!("We currently only implement very specific arguments for UDFs!");
            }
            let node = &call.args[0];
            match node.as_str() {
                "trace" => make_storage_rpc_value_from_trace(
                    &call.id,
                    id_to_property,
                ),
                _ => make_storage_rpc_value_from_target(node, &call.id, id_to_property),
            }
        }
    }
}

fn make_aggr_block(
    agg: &Aggregate,
    query_data: &VisitorResults,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    let mut to_return = String::new();
    for arg in &agg.args {
        to_return.push_str(&make_return_block(arg, query_data, id_to_property));
    }
    to_return
}

fn generate_property_blocks(
    properties: &IndexSet<Property>,
    scalar_udf_table: &IndexMap<String, ScalarUdf>,
    property_to_type: &IndexMap<&str, &str>,
    id_to_property: &IndexMap<String, u64>,
) -> Vec<String> {
    // TODO:  here, we can have duplicates because they have different entities,
    // but we still just need to collect one version of the property
    let mut property_blocks = Vec::new();
    for property in properties {
        // There is nothing to fetch so ignore.
        // TODO: What do we actually need here?  this is no longer relevant because we don't return traces, right?
        if property.members.is_empty() || scalar_udf_table.contains_key(&property.to_dot_string()) {
            continue;
        }
        // Now collect the property
        let get_prop_block = format!(
            "//TODO(jessica) {property} {property_str}",
            property=property.as_vec_str(),
            property_str = property.to_dot_string());
        property_blocks.push(get_prop_block);
    }
    property_blocks
}

// TODO(jessica) is it still necessary to have UDFs?
fn generate_udf_blocks(
    scalar_udf_table: &IndexMap<String, ScalarUdf>,
    aggregation_udf_table: &IndexMap<String, AggregationUdf>,
    udf_calls: &IndexSet<UdfCall>,
    id_to_property: &IndexMap<String, u64>,
) -> Vec<String> {
    let mut udf_blocks = Vec::new();
    udf_blocks
}

pub fn generate_code_blocks(query_data: VisitorResults, udf_paths: Vec<String>) -> CodeStruct {
    // TODO: dynamically retrieve this from https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/advanced/attributes

    let property_to_type: IndexMap<&str, &str> = [
        ("request.path", "String"),
        ("request.url_path", "String"),
        ("request.host", "String"),
        ("request.scheme", "String"),
        ("request.method", "String"),
        ("request.headers", "Map"),
        ("request.referer", "String"),
        ("request.useragent", "String"),
        ("request.time", "Timestamp"),
        ("request.id", "String"),
        ("request.protocol", "String"),
        ("request.duration", "Duration"),
        ("request.size", "int"),
        ("request.total_size", "int"),
        ("response.code", "int"),
        ("response.code_details", "String"),
        ("response.flags", "int"),
        ("response.grpc_status", "int"),
        ("response.headers", "Map"),
        ("response.trailers", "Map"),
        ("response.size", "int"),
        ("response.total_size", "int"),
        ("source.address", "String"),
        ("source.port", "int"),
        ("destination.address", "String"),
        ("destination.port", "int"),
        ("connection.id", "u64"),
        ("connection.mlts", "bool"), // More strings here
        ("upstream.port", "int"),
        ("metadata", "metadata"), // and more strings here
        ("filter_state", "Map"),
        ("node", "Node"),
        ("cluster_metadata", "metadata"),
        ("listener_direction", "int"),
        ("listener_metadata", "metadata"),
        ("route_metadata", "metadata"),
        ("upstream_host_metadata", "metadata"),
        ("node.metadata.WORKLOAD_NAME", "String"),
    ]
    .iter()
    .cloned()
    .collect();
    let mut code_struct = CodeStruct::new();

    let mut scalar_udf_table: IndexMap<String, ScalarUdf> = IndexMap::new();
    // where we store udf implementations
    let mut aggregation_udf_table: IndexMap<String, AggregationUdf> = IndexMap::new();
    for udf_path in udf_paths {
        match parse_udf(udf_path) {
            ScalarOrAggregationUdf::ScalarUdf(udf) => {
                scalar_udf_table.insert(udf.id.clone(), udf);
            }
            ScalarOrAggregationUdf::AggregationUdf(udf) => {
                aggregation_udf_table.insert(udf.id.clone(), udf);
            }
        }
    }
    code_struct.id_to_property = assign_id_to_property(&query_data.properties, &scalar_udf_table);

    // all the properties we collect
    code_struct.collect_properties_blocks = generate_property_blocks(
        &query_data.properties,
        &scalar_udf_table,
        &property_to_type,
        &code_struct.id_to_property,
    );
    code_struct.udf_blocks = generate_udf_blocks(
        &scalar_udf_table,
        &aggregation_udf_table,
        &query_data.udf_calls,
        &code_struct.id_to_property,
    );
    code_struct.target_blocks = make_struct_filter_blocks(
        &query_data.attr_filters,
        &query_data.struct_filters,
        &code_struct.id_to_property,
    );
    code_struct.trace_lvl_prop_blocks = make_attr_filter_blocks(
        &query_data.attr_filters,
        &code_struct.id_to_property,
    );

    let resp_block = match query_data.return_expr {
        IrReturnEnum::PropertyOrUDF(ref entity_ref) => {
            make_return_block(entity_ref, &query_data, &code_struct.id_to_property)
        }
        IrReturnEnum::Aggregate(ref agg) => {
            make_aggr_block(agg, &query_data, &code_struct.id_to_property)
        }
    };
    code_struct.response_blocks.push(resp_block);
    code_struct.aggregation_udf_table = aggregation_udf_table;
    code_struct.scalar_udf_table = scalar_udf_table;
    code_struct
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::antlr_gen::lexer::CypherLexer;
    use crate::antlr_gen::parser::CypherParser;
    use crate::to_ir::visit_result;
    use antlr_rust::common_token_stream::CommonTokenStream;
    use antlr_rust::token_factory::CommonTokenFactory;
    use antlr_rust::InputStream;

    static COUNT: &str = "
    // udf_type: Scalar
    // leaf_func: leaf
    // mid_func: mid
    // id: count

    use petgraph::Graph;

    struct ServiceName {
        fn leaf(my_node: String, graph: Graph) {
        return 0;
        }

        fn mid(my_node: String, graph: Graph) {
        return 1;
        }
    }
    ";

    static AVG: &str = "
    // udf_type: Aggregation
    // init_func: init
    // exec_func: exec
    // struct_name: Avg
    // id: avg

    #[derive(Clone, Copy, Debug)]
    pub struct Avg {
        avg: u64,
        total: u64,
        num_instances: u64,
    }

    impl Avg {
        fn new() -> Avg {
            Avg { avg: 0, total: 0 , num_instances: 0}
        }
        fn execute(&mut self, _trace_id: u64, instance: String) {
            self.total += instance.parse::<u64>().unwrap();
            self.num_instances += 1;
            self.avg = self.total/self.num_instances;
            self.avg.to_string()
        }
    }
    ";
    fn get_codegen_from_query(input: String) -> VisitorResults {
        let tf = CommonTokenFactory::default();
        let query_stream = InputStream::new_owned(input.to_string().into_boxed_str());
        let mut _lexer = CypherLexer::new_with_token_factory(query_stream, &tf);
        let token_source = CommonTokenStream::new(_lexer);
        let mut parser = CypherParser::new(token_source);
        let result = parser.oC_Cypher().expect("parsed unsuccessfully");
        visit_result(result)
    }

    #[test]
    fn get_codegen_doesnt_throw_error() {
        let result =
            get_codegen_from_query("MATCH (a) -[]-> (b {})-[]->(c) RETURN a.count".to_string());
        assert!(!result.struct_filters.is_empty());
        let _codegen = generate_code_blocks(result, [COUNT.to_string()].to_vec());
    }

    #[test]
    fn get_codegen_doesnt_throw_error_with_mult_periods() {
        let result = get_codegen_from_query(
            "MATCH (a) -[]-> (b {})-[]->(c) RETURN a.node.metadata.WORKLOAD_NAME".to_string(),
        );
        assert!(!result.struct_filters.is_empty());
        let _codegen = generate_code_blocks(result, [COUNT.to_string()].to_vec());
    }
    #[test]
    fn get_group_by() {
        let result = get_codegen_from_query(
            "MATCH (a {}) WHERE a.node.metadata.WORKLOAD_NAME = 'productpage-v1' RETURN a.request.total_size, count(a.request.total_size)".to_string(),
        );
        assert!(!result.struct_filters.is_empty());
        // Do not throw an error parsing this expression.
        let _codegen = generate_code_blocks(result, [COUNT.to_string()].to_vec());
    }

    #[test]
    fn test_where() {
        let result = get_codegen_from_query(
            "MATCH (a) -[]-> (b)-[]->(c) WHERE b.node.metadata.WORKLOAD_NAME = 'reviews-v1' AND trace.request.total_size = 1 RETURN a.request.total_size, avg(a.request.total_size)".to_string(),
        );
        assert!(!result.struct_filters.is_empty());
        assert!(!result.attr_filters.is_empty());
        // Do not throw an error parsing this expression.
        let _codegen = generate_code_blocks(result, [AVG.to_string()].to_vec());
    }

    #[test]
    fn test_aggr_udf() {
        let result = get_codegen_from_query(
            "MATCH (a) -[]-> (b)-[]->(c) RETURN a.request.total_size, avg(a.request.total_size)"
                .to_string(),
        );
        // Do not throw an error parsing this expression.
        let codegen = generate_code_blocks(result, [AVG.to_string()].to_vec());
        assert!(codegen.aggregation_udf_table.keys().count() == 1);
    }
}
