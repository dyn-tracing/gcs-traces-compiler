use super::codegen_common::assign_id_to_property;
use super::codegen_common::parse_udf;
use super::codegen_common::AggregationUdf;
use super::codegen_common::CodeStruct;
use super::codegen_common::ScalarOrAggregationUdf;
use super::codegen_common::ScalarUdf;
use super::ir::Aggregate;
use super::ir::AttributeFilter;
use super::ir::IrReturnEnum;
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
    vert_to_identifier: &mut IndexMap<String, u64>
) -> Vec<String> {
    let mut create_struct_blocks = Vec::new();
    let mut vertex_num = 0;
    for struct_filter in struct_filters {
        for vertex in &struct_filter.vertices {
            // TODO(jessica): insert vertices
            // TODO:  there's a more efficient way to do this if I just make a map
            let mut has_service_name = false;
            for property_filter in attr_filters {
                if property_filter.node == *vertex && property_filter.property == "service.name" {
                    has_service_name = true;
                    create_struct_blocks.push(format!(
                        "query_trace.node_names.insert(std::make_pair({num}, {value}));\n",
                        value = property_filter.value,
                        num = vertex_num
                    ));

                }
            }
            if !has_service_name {
                create_struct_blocks.push(format!(
                    "query_trace.node_names.insert(std::make_pair({num}, {value}));\n",
                    value = "ASTERISK_SERVICE",
                    num = vertex_num
                ));
            }
            vert_to_identifier.insert(vertex.clone(), vertex_num);
            vertex_num += 1;
        }

        for edge in &struct_filter.edges {
            create_struct_blocks.push(format!(
                "query_trace.edges.insert(std::make_pair({v1}, {v2}));\n",
                v1 = vert_to_identifier[&edge.0],
                v2 = vert_to_identifier[&edge.1]
            ))
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
    create_struct_blocks
}

fn make_attr_filter_blocks(
    attr_filters: &[AttributeFilter],
    property_to_type: &IndexMap<&str, &str>,
    vert_to_identifier: &IndexMap<String, u64>,
) -> Vec<String> {
    let mut prop_blocks = Vec::new();
    let mut i = 0;
    for attr_filter in attr_filters {
        let mut prop = attr_filter.property.clone();
        if prop.starts_with('.') {
            prop.remove(0);
        }
        if attr_filter.node == "trace" {
            // TODO(jessica): pretty sure this can only be trace ID
            let trace_filter_block = format!(
                "std::string batch_name = query_index_for_traceID(client, \"{traceID}\");",
                traceID = attr_filter.value
            );
            prop_blocks.push(trace_filter_block);
        } else {
            i += 1;
            // this is a span level attribute
            let split: Vec<&str> = attr_filter.property.split(".").collect();
            if split.len() == 3 { // should be node name plus span plus attr
                // this is a simple attribute check of something in the span spec
                // TODO(jessica) expand from equality to less than/greater than checks
                let relationship_str: &str;
                if attr_filter.relationship == "=" {
                    relationship_str = "Equals_to"
                } else if attr_filter.relationship == "<" {
                    relationship_str = "Greater_than"
                } else {
                    relationship_str = "Lesser_than"
                }
                prop_blocks.push(format!("
                    \tquery_condition condition{i};
                    \tcondition{i}.node_index {equals} {node_index},
                    \tcondition{i}.type = {var_type}_value;
                    \tget_value_func condition{i}_union;
                    \tcondition{i}_union.{var_type}_func = &opentelemetry::proto::trace::v1::Span::{prop_name};
                    \tcondition{i}.func = condition{i}_union;
                    \tcondition{i}.node_property_value {equals} {value},
                    \tcondition{i}.comp {equals} {relationship};
                    \tconditions.push_back(condition{i});
                ",
                i=i,
                node_index=vert_to_identifier[&attr_filter.node],
                equals = "\u{003D}".to_string(),
                prop_name= split[2],
                value = attr_filter.value,
                relationship=relationship_str,
                var_type = property_to_type[prop.as_str()],
                ));
            } else {
                // a few options here:  you could be doing attributes, events, or links
                if split[1] == "attribute" {
                    // TODO(jessica)
                } else if split[1] == "event" {
                    // TODO(jessica)
                } else if split[1] == "link" {
                    // TODO(jessica)

                }

            }

        }
    }
    prop_blocks
}

fn make_storage_rpc_value_from_trace(
    property: &str,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    format!("//TODO {prop}", prop = id_to_property[property])
}

fn make_storage_rpc_value_from_target(
    entity: &str,
    property: &str,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    format!(
        "//TODO {node_id} = {property_name}",
        node_id = entity,
        property_name = property
    )
}

fn make_return_block(
    entity_ref: &PropertyOrUDF,
    _query_data: &VisitorResults,
    id_to_property: &IndexMap<String, u64>,
) -> String {
    match entity_ref {
        PropertyOrUDF::Property(prop) => match prop.parent.as_str() {
            "trace" => make_storage_rpc_value_from_trace(&prop.to_dot_string(), id_to_property),
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
                "trace" => make_storage_rpc_value_from_trace(&call.id, id_to_property),
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

// TODO(jessica) is it still necessary to have UDFs?
fn generate_udf_blocks(
    _scalar_udf_table: &IndexMap<String, ScalarUdf>,
    _aggregation_udf_table: &IndexMap<String, AggregationUdf>,
    _udf_calls: &IndexSet<UdfCall>,
    _id_to_property: &IndexMap<String, u64>,
) -> Vec<String> {
    Vec::new()
}

pub fn generate_code_blocks(query_data: VisitorResults, udf_paths: Vec<String>) -> CodeStruct {
    // TODO: dynamically retrieve this from https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/advanced/attributes

    let property_to_type: IndexMap<&str, &str> = [
        ("trace_id", "bytes"),
        ("span.span_id", "bytes"),
        ("span.trace_state", "string"),
        ("span.parent_span_id", "bytes"),
        ("span.name", "string"),
        ("span.start_time_unix_nano", "int"),
        ("span.end_time_unix_nano", "int"),
        ("span.dropped_attributes_count", "int"),
        ("span.dropped_events_count", "int"),
        ("span.dropped_links_count", "int"),
        ("span.attributes.key", "string"),
        ("span.attributes.value", "bytes"),
        ("span.event.time_unix_nano", "int"),
        ("span.event.name", "String"),
        ("span.event.attributes.key", "String"),
        ("span.event.attributes.value", "bytes"),
        ("span.event.dropped_attributes_count", "int"),
        ("span.link.trace_id", "bytes"),
        ("span.link.span_id", "bytes"),
        ("span.link.trace_state", "string"),
        ("span.link.dropped_attributes_count", "string"),
        ("span.link.attributes.key", "string"),
        ("span.link.attributes.value", "bytes"),
        ("span.status.message", "string"),
        ("span.service.name", "string"),
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

    code_struct.udf_blocks = generate_udf_blocks(
        &scalar_udf_table,
        &aggregation_udf_table,
        &query_data.udf_calls,
        &code_struct.id_to_property,
    );
    let mut vert_to_identifier = IndexMap::new();
    code_struct.create_struct_blocks =
        make_struct_filter_blocks(&query_data.attr_filters, &query_data.struct_filters, &mut vert_to_identifier);
    code_struct.attribute_blocks =
        make_attr_filter_blocks(&query_data.attr_filters, &property_to_type, &vert_to_identifier);

    let resp_block = match query_data.return_expr {
        IrReturnEnum::PropertyOrUDF(ref entity_ref) => {
            make_return_block(entity_ref, &query_data, &code_struct.id_to_property)
        }
        IrReturnEnum::Aggregate(ref agg) => {
            make_aggr_block(agg, &query_data, &code_struct.id_to_property)
        }
    };
    code_struct.return_blocks.push(resp_block);
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
            "MATCH (a) -[]-> (b {})-[]->(c) RETURN a.service.name".to_string(),
        );
        assert!(!result.struct_filters.is_empty());
        let _codegen = generate_code_blocks(result, [COUNT.to_string()].to_vec());
    }
    #[test]
    fn get_group_by() {
        let result = get_codegen_from_query(
            "MATCH (a {}) WHERE a.service.name = 'productpage-v1' RETURN a.request.total_size, count(a.request.total_size)".to_string(),
        );
        assert!(!result.struct_filters.is_empty());
        // Do not throw an error parsing this expression.
        let _codegen = generate_code_blocks(result, [COUNT.to_string()].to_vec());
    }

    #[test]
    fn test_where() {
        let result = get_codegen_from_query(
            "MATCH (a) -[]-> (b)-[]->(c) WHERE b.service.name = 'reviews-v1' AND trace.request.total_size = 1 RETURN a.request.total_size, avg(a.request.total_size)".to_string(),
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
