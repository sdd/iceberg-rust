// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor, OpLiteral};
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, BoundReference, PredicateOperator};
use crate::spec::{FieldSummary, ManifestFile, PartitionSpecRef, Schema, SchemaRef};
use std::sync::Arc;

pub(crate) struct ManifestEvaluator {
    partition_schema: SchemaRef,
    partition_filter: BoundPredicate,
    case_sensitive: bool,
}

impl ManifestEvaluator {
    pub(crate) fn new(
        partition_spec: PartitionSpecRef,
        table_schema: SchemaRef,
        partition_filter: BoundPredicate,
        case_sensitive: bool,
    ) -> crate::Result<Self> {
        let partition_type = partition_spec.partition_type(&table_schema)?;

        // this is needed as SchemaBuilder.with_fields expects an iterator over
        // Arc<NestedField> rather than &Arc<NestedField>
        let cloned_partition_fields: Vec<_> =
            partition_type.fields().iter().map(Arc::clone).collect();

        let partition_schema = Schema::builder()
            .with_fields(cloned_partition_fields)
            .build()?;

        let partition_schema_ref = Arc::new(partition_schema);

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());
        let unbound_partition_filter = inclusive_projection.project(&partition_filter)?;

        let partition_filter =
            unbound_partition_filter.bind(partition_schema_ref.clone(), case_sensitive)?;

        Ok(Self {
            partition_schema: partition_schema_ref,
            partition_filter,
            case_sensitive,
        })
    }

    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> crate::Result<bool> {
        if manifest_file.partitions.is_empty() {
            return Ok(true);
        }

        struct ManifestFilterVisitor<'a> {
            manifest_evaluator: &'a ManifestEvaluator,
            partitions: &'a Vec<FieldSummary>,
        }

        impl<'a> ManifestFilterVisitor<'a> {
            fn new(
                manifest_evaluator: &'a ManifestEvaluator,
                partitions: &'a Vec<FieldSummary>,
            ) -> Self {
                ManifestFilterVisitor {
                    manifest_evaluator,
                    partitions,
                }
            }
        }

        // Remove this annotation once all todos have been removed
        #[allow(unused_variables)]
        impl BoundPredicateVisitor for ManifestFilterVisitor<'_> {
            type T = bool;

            fn always_true(&mut self) -> crate::Result<Self::T> {
                Ok(true)
            }

            fn always_false(&mut self) -> crate::Result<Self::T> {
                Ok(false)
            }

            fn and(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
                Ok(lhs && rhs)
            }

            fn or(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
                Ok(lhs || rhs)
            }

            fn not(&mut self, inner: Self::T) -> crate::Result<Self::T> {
                Ok(!inner)
            }

            fn op(
                &mut self,
                op: PredicateOperator,
                reference: &BoundReference,
                literal: Option<OpLiteral>,
                predicate: &BoundPredicate,
            ) -> crate::Result<Self::T> {
                Ok(match op {
                    PredicateOperator::IsNull => {
                        self.field_summary_for_reference(reference).contains_null
                    }
                    PredicateOperator::NotNull => {
                        todo!()
                    }
                    PredicateOperator::IsNan => self
                        .field_summary_for_reference(reference)
                        .contains_nan
                        .is_some(),
                    PredicateOperator::NotNan => {
                        todo!()
                    }
                    PredicateOperator::LessThan => {
                        todo!()
                    }
                    PredicateOperator::LessThanOrEq => {
                        todo!()
                    }
                    PredicateOperator::GreaterThan => {
                        todo!()
                    }
                    PredicateOperator::GreaterThanOrEq => {
                        todo!()
                    }
                    PredicateOperator::Eq => {
                        todo!()
                    }
                    PredicateOperator::NotEq => {
                        todo!()
                    }
                    PredicateOperator::StartsWith => {
                        todo!()
                    }
                    PredicateOperator::NotStartsWith => {
                        todo!()
                    }
                    PredicateOperator::In => {
                        todo!()
                    }
                    PredicateOperator::NotIn => {
                        todo!()
                    }
                })
            }
        }

        impl ManifestFilterVisitor<'_> {
            fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
                let pos = reference.accessor().position();
                &self.partitions[pos]
            }
        }

        let mut evaluator = ManifestFilterVisitor::new(self, &manifest_file.partitions);

        visit(&mut evaluator, &self.partition_filter)
    }
}

#[cfg(test)]
mod test {
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{Bind, Predicate, PredicateOperator, Reference, UnaryExpression};
    use crate::spec::{
        FieldSummary, ManifestContentType, ManifestFile, NestedField, PartitionField,
        PartitionSpec, PrimitiveType, Schema, Transform, Type,
    };
    use std::sync::Arc;

    #[test]
    fn test_manifest_file_no_partitions() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::AlwaysTrue
            .bind(table_schema_ref.clone(), false)
            .unwrap();

        let case_sensitive = false;

        let manifest_file_partitions = vec![];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator = ManifestEvaluator::new(
            partition_spec_ref,
            table_schema_ref,
            partition_filter,
            case_sensitive,
        )
        .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(result);
    }

    #[test]
    fn test_manifest_file_trivial_partition_passing_filter() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(table_schema_ref.clone(), true)
        .unwrap();

        let manifest_file_partitions = vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator =
            ManifestEvaluator::new(partition_spec_ref, table_schema_ref, partition_filter, true)
                .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(result);
    }

    #[test]
    fn test_manifest_file_trivial_partition_rejected_filter() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("a"),
        ))
        .bind(table_schema_ref.clone(), true)
        .unwrap();

        let manifest_file_partitions = vec![FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator =
            ManifestEvaluator::new(partition_spec_ref, table_schema_ref, partition_filter, true)
                .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(!result);
    }

    fn create_test_schema_and_partition_spec() -> (Arc<Schema>, Arc<PartitionSpec>) {
        let table_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()
            .unwrap();
        let table_schema_ref = Arc::new(table_schema);

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();
        let partition_spec_ref = Arc::new(partition_spec);
        (table_schema_ref, partition_spec_ref)
    }

    fn create_test_manifest_file(manifest_file_partitions: Vec<FieldSummary>) -> ManifestFile {
        ManifestFile {
            manifest_path: "/test/path".to_string(),
            manifest_length: 0,
            partition_spec_id: 1,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_data_files_count: None,
            existing_data_files_count: None,
            deleted_data_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: manifest_file_partitions,
            key_metadata: vec![],
        }
    }
}
