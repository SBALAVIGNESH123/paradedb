// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! `MppPartitionAdapterExec` — bridges the row-routing shuffle model to
//! DataFusion's native `Partitioning::Hash` concept.
//!
//! The MPP shuffle topology
//! (`CoalescePartitionsExec(UnionExec(ShuffleExec, DrainGatherExec))`) emits a
//! single-partition stream whose rows are exactly this participant's slice of
//! a globally hash-partitioned dataset. Downstream DF operators see only that
//! one partition, so partition-aware optimizer rules (EnforceDistribution,
//! group-by partitioning matchers, etc.) treat the data as opaque
//! `UnknownPartitioning(1)`.
//!
//! This adapter promotes the shape:
//!
//! - declares `Partitioning::Hash(keys, N)` (N = participant count),
//! - `execute(participant_index)` forwards `child.execute(0)` — real data,
//! - `execute(p)` for `p != participant_index` returns an empty stream.
//!
//! Globally, partition `p`'s rows live on participant `p`. Locally, only
//! one of the N partitions is non-empty. From DataFusion's per-process view
//! that's a valid hash-partitioned producer: some partitions may be empty,
//! the consumer iterates them all and merges via `CoalescePartitionsExec` (or
//! a HashJoin probe loop).
//!
//! Parallels datafusion-distributed's `PartitionIsolatorExec` (see
//! `src/execution_plans/partition_isolator.rs` in that repo) but inverted:
//! `PartitionIsolatorExec` narrows N input partitions to a slice the task
//! owns; this adapter widens 1 input partition to N declared output
//! partitions, with N-1 of them empty by construction.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::EmptyRecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

#[derive(Debug)]
pub struct MppPartitionAdapterExec {
    input: Arc<dyn ExecutionPlan>,
    participant_index: u32,
    participant_count: u32,
    properties: Arc<PlanProperties>,
}

impl MppPartitionAdapterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        hash_keys: Vec<Arc<dyn PhysicalExpr>>,
        participant_index: u32,
        participant_count: u32,
    ) -> Self {
        assert!(
            participant_count >= 1,
            "MppPartitionAdapterExec: participant_count must be >= 1"
        );
        assert!(
            participant_index < participant_count,
            "MppPartitionAdapterExec: participant_index ({participant_index}) >= participant_count ({participant_count})",
        );
        assert_eq!(
            input.properties().partitioning.partition_count(),
            1,
            "MppPartitionAdapterExec: expects a 1-partition input (the participant's local shuffle output)"
        );

        let eq_properties = EquivalenceProperties::new(input.schema());
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::Hash(hash_keys, participant_count as usize),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            input,
            participant_index,
            participant_count,
            properties,
        }
    }

    #[cfg(test)]
    pub fn participant_index(&self) -> u32 {
        self.participant_index
    }

    #[cfg(test)]
    pub fn participant_count(&self) -> u32 {
        self.participant_count
    }
}

impl DisplayAs for MppPartitionAdapterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MppPartitionAdapterExec: participant={} of {}",
            self.participant_index, self.participant_count
        )
    }
}

impl ExecutionPlan for MppPartitionAdapterExec {
    fn name(&self) -> &str {
        "MppPartitionAdapterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "MppPartitionAdapterExec expects exactly one child, got {}",
                children.len()
            )));
        }
        let hash_keys = match self.properties.partitioning.clone() {
            Partitioning::Hash(exprs, _) => exprs,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "MppPartitionAdapterExec.properties.partitioning lost Hash variant: {other:?}"
                )));
            }
        };
        Ok(Arc::new(Self::new(
            children.into_iter().next().unwrap(),
            hash_keys,
            self.participant_index,
            self.participant_count,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.participant_count as usize {
            return Err(DataFusionError::Internal(format!(
                "MppPartitionAdapterExec: partition {partition} >= participant_count {}",
                self.participant_count
            )));
        }
        if partition as u32 == self.participant_index {
            self.input.execute(0, context)
        } else {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::Column;
    use futures::StreamExt;

    fn one_row_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![42]))])
                .unwrap();
        MemorySourceConfig::try_new_from_batches(schema, vec![batch]).unwrap()
    }

    fn key_a() -> Vec<Arc<dyn PhysicalExpr>> {
        vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>]
    }

    #[test]
    fn declares_hash_partitioning_with_n_partitions() {
        let adapter = MppPartitionAdapterExec::new(one_row_input(), key_a(), 0, 4);
        match &adapter.properties().partitioning {
            Partitioning::Hash(exprs, n) => {
                assert_eq!(*n, 4);
                assert_eq!(exprs.len(), 1);
            }
            other => panic!("expected Hash partitioning, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_partition_yields_input_rows() {
        let adapter: Arc<dyn ExecutionPlan> =
            Arc::new(MppPartitionAdapterExec::new(one_row_input(), key_a(), 2, 4));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ctx = SessionContext::new();
        rt.block_on(async {
            let mut s = adapter.execute(2, ctx.task_ctx()).unwrap();
            let mut total = 0;
            while let Some(batch) = s.next().await {
                total += batch.unwrap().num_rows();
            }
            assert_eq!(total, 1, "self partition should pass child rows through");
        });
    }

    #[test]
    fn execute_non_self_partition_yields_empty() {
        let adapter: Arc<dyn ExecutionPlan> =
            Arc::new(MppPartitionAdapterExec::new(one_row_input(), key_a(), 2, 4));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ctx = SessionContext::new();
        rt.block_on(async {
            for non_self in [0usize, 1, 3] {
                let mut s = adapter.execute(non_self, ctx.task_ctx()).unwrap();
                let mut total = 0;
                while let Some(batch) = s.next().await {
                    total += batch.unwrap().num_rows();
                }
                assert_eq!(
                    total, 0,
                    "non-self partition {non_self} must yield empty stream"
                );
            }
        });
    }

    #[test]
    fn execute_out_of_range_partition_errors() {
        let adapter: Arc<dyn ExecutionPlan> =
            Arc::new(MppPartitionAdapterExec::new(one_row_input(), key_a(), 0, 2));
        let ctx = SessionContext::new();
        match adapter.execute(5, ctx.task_ctx()) {
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("partition 5") && msg.contains("participant_count 2"),
                    "expected out-of-range message, got: {msg}"
                );
            }
            Ok(_) => panic!("out-of-range partition should error"),
        }
    }

    #[test]
    fn with_new_children_preserves_hash_keys_and_participant_state() {
        let adapter: Arc<dyn ExecutionPlan> =
            Arc::new(MppPartitionAdapterExec::new(one_row_input(), key_a(), 1, 3));
        let rebuilt = adapter
            .clone()
            .with_new_children(vec![one_row_input()])
            .expect("with_new_children should succeed");
        let downcast = rebuilt
            .as_any()
            .downcast_ref::<MppPartitionAdapterExec>()
            .expect("rebuilt is still MppPartitionAdapterExec");
        assert_eq!(downcast.participant_index(), 1);
        assert_eq!(downcast.participant_count(), 3);
        match &downcast.properties().partitioning {
            Partitioning::Hash(exprs, n) => {
                assert_eq!(*n, 3);
                assert_eq!(exprs.len(), 1);
            }
            other => panic!("expected Hash partitioning, got {other:?}"),
        }
    }
}
