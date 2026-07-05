/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt::Debug;
use std::sync::Arc;

use buck2_artifact::artifact::artifact_type::Artifact;
use buck2_core::provider::label::ConfiguredProvidersLabel;
use buck2_hash::StdBuckHashMap;
use buck2_interpreter::starlark_profiler::data::StarlarkProfileDataAndStats;

use crate::analysis::registry::RecordedAnalysisValues;
use crate::artifact_groups::promise::PromiseArtifactId;

// TODO(@wendyy) move into `buck2_node`
pub mod anon_promises_dyn;
// TODO(@wendyy) move into `buck2_interpreter_for_build`
pub mod anon_targets_registry;
pub mod calculation;
pub mod extra_v;
pub mod registry;

use allocative::Allocative;
use dupe::Dupe;

use crate::interpreter::rule_defs::provider::collection::FrozenProviderCollectionValue;
use crate::interpreter::rule_defs::provider::collection::FrozenProviderCollectionValueRef;
use crate::validation::transitive_validations::TransitiveValidations;

#[derive(Debug, Clone, Dupe, Allocative, pagable::Pagable)]
pub struct AnalysisResult {
    analysis_values: Arc<RecordedAnalysisValues>,
    /// Profiling data after running analysis, for this analysis only, without dependencies.
    /// `None` when profiling is disabled.
    /// For forward node, this value is shared with underlying analysis (including this field).
    #[pagable(discard = "None")]
    pub profile_data: Option<Arc<StarlarkProfileDataAndStats>>,
    promise_artifact_map: Arc<StdBuckHashMap<PromiseArtifactId, Artifact>>,
    pub num_declared_actions: u64,
    pub num_declared_artifacts: u64,
    /// `None` means there are no `ValidationInfo` providers in transitive dependencies.
    pub validations: Option<TransitiveValidations>,
}

impl AnalysisResult {
    /// Create a new AnalysisResult
    pub fn new(
        analysis_values: RecordedAnalysisValues,
        profile_data: Option<Arc<StarlarkProfileDataAndStats>>,
        promise_artifact_map: StdBuckHashMap<PromiseArtifactId, Artifact>,
        num_declared_actions: u64,
        num_declared_artifacts: u64,
        validations: Option<TransitiveValidations>,
    ) -> Self {
        Self {
            analysis_values: Arc::new(analysis_values),
            profile_data,
            promise_artifact_map: Arc::new(promise_artifact_map),
            num_declared_actions,
            num_declared_artifacts,
            validations,
        }
    }

    pub fn providers(&self) -> buck2_error::Result<FrozenProviderCollectionValueRef<'_>> {
        self.analysis_values.provider_collection()
    }

    /// Persist support: the action graph without the provider heap (the S3
    /// "analysis dodge" - see dice/dice/docs/persistence_plan.md). Validations
    /// and profile data ride the heap's fate.
    pub fn actions_only_for_persist(&self) -> Self {
        Self {
            analysis_values: Arc::new(self.analysis_values.actions_only_for_persist()),
            profile_data: None,
            promise_artifact_map: self.promise_artifact_map.dupe(),
            num_declared_actions: self.num_declared_actions,
            num_declared_artifacts: self.num_declared_artifacts,
            validations: None,
        }
    }

    pub fn promise_artifact_map(&self) -> &Arc<StdBuckHashMap<PromiseArtifactId, Artifact>> {
        &self.promise_artifact_map
    }

    /// Used to lookup an inner named provider result.
    pub fn lookup_inner(
        &self,
        label: &ConfiguredProvidersLabel,
    ) -> buck2_error::Result<FrozenProviderCollectionValue> {
        Ok(self.providers()?.lookup_inner(label)?.to_owned())
    }

    pub fn analysis_values(&self) -> &RecordedAnalysisValues {
        &self.analysis_values
    }
}

#[cfg(test)]
mod tests {
    use buck2_core::deferred::key::DeferredHolderKey;
    use pagable::PagableDeserialize;
    use pagable::PagableSerialize;
    use pagable::testing::TestingDeserializer;
    use pagable::testing::TestingSerializer;

    use super::*;
    use crate::actions::registry::RecordedActions;
    use crate::analysis::registry::RecordedAnalysisValues;

    /// The S3 dodge's wire form: an actions-only AnalysisResult must
    /// round-trip through pagable, keep its counts, and answer provider
    /// reads with the standard missing-storage error, not a panic.
    #[test]
    fn actions_only_analysis_round_trips() -> pagable::Result<()> {
        let key = DeferredHolderKey::testing_new("cell//pkg:target");
        let values = RecordedAnalysisValues::testing_new_actions_only(key, RecordedActions::new(0));
        let res = AnalysisResult::new(values, None, Default::default(), 3, 4, None);
        let stripped = res.actions_only_for_persist();

        let mut ser = TestingSerializer::new();
        stripped.pagable_serialize(&mut ser)?;
        let bytes = ser.finish();
        let mut de = TestingDeserializer::new(&bytes);
        let restored = AnalysisResult::pagable_deserialize(&mut de)?;

        assert_eq!(restored.num_declared_actions, 3);
        assert_eq!(restored.num_declared_artifacts, 4);
        assert!(restored.providers().is_err(), "storage-less: clean error");
        Ok(())
    }
}
