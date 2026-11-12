/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import trainer "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"

// diagnosticValidOptimizationJob uses the same required spec fields as the
// integration fixture, rather than the intentionally minimal unit-test fixture.
func diagnosticValidOptimizationJob() *trainer.OptimizationJob {
	job := getBaseOptJob()
	job.Spec.NumTrials = 1
	job.Spec.ParallelTrials = 1
	job.Spec.Parameters = []trainer.Parameter{{
		Name: "lr",
		SearchSpace: &trainer.SearchSpace{
			Uniform: trainer.UniformSpace{Min: "0.01", Max: "0.1"},
		},
	}}
	group := trainer.GroupVersion.Group
	kind := trainer.TrainingRuntimeKind
	image := "docker.io/kubeflow/trainer:latest"
	job.Spec.TrainJobTemplate.Spec.RuntimeRef = trainer.RuntimeRef{
		Name: "alpha", APIGroup: &group, Kind: &kind,
	}
	job.Spec.TrainJobTemplate.Spec.Trainer.Image = &image
	return job
}
