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

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	trainer "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	optimizationjob "github.com/kubeflow/trainer/v2/pkg/util/optimizationjob"
	utiltesting "github.com/kubeflow/trainer/v2/pkg/util/testing"
)

// TestDiagnosticRestartAfterAmbiguousCreate is a diagnostic, not a production
// regression test. It deliberately holds the informer LIST stale across a
// simulated reconciler restart to see whether losing the in-memory expectations
// can exceed the trial budget. Real manager startup cache-sync semantics must be
// checked separately before interpreting this as a Kubernetes bug.
func TestDiagnosticRestartAfterAmbiguousCreate(t *testing.T) {
	ctx := context.Background()
	optJob := diagnosticValidOptimizationJob()
	name := optimizationjob.GetAlgorithmServiceName(optJob)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: optJob.Namespace},
		Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: optJob.Namespace}}

	builder := utiltesting.NewClientBuilder().
		WithStatusSubresource(&trainer.OptimizationJob{}, &trainer.TrainJob{}, &appsv1.Deployment{}).
		WithObjects(optJob, deploy, svc)
	if err := SetupIndexes(ctx, utiltesting.AsIndex(builder)); err != nil {
		t.Fatalf("setup indexes: %v", err)
	}
	backing := builder.Build()
	cacheStale := true
	firstCreate := true

	cacheClient := interceptor.NewClient(backing, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if jobs, ok := list.(*trainer.TrainJobList); ok && cacheStale {
				jobs.Items = nil
			}
			return nil
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*trainer.TrainJob); ok && firstCreate {
				firstCreate = false
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				return fmt.Errorf("injected API timeout after a committed TrainJob Create")
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	suggestions := &mockSearchAlgorithmClient{
		mockedAssignments: [][]trainer.ParameterAssignment{{{Name: "lr", Value: "0.01"}}},
	}
	newController := func() *OptimizationJobReconciler {
		return NewOptimizationJobReconciler(
			cacheClient, backing.Scheme(), &events.FakeRecorder{}, suggestions, backing,
		)
	}
	reconcileOnce := func(r *OptimizationJobReconciler) {
		t.Helper()
		key := types.NamespacedName{Namespace: optJob.Namespace, Name: optJob.Name}
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}
	actualTrainJobs := func() int {
		t.Helper()
		list := &trainer.TrainJobList{}
		if err := backing.List(ctx, list, client.InNamespace(optJob.Namespace)); err != nil {
			t.Fatalf("list committed TrainJobs: %v", err)
		}
		return len(list.Items)
	}

	original := newController()
	reconcileOnce(original)
	if n := actualTrainJobs(); n != 1 {
		t.Fatalf("initial API-timeout-after-commit: got %d trials, want 1", n)
	}
	before := suggestions.calls
	reconcileOnce(original)
	if n := actualTrainJobs(); n != 1 || suggestions.calls != before {
		t.Fatalf("same-controller control: trials=%d, suggestions=%d (before=%d)", n, suggestions.calls, before)
	}
	t.Logf("CONTROL PASS: same controller, stale LIST, API timeout: committed trials=%d, suggestions=%d", actualTrainJobs(), suggestions.calls)

	// A newly constructed controller has no process-local pending expectations.
	afterRestart := newController()
	reconcileOnce(afterRestart)
	n := actualTrainJobs()
	t.Logf("DIAGNOSTIC: simulated restart while LIST remains stale: committed trials=%d, requested budget=%d", n, optJob.Spec.NumTrials)
	if n != 2 {
		t.Fatalf("diagnostic hypothesis not reproduced: committed trials=%d, expected 2", n)
	}
	if n <= int(optJob.Spec.NumTrials) {
		t.Fatalf("diagnostic invariant unexpectedly holds: %d <= %d", n, optJob.Spec.NumTrials)
	}

	cacheStale = false
	reconcileOnce(newController())
	if n := actualTrainJobs(); n != 2 {
		t.Fatalf("synced-cache control: expected no additional trials, got %d", n)
	}
	t.Log("REPRODUCED ONLY UNDER INJECTED CROSS-RESTART STALE LIST; real startup cache-sync behavior not established")
}
