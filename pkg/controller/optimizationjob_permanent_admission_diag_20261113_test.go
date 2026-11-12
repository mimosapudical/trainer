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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	trainer "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/v2/pkg/constants"
	optimizationjob "github.com/kubeflow/trainer/v2/pkg/util/optimizationjob"
	utiltesting "github.com/kubeflow/trainer/v2/pkg/util/testing"
)

// TestDiagnosticPermanentTrialAdmissionRejection verifies the current retry
// behavior when the Kubernetes API returns an explicitly non-retryable Invalid
// response for every child TrainJob creation. This is a standalone diagnostic
// of behavior, not a proposed policy for classifying all failures as fatal.
func TestDiagnosticPermanentTrialAdmissionRejection(t *testing.T) {
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
	attempts := 0
	rejecting := interceptor.NewClient(backing, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*trainer.TrainJob); ok {
				attempts++
				return apierrors.NewInvalid(
					trainer.GroupVersion.WithKind("TrainJob").GroupKind(), obj.GetName(),
					field.ErrorList{field.Invalid(field.NewPath("spec", "trainer"), "invalid", "injected permanent admission rejection")},
				)
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	suggestions := &mockSearchAlgorithmClient{
		mockedAssignments: [][]trainer.ParameterAssignment{{{Name: "lr", Value: "0.01"}}},
	}
	r := NewOptimizationJobReconciler(rejecting, backing.Scheme(), &events.FakeRecorder{}, suggestions, backing)
	key := types.NamespacedName{Namespace: optJob.Namespace, Name: optJob.Name}

	for i := 0; i < 3; i++ {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		if err == nil {
			t.Fatalf("reconcile attempt %d: expected Invalid failure", i+1)
		}
		if !apierrors.IsInvalid(err) {
			// The controller wraps individual API errors in an aggregated error.
			t.Logf("attempt %d surfaced wrapped error: %v", i+1, err)
		}
	}
	if attempts != 3 || suggestions.calls != 3 {
		t.Fatalf("expected 3 repeated failed creates and suggestion calls; got creates=%d suggestions=%d", attempts, suggestions.calls)
	}

	var state trainer.OptimizationJob
	if err := backing.Get(ctx, key, &state); err != nil {
		t.Fatalf("get optimization status: %v", err)
	}
	if state.Status == nil {
		t.Fatal("status was never created")
	}
	if meta.IsStatusConditionTrue(state.Status.Conditions, constants.OptimizationJobFailed) {
		t.Fatal("controller now fails permanently invalid trial creation: diagnostic must be updated")
	}
	list := &trainer.TrainJobList{}
	if err := backing.List(ctx, list, client.InNamespace(optJob.Namespace)); err != nil {
		t.Fatalf("list trials: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("expected no trial to be created; got %d", len(list.Items))
	}
	t.Logf("REPRODUCED: Invalid admission errors=%d, suggestions=%d, created trials=0, Failed=False", attempts, suggestions.calls)
}
