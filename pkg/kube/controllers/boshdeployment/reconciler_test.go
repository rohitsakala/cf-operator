package boshdeployment_test

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bdm "code.cloudfoundry.org/cf-operator/pkg/bosh/manifest"
	"code.cloudfoundry.org/cf-operator/pkg/bosh/manifest/fakes"
	bdc "code.cloudfoundry.org/cf-operator/pkg/kube/apis/boshdeployment/v1alpha1"
	"code.cloudfoundry.org/cf-operator/pkg/kube/controllers"
	cfd "code.cloudfoundry.org/cf-operator/pkg/kube/controllers/boshdeployment"
	cfakes "code.cloudfoundry.org/cf-operator/pkg/kube/controllers/fakes"
	cfctx "code.cloudfoundry.org/cf-operator/pkg/kube/util/context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReconcileBoshDeployment", func() {
	var (
		recorder   *record.FakeRecorder
		manager    *cfakes.FakeManager
		reconciler reconcile.Reconciler
		request    reconcile.Request
		resolver   fakes.FakeResolver
		manifest   *bdm.Manifest
		log        *zap.SugaredLogger
		ctrsConfig *cfctx.Config
	)

	BeforeEach(func() {
		controllers.AddToScheme(scheme.Scheme)
		recorder = record.NewFakeRecorder(20)
		manager = &cfakes.FakeManager{}
		manager.GetSchemeReturns(scheme.Scheme)
		manager.GetRecorderReturns(recorder)
		resolver = fakes.FakeResolver{}

		request = reconcile.Request{NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"}}
		manifest = &bdm.Manifest{
			Name: "fake-manifest",
			Releases: []*bdm.Release{
				{
					Name:    "bar",
					URL:     "docker.io/cfcontainerization",
					Version: "1.0",
					Stemcell: &bdm.ReleaseStemcell{
						OS:      "opensuse",
						Version: "42.3",
					},
				},
			},
			InstanceGroups: []*bdm.InstanceGroup{
				{
					Name: "fakepod",
					Jobs: []bdm.Job{
						{
							Name:    "foo",
							Release: "bar",
							Properties: bdm.JobProperties{
								Properties: map[string]interface{}{
									"password": "((foo_password))",
								},
							},
						},
					},
				},
			},
			Variables: []bdm.Variable{
				{
					Name: "foo_password",
					Type: "password",
				},
			},
		}
		core, _ := observer.New(zapcore.InfoLevel)
		ctrsConfig = &cfctx.Config{ //Set the context to be TODO
			CtxTimeOut: 10 * time.Second,
			CtxType:    cfctx.NewContext(),
		}
		log = zap.New(core).Sugar()
	})

	JustBeforeEach(func() {
		resolver.ResolveManifestReturns(manifest, nil)
		reconciler = cfd.NewReconciler(log, ctrsConfig, manager, &resolver, controllerutil.SetControllerReference)
	})

	Describe("Reconcile", func() {
		Context("when the manifest can not be resolved", func() {
			var (
				client *cfakes.FakeClient
			)
			BeforeEach(func() {
				client = &cfakes.FakeClient{}
				manager.GetClientReturns(client)
			})

			It("returns an empty Result when the resource was not found", func() {
				client.GetReturns(errors.NewNotFound(schema.GroupResource{}, "not found is requeued"))

				reconciler.Reconcile(request)
				result, err := reconciler.Reconcile(request)
				Expect(err).ToNot(HaveOccurred())
				Expect(reconcile.Result{}).To(Equal(result))
			})

			It("throws an error when the request failed", func() {
				client.GetReturns(errors.NewBadRequest("bad request returns error"))

				_, err := reconciler.Reconcile(request)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("bad request returns error"))

				// check for events
				Expect(<-recorder.Events).To(ContainSubstring("GetBOSHDeployment Error"))
			})

			It("handles errors when resolving the BOSHDeployment", func() {
				resolver.ResolveManifestReturns(nil, fmt.Errorf("resolver error"))

				_, err := reconciler.Reconcile(request)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("resolver error"))

				// check for events
				Expect(<-recorder.Events).To(ContainSubstring("ResolveManifest Error"))
			})

			It("handles errors when missing instance groups", func() {
				resolver.ResolveManifestReturns(&bdm.Manifest{
					InstanceGroups: []*bdm.InstanceGroup{},
				}, nil)

				_, err := reconciler.Reconcile(request)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("manifest is missing instance groups"))

				// check for events
				Expect(<-recorder.Events).To(ContainSubstring("MissingInstance Error"))
			})
		})

		Context("when the manifest can be resolved", func() {
			var (
				client client.Client
			)
			BeforeEach(func() {
				client = fake.NewFakeClient(
					&bdc.BOSHDeployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "foo",
							Namespace: "default",
						},
						Spec: bdc.BOSHDeploymentSpec{},
					},
				)
				manager.GetClientReturns(client)
			})

			Context("With an empty manifest", func() {
				BeforeEach(func() {
					manifest = &bdm.Manifest{}
				})

				It("raises an error if there are no instance groups defined in the manifest", func() {
					_, err := reconciler.Reconcile(request)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("manifest is missing instance groups"))
				})
			})

			It("handles errors when setting the owner reference on the object", func() {
				ctrsConfig := &cfctx.Config{ //Set the context to be TODO
					CtxTimeOut: 10 * time.Second,
					CtxType:    cfctx.NewContext(),
				}
				reconciler = cfd.NewReconciler(log, ctrsConfig, manager, &resolver, func(owner, object metav1.Object, scheme *runtime.Scheme) error {
					return fmt.Errorf("failed to set reference")
				})

				// First reconcile doesn't create any resources
				_, err := reconciler.Reconcile(request)
				Expect(err).ToNot(HaveOccurred())
				// We need a second reconcile to create some stuff
				_, err = reconciler.Reconcile(request)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to set reference"))

				// check for events
				Expect(<-recorder.Events).To(ContainSubstring("VariableGeneration Error"))

				instance := &bdc.BOSHDeployment{}
				err = client.Get(context.Background(), types.NamespacedName{Name: "foo", Namespace: "default"}, instance)
				Expect(err).ToNot(HaveOccurred())
			})

			It("goes from ops applied to variable generated state successfully", func() {
				result, err := reconciler.Reconcile(request)
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{
					Requeue: true,
				}))

				instance := &bdc.BOSHDeployment{}
				err = client.Get(context.Background(), types.NamespacedName{Name: "foo", Namespace: "default"}, instance)
				Expect(err).ToNot(HaveOccurred())
				Expect(instance.Status.State).To(Equal(cfd.OpsAppliedState))

				result, err = reconciler.Reconcile(request)
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{
					Requeue: true,
				}))

				err = client.Get(context.Background(), types.NamespacedName{Name: "foo", Namespace: "default"}, instance)
				Expect(err).ToNot(HaveOccurred())
				Expect(instance.Status.State).To(Equal(cfd.VariableGeneratedState))
			})
		})
	})
})
