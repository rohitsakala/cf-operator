package extendedjob

import (
	"code.cloudfoundry.org/cf-operator/pkg/kube/apis"
	ejv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedjob/v1alpha1"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/context"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/owner"
	eowner "code.cloudfoundry.org/cf-operator/pkg/kube/util/owner"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Owner interface to manage ownership on configs and secrets
type Owner interface {
	Sync(context.Context, apis.Object, corev1.PodSpec) error
	RemoveOwnerReferences(context.Context, apis.Object, []apis.Object) error
	ListConfigsOwnedBy(context.Context, apis.Object) ([]apis.Object, error)
}

// AddOwnership creates a new ExtendedJob controller to update ownership on configs for auto errands.
func AddOwnership(log *zap.SugaredLogger, config *context.Config, mgr manager.Manager) error {
	l := log.Named("ext-job-owner-reconciler")
	owner := eowner.NewOwner(mgr.GetClient(), l, mgr.GetScheme())
	r := NewOwnershipReconciler(l, config, mgr, controllerutil.SetControllerReference, owner)
	c, err := controller.New("ext-job-owner-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Only trigger if Spec.UpdateOnConfigChange is relevant and it's an auto errand
	p := predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return false },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			o := e.ObjectOld.(*ejv1.ExtendedJob)
			n := e.ObjectNew.(*ejv1.ExtendedJob)
			return n.IsAutoErrand() && (updateOnConfigChanged(n, o) || n.ToBeDeleted())
		},
	}
	err = c.Watch(&source.Kind{Type: &ejv1.ExtendedJob{}}, &handler.EnqueueRequestForObject{}, p)
	if err != nil {
		return err
	}

	// pick up new configs which are referenced by an extended job
	ctx, _ := context.NewBackgroundContextWithTimeout(config.CtxType, config.CtxTimeOut)
	p = predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			o := e.Object.(*corev1.ConfigMap)

			reconcile, err := hasConfigReferences(ctx, mgr.GetClient(), *o)
			if err != nil {
				log.Errorf("Failed to query extended jobs: %s", err)
			}

			return reconcile
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc:  func(e event.UpdateEvent) bool { return false },
	}

	mapConfigs := handler.ToRequestsFunc(func(a handler.MapObject) []reconcile.Request {
		configMap := a.Object.(*corev1.ConfigMap)
		return reconcilesForConfigMap(ctx, mgr, log, *configMap)
	})

	err = c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestsFromMapFunc{ToRequests: mapConfigs}, p)
	if err != nil {
		return err
	}

	// and for secrets
	p = predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			o := e.Object.(*corev1.Secret)

			reconcile, err := hasSecretReferences(ctx, mgr.GetClient(), *o)
			if err != nil {
				log.Errorf("Failed to query extended jobs: %s", err)
			}

			return reconcile
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc:  func(e event.UpdateEvent) bool { return false },
	}

	mapSecrets := handler.ToRequestsFunc(func(a handler.MapObject) []reconcile.Request {
		secret := a.Object.(*corev1.Secret)
		return reconcilesForSecret(ctx, mgr, log, *secret)
	})

	err = c.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestsFromMapFunc{ToRequests: mapSecrets}, p)
	if err != nil {
		return err
	}

	return nil
}

func updateOnConfigChanged(n, o *ejv1.ExtendedJob) bool {
	return (o.Spec.UpdateOnConfigChange == false && n.Spec.UpdateOnConfigChange == true) ||
		(o.Spec.UpdateOnConfigChange == true && n.Spec.UpdateOnConfigChange == false)
}

// config name referenced by any extjob?
func hasConfigReferences(ctx context.Context, c client.Client, o corev1.ConfigMap) (bool, error) {
	extJobs := &ejv1.ExtendedJobList{}
	err := c.List(ctx, &client.ListOptions{}, extJobs)
	if err != nil {
		return false, err
	}

	if len(extJobs.Items) < 1 {
		return false, nil
	}

	for _, extJob := range extJobs.Items {
		configMapNames, _ := owner.GetConfigNamesFromSpec(extJob.Spec.Template.Spec)
		if _, ok := configMapNames[o.GetName()]; ok {
			return true, nil
		}
	}

	return false, nil
}

// secret name referenced by any extjob?
func hasSecretReferences(ctx context.Context, c client.Client, o corev1.Secret) (bool, error) {
	extJobs := &ejv1.ExtendedJobList{}
	err := c.List(ctx, &client.ListOptions{}, extJobs)
	if err != nil {
		return false, err
	}

	if len(extJobs.Items) < 1 {
		return false, nil
	}

	for _, extJob := range extJobs.Items {
		_, secretNames := owner.GetConfigNamesFromSpec(extJob.Spec.Template.Spec)
		if _, ok := secretNames[o.GetName()]; ok {
			return true, nil
		}
	}

	return false, nil
}

func reconcilesForConfigMap(ctx context.Context, mgr manager.Manager, log *zap.SugaredLogger, configMap corev1.ConfigMap) []reconcile.Request {
	reconciles := []reconcile.Request{}

	extJobs := &ejv1.ExtendedJobList{}
	err := mgr.GetClient().List(ctx, &client.ListOptions{}, extJobs)
	if err != nil || len(extJobs.Items) < 1 {
		return reconciles
	}

	for _, extJob := range extJobs.Items {
		configMapNames, _ := eowner.GetConfigNamesFromSpec(extJob.Spec.Template.Spec)
		if _, ok := configMapNames[configMap.GetName()]; ok {
			log.Debugf("==== RECONCILE WANT: %s <- %s", configMap.GetName(), extJob.GetName())
			reconciles = append(reconciles, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      extJob.GetName(),
					Namespace: extJob.GetNamespace(),
				},
			})
		}
	}

	return reconciles
}

func reconcilesForSecret(ctx context.Context, mgr manager.Manager, log *zap.SugaredLogger, secret corev1.Secret) []reconcile.Request {
	reconciles := []reconcile.Request{}

	extJobs := &ejv1.ExtendedJobList{}
	err := mgr.GetClient().List(ctx, &client.ListOptions{}, extJobs)
	if err != nil || len(extJobs.Items) < 1 {
		return reconciles
	}

	for _, extJob := range extJobs.Items {
		_, secretNames := eowner.GetConfigNamesFromSpec(extJob.Spec.Template.Spec)
		if _, ok := secretNames[secret.GetName()]; ok {
			reconciles = append(reconciles, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      extJob.GetName(),
					Namespace: extJob.GetNamespace(),
				},
			})
		}
	}

	return reconciles
}
