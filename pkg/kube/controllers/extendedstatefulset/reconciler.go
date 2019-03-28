package extendedstatefulset

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"k8s.io/api/apps/v1beta2"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	podUtils "k8s.io/kubernetes/pkg/api/v1/pod"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"code.cloudfoundry.org/cf-operator/pkg/kube/apis"
	essv1a1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedstatefulset/v1alpha1"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/context"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/finalizer"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/owner"
)

const (
	// OptimisticLockErrorMsg is an error message shown when locking fails
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
	// EnvKubeAz is set by available zone name
	EnvKubeAz = "KUBE_AZ"
	// EnvBoshAz is set by available zone name
	EnvBoshAz = "BOSH_AZ"
	// EnvReplicas describes the number of replicas in the ExtendedStatefulSet
	EnvReplicas = "REPLICAS"
	// EnvCfOperatorAz is set by available zone name
	EnvCfOperatorAz = "CF_OPERATOR_AZ"
	// EnvCfOperatorAzIndex is set by available zone index
	EnvCfOperatorAzIndex = "CF_OPERATOR_AZ_INDEX"
)

// Check that ReconcileExtendedStatefulSet implements the reconcile.Reconciler interface
var _ reconcile.Reconciler = &ReconcileExtendedStatefulSet{}

type setReferenceFunc func(owner, object metav1.Object, scheme *runtime.Scheme) error

// Owner bundles funcs to manage ownership on referenced configmaps and secrets
type Owner interface {
	Update(context.Context, apis.Object, []apis.Object, []apis.Object) error
	RemoveOwnerReferences(context.Context, apis.Object, []apis.Object) error
	ListConfigs(context.Context, string, corev1.PodSpec) ([]apis.Object, error)
	ListConfigsOwnedBy(context.Context, apis.Object) ([]apis.Object, error)
}

// NewReconciler returns a new reconcile.Reconciler
func NewReconciler(log *zap.SugaredLogger, ctrConfig *context.Config, mgr manager.Manager, srf setReferenceFunc) reconcile.Reconciler {
	reconcilerLog := log.Named("extendedstatefulset-reconciler")
	reconcilerLog.Info("Creating a reconciler for ExtendedStatefulSet")

	return &ReconcileExtendedStatefulSet{
		log:          reconcilerLog,
		ctrConfig:    ctrConfig,
		client:       mgr.GetClient(),
		scheme:       mgr.GetScheme(),
		setReference: srf,
		owner:        owner.NewOwner(mgr.GetClient(), reconcilerLog, mgr.GetScheme()),
	}
}

// ReconcileExtendedStatefulSet reconciles an ExtendedStatefulSet object
type ReconcileExtendedStatefulSet struct {
	client       client.Client
	scheme       *runtime.Scheme
	setReference setReferenceFunc
	log          *zap.SugaredLogger
	ctrConfig    *context.Config
	owner        Owner
}

// Reconcile reads that state of the cluster for a ExtendedStatefulSet object
// and makes changes based on the state read and what is in the ExtendedStatefulSet.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileExtendedStatefulSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	r.log.Info("Reconciling ExtendedStatefulSet ", request.NamespacedName)

	// Fetch the ExtendedStatefulSet we need to reconcile
	exStatefulSet := &essv1a1.ExtendedStatefulSet{}

	// Set the ctx to be Background, as the top-level context for incoming requests.
	ctx, cancel := context.NewBackgroundContextWithTimeout(r.ctrConfig.CtxType, r.ctrConfig.CtxTimeOut)
	defer cancel()

	err := r.client.Get(ctx, request.NamespacedName, exStatefulSet)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			r.log.Debug("Skip reconcile: ExtendedStatefulSet not found")
			return reconcile.Result{}, nil
		}

		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Clean up exStatefulSet
	if exStatefulSet.ToBeDeleted() {
		r.log.Debug("ExtendedStatefulSet '", exStatefulSet.Name, "' instance marked for deletion. Clean up process.")
		return r.handleDelete(ctx, exStatefulSet)
	}

	// TODO: generate an ID for the request

	// Get the actual StatefulSet
	actualStatefulSet, actualVersion, err := r.getActualStatefulSet(ctx, exStatefulSet)
	if err != nil {
		r.log.Error("Could not retrieve latest StatefulSet owned by ExtendedStatefulSet '", request.NamespacedName, "': ", err)
		return reconcile.Result{}, err
	}

	// Calculate the desired statefulSets
	desiredStatefulSets, desiredVersion, err := r.calculateDesiredStatefulSets(exStatefulSet, actualStatefulSet)
	if err != nil {
		r.log.Error("Could not calculate StatefulSet owned by ExtendedStatefulSet '", request.NamespacedName, "': ", err)
		return reconcile.Result{}, err
	}

	for _, desiredStatefulSet := range desiredStatefulSets {
		// If actual version is zero, there is no StatefulSet live
		if actualVersion != desiredVersion {
			// If it doesn't exist, create it
			r.log.Info("StatefulSet '", desiredStatefulSet.Name, "' owned by ExtendedStatefulSet '", request.NamespacedName, "' not found, will be created.")

			// Record the template before creating the StatefulSet, so we don't include default values such as
			// `ImagePullPolicy`, `TerminationMessagePath`, etc. in the signature.
			originalTemplate := exStatefulSet.Spec.Template.DeepCopy()
			if err := r.createStatefulSet(ctx, exStatefulSet, &desiredStatefulSet); err != nil {
				r.log.Error("Could not create StatefulSet for ExtendedStatefulSet '", request.NamespacedName, "': ", err)
				return reconcile.Result{}, err
			}
			exStatefulSet.Spec.Template = *originalTemplate
		} else {
			// If it does exist, do a deep equal and check that we own it
			r.log.Info("StatefulSet '", desiredStatefulSet.Name, "' owned by ExtendedStatefulSet '", request.NamespacedName, "' has not changed, checking if any other changes are necessary.")
		}
	}

	statefulSetVersions, err := r.listStatefulSetVersions(ctx, exStatefulSet)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Update StatefulSets configSHA1 and trigger statefulSet rollingUpdate if necessary
	if exStatefulSet.Spec.UpdateOnEnvChange {
		r.log.Debugf("Considering configurations to trigger update.")

		err = r.updateStatefulSetsConfigSHA1(ctx, exStatefulSet)
		if err != nil {
			// TODO fix the object has been modified
			r.log.Error("Could not update StatefulSets owned by ExtendedStatefulSet '", request.NamespacedName, "': ", err)
			return reconcile.Result{Requeue: true, RequeueAfter: 1 * time.Second}, err
		}
	}

	// Update the status of the resource
	if !reflect.DeepEqual(statefulSetVersions, exStatefulSet.Status.Versions) {
		r.log.Debugf("Updating ExtendedStatefulSet '%s'", request.NamespacedName)
		exStatefulSet.Status.Versions = statefulSetVersions
		updateErr := r.client.Update(ctx, exStatefulSet)
		if updateErr != nil {
			r.log.Errorf("Failed to update exStatefulSet status: %v", updateErr)
		}
	}

	maxAvailableVersion := exStatefulSet.GetMaxAvailableVersion(statefulSetVersions)

	if len(statefulSetVersions) > 1 {
		// Cleanup versions smaller than the max available version
		err = r.cleanupStatefulSets(ctx, exStatefulSet, maxAvailableVersion, &statefulSetVersions)
		if err != nil {
			r.log.Error("Could not cleanup StatefulSets owned by ExtendedStatefulSet '", request.NamespacedName, "': ", err)
			return reconcile.Result{}, err
		}
	}

	if !statefulSetVersions[desiredVersion] {
		r.log.Debug("Waiting for the desired version to become available for ExtendedStatefulSet ", request.NamespacedName)
		return reconcile.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	// Reconcile stops since only one version or no version exists.
	r.log.Debug("Version '", desiredVersion, "' is available")
	return reconcile.Result{}, nil
}

// calculateDesiredStatefulSets generates the desired StatefulSets that should exist
func (r *ReconcileExtendedStatefulSet) calculateDesiredStatefulSets(exStatefulSet *essv1a1.ExtendedStatefulSet, actualStatefulSet *v1beta2.StatefulSet) ([]v1beta2.StatefulSet, int, error) {
	var desiredStatefulSets []v1beta2.StatefulSet

	template := exStatefulSet.Spec.Template

	// Place the StatefulSet in the same namespace as the ExtendedStatefulSet
	template.SetNamespace(exStatefulSet.Namespace)

	desiredVersion, err := exStatefulSet.DesiredVersion(actualStatefulSet)
	if err != nil {
		return nil, 0, err
	}

	templateSHA1, err := exStatefulSet.CalculateStatefulSetSHA1()
	if err != nil {
		return nil, 0, err
	}

	if exStatefulSet.Spec.ZoneNodeLabel == "" {
		exStatefulSet.Spec.ZoneNodeLabel = essv1a1.DefaultZoneNodeLabel
	}

	if len(exStatefulSet.Spec.Zones) > 0 {
		for zoneIndex, zoneName := range exStatefulSet.Spec.Zones {
			r.log.Debugf("Generating a template for zone '%d/%s'", zoneIndex, zoneName)
			statefulSet, err := r.generateSingleStatefulSet(exStatefulSet, &template, zoneIndex, zoneName, desiredVersion, templateSHA1)
			if err != nil {
				return desiredStatefulSets, desiredVersion, errors.Wrapf(err, "Could not generate StatefulSet template for AZ '%d/%s'", zoneIndex, zoneName)
			}
			desiredStatefulSets = append(desiredStatefulSets, *statefulSet)
		}

	} else {
		r.log.Debug("Generating a template for single zone")
		statefulSet, err := r.generateSingleStatefulSet(exStatefulSet, &template, -1, "", desiredVersion, templateSHA1)
		if err != nil {
			return desiredStatefulSets, desiredVersion, errors.Wrap(err, "Could not generate StatefulSet template for single zone")
		}
		desiredStatefulSets = append(desiredStatefulSets, *statefulSet)
	}

	// Set version and template SHA1
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}

	template.Annotations[essv1a1.AnnotationStatefulSetSHA1] = templateSHA1
	template.Annotations[essv1a1.AnnotationVersion] = fmt.Sprintf("%d", desiredVersion)

	return desiredStatefulSets, desiredVersion, nil
}

// createStatefulSet creates a StatefulSet
func (r *ReconcileExtendedStatefulSet) createStatefulSet(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet, statefulSet *v1beta2.StatefulSet) error {

	// Set the owner of the StatefulSet, so it's garbage collected,
	// and we can find it later
	r.log.Info("Setting owner for StatefulSet '", statefulSet.Name, "' to ExtendedStatefulSet '", exStatefulSet.Name, "' in namespace '", exStatefulSet.Namespace, "'.")
	if err := r.setReference(exStatefulSet, statefulSet, r.scheme); err != nil {
		return errors.Wrapf(err, "could not set owner for StatefulSet '%s' to ExtendedStatefulSet '%s' in namespace '%s'", statefulSet.Name, exStatefulSet.Name, exStatefulSet.Namespace)
	}

	// Create the StatefulSet
	if err := r.client.Create(ctx, statefulSet); err != nil {
		return errors.Wrapf(err, "could not create StatefulSet '%s' for ExtendedStatefulSet '%s' in namespace '%s'", statefulSet.Name, exStatefulSet.Name, exStatefulSet.Namespace)
	}

	r.log.Info("Created StatefulSet '", statefulSet.Name, "' for ExtendedStatefulSet '", exStatefulSet.Name, "' in namespace '", exStatefulSet.Namespace, "'.")

	return nil
}

// cleanupStatefulSets cleans up StatefulSets and versions if they are no longer required
func (r *ReconcileExtendedStatefulSet) cleanupStatefulSets(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet, maxAvailableVersion int, versions *map[int]bool) error {
	r.log.Infof("Cleaning up StatefulSets for ExtendedStatefulSet '%s' less than version %d.", exStatefulSet.Name, maxAvailableVersion)

	statefulSets, err := r.listStatefulSets(ctx, exStatefulSet)
	if err != nil {
		return errors.Wrapf(err, "couldn't list StatefulSets for cleanup")
	}

	for _, statefulSet := range statefulSets {
		r.log.Debug("Considering StatefulSet '", statefulSet.Name, "' for cleanup.")

		strVersion, found := statefulSet.Annotations[essv1a1.AnnotationVersion]
		if !found {
			return errors.Errorf("version annotation is not found from: %+v", statefulSet.Annotations)
		}

		version, err := strconv.Atoi(strVersion)
		if err != nil {
			return errors.Wrapf(err, "version annotation is not an int: %s", strVersion)
		}

		if version >= maxAvailableVersion {
			continue
		}

		r.log.Debugf("Deleting StatefulSet '%s'", statefulSet.Name)
		err = r.client.Delete(ctx, &statefulSet, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if err != nil {
			r.log.Error("Could not delete StatefulSet  '", statefulSet.Name, "': ", err)
			return err
		}

		delete(*versions, version)
	}

	return nil
}

// listStatefulSets gets all StatefulSets owned by the ExtendedStatefulSet
func (r *ReconcileExtendedStatefulSet) listStatefulSets(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet) ([]v1beta2.StatefulSet, error) {
	r.log.Debug("Listing StatefulSets owned by ExtendedStatefulSet '", exStatefulSet.Name, "'.")

	result := []v1beta2.StatefulSet{}

	// Get owned resources
	// Go through each StatefulSet
	allStatefulSets := &v1beta2.StatefulSetList{}
	err := r.client.List(
		ctx,
		&client.ListOptions{
			Namespace:     exStatefulSet.Namespace,
			LabelSelector: labels.Everything(),
		},
		allStatefulSets)
	if err != nil {
		return nil, err
	}

	for _, statefulSet := range allStatefulSets.Items {
		if metav1.IsControlledBy(&statefulSet, exStatefulSet) {
			result = append(result, statefulSet)
			r.log.Debug("StatefulSet '", statefulSet.Name, "' owned by ExtendedStatefulSet '", exStatefulSet.Name, "'.")
		} else {
			r.log.Debug("StatefulSet '", statefulSet.Name, "' is not owned by ExtendedStatefulSet '", exStatefulSet.Name, "', ignoring.")
		}
	}

	return result, nil
}

// getActualStatefulSet gets the latest (by version) StatefulSet owned by the ExtendedStatefulSet
func (r *ReconcileExtendedStatefulSet) getActualStatefulSet(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet) (*v1beta2.StatefulSet, int, error) {
	r.log.Debug("Listing StatefulSets owned by ExtendedStatefulSet '", exStatefulSet.Name, "'.")

	// Default response is an empty StatefulSet with version '0' and an empty signature
	result := &v1beta2.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				essv1a1.AnnotationStatefulSetSHA1: "",
				essv1a1.AnnotationVersion:         "0",
			},
		},
	}
	maxVersion := 0

	// Get all owned StatefulSets
	statefulSets, err := r.listStatefulSets(ctx, exStatefulSet)
	if err != nil {
		return nil, 0, err
	}

	for _, ss := range statefulSets {
		strVersion := ss.Annotations[essv1a1.AnnotationVersion]
		version, err := strconv.Atoi(strVersion)
		if err != nil {
			return nil, 0, err
		}

		if ss.Annotations != nil && version > maxVersion {
			result = &ss
			maxVersion = version
		}
	}

	return result, maxVersion, nil
}

// listStatefulSetVersions gets all StatefulSets' versions and ready status owned by the ExtendedStatefulSet
func (r *ReconcileExtendedStatefulSet) listStatefulSetVersions(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet) (map[int]bool, error) {
	result := map[int]bool{}

	statefulSets, err := r.listStatefulSets(ctx, exStatefulSet)
	if err != nil {
		return nil, err
	}

	for _, statefulSet := range statefulSets {
		strVersion, found := statefulSet.Annotations[essv1a1.AnnotationVersion]
		if !found {
			return result, errors.Errorf("version annotation is not found from: %+v", statefulSet.Annotations)
		}

		version, err := strconv.Atoi(strVersion)
		if err != nil {
			return result, errors.Wrapf(err, "version annotation is not an int: %s", strVersion)
		}

		ready, err := r.isStatefulSetReady(ctx, &statefulSet)
		if err != nil {
			return nil, err
		}

		result[version] = ready
	}

	return result, nil
}

// isStatefulSetReady returns true if one owned Pod is running
func (r *ReconcileExtendedStatefulSet) isStatefulSetReady(ctx context.Context, statefulSet *v1beta2.StatefulSet) (bool, error) {
	labelsSelector := labels.Set{
		"controller-revision-hash": statefulSet.Status.CurrentRevision,
	}

	podList := &corev1.PodList{}
	err := r.client.List(
		ctx,
		&client.ListOptions{
			Namespace:     statefulSet.Namespace,
			LabelSelector: labelsSelector.AsSelector(),
		},
		podList,
	)
	if err != nil {
		return false, err
	}

	for _, pod := range podList.Items {
		if metav1.IsControlledBy(&pod, statefulSet) {
			if podUtils.IsPodReady(&pod) {
				r.log.Debug("Pod '", statefulSet.Name, "' owned by StatefulSet '", statefulSet.Name, "' is running.")
				return true, nil
			}
		}
	}

	return false, nil
}

// updateStatefulSetsConfigSHA1 Update StatefulSets configSHA1 and config OwnerReferences if necessary
func (r *ReconcileExtendedStatefulSet) updateStatefulSetsConfigSHA1(ctx context.Context, exStatefulSet *essv1a1.ExtendedStatefulSet) error {
	statefulSets, err := r.listStatefulSets(ctx, exStatefulSet)
	if err != nil {
		return errors.Wrapf(err, "list StatefulSets owned by %s/%s", exStatefulSet.GetNamespace(), exStatefulSet.GetName())
	}

	for _, statefulSet := range statefulSets {
		r.log.Debug("Getting all ConfigMaps and Secrets that are referenced in '", statefulSet.Name, "' Spec.")

		namespace := statefulSet.GetNamespace()

		currentConfigRef, err := r.owner.ListConfigs(ctx, namespace, statefulSet.Spec.Template.Spec)
		if err != nil {
			return errors.Wrapf(err, "could not list ConfigMaps and Secrets from '%s' spec", statefulSet.Name)
		}

		existingConfigs, err := r.owner.ListConfigsOwnedBy(ctx, exStatefulSet)
		if err != nil {
			return errors.Wrapf(err, "could not list ConfigMaps and Secrets owned by '%s'", exStatefulSet.Name)
		}

		currentsha, err := calculateConfigHash(currentConfigRef)
		if err != nil {
			return err
		}

		// determines which children need to have their OwnerReferences added/updated
		// and which need to have their OwnerReferences removed and then performs all
		// updates
		r.log.Debug("Updating ownerReferences for StatefulSet '", exStatefulSet.Name, "' in namespace '", exStatefulSet.Namespace, "'.")

		err = r.owner.Update(ctx, exStatefulSet, existingConfigs, currentConfigRef)
		if err != nil {
			return fmt.Errorf("error updating OwnerReferences: %v", err)
		}

		oldsha, _ := statefulSet.Spec.Template.Annotations[essv1a1.AnnotationConfigSHA1]

		// If the current config sha doesn't match the existing config sha, update it
		if currentsha != oldsha {
			r.log.Debug("StatefulSet '", statefulSet.Name, "' configuration has changed.")

			err = r.updateConfigSHA1(ctx, &statefulSet, currentsha)
			if err != nil {
				return errors.Wrapf(err, "update StatefulSet config sha1")
			}
		}
	}

	// Add the object's Finalizer and update if necessary
	if !finalizer.HasFinalizer(exStatefulSet) {
		r.log.Debug("Adding Finalizer to ExtendedStatefulSet '", exStatefulSet.Name, "'.")
		// Fetch latest ExtendedStatefulSet before update
		key := types.NamespacedName{Namespace: exStatefulSet.GetNamespace(), Name: exStatefulSet.GetName()}
		err := r.client.Get(ctx, key, exStatefulSet)
		if err != nil {
			return errors.Wrapf(err, "could not get ExtendedStatefulSet '%s'", exStatefulSet.GetName())
		}

		finalizer.AddFinalizer(exStatefulSet)

		err = r.client.Update(ctx, exStatefulSet)
		if err != nil {
			r.log.Error("Could not add finalizer from ExtendedStatefulSet '", exStatefulSet.GetName(), "': ", err)
			return err
		}
	}

	return nil
}

// calculateConfigHash calculates the SHA1 of the JSON representation of configuration objects
func calculateConfigHash(children []apis.Object) (string, error) {
	// hashSource contains all the data to be hashed
	hashSource := struct {
		ConfigMaps map[string]map[string]string `json:"configMaps"`
		Secrets    map[string]map[string][]byte `json:"secrets"`
	}{
		ConfigMaps: make(map[string]map[string]string),
		Secrets:    make(map[string]map[string][]byte),
	}

	// Add the data from each child to the hashSource
	// All children should be in the same namespace so each one should have a
	// unique name
	for _, obj := range children {
		switch child := obj.(type) {
		case *corev1.ConfigMap:
			cm := corev1.ConfigMap(*child)
			hashSource.ConfigMaps[cm.GetName()] = cm.Data
		case *corev1.Secret:
			s := corev1.Secret(*child)
			hashSource.Secrets[s.GetName()] = s.Data
		default:
			return "", fmt.Errorf("passed unknown type: %v", reflect.TypeOf(child))
		}
	}

	// Convert the hashSource to a byte slice so that it can be hashed
	hashSourceBytes, err := json.Marshal(hashSource)
	if err != nil {
		return "", fmt.Errorf("unable to marshal JSON: %v", err)
	}

	return fmt.Sprintf("%x", sha1.Sum(hashSourceBytes)), nil
}

// updateConfigSHA1 updates the configuration sha1 of the given StatefulSet to the
// given string
func (r *ReconcileExtendedStatefulSet) updateConfigSHA1(ctx context.Context, actualStatefulSet *v1beta2.StatefulSet, hash string) error {
	var err error
	for i := 0; i < 3; i++ {
		key := types.NamespacedName{Namespace: actualStatefulSet.GetNamespace(), Name: actualStatefulSet.GetName()}
		err = r.client.Get(ctx, key, actualStatefulSet)
		if err != nil {
			return errors.Wrapf(err, "could not get StatefulSet '%s'", actualStatefulSet.GetName())
		}
		// Get the existing annotations
		annotations := actualStatefulSet.Spec.Template.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}

		// Update the annotations
		annotations[essv1a1.AnnotationConfigSHA1] = hash
		actualStatefulSet.Spec.Template.SetAnnotations(annotations)

		r.log.Debug("Updating new config sha1 for StatefulSet '", actualStatefulSet.GetName(), "'.")
		err = r.client.Update(ctx, actualStatefulSet)
		if err == nil || !strings.Contains(err.Error(), OptimisticLockErrorMsg) {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return errors.Wrapf(err, "Could not update StatefulSet '%s'", actualStatefulSet.GetName())
	}
	return nil
}

// handleDelete removes all existing Owner References pointing to ExtendedStatefulSet
// and object's Finalizers
func (r *ReconcileExtendedStatefulSet) handleDelete(ctx context.Context, extendedStatefulSet *essv1a1.ExtendedStatefulSet) (reconcile.Result, error) {
	r.log.Debug("Considering existing Owner References of ExtendedStatefulSet '", extendedStatefulSet.Name, "'.")

	// Fetch all ConfigMaps and Secrets with an OwnerReference pointing to the object
	existingConfigs, err := r.owner.ListConfigsOwnedBy(ctx, extendedStatefulSet)
	if err != nil {
		r.log.Error("Could not retrieve all ConfigMaps and Secrets owned by ExtendedStatefulSet '", extendedStatefulSet.Name, "': ", err)
		return reconcile.Result{}, err
	}

	// Remove StatefulSet OwnerReferences from the existingConfigs
	err = r.owner.RemoveOwnerReferences(ctx, extendedStatefulSet, existingConfigs)
	if err != nil {
		r.log.Error("Could not remove OwnerReferences pointing to ExtendedStatefulSet '", extendedStatefulSet.Name, "': ", err)
		return reconcile.Result{Requeue: true, RequeueAfter: 1 * time.Second}, err
	}

	// Remove the object's Finalizer and update if necessary
	copy := extendedStatefulSet.DeepCopy()
	finalizer.RemoveFinalizer(copy)
	if !reflect.DeepEqual(extendedStatefulSet, copy) {
		r.log.Debug("Removing finalizer from ExtendedStatefulSet '", copy.Name, "'.")
		key := types.NamespacedName{Namespace: copy.GetNamespace(), Name: copy.GetName()}
		err := r.client.Get(ctx, key, copy)
		if err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "could not get ExtendedStatefulSet ''%s'", copy.GetName())
		}

		finalizer.RemoveFinalizer(copy)

		err = r.client.Update(ctx, copy)
		if err != nil {
			r.log.Error("Could not remove finalizer from ExtendedStatefulSet '", copy.GetName(), "': ", err)
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// generateSingleStatefulSet creates a StatefulSet from one zone
func (r *ReconcileExtendedStatefulSet) generateSingleStatefulSet(extendedStatefulSet *essv1a1.ExtendedStatefulSet, template *v1beta2.StatefulSet, zoneIndex int, zone string, version int, templateSha1 string) (*v1beta2.StatefulSet, error) {
	statefulSet := template.DeepCopy()

	// Get the labels and annotations
	labels := statefulSet.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	annotations := statefulSet.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	statefulSetNamePrefix := extendedStatefulSet.GetName()

	// Update available-zone specified properties
	if zoneIndex >= 0 && len(zone) != 0 {
		// Reset name prefix with zoneIndex
		statefulSetNamePrefix = fmt.Sprintf("%s-z%d", extendedStatefulSet.GetName(), zoneIndex)

		labels[essv1a1.LabelAZIndex] = strconv.Itoa(zoneIndex)
		labels[essv1a1.LabelAZName] = zone

		zonesBytes, err := json.Marshal(extendedStatefulSet.Spec.Zones)
		if err != nil {
			return &v1beta2.StatefulSet{}, errors.Wrapf(err, "Could not marshal zones: '%v'", extendedStatefulSet.Spec.Zones)
		}
		annotations[essv1a1.AnnotationZones] = string(zonesBytes)

		// Get the pod labels and annotations
		podLabels := statefulSet.Spec.Template.GetLabels()
		if podLabels == nil {
			podLabels = make(map[string]string)
		}
		podLabels[essv1a1.LabelAZIndex] = strconv.Itoa(zoneIndex)
		podLabels[essv1a1.LabelAZName] = zone

		podAnnotations := statefulSet.Spec.Template.GetAnnotations()
		if podAnnotations == nil {
			podAnnotations = make(map[string]string)
		}
		podAnnotations[essv1a1.AnnotationZones] = string(zonesBytes)

		statefulSet.Spec.Template.SetLabels(podLabels)
		statefulSet.Spec.Template.SetAnnotations(podAnnotations)

		statefulSet = r.updateAffinity(statefulSet, extendedStatefulSet.Spec.ZoneNodeLabel, zoneIndex, zone)

		r.injectContainerEnv(&statefulSet.Spec.Template.Spec, zoneIndex, zone, extendedStatefulSet.Spec.Template.Spec.Replicas)
	}

	annotations[essv1a1.AnnotationStatefulSetSHA1] = templateSha1
	annotations[essv1a1.AnnotationVersion] = fmt.Sprintf("%d", version)

	// Set updated properties
	statefulSet.SetName(fmt.Sprintf("%s-v%d", statefulSetNamePrefix, version))
	statefulSet.SetLabels(labels)
	statefulSet.SetAnnotations(annotations)

	// Add version to VolumeClaimTemplate's names if present
	for indexV, volumeClaimTemplate := range statefulSet.Spec.VolumeClaimTemplates {
		actualVolumeClaimTemplateName := volumeClaimTemplate.GetName()
		desiredVolumeClaimTemplateName := fmt.Sprintf("%s-v%d", volumeClaimTemplate.GetName(), version)

		volumeClaimTemplate.SetName(desiredVolumeClaimTemplateName)
		statefulSet.Spec.VolumeClaimTemplates[indexV] = volumeClaimTemplate

		// change the name in the container's volume mounts
		for indexC, container := range statefulSet.Spec.Template.Spec.Containers {
			for indexM, volumeMount := range container.VolumeMounts {
				if volumeMount.Name == actualVolumeClaimTemplateName {
					volumeMount.Name = desiredVolumeClaimTemplateName
					statefulSet.Spec.Template.Spec.Containers[indexC].VolumeMounts[indexM] = volumeMount
				}
			}
		}
	}

	return statefulSet, nil
}

// updateAffinity Update current statefulSet Affinity from AZ specification
func (r *ReconcileExtendedStatefulSet) updateAffinity(statefulSet *v1beta2.StatefulSet, zoneNodeLabel string, zoneIndex int, zoneName string) *v1beta2.StatefulSet {
	nodeInZoneSelector := corev1.NodeSelectorRequirement{
		Key:      zoneNodeLabel,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{zoneName},
	}

	affinity := statefulSet.Spec.Template.Spec.Affinity
	// Check if optional properties were set
	if affinity == nil {
		affinity = &corev1.Affinity{}
	}

	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						nodeInZoneSelector,
					},
				},
			},
		}
	} else {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				nodeInZoneSelector,
			},
		})
	}

	statefulSet.Spec.Template.Spec.Affinity = affinity

	return statefulSet
}

// injectContainerEnv inject AZ info to container envs
func (r *ReconcileExtendedStatefulSet) injectContainerEnv(podSpec *corev1.PodSpec, zoneIndex int, zoneName string, replicas *int32) {
	for i := 0; i < len(podSpec.Containers); i++ {
		envs := podSpec.Containers[i].Env

		envs = upsertEnvs(envs, EnvKubeAz, zoneName)
		envs = upsertEnvs(envs, EnvBoshAz, zoneName)
		envs = upsertEnvs(envs, EnvCfOperatorAz, zoneName)
		envs = upsertEnvs(envs, EnvCfOperatorAzIndex, strconv.Itoa(zoneIndex))
		envs = upsertEnvs(envs, EnvReplicas, strconv.Itoa(int(*replicas)))

		podSpec.Containers[i].Env = envs
	}
}

func upsertEnvs(envs []corev1.EnvVar, name string, value string) []corev1.EnvVar {
	for idx, env := range envs {
		if env.Name == name {
			envs[idx].Value = value
			return envs
		}
	}

	envs = append(envs, corev1.EnvVar{
		Name:  name,
		Value: value,
	})
	return envs
}
