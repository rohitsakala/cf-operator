package boshdeployment

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bdm "code.cloudfoundry.org/cf-operator/pkg/bosh/manifest"
	bdv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/boshdeployment/v1alpha1"
	ejv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedjob/v1alpha1"
	esv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedsecret/v1alpha1"
	estsv1 "code.cloudfoundry.org/cf-operator/pkg/kube/apis/extendedstatefulset/v1alpha1"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/context"
)

// State of instance
const (
	CreatedState              = "Created"
	UpdatedState              = "Updated"
	OpsAppliedState           = "OpsApplied"
	VariableGeneratedState    = "VariableGenerated"
	VariableInterpolatedState = "VariableInterpolated"
	DataGatheredState         = "DataGathered"
	DeployingState            = "Deploying"
	DeployedState             = "Deployed"
)

// Check that ReconcileBOSHDeployment implements the reconcile.Reconciler interface
var _ reconcile.Reconciler = &ReconcileBOSHDeployment{}

type setReferenceFunc func(owner, object metav1.Object, scheme *runtime.Scheme) error

// NewReconciler returns a new reconcile.Reconciler
func NewReconciler(log *zap.SugaredLogger, ctrConfig *context.Config, mgr manager.Manager, resolver bdm.Resolver, srf setReferenceFunc) reconcile.Reconciler {

	reconcilerLog := log.Named("boshdeployment-reconciler")
	reconcilerLog.Info("Creating a reconciler for BoshDeployment")

	return &ReconcileBOSHDeployment{
		log:          reconcilerLog,
		ctrConfig:    ctrConfig,
		client:       mgr.GetClient(),
		scheme:       mgr.GetScheme(),
		recorder:     mgr.GetRecorder("RECONCILER RECORDER"),
		resolver:     resolver,
		setReference: srf,
	}
}

// ReconcileBOSHDeployment reconciles a BOSHDeployment object
type ReconcileBOSHDeployment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client       client.Client
	scheme       *runtime.Scheme
	recorder     record.EventRecorder
	resolver     bdm.Resolver
	setReference setReferenceFunc
	log          *zap.SugaredLogger
	ctrConfig    *context.Config
}

// Reconcile reads that state of the cluster for a BOSHDeployment object and makes changes based on the state read
// and what is in the BOSHDeployment.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileBOSHDeployment) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	r.log.Infof("Reconciling BOSHDeployment %s", request.NamespacedName)

	// Fetch the BOSHDeployment instance
	instance := &bdv1.BOSHDeployment{}

	// Set the ctx to be Background, as the top-level context for incoming requests.
	ctx, cancel := context.NewBackgroundContextWithTimeout(r.ctrConfig.CtxType, r.ctrConfig.CtxTimeOut)
	defer cancel()

	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			r.log.Debug("Skip reconcile: BOSHDeployment not found")
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		r.recorder.Event(instance, corev1.EventTypeWarning, "GetBOSHDeployment Error", err.Error())
		r.log.Errorf("Failed to get BOSHDeployment '%s': %v", request.NamespacedName, err)
		return reconcile.Result{}, err
	}

	// Get state from instance
	instanceState := instance.Status.State

	// Apply ops files
	manifest, err := r.applyOps(ctx, instance)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Compute SHA1 of the manifest (with ops applied), so we can figure out if anything
	// has changed.
	currentManifestSHA1, err := manifest.SHA1()
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, "could not calculate manifest SHA1")
	}
	oldManifestSHA1, _ := instance.Annotations[bdv1.AnnotationManifestSHA1]
	if oldManifestSHA1 == currentManifestSHA1 && instance.Status.State == DeployedState {
		r.log.Infof("Skip reconcile: deployed BoshDeployment '%s/%s' manifest has not changed", instance.GetNamespace(), instance.GetName())
		return reconcile.Result{}, nil
	}

	// If we have no instance groups, we should stop. There must be something wrong
	// with the manifest.
	if len(manifest.InstanceGroups) < 1 {
		err := fmt.Errorf("manifest is missing instance groups")
		r.log.Errorf("No instance groups defined in manifest %s", manifest.Name)
		r.recorder.Event(instance, corev1.EventTypeWarning, "MissingInstance Error", err.Error())
		return reconcile.Result{}, err
	}

	// Generate all the kube objects we need for the manifest
	r.log.Debug("Converting bosh manifest to kube objects")
	kubeConfigs, err := manifest.ConvertToKube(r.ctrConfig.Namespace)
	if err != nil {
		r.log.Errorf("Error converting bosh manifest %s to kube objects: %s", manifest.Name, err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "BadManifest Error", err.Error())
		return reconcile.Result{}, errors.Wrap(err, "error converting manifest to kube objects")
	}

	if instanceState == "" {
		// Set a "Created" state if this has just been created
		instanceState = CreatedState
	} else if currentManifestSHA1 != oldManifestSHA1 {
		// Set an "Updated" state if the signature of the manifest has changed
		instanceState = UpdatedState
	}

	r.log.Debugf("BoshDeployment '%s/%s' is in state: %s", instance.GetNamespace(), instance.GetName(), instanceState)

	switch instanceState {
	case CreatedState:
		fallthrough
	case UpdatedState:
		// Set manifest SHA1
		if instance.Annotations == nil {
			instance.Annotations = map[string]string{}
		}
		instance.Annotations[bdv1.AnnotationManifestSHA1] = currentManifestSHA1
		instance.Status.State = OpsAppliedState

	case OpsAppliedState:
		err = r.generateVariableSecrets(ctx, instance, manifest, &kubeConfigs)
		if err != nil {
			r.log.Errorf("Failed to generate variables: %v", err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "VariableGeneration Error", err.Error())
			return reconcile.Result{}, err
		}

	case VariableGeneratedState:
		err = r.createVariableInterpolationExJob(ctx, instance, manifest, kubeConfigs)
		if err != nil {
			r.log.Errorf("Failed to create variable interpolation exJob: %v", err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "VariableInterpolation Error", err.Error())
			return reconcile.Result{}, err
		}

	case VariableInterpolatedState:
		err = r.createDataGatheringJob(ctx, instance, manifest, kubeConfigs)
		if err != nil {
			r.log.Errorf("Failed to create data gathering exJob: %v", err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "DataGathering Error", err.Error())
			return reconcile.Result{}, err
		}

	case DataGatheredState:
		err = r.deployInstanceGroups(ctx, instance, &kubeConfigs)
		if err != nil {
			r.log.Errorf("Failed to deploy instance groups: %v", err)
			return reconcile.Result{}, err
		}

	case DeployingState:
		err = r.actionOnDeploying(ctx, instance, &kubeConfigs)
		if err != nil {
			r.log.Errorf("Failed to  data: %v", err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "InstanceDeployment Error", err.Error())
			return reconcile.Result{}, err
		}

	case DeployedState:
		r.log.Infof("Skip reconcile: BoshDeployment '%s/%s' already has been deployed", instance.GetNamespace(), instance.GetName())
		return reconcile.Result{}, nil
	default:
		return reconcile.Result{}, errors.New("unknown instance state")
	}

	r.log.Debugf("requeue the reconcile: BoshDeployment '%s/%s' is in state '%s'", instance.GetNamespace(), instance.GetName(), instance.Status.State)
	return reconcile.Result{Requeue: true}, r.updateInstanceState(ctx, instance)
}

// updateInstanceState update instance state
func (r *ReconcileBOSHDeployment) updateInstanceState(ctx context.Context, currentInstance *bdv1.BOSHDeployment) error {
	currentManifestSHA1, _ := currentInstance.GetAnnotations()[bdv1.AnnotationManifestSHA1]

	// Fetch latest BOSHDeployment before update
	foundInstance := &bdv1.BOSHDeployment{}
	key := types.NamespacedName{Namespace: currentInstance.GetNamespace(), Name: currentInstance.GetName()}
	err := r.client.Get(ctx, key, foundInstance)
	if err != nil {
		r.log.Errorf("Failed to get BOSHDeployment instance '%s': %v", currentInstance.GetName(), err)
		return err
	}
	oldManifestSHA1, _ := foundInstance.GetAnnotations()[bdv1.AnnotationManifestSHA1]

	if oldManifestSHA1 != currentManifestSHA1 {
		// Set manifest SHA1
		if foundInstance.Annotations == nil {
			foundInstance.Annotations = map[string]string{}
		}

		foundInstance.Annotations[bdv1.AnnotationManifestSHA1] = currentManifestSHA1
	}

	// Update the Status of the resource
	if !reflect.DeepEqual(foundInstance.Status.State, currentInstance.Status.State) {
		r.log.Debugf("Updating boshDeployment from '%s' to '%s'", foundInstance.Status.State, currentInstance.Status.State)

		newInstance := foundInstance.DeepCopy()
		newInstance.Status.State = currentInstance.Status.State

		err = r.client.Update(ctx, newInstance)
		if err != nil {
			r.log.Errorf("Failed to update BOSHDeployment instance status: %v", err)
			return err
		}
	}

	return nil
}

// applyOps apply ops files after BoshDeployment instance created
func (r *ReconcileBOSHDeployment) applyOps(ctx context.Context, instance *bdv1.BOSHDeployment) (*bdm.Manifest, error) {
	// Create temp manifest as variable interpolation job input
	// retrieve manifest
	r.log.Debug("Resolving manifest")
	manifest, err := r.resolver.ResolveManifest(instance.Spec, instance.GetNamespace())
	if err != nil {
		r.recorder.Event(instance, corev1.EventTypeWarning, "ResolveManifest Error", err.Error())
		r.log.Errorf("Error resolving the manifest %s: %s", instance.GetName(), err)
		return nil, err
	}

	return manifest, nil
}

// generateVariableSecrets create variables extendedSecrets
func (r *ReconcileBOSHDeployment) generateVariableSecrets(ctx context.Context, instance *bdv1.BOSHDeployment, manifest *bdm.Manifest, kubeConfig *bdm.KubeConfig) error {
	r.log.Debug("Creating variables extendedSecrets")
	var err error
	for _, variable := range kubeConfig.Variables {
		// Set BOSHDeployment instance as the owner and controller
		if err := r.setReference(instance, &variable, r.scheme); err != nil {
			return errors.Wrap(err, "could not set reference for an ExtendedStatefulSet for a BOSH Deployment")
		}

		foundSecret := &esv1.ExtendedSecret{}
		err = r.client.Get(ctx, types.NamespacedName{Name: variable.GetName(), Namespace: variable.GetNamespace()}, foundSecret)
		if apierrors.IsNotFound(err) {
			err = r.client.Create(ctx, &variable)
			if err != nil {
				return errors.Wrapf(err, "could not create ExtendedSecret %s", variable.GetName())
			}
		} else {
			foundSecret.Spec = variable.Spec
			err = r.client.Update(ctx, foundSecret)
			if err != nil {
				return errors.Wrapf(err, "could not update ExtendedSecret %s", variable.GetName())
			}
		}
	}

	instance.Status.State = VariableGeneratedState

	return nil
}

// createVariableInterpolationExJob create temp manifest and variable interpolation exJob
func (r *ReconcileBOSHDeployment) createVariableInterpolationExJob(ctx context.Context, instance *bdv1.BOSHDeployment, manifest *bdm.Manifest, kubeConfig bdm.KubeConfig) error {

	// Create temp manifest as variable interpolation job input.
	// Ops files have been applied on this manifest.
	tempManifestBytes, err := yaml.Marshal(manifest)
	if err != nil {
		return errors.Wrap(err, "could not marshal temp manifest")
	}

	tempManifestSecretName := manifest.CalculateSecretName(bdm.DeploymentSecretTypeManifestWithOps, "")

	// Create a secret object for the manifest
	tempManifestSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempManifestSecretName,
			Namespace: instance.GetNamespace(),
		},
		StringData: map[string]string{
			"manifest.yaml": string(tempManifestBytes),
		},
	}

	// If the secret is missing, create it. Update it otherwise
	foundSecret := &corev1.Secret{}
	err = r.client.Get(ctx, types.NamespacedName{Name: tempManifestSecret.GetName(), Namespace: tempManifestSecret.GetNamespace()}, foundSecret)
	if apierrors.IsNotFound(err) {
		err = r.client.Create(ctx, tempManifestSecret)
		if err != nil {
			return errors.Wrap(err, "could not create temp manifest secret")
		}
		foundSecret = tempManifestSecret
	} else {
		foundSecret.Data = map[string][]byte{}
		foundSecret.StringData = map[string]string{
			"manifest.yaml": string(tempManifestBytes),
		}
		err = r.client.Update(ctx, foundSecret)
		if err != nil {
			return errors.Wrap(err, "could not update temp manifest secret")
		}
	}

	// Generate the ExtendedJob object
	r.log.Debug("Creating variable interpolation extendedJob")
	varIntExJob := kubeConfig.VariableInterpolationJob
	// Set BOSHDeployment instance as the owner and controller
	if err := r.setReference(instance, varIntExJob, r.scheme); err != nil {
		r.log.Errorf("Failed to set ownerReference for ExtendedJob '%s': %v", varIntExJob.GetName(), err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "NewJobForVariableInterpolation Error", err.Error())
		return err
	}

	// Check if this job already exists and create/update accordingly
	foundExJob := &ejv1.ExtendedJob{}
	err = r.client.Get(ctx, types.NamespacedName{Name: varIntExJob.Name, Namespace: varIntExJob.Namespace}, foundExJob)
	if err != nil && apierrors.IsNotFound(err) {
		r.log.Infof("Creating a new ExtendedJob %s/%s\n", varIntExJob.Namespace, varIntExJob.Name)
		err = r.client.Create(ctx, varIntExJob)
		if err != nil {
			r.log.Errorf("Failed to create ExtendedJob '%s': %v", varIntExJob.GetName(), err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "CreateJobForVariableInterpolation Error", err.Error())
			return err
		}
	} else if err != nil {
		r.log.Errorf("Failed to get ExtendedJob '%s': %v", varIntExJob.GetName(), err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "GetJobForVariableInterpolation Error", err.Error())
		return err
	}

	instance.Status.State = VariableInterpolatedState

	return nil
}

// createDataGatheringJob gather data from manifest
func (r *ReconcileBOSHDeployment) createDataGatheringJob(ctx context.Context, instance *bdv1.BOSHDeployment, manifest *bdm.Manifest, kubeConfig bdm.KubeConfig) error {

	// Generate the ExtendedJob object
	dataGatheringExJob := kubeConfig.DataGatheringJob
	r.log.Debugf("Creating data gathering extendedJob %s/%s", dataGatheringExJob.Namespace, dataGatheringExJob.Name)

	// Set BOSHDeployment instance as the owner and controller
	if err := r.setReference(instance, dataGatheringExJob, r.scheme); err != nil {
		r.log.Errorf("Failed to set ownerReference for ExtendedJob '%s': %v", dataGatheringExJob.GetName(), err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "NewJobForDataGathering Error", err.Error())
		return err
	}

	// Check if this job already exists and create/update accordingly
	foundExJob := &ejv1.ExtendedJob{}
	err := r.client.Get(ctx, types.NamespacedName{Name: dataGatheringExJob.Name, Namespace: dataGatheringExJob.Namespace}, foundExJob)
	if err != nil && apierrors.IsNotFound(err) {
		r.log.Infof("Creating a new ExtendedJob %s/%s\n", dataGatheringExJob.Namespace, dataGatheringExJob.Name)
		err = r.client.Create(ctx, dataGatheringExJob)
		if err != nil {
			r.log.Errorf("Failed to create ExtendedJob '%s': %v", dataGatheringExJob.GetName(), err)
			r.recorder.Event(instance, corev1.EventTypeWarning, "GetJobForDataGathering Error", err.Error())
			return err
		}
	} else if err != nil {
		r.log.Errorf("Failed to get ExtendedJob '%s': %v", dataGatheringExJob.GetName(), err)
		r.recorder.Event(instance, corev1.EventTypeWarning, "GetJobForDataGathering Error", err.Error())
		return err
	}

	instance.Status.State = DataGatheredState

	return nil
}

// deployInstanceGroups create ExtendedJobs and ExtendedStatefulSets
func (r *ReconcileBOSHDeployment) deployInstanceGroups(ctx context.Context, instance *bdv1.BOSHDeployment, kubeConfigs *bdm.KubeConfig) error {
	r.log.Debug("Creating extendedJobs and extendedStatefulSets of instance groups")
	for _, eJob := range kubeConfigs.Errands {
		// Set BOSHDeployment instance as the owner and controller
		if err := r.setReference(instance, &eJob, r.scheme); err != nil {
			r.recorder.Event(instance, corev1.EventTypeWarning, "NewExtendedJobForDeployment Error", err.Error())
			return errors.Wrap(err, "couldn't set reference for an ExtendedJob for a BOSH Deployment")
		}

		// Check to see if the object already exists
		existingEJob := &ejv1.ExtendedJob{}
		err := r.client.Get(ctx, types.NamespacedName{Name: eJob.Name, Namespace: eJob.Namespace}, existingEJob)
		if err != nil && apierrors.IsNotFound(err) {
			r.log.Infof("Creating a new ExtendedJob %s/%s for Deployment Manifest %s\n", eJob.Namespace, eJob.Name, instance.Name)

			// Create the extended job
			err := r.client.Create(ctx, &eJob)
			if err != nil {
				r.recorder.Event(instance, corev1.EventTypeWarning, "CreateExtendedJobForDeployment Error", err.Error())
				r.log.Errorf("Error creating ExtendedJob %s for deployment manifest %s: %s", eJob.Name, instance.GetName(), err)
				return errors.Wrap(err, "couldn't create an ExtendedJob for a BOSH Deployment")
			}
		}
	}

	for _, eSts := range kubeConfigs.InstanceGroups {
		// Set BOSHDeployment instance as the owner and controller
		if err := r.setReference(instance, &eSts, r.scheme); err != nil {
			r.recorder.Event(instance, corev1.EventTypeWarning, "NewExtendedStatefulSetForDeployment Error", err.Error())
			return errors.Wrap(err, "couldn't set reference for an ExtendedStatefulSet for a BOSH Deployment")
		}

		// Check to see if the object already exists
		existingESts := &estsv1.ExtendedStatefulSet{}
		err := r.client.Get(ctx, types.NamespacedName{Name: eSts.Name, Namespace: eSts.Namespace}, existingESts)
		if err != nil && apierrors.IsNotFound(err) {
			r.log.Infof("Creating a new ExtendedStatefulSet %s/%s for Deployment Manifest %s\n", eSts.Namespace, eSts.Name, instance.Name)

			// Create the extended statefulset
			err := r.client.Create(ctx, &eSts)
			if err != nil {
				r.recorder.Event(instance, corev1.EventTypeWarning, "CreateExtendedStatefulSetForDeployment Error", err.Error())
				r.log.Errorf("Error creating ExtendedStatefulSet %s for deployment manifest %s: %s", eSts.Name, instance.GetName(), err)
				return errors.Wrap(err, "couldn't create an ExtendedStatefulSet for a BOSH Deployment")
			}
		}
	}

	instance.Status.State = DeployingState

	return nil
}

// actionOnDeploying check out deployment status
func (r *ReconcileBOSHDeployment) actionOnDeploying(ctx context.Context, instance *bdv1.BOSHDeployment, kubeConfigs *bdm.KubeConfig) error {
	// TODO Check deployment
	instance.Status.State = DeployedState

	return nil
}
