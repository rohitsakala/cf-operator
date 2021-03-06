package operator

import (
	"go.uber.org/zap"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"code.cloudfoundry.org/cf-operator/pkg/kube/controllers"
	"code.cloudfoundry.org/cf-operator/pkg/kube/util/context"

	credsgen "code.cloudfoundry.org/cf-operator/pkg/credsgen/in_memory_generator"
)

// NewManager adds schemes, controllers and starts the manager
func NewManager(log *zap.SugaredLogger, ctrConfig *context.Config, cfg *rest.Config, options manager.Options) (mgr manager.Manager, err error) {
	mgr, err = manager.New(cfg, options)
	if err != nil {
		return
	}

	log.Info("Registering Components.")
	ctrConfig.Namespace = options.Namespace

	// Setup Scheme for all resources
	if err = controllers.AddToScheme(mgr.GetScheme()); err != nil {
		return
	}

	// Setup Hooks for all resources
	if err = controllers.AddHooks(log, ctrConfig, mgr, credsgen.NewInMemoryGenerator(log)); err != nil {
		return
	}

	// Setup all Controllers
	err = controllers.AddToManager(log, ctrConfig, mgr)
	return
}
