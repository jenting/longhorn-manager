package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

type BackupTargetController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	btStoreSynced cache.InformerSynced
}

func NewBackupTargetController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupTargetInformer lhinformers.BackupTargetInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupTargetController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bt := &BackupTargetController{
		baseController: newBaseController("longhorn-backup-target", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-target-controller"}),

		btStoreSynced: backupTargetInformer.Informer().HasSynced,
	}

	backupTargetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bt.enqueueBackupTarget,
		UpdateFunc: func(old, cur interface{}) { bt.enqueueBackupTarget(cur) },
	})

	return bt
}

func (bt *BackupTargetController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bt.queue.ShutDown()

	bt.logger.Infof("Start Longhorn Backup Target controller")
	defer bt.logger.Infof("Shutting down Longhorn Backup Target controller")

	if !cache.WaitForNamedCacheSync(bt.name, stopCh, bt.btStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bt.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (bt *BackupTargetController) worker() {
	for bt.processNextWorkItem() {
	}
}

func (bt *BackupTargetController) processNextWorkItem() bool {
	key, quit := bt.queue.Get()
	if quit {
		return false
	}
	defer bt.queue.Done(key)

	err := bt.reconcileBackupTarget(key.(string))
	bt.handleErr(err, key)
	return true
}

func (bt *BackupTargetController) handleErr(err error, key interface{}) {
	if err == nil {
		bt.queue.Forget(key)
		return
	}

	if bt.queue.NumRequeues(key) < maxRetries {
		bt.logger.WithError(err).Warnf("Error syncing Longhorn backup target %v", key)
		bt.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bt.logger.WithError(err).Warnf("Dropping Longhorn backup target %v out of the queue", key)
	bt.queue.Forget(key)
}

func (bt *BackupTargetController) reconcileBackupTarget(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup target %v", bt.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bt.namespace {
		return nil
	}

	backupTarget, err := bt.ds.GetBackupTarget(name)
	if err != nil {
		bt.logger.WithError(err).Errorf("Failed to retrieve backup target %s from datastore", name)
		return nil
	}

	bt.logger.Infof("BackupTargetURL %s", backupTarget.Spec.BackupTargetURL)
	bt.logger.Infof("CredentialSecret %s", backupTarget.Spec.CredentialSecret)
	bt.logger.Infof("PollInterval %s", backupTarget.Spec.PollInterval)
	return nil
}

func (bt *BackupTargetController) enqueueBackupTarget(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bt.queue.AddRateLimited(key)
}
