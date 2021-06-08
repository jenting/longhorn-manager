package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	BackupStatusQueryInterval = 2 * time.Second
)

type BackupController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	bvStoreSynced cache.InformerSynced
	bStoreSynced  cache.InformerSynced
}

func NewBackupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupVolumeInformer lhinformers.BackupVolumeInformer,
	backupInformer lhinformers.BackupInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bc := &BackupController{
		baseController: newBaseController("longhorn-backup", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-controller"}),

		bvStoreSynced: backupVolumeInformer.Informer().HasSynced,
		bStoreSynced:  backupInformer.Informer().HasSynced,
	}

	backupInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bc.enqueueBackup,
		UpdateFunc: func(old, cur interface{}) { bc.enqueueBackup(cur) },
		DeleteFunc: bc.enqueueBackup,
	})

	return bc
}

func (bc *BackupController) enqueueBackup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bc.queue.AddRateLimited(key)
}

func (bc *BackupController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bc.queue.ShutDown()

	bc.logger.Infof("Start Longhorn Backup Snapshot controller")
	defer bc.logger.Infof("Shutting down Longhorn Backup Snapshot controller")

	if !cache.WaitForNamedCacheSync(bc.name, stopCh, bc.bvStoreSynced, bc.bStoreSynced) {
		return
	}

	// must remain single threaded since backup snapshot monitor is not thread-safe now
	go wait.Until(bc.worker, time.Second, stopCh)

	<-stopCh
}

func (bc *BackupController) worker() {
	for bc.processNextWorkItem() {
	}
}

func (bc *BackupController) processNextWorkItem() bool {
	key, quit := bc.queue.Get()
	if quit {
		return false
	}
	defer bc.queue.Done(key)
	err := bc.syncHandler(key.(string))
	bc.handleErr(err, key)
	return true
}

func (bc *BackupController) handleErr(err error, key interface{}) {
	if err == nil {
		bc.queue.Forget(key)
		return
	}

	if bc.queue.NumRequeues(key) < maxRetries {
		bc.logger.WithError(err).Warnf("Error syncing Longhorn backup snapshot %v", key)
		bc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bc.logger.WithError(err).Warnf("Dropping Longhorn backup snapshot %v out of the queue", key)
	bc.queue.Forget(key)
}

func (bc *BackupController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup snapshot %v", bc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bc.namespace {
		return nil
	}
	return bc.reconcile(bc.logger, name)
}

func (bc *BackupController) GetEngineClient(volumeName string) (client engineapi.EngineClient, err error) {
	var e *longhorn.Engine

	defer func() {
		err = errors.Wrapf(err, "cannot get client for volume %v", volumeName)
	}()
	es, err := bc.ds.ListVolumeEngines(volumeName)
	if err != nil {
		return nil, err
	}
	if len(es) == 0 {
		return nil, fmt.Errorf("cannot find engine")
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("more than one engine exists")
	}
	for _, e = range es {
		break
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	if isReady, err := bc.ds.CheckEngineImageReadiness(e.Status.CurrentImage, bc.controllerID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot get engine client with image %v: %v", e.Status.CurrentImage, err)
		}
		return nil, fmt.Errorf("cannot get engine client with image %v because it isn't deployed on this node", e.Status.CurrentImage)
	}

	engineCollection := &engineapi.EngineCollection{}
	return engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: e.Status.CurrentImage,
		IP:          e.Status.IP,
		Port:        e.Status.Port,
	})
}

func (bc *BackupController) backupCreation(log logrus.FieldLogger, backupTargetClient *engineapi.BackupTarget,
	volumeName, backupName, snapshotName, backingImageName, backingImageURL string, labels map[string]string) error {
	backupTarget, err := bc.ds.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}

	credential, err := manager.GetBackupCredentialConfig(bc.ds)
	if err != nil {
		return err
	}

	engine, err := bc.GetEngineClient(volumeName)
	if err != nil {
		return err
	}

	// blocks till the backup snapshot creation has been started
	backupID, err := engine.SnapshotBackup(backupName, snapshotName, backupTarget, backingImageName, backingImageURL, labels, credential)
	if err != nil {
		log.WithError(err).Errorf("Failed to initiate backup snapshot for snapshot %v of volume %v with label %v", snapshotName, volumeName, labels)
		return err
	}
	log.Debugf("Initiated backup snapshot %v for snapshot %v of volume %v with label %v", backupID, snapshotName, volumeName, labels)

	go func() {
		bks := &types.BackupStatus{}
		for {
			engines, err := bc.ds.ListVolumeEngines(volumeName)
			if err != nil {
				logrus.Errorf("fail to get engines for volume %v", volumeName)
				return
			}

			for _, e := range engines {
				backupStatusList := e.Status.BackupStatus
				for _, b := range backupStatusList {
					if b.SnapshotName == snapshotName {
						bks = b
						break
					}
				}
			}
			if bks.Error != "" {
				logrus.Errorf("Failed to updated volume LastBackup for %v due to backup snapshot error %v", volumeName, bks.Error)
				break
			}
			if bks.Progress == 100 {
				// Request backup snapshot volume controller to reconcile BackupVolume immediately.
				backupVolumeCR, err := bc.ds.GetBackupVolume(volumeName)
				if err == nil {
					backupVolumeCR.Spec.ForceSync = true
					_, err = bc.ds.UpdateBackupVolume(backupVolumeCR)
					if err != nil && !datastore.ErrorIsConflict(err) {
						log.WithError(err).Errorf("Error updating backup snapshot volume %s spec", volumeName)
					}
				}

				break
			}
			time.Sleep(BackupStatusQueryInterval)
		}
	}()
	return nil
}

func (bc *BackupController) getBackupVolume(log logrus.FieldLogger, backupCR *v1beta1.Backup) (string, error) {
	backupVolumeName, ok := backupCR.Labels[types.LonghornLabelVolume]
	if !ok {
		log.Warn("Cannot find the volume label")
		return "", fmt.Errorf("Cannot find the volume label")
	}
	return backupVolumeName, nil
}

func (bc *BackupController) getBackupMetadataURL(log logrus.FieldLogger, backupCR *v1beta1.Backup, backupTargetURL string) (string, error) {
	backupVolumeName, err := bc.getBackupVolume(log, backupCR)
	if err != nil {
		return "", err
	}
	return backupstore.EncodeMetadataURL(backupCR.Name, backupVolumeName, backupTargetURL), nil
}

func (bc *BackupController) reconcile(log logrus.FieldLogger, backupName string) (err error) {
	backupTarget, err := bc.ds.GetBackupTarget(defaultBackupTargetName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		log.Warnf("Backup target %s be deleted", defaultBackupTargetName)
		return nil
	}

	if isResponsible, err := shouldProcess(bc.ds, bc.controllerID, backupTarget.Spec.PollInterval); err != nil || !isResponsible {
		if err != nil {
			log.WithError(err).Warn("Failed to select node, will try again next poll interval")
		}
		return nil
	}

	backupTargetClient, err := manager.GenerateBackupTarget(bc.ds)
	if err != nil {
		log.WithError(err).Error("Error generate backup snapshot target client")
		// Ignore error to prevent enqueue
		return nil
	}

	log = log.WithFields(logrus.Fields{
		"backup": backupName,
	})

	// Reconcile delete BackupCR
	backupCR, err := bc.ds.GetBackup(backupName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		// Delete event without finalizer, ignore error to prevent enqueue
		return nil
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if backupCR.DeletionTimestamp != nil {
		// Delete the backup snapshot from the remote backup snapshot target
		backupURL, err := bc.getBackupMetadataURL(log, backupCR, backupTarget.Spec.BackupTargetURL)
		if err != nil {
			log.WithError(err).Error("Get backup snapshot URL")
			// Ignore error to prevent enqueue
			return nil
		}
		if err := backupTargetClient.DeleteBackup(backupURL); err != nil {
			log.WithError(err).Error("Error deleting remote backup")
			return err
		}

		// Request backup snapshot volume controller to reconcile BackupVolume immediately.
		volumeName, err := bc.getBackupVolume(log, backupCR)
		if err != nil {
			return err
		}
		backupVolumeCR, err := bc.ds.GetBackupVolume(volumeName)
		if err == nil {
			backupVolumeCR.Spec.ForceSync = true
			_, err = bc.ds.UpdateBackupVolume(backupVolumeCR)
			if err != nil && !datastore.ErrorIsConflict(err) {
				log.WithError(err).Errorf("Error updating backup snapshot volume %s spec", volumeName)
			}
		}
		return bc.ds.RemoveFinalizerForBackup(backupCR)
	}

	// Perform snapshot backup
	if backupCR.Spec.SnapshotName != "" && !backupCR.Status.BackupCreate {
		volumeName, err := bc.getBackupVolume(log, backupCR)
		if err != nil {
			return err
		}

		err = bc.backupCreation(bc.logger, backupTargetClient,
			volumeName, backupCR.Name, backupCR.Spec.SnapshotName,
			backupCR.Spec.BackingImage, backupCR.Spec.BackingImageURL, backupCR.Spec.Labels)
		if err != nil {
			log.WithError(err).Error("Backup creation")
			return err
		}

		backupCR.Status.BackupCreate = true
		_, err = bc.ds.UpdateBackupStatus(backupCR)
		if err != nil && !datastore.ErrorIsConflict(err) {
			log.WithError(err).Error("Error updating backup snapshot status")
			return err
		}
		return nil
	}
	if backupCR.Status.LastSyncedAt != nil {
		// Already synced from backup snapshot target
		return nil
	}

	backupURL, err := bc.getBackupMetadataURL(log, backupCR, backupTarget.Spec.BackupTargetURL)
	if err != nil {
		log.WithError(err).Error("Get backup snapshot URL")
		// Ignore error to prevent enqueue
		return nil
	}
	backup, err := backupTargetClient.InspectVolumeSnapshotBackupMetadata(backupURL)
	if err != nil || backup == nil {
		log.WithError(err).Error("Cannot inspect the backup snapshot metadata")
		// Ignore error to prevent enqueue
		return nil
	}

	backupCR.Status.URL = backup.URL
	backupCR.Status.SnapshotName = backup.SnapshotName
	backupCR.Status.SnapshotCreateAt = backup.SnapshotCreated
	backupCR.Status.BackupCreateAt = backup.Created
	backupCR.Status.Size = backup.Size
	backupCR.Status.Labels = backup.Labels
	backupCR.Status.Messages = backup.Messages
	backupCR.Status.LastSyncedAt = &metav1.Time{Time: time.Now().UTC()}
	if _, err = bc.ds.UpdateBackupStatus(backupCR); err != nil && !datastore.ErrorIsConflict(err) {
		log.WithError(err).Error("Error updating backup snapshot status")
		return err
	}
	return nil
}
