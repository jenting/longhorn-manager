package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
)

type BackupVolumeController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	sStoreSynced cache.InformerSynced

	// backup store monitor
	bsMonitor *BackupStoreMonitor
}

type BackupStoreMonitor struct {
	logger       logrus.FieldLogger
	controllerID string

	backupTarget                 string
	backupTargetCredentialSecret string
	pollInterval                 time.Duration

	target *engineapi.BackupTarget
	ds     *datastore.DataStore
	stopCh chan struct{}
}

func NewBackupVolumeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	settingInformer lhinformers.SettingInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupVolumeController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bvs := &BackupVolumeController{
		baseController: newBaseController("longhorn-backup-volume", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-volume-controller"}),

		sStoreSynced: settingInformer.Informer().HasSynced,
	}

	settingInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bvs.enqueueBackupVolume,
		UpdateFunc: func(old, cur interface{}) { bvs.enqueueBackupVolume(cur) },
		DeleteFunc: bvs.enqueueBackupVolume,
	})

	return bvs
}

func (bvs *BackupVolumeController) enqueueBackupVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bvs.queue.AddRateLimited(key)
}

func (bvs *BackupVolumeController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bvs.queue.ShutDown()

	bvs.logger.Infof("Start Longhorn Backup Volume controller")
	defer bvs.logger.Infof("Shutting down Longhorn Backup Volume controller")

	if !cache.WaitForNamedCacheSync(bvs.name, stopCh, bvs.sStoreSynced) {
		return
	}

	// must remain single threaded since backup store monitor is not thread-safe now
	go wait.Until(bvs.worker, time.Second, stopCh)

	<-stopCh
}

func (bvs *BackupVolumeController) worker() {
	for bvs.processNextWorkItem() {
	}
}

func (bvs *BackupVolumeController) processNextWorkItem() bool {
	key, quit := bvs.queue.Get()
	if quit {
		return false
	}
	defer bvs.queue.Done(key)
	err := bvs.syncSettingBackupStoreHandler(key.(string))
	bvs.handleErr(err, key)
	return true
}

func (bvs *BackupVolumeController) handleErr(err error, key interface{}) {
	if err == nil {
		bvs.queue.Forget(key)
		return
	}

	if bvs.queue.NumRequeues(key) < maxRetries {
		bvs.logger.WithError(err).Warnf("Error syncing Longhorn backup store %v", key)
		bvs.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bvs.logger.WithError(err).Warnf("Dropping Longhorn backup store %v out of the queue", key)
	bvs.queue.Forget(key)
}

func (bvs *BackupVolumeController) syncSettingBackupStoreHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup store %v", bvs.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bvs.namespace {
		return nil
	}

	switch name {
	case string(types.SettingNameBackupTargetCredentialSecret):
		fallthrough
	case string(types.SettingNameBackupTarget):
		if err := bvs.syncBackupTarget(); err != nil {
			return err
		}
	case string(types.SettingNameBackupstorePollInterval):
		if err := bvs.updateBackupstorePollInterval(); err != nil {
			return err
		}
	}
	return nil
}

func (bvs *BackupVolumeController) syncBackupTarget() (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	targetSetting, err := bvs.ds.GetSetting(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}

	secretSetting, err := bvs.ds.GetSetting(types.SettingNameBackupTargetCredentialSecret)
	if err != nil {
		return err
	}

	pollIntervalInt, err := bvs.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}
	if pollIntervalInt < 0 {
		return fmt.Errorf("backup store poll interval must be non-negative")
	}

	if bvs.bsMonitor != nil {
		if bvs.bsMonitor.backupTarget == targetSetting.Value &&
			bvs.bsMonitor.backupTargetCredentialSecret == secretSetting.Value {
			// already monitoring
			return nil
		}
		bvs.logger.Infof("Restarting backup store monitor because backup target changed from %v to %v", bvs.bsMonitor.backupTarget, targetSetting.Value)
		bvs.bsMonitor.Stop()
		bvs.bsMonitor = nil
	}

	if targetSetting.Value == "" {
		return nil
	}

	target, err := manager.GenerateBackupTarget(bvs.ds)
	if err != nil {
		return err
	}

	bvs.bsMonitor = &BackupStoreMonitor{
		logger:       bvs.logger,
		controllerID: bvs.controllerID,

		backupTarget:                 targetSetting.Value,
		backupTargetCredentialSecret: secretSetting.Value,
		pollInterval:                 time.Duration(pollIntervalInt) * time.Second,

		target: target,
		ds:     bvs.ds,
		stopCh: make(chan struct{}),
	}

	go bvs.bsMonitor.Start()

	return nil
}

func (bvs *BackupVolumeController) updateBackupstorePollInterval() (err error) {
	if bvs.bsMonitor == nil {
		return nil
	}

	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	pollIntervalInt, err := bvs.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}
	if pollIntervalInt < 0 {
		return fmt.Errorf("backup store poll interval must be non-negative")
	}

	pollInterval := time.Duration(pollIntervalInt) * time.Second
	if bvs.bsMonitor.pollInterval == pollInterval {
		return nil
	}

	bvs.bsMonitor.Stop()

	bvs.bsMonitor.pollInterval = pollInterval
	bvs.bsMonitor.stopCh = make(chan struct{})

	go bvs.bsMonitor.Start()
	return nil
}

func (bm *BackupStoreMonitor) Start() {
	log := bm.logger.WithFields(logrus.Fields{
		"backupStoreURL": bm.target.URL,
		"pollInterval":   bm.pollInterval,
	})

	log.Debug("Start backup store monitoring")
	defer func() {
		log.Debug("Stop backup store monitoring")
	}()

	// since this is run on each node, but we only need a single update
	// we pick a consistent random ready node for each poll run
	shouldProcess := func() (bool, error) {
		defaultEngineImage, err := bm.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
		if err != nil {
			return false, err
		}

		nodes, err := bm.ds.ListReadyNodesWithEngineImage(defaultEngineImage)
		if err != nil {
			return false, err
		}

		// for the random ready evaluation
		// we sort the candidate list (this will normalize the list across nodes)
		var candidates []string
		for node := range nodes {
			candidates = append(candidates, node)
		}

		if len(candidates) == 0 {
			return false, fmt.Errorf("no ready nodes with engine image %v available", defaultEngineImage)
		}
		sort.Strings(candidates)

		// we use a time index to derive an index into the candidate list (normalizes time differences across nodes)
		// we arbitrarily choose the pollInterval as your time normalization factor, since this also has the benefit of
		// doing round robin across the at the time available candidate nodes.
		pollInterval := int64(bm.pollInterval.Seconds())
		midPoint := pollInterval / 2
		timeIndex := int((time.Now().UTC().Unix() + midPoint) / pollInterval)
		candidateIndex := timeIndex % len(candidates)
		responsibleNode := candidates[candidateIndex]
		return bm.controllerID == responsibleNode, nil
	}

	wait.Until(func() {
		if isResponsible, err := shouldProcess(); err != nil || !isResponsible {
			if err != nil {
				log.WithError(err).Warn("Failed to select node for backup store monitoring, will try again next poll pollInterval")
			}
			return
		}

		// get a list of all the backup volumes that are stored in the backup store
		log.Debug("Pulling backup store for backup volume name")
		res, err := bm.target.ListBackupVolumeNames()
		if err != nil {
			log.WithError(err).Error("Error listing backup volume name in the backup store")
			return
		}
		backupStoreBackups := sets.NewString(res...)
		log.WithField("BackupVolumeCount", len(backupStoreBackups)).Debug("Got backup volumes from backup store")

		// get a list of all the backup volumes that exist as custom resources in the cluster
		clusterBackupVolumes, err := bm.ds.ListBackupVolume()
		if err != nil {
			log.WithError(err).Error("Error listing backup volumes in the cluster, proceeding with pull into cluster")
		} else {
			log.WithField("clusterBackupVolumeCount", len(clusterBackupVolumes)).Debug("Got backup volumes in the cluster")
		}

		clusterBackupVolumesSet := sets.NewString()
		for _, b := range clusterBackupVolumes {
			clusterBackupVolumesSet.Insert(b.Name)
		}

		backupVolumes := make(map[string]*engineapi.BackupVolume)

		// get a list of backup volumes that *are* in the backup store and *aren't* in the cluster
		// and create the BackupVolume CR in the cluster
		backupVolumesToPull := backupStoreBackups.Difference(clusterBackupVolumesSet)
		if count := backupVolumesToPull.Len(); count > 0 {
			log.Infof("Found %v backup volumes in the backup store that do not exist in the cluster and need to be pulled", count)
		} else {
			log.Debug("No backup volumes found in the backup store that need to be pulled into the cluster")
		}
		for backupVolumeName := range backupVolumesToPull {
			log.WithField("backupVolume", backupVolumeName).Info("Attempting to pull backup volume into cluster")

			backupVolumeMetadataURL := backupstore.EncodeMetadataURL("", backupVolumeName, bm.target.URL)
			backupVolume, err := bm.target.InspectBackupVolumeMetadata(backupVolumeMetadataURL)
			if err != nil || backupVolume == nil {
				log.WithError(err).Error("Error getting backup volume metadata from backup store")
				continue
			}
			backupVolumes[backupVolumeName] = backupVolume

			backupVolumeCR := &longhorn.BackupVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: backupVolumeName,
				},
				Spec: types.BackupVolumeSpec{
					BackupStoreURL: bm.target.URL,
					PollInterval:   bm.pollInterval.String(),
				},
				Status: types.BackupVolumeStatus{
					Size:                backupVolume.Size,
					Labels:              backupVolume.Labels,
					CreateTimestamp:     backupVolume.Created,
					LastBackupName:      backupVolume.LastBackupName,
					LastBackupTimestamp: backupVolume.LastBackupAt,
					DataStored:          backupVolume.DataStored,
					Messages:            backupVolume.Messages,
				},
			}

			_, err = bm.ds.CreateBackupVolume(backupVolumeCR)
			switch {
			case err != nil && datastore.ErrorIsConflict(err):
				log.Debug("Backup volume already exists in cluster")
			case err != nil && !datastore.ErrorIsConflict(err):
				log.WithError(err).Error("Error pulling backup volume into cluster")
			}
		}

		// get a list of backup volumes that *are* in the backup store and *are* in the cluster
		// and update the BackupVolume CR status in the cluster
		backupVolumesToSync := backupStoreBackups.Intersection(clusterBackupVolumesSet)
		if count := backupVolumesToSync.Len(); count > 0 {
			log.Infof("Found %v backup volumes need to be sync to the cluster", count)
		} else {
			log.Debug("No backup volumes need to be sync to the cluster")
		}
		for backupVolumeName := range backupVolumesToSync {
			log.WithField("backupVolume", backupVolumeName).Info("Attempting to sync backup volume into cluster")

			backupVolumeMetadataURL := backupstore.EncodeMetadataURL("", backupVolumeName, bm.target.URL)
			backupVolume, err := bm.target.InspectBackupVolumeMetadata(backupVolumeMetadataURL)
			if err != nil || backupVolume == nil {
				log.WithError(err).Error("Error getting backup volume metadata from backup store")
				continue
			}
			backupVolumes[backupVolumeName] = backupVolume

			backupVolumeCR, err := bm.ds.GetBackupVolume(backupVolumeName)
			if err != nil {
				log.WithError(err).Error("Error getting backup volume CR from the cluster CR")
				continue
			}

			backupVolumeCR.Spec.BackupStoreURL = bm.target.URL
			backupVolumeCR.Spec.PollInterval = bm.pollInterval.String()
			backupVolumeCR.Status.Size = backupVolume.Size
			backupVolumeCR.Status.Labels = backupVolume.Labels
			backupVolumeCR.Status.CreateTimestamp = backupVolume.Created
			backupVolumeCR.Status.LastBackupName = backupVolume.LastBackupName
			backupVolumeCR.Status.LastBackupTimestamp = backupVolume.LastBackupAt
			backupVolumeCR.Status.DataStored = backupVolume.DataStored
			backupVolumeCR.Status.Messages = backupVolume.Messages
			backupVolumeCR.Status.LastSyncedTimestmp = &metav1.Time{Time: time.Now().UTC()}

			_, err = bm.ds.UpdateBackupVolume(backupVolumeCR)
			if err != nil {
				log.WithError(err).Error("Error updating backup volume status in the cluster")
			}
		}

		// get a list of backup volumes that *are* in the cluster and *aren't* in the backup store
		// and delete the BackupVolume CR in the cluster
		backupVolumesToDelete := clusterBackupVolumesSet.Difference(backupStoreBackups)
		if count := backupVolumesToDelete.Len(); count > 0 {
			log.Infof("Found %v backup volumes in the cluster that do not exist in the backup store and need to be deleted", count)
		} else {
			log.Debug("No backup volumes found in the cluster that need to be deleted into the cluster")
		}
		for backupVolumeName := range backupVolumesToDelete {
			log.WithField("backupVolume", backupVolumeName).Debug("Attempting to delete backup volume in the cluster")

			if err := bm.ds.DeleteBackupVolume(backupVolumeName); err != nil {
				log.WithError(err).Error("Error deleting backup volume in the cluster")
				continue
			}
		}

		log.Info("Successfully synced all backup volumes into cluster")

		// update the backup volumes information to volumes.longhorn.io CR
		manager.SyncVolumesLastBackupWithBackupVolumes(backupVolumes, bm.ds.ListVolumes, bm.ds.GetVolume, bm.ds.UpdateVolumeStatus)

		// update the current cluster backup volumes
		clusterBackupVolumes, err = bm.ds.ListBackupVolume()
		if err != nil {
			log.WithError(err).Error("Error listing backup volumes in the cluster")
			return
		}

		// loop each backup volumes
		for backupVolumeName := range clusterBackupVolumes {
			log = log.WithField("backupVolume", backupVolumeName)

			// get a list of all the volume snapshot backup names that are stored in the backup store
			log.Debug("Pulling backup store for volume snapshot backup names")
			res, err := bm.target.ListVolumeSnapshotBackupNames(backupVolumeName)
			if err != nil {
				log.WithError(err).Error("Error listing volume snapshot backup names in the backup store")
				return
			}
			backupStoreVolumeSnapshotBackups := sets.NewString(res...)
			log.WithField("backupStoreVolumeSnapshotBackupCount", len(backupStoreVolumeSnapshotBackups)).Debug("Got volume snapshot backup from backup store")

			// get a list of all the volume backups that exist as custom resources in the cluster
			clusterBackupVolume, err := bm.ds.GetBackupVolume(backupVolumeName)
			if err != nil {
				log.WithError(err).Error("Error getting backup volumes from cluster, proceeding with pull into cluster")
			} else {
				log.WithField("clusterVolumeSnapshotBackupCount", len(clusterBackupVolume.Status.Backups)).Debug("Got volume snapshot backup from cluster")
			}

			clusterVolumeSnapshotBackupsSet := sets.NewString()
			for clusterVolumeSnapshotBackupName := range clusterBackupVolume.Status.Backups {
				clusterVolumeSnapshotBackupsSet.Insert(clusterVolumeSnapshotBackupName)
			}

			// get a list of volume backups that *are* in the backup store and *aren't* in the cluster
			volumeSnapshotBackupsToPull := backupStoreVolumeSnapshotBackups.Difference(clusterVolumeSnapshotBackupsSet)
			if count := volumeSnapshotBackupsToPull.Len(); count > 0 {
				log.Infof("Found %v volume snapshot backups in the backup store that do not exist in the cluster and need to be pulled", count)
			} else {
				log.Debug("No volume snapshot backups found in the backup store that need to be pulled into the cluster")
			}
			// pull each volume snapshot backups from the backup store
			for volumeSnapshotBackupName := range volumeSnapshotBackupsToPull {
				log = log.WithField("volumeSnapshotBackup", volumeSnapshotBackupName)
				log.Debug("Attempting to pull volume snapshot backup into cluster")

				volumeSnapshotBackupMetadataURL := backupstore.EncodeMetadataURL(volumeSnapshotBackupName, backupVolumeName, bm.target.URL)
				volumeSnapshotBackup, err := bm.target.InspectVolumeSnapshotBackupMetadata(volumeSnapshotBackupMetadataURL)
				if err != nil {
					log.WithError(err).Error("Error getting volume backup metadata from backup store")
					continue
				}

				if clusterBackupVolume.Status.Backups == nil {
					clusterBackupVolume.Status.Backups = make(map[string]*types.VolumeSnapshotBackup)
				}
				clusterBackupVolume.Status.Backups[volumeSnapshotBackupName] = &types.VolumeSnapshotBackup{
					URL:                     volumeSnapshotBackup.URL,
					SnapshotName:            volumeSnapshotBackup.SnapshotName,
					SnapshotCreateTimestamp: volumeSnapshotBackup.SnapshotCreated,
					BackupCreateTimestamp:   volumeSnapshotBackup.Created,
					Size:                    volumeSnapshotBackup.Size,
					Labels:                  volumeSnapshotBackup.Labels,
					VolumeName:              volumeSnapshotBackup.VolumeName,
					VolumeSize:              volumeSnapshotBackup.VolumeSize,
					VolumeCreateTimestamp:   volumeSnapshotBackup.VolumeCreated,
					Messages:                volumeSnapshotBackup.Messages,
				}
				clusterBackupVolume.Status.LastSyncedTimestmp = &metav1.Time{Time: time.Now().UTC()}
			}

			// get a list of volume backups that *are* in the cluster and *aren't* in the backup store
			volumeSnapshotBackupsToDelete := clusterVolumeSnapshotBackupsSet.Difference(backupStoreVolumeSnapshotBackups)
			if count := volumeSnapshotBackupsToDelete.Len(); count > 0 {
				log.Infof("Found %v volume snapshot backups in the backup store that do not exist in the backup store and need to be deleted", count)
			} else {
				log.Debug("No volume snapshot backups found in the cluster store that need to be deleted in the cluster")
			}
			// delete the volume snapshot backups in the cluster
			for volumeSnapshotBackupName := range volumeSnapshotBackupsToDelete {
				log = log.WithField("volumeSnapshotBackup", volumeSnapshotBackupName)
				log.Debug("Attempting to delete volume snapshot backup in the cluster")
				delete(clusterBackupVolume.Status.Backups, volumeSnapshotBackupName)
			}

			_, err = bm.ds.UpdateBackupVolumeStatus(clusterBackupVolume)
			if err != nil {
				log.WithError(err).Error("Error syncing volume snapshot backups into cluster")
			}
			log.Info("Successfully synced volume snapshot backups into cluster")
		}
	}, bm.pollInterval, bm.stopCh)
}

func (bm *BackupStoreMonitor) Stop() {
	if bm.pollInterval != time.Duration(0) {
		bm.stopCh <- struct{}{}
	}
}
