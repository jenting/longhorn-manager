package controller

import (
	"fmt"
	"sort"
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
	"github.com/longhorn/longhorn-manager/engineapi"
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

	pollInterval time.Duration

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

func (bvs *BackupVolumeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bvs.queue.ShutDown()

	bvs.logger.Infof("Start Longhorn Backup Volume controller")
	defer bvs.logger.Infof("Shutting down Longhorn Backup Volume controller")

	if !cache.WaitForNamedCacheSync(bvs.name, stopCh, bvs.sStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bvs.worker, time.Second, stopCh)
	}
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

	interval, err := bvs.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
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
		manager.SyncVolumesLastBackupWithBackupVolumes(nil,
			bvs.ds.ListVolumes, bvs.ds.GetVolume, bvs.ds.UpdateVolumeStatus)
	}

	if targetSetting.Value == "" {
		return nil
	}

	target, err := manager.GenerateBackupTarget(bvs.ds)
	if err != nil {
		return err
	}
	bvs.bsMonitor = &BackupStoreMonitor{
		logger:       bvs.logger.WithField("component", "backup-store-monitor"),
		controllerID: bvs.controllerID,

		backupTarget:                 targetSetting.Value,
		backupTargetCredentialSecret: secretSetting.Value,

		pollInterval: time.Duration(interval) * time.Second,

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

	interval, err := bvs.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}

	if bvs.bsMonitor.pollInterval == time.Duration(interval)*time.Second {
		return nil
	}

	bvs.bsMonitor.Stop()

	bvs.bsMonitor.pollInterval = time.Duration(interval) * time.Second
	bvs.bsMonitor.stopCh = make(chan struct{})

	go bvs.bsMonitor.Start()
	return nil
}

func (bm *BackupStoreMonitor) Start() {
	log := bm.logger.WithFields(logrus.Fields{
		"backupTarget": bm.target.URL,
		"pollInterval": bm.pollInterval,
	})
	if bm.pollInterval == time.Duration(0) {
		log.Info("Disabling backup store monitoring")
		return
	}
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
		interval := int64(bm.pollInterval.Seconds())
		midPoint := interval / 2
		timeIndex := int((time.Now().UTC().Unix() + midPoint) / interval)
		candidateIndex := timeIndex % len(candidates)
		responsibleNode := candidates[candidateIndex]
		return bm.controllerID == responsibleNode, nil
	}

	wait.Until(func() {
		if isResponsible, err := shouldProcess(); err != nil || !isResponsible {
			if err != nil {
				log.WithError(err).Warn("Failed to select node for backup store monitoring, will try again next poll interval")
			}
			return
		}

		bm.logger.Debug("Polling backup store for new volume backups")
		backupVolumeNames, err := bm.target.ListBackupVolumeNames()
		if err != nil {
			bm.logger.WithError(err).Warn("Failed to list backup volumes, cannot update volume names last backup")
		}

		backupVolumes := make(map[string]*engineapi.BackupVolume)
		for _, backupVolumeName := range backupVolumeNames {
			backupVolume, err := bm.target.InspectBackupVolumeMetadata(backupVolumeName)
			if err != nil {
				bm.logger.WithError(err).Warn("Failed to list backup volumes, cannot update volume names last backup")
				continue
			}
			backupVolumes[backupVolumeName] = backupVolume
		}
		manager.SyncVolumesLastBackupWithBackupVolumes(backupVolumes,
			bm.ds.ListVolumes, bm.ds.GetVolume, bm.ds.UpdateVolumeStatus)
		bm.logger.Debug("Refreshed all volumes last backup based on backup store information")
	}, bm.pollInterval, bm.stopCh)
}

func (bm *BackupStoreMonitor) Stop() {
	if bm.pollInterval != time.Duration(0) {
		bm.stopCh <- struct{}{}
	}
}

func (bvs *BackupVolumeController) enqueueBackupVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bvs.queue.AddRateLimited(key)
}
