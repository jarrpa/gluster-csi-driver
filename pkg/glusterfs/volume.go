/*
Copyright 2018 The Gluster CSI Authors.

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

package glusterfs

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type VolumeEntry struct {
	Attributes map[string]string
	Secrets    map[string]string
}
type volumeCacheMap map[string]*VolumeEntry

var (
	volCache    = make(volumeCacheMap)
	volCacheMtx sync.Mutex
)

func buildVolumeCache() error {
	volCacheMtx.Lock()
	defer volCacheMtx.Unlock()

	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driverName {
			vol := VolumeEntry{
				Attributes: pv.Spec.CSI.VolumeAttributes,
			}
			volCache[pv.Spec.CSI.VolumeHandle] = &vol
		}
	}

	return nil
}

func (m volumeCacheMap) add(volId string, volEnt *VolumeEntry) {
	volCacheMtx.Lock()
	defer volCacheMtx.Unlock()

	m[volId] = volEnt
	glog.Infof("Volume %s added to cache", volId)
}

func (m volumeCacheMap) get(volId string) (*VolumeEntry, error) {
	volCacheMtx.Lock()
	defer volCacheMtx.Unlock()

	ent, ok := m[volId]
	if !ok {
		return nil, fmt.Errorf("Volume %s not found in cache", volId)
	}

	return ent, nil
}

func (m volumeCacheMap) remove(volId string) {
	volCacheMtx.Lock()
	defer volCacheMtx.Unlock()

	delete(m, volId)
	glog.Infof("Volume %s deleted from cache", volId)
}

// TODO: Find PV
func findPV(volId string) (*VolumeEntry, error) {
	return nil, fmt.Errorf("No PV found for volume %s", volId)
}
