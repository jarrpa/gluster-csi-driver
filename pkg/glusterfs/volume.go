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
