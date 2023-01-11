package restore

import (
	"fmt"
	"reflect"

	v1 "github.com/rancher/backup-restore-operator/pkg/apis/resources.cattle.io/v1"
	"github.com/sirupsen/logrus"
	k8sv1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func (h *handler) scaleDownControllersFromResourceSet(objFromBackupCR ObjectsFromBackupCR) {
	for ind, controllerRef := range objFromBackupCR.backupResourceSet.ControllerReferences {
		controllerObj, dr := h.getObjFromControllerRef(controllerRef)
		if controllerObj == nil {
			continue
		}
		spec, specFound := controllerObj.Object["spec"].(map[string]interface{})
		if !specFound {
			logrus.Errorf("Invalid controllerRef %v, spec not found, skipping it", controllerRef.Name)
			continue
		}
		replicas, int32replicaFound := spec["replicas"].(int32)
		if !int32replicaFound {
			int64replica, int64replicaFound := spec["replicas"].(int64)
			if !int64replicaFound {
				logrus.Errorf("Invalid controllerRef %v, replicas not found, skipping it", controllerRef.Name)
				continue
			}
			replicas = int32(int64replica)
		}
		// save the current replicas
		controllerRef.Replicas = replicas
		objFromBackupCR.backupResourceSet.ControllerReferences[ind] = controllerRef
		spec["replicas"] = 0
		// update controller to scale it down
		logrus.Infof("Scaling down controllerRef %v/%v/%v to 0", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name)
		_, err := dr.Update(h.ctx, controllerObj, k8sv1.UpdateOptions{})
		if err != nil {
			logrus.Errorf("Error scaling down %v/%v/%v, skipping it", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name)
		}
	}
}

//All controller replicas have been set to 0 in the back up files. This function will check a map stored in the root of
//of the backup file for the number of replicas to scale up to.
func (h *handler) scaleUpControllersFromResourceSet(objFromBackupCR ObjectsFromBackupCR) {
	for _, controllerRef := range objFromBackupCR.backupResourceSet.ControllerReferences {
		//Check handler for replicaMap (entries are stored with the key {namespace}/{name})
		entryName := controllerRef.Namespace + "/" + controllerRef.Name
		replicas := int32(h.controllerReplicas[entryName].(int64))
		if fmt.Sprintf("%s", reflect.TypeOf(replicas)) != "int32" {
			//TODO: throw err
			logrus.Errorf("Error scaling up %v/%v/%v, edit it to scale back to %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, replicas)
		}
		//Get controller object
		controllerObj, dr := h.getObjFromControllerRef(controllerRef)
		if controllerObj == nil {
			continue
		}
		controllerObj.Object["spec"].(map[string]interface{})["replicas"] = replicas
		// update controller to scale it back up
		logrus.Infof("Scaling up controllerRef %v/%v/%v to %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, replicas)
		_, err := dr.Update(h.ctx, controllerObj, k8sv1.UpdateOptions{})
		if err != nil {
			logrus.Errorf("Error scaling up %v/%v/%v, edit it to scale back to %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, replicas)
		}
	}
}

func (h *handler) getObjFromControllerRef(controllerRef v1.ControllerReference) (*unstructured.Unstructured, dynamic.ResourceInterface) {
	logrus.Infof("Processing controllerRef %v/%v/%v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name)
	var dr dynamic.ResourceInterface
	gv, err := schema.ParseGroupVersion(controllerRef.APIVersion)
	if err != nil {
		logrus.Errorf("Error parsing apiversion %v for controllerRef %v, skipping it", controllerRef.APIVersion, controllerRef.Name)
		return nil, dr
	}
	gvr := gv.WithResource(controllerRef.Resource)
	dr = h.dynamicClient.Resource(gvr)
	if controllerRef.Namespace != "" {
		dr = h.dynamicClient.Resource(gvr).Namespace(controllerRef.Namespace)
	}
	controllerObj, err := dr.Get(h.ctx, controllerRef.Name, k8sv1.GetOptions{})
	if err != nil {
		logrus.Infof("Error getting object for controllerRef %v/%v/%v: %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, err)
		return nil, dr
	}
	return controllerObj, dr
}
