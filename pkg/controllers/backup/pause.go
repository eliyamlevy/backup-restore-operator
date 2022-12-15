package restore

import (
	"strconv"
	"strings"

	"github.com/rancher/backup-restore-operator/pkg/resourcesets"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8sv1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func (h *handler) scaleDownControllerAndStoreReplicaCount(rh *resourcesets.ResourceHandler, configMaps v1core.ConfigMapController) {
	replicaMap := make(map[string]string)
	for _, resourceList := range rh.GVResourceToObjects {
		for _, resource := range resourceList {
			//check if deployment
			if resource.GetKind() == "deployment" {
				//check if controller
				if strings.Contains(resource.GetName(), "controller") {
					//scale down and persist replica size
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
					} else {
						replicaMap[controllerRef.Name] = strconv.Itoa(int(controllerRef.Replicas))
					}
				}
			}
		}
	}
	// The number of replicas needs to persist across migration create configMaps for each controller storing the
	// number of replicas.
	cm := corev1.ConfigMap{
		TypeMeta: k8sv1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: k8sv1.ObjectMeta{
			Name:      "controller-replicas",
			Namespace: "cattle-resources-system",
		},
		Data: replicaMap,
	}
	configMaps.Create(cm)
}

func (h *handler) scaleUpControllersFromResourceSet(objFromBackupCR ObjectsFromBackupCR) {
	for _, controllerRef := range objFromBackupCR.backupResourceSet.ControllerReferences {
		controllerObj, dr := h.getObjFromControllerRef(controllerRef)
		if controllerObj == nil {
			continue
		}
		controllerObj.Object["spec"].(map[string]interface{})["replicas"] = controllerRef.Replicas
		// update controller to scale it back up
		logrus.Infof("Scaling up controllerRef %v/%v/%v to %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, controllerRef.Replicas)
		_, err := dr.Update(h.ctx, controllerObj, k8sv1.UpdateOptions{})
		if err != nil {
			logrus.Errorf("Error scaling up %v/%v/%v, edit it to scale back to %v", controllerRef.APIVersion, controllerRef.Resource, controllerRef.Name, controllerRef.Replicas)
		}
	}
}

func (h *handler) getObjFromControllerRef(resource *unstructured.Unstructured) dynamic.ResourceInterface {
	logrus.Infof("Processing controllerRef %v/%v/%v", resource.GetAPIVersion(), controllerRef.Resource, controllerRef.Name)
	var dr dynamic.ResourceInterface
	gv, err := schema.ParseGroupVersion(resource.GetAPIVersion())
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
