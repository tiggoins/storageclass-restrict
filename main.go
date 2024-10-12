package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/spf13/pflag"
)

type Config struct {
	context      context.Context
	client       *kubernetes.Clientset
	storageclass string
	action       string
	namespace    string
	size         string
}

var (
	patchAddTemplate = `{
		"spec": {
			"hard": {
				"%s.storageclass.storage.k8s.io/requests.storage": "%s"
			}
		}
	
	}`

	patchDeleteTemplate = `{
		"spec": {
			"hard": {
				"%s.storageclass.storage.k8s.io/requests.storage": null
			}
		}
	
	}`
)

func main() {
	var errorList []error
	c := NewConfig()
	if err := c.PatchStorageclassRestricted(); err != nil {
		errorList = append(errorList, err)
	}

	if len(errorList) == 0 {
		klog.Infoln("\033[32msuccessfully added or removed storageclass restrictions for all namespaces.\033[0m")
		return
	} else {
		aggregatedError := utilerrors.NewAggregate(errorList)
		klog.Infof("Errors occurred: %v\n", aggregatedError)
	}
}

func NewConfig() *Config {
	config := new(Config)
	pflag.StringVarP(&config.storageclass, "storageclass", "s", "", "specify the storage class you want to restrict usage of..")
	pflag.StringVarP(&config.action, "action", "a", "add", "specify the action you want to take (add or remove restriction; the default action is add).")
	pflag.StringVarP(&config.namespace, "namespace", "n", "", "specify the namespace(default to all namespace.)")
	pflag.StringVarP(&config.size, "quota", "q", "0", "specify the size of usage of storageclass.(for example 50G | 200T,default to 0 represent disable)")

	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -s <size> -q <quota> -n <namespace> -a [add|remove] \n", os.Args[0])
		fmt.Fprintln(os.Stderr, "  举例: ")
		fmt.Fprintf(os.Stderr, "  	禁用prometheus对rbd-ceph-csi的使用  %s -s rbd-ceph-csi -a add -n prometheus \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	允许prometheus对rbd-ceph-csi的使用  %s -s rbd-ceph-csi -a remove -n prometheus \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	禁用所有命名空间对rbd-ceph-csi的使用  %s -s rbd-ceph-csi -a add \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	将prometheus命名空间对rbd-ceph-csi的限额调整为50G  %s -s rbd-ceph-csi -a add -n prometheus -q 50G \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		pflag.PrintDefaults()
	}
	pflag.Parse()

	if config.namespace == "" {
		config.namespace = metav1.NamespaceAll
	}

	if config.storageclass == "" {
		klog.Exitln("storageclass is empty,please specify storageclass")
	}

	config.ParseSize()

	config.context = context.TODO()
	if config.action != "add" && config.action != "remove" {
		klog.Exitf("action must be add or remove,and you provide %s", config.action)
	}

	c, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		klog.Exitf("error happened when building config,%v\n", err.Error())
	}
	client, err := kubernetes.NewForConfig(c)
	if err != nil || client == nil {
		klog.Exitf("error happened when construct kubernetes client,%v\n", err.Error())
	}
	config.client = client
	config.CheckIfStorageclassExist()

	return config
}

func (c *Config) CheckIfStorageclassExist() {
	_, err := c.client.StorageV1().StorageClasses().Get(c.context, c.storageclass, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Exitf("storageclass %s not exist", c.storageclass)
		}
		klog.Exitf("error happened when get storageclass %s,error: %v", c.storageclass, err.Error())
	}
}

func (c *Config) ParseSize() {
	q, err := resource.ParseQuantity(c.size)
	if err != nil {
		klog.Exitf("%v , for example: 50G / 200T", err.Error())
	}

	c.size = q.String()
}

func (c *Config) PatchStorageclassRestricted() error {
	var errorList []error
	rqs, err := c.client.CoreV1().ResourceQuotas(c.namespace).List(c.context, metav1.ListOptions{})
	if err != nil {
		return err
	}

	if len(rqs.Items) == 0 {
		return fmt.Errorf("no ResourceQuota found in namespace/%s", c.namespace)
	}

	for _, rq := range rqs.Items {
		var patchData []byte
		switch c.action {
		case "add":
			patchData = []byte(fmt.Sprintf(patchAddTemplate, c.storageclass, c.size))
		case "remove":
			patchData = []byte(fmt.Sprintf(patchDeleteTemplate, c.storageclass))
		default:
		}

		patchType := types.StrategicMergePatchType
		_, err = c.client.CoreV1().ResourceQuotas(rq.Namespace).Patch(c.context, rq.Name, patchType, patchData, metav1.PatchOptions{
			FieldManager: "storageclass-restriction",
		})
		if err != nil {
			klog.Warningf("failed to %s the storageclass/%s limits from namespace/%s: %v", c.action, c.storageclass, rq.Namespace, err)
			errorList = append(errorList, err)
			continue
		}
		klog.V(2).Infof("successful %s the storageclass/%s limits from namespace/%s", c.action, c.storageclass, rq.Namespace)
	}

	return utilerrors.NewAggregate(errorList)
}
