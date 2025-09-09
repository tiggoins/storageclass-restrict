package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/spf13/pflag"
)

type Config struct {
	context         context.Context
	client          *kubernetes.Clientset
	oldStorageclass string
	newStorageclass string
	namespace       string
	mode            string // 新增：操作模式 "migrate" 或 "set-zero"
}

var (
	// 迁移配额的patch模板：将现有配额设置给原存储类，新存储类设为0
	patchMigrateTemplate = `{
		"spec": {
			"hard": {
				"%s.storageclass.storage.k8s.io/requests.storage": "%s",
				"%s.storageclass.storage.k8s.io/requests.storage": "0"
			}
		}
	}`

	// 设置存储类配额为0的patch模板
	patchSetZeroTemplate = `{
		"spec": {
			"hard": {
				"%s.storageclass.storage.k8s.io/requests.storage": "0"
			}
		}
	}`
)

func main() {
	var errorList []error
	c := NewConfig()
	
	switch c.mode {
	case "migrate":
		if err := c.MigrateStorageclassQuota(); err != nil {
			errorList = append(errorList, err)
		}
	case "set-zero":
		if err := c.SetStorageclassQuotaToZero(); err != nil {
			errorList = append(errorList, err)
		}
	default:
		klog.Exitf("invalid mode: %s", c.mode)
	}

	if len(errorList) == 0 {
		if c.mode == "migrate" {
			klog.Infoln("\033[32msuccessfully migrated storageclass quota for all namespaces.\033[0m")
		} else {
			klog.Infoln("\033[32msuccessfully set storageclass quota to zero for all namespaces.\033[0m")
		}
		return
	} else {
		aggregatedError := utilerrors.NewAggregate(errorList)
		klog.Infof("Errors occurred: %v\n", aggregatedError)
	}
}

func NewConfig() *Config {
	config := new(Config)
	pflag.StringVarP(&config.oldStorageclass, "old-storageclass", "o", "", "specify the original storage class to set quota for (migrate mode)")
	pflag.StringVarP(&config.newStorageclass, "new-storageclass", "s", "", "specify the new storage class (migrate mode) or target storage class (set-zero mode)")
	pflag.StringVarP(&config.namespace, "namespace", "n", "", "specify the namespace (default to all namespace)")
	pflag.StringVarP(&config.mode, "mode", "m", "migrate", "operation mode: 'migrate' or 'set-zero'")

	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Migration mode: %s -m migrate -o <old-storageclass> -s <new-storageclass> [-n <namespace>]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Set-zero mode:  %s -m set-zero -s <storageclass> [-n <namespace>]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "  举例: ")
		fmt.Fprintf(os.Stderr, "  	迁移模式 - 将prometheus命名空间的配额从new-storage迁移到rbd-ceph-csi:\n")
		fmt.Fprintf(os.Stderr, "  	%s -m migrate -o rbd-ceph-csi -s new-storage -n prometheus\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	迁移模式 - 迁移所有命名空间的配额从new-storage到rbd-ceph-csi:\n")
		fmt.Fprintf(os.Stderr, "  	%s -m migrate -o rbd-ceph-csi -s new-storage\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	设零模式 - 将新存储类storageclass-c的配额设为0:\n")
		fmt.Fprintf(os.Stderr, "  	%s -m set-zero -s storageclass-c\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  	设零模式 - 将prometheus命名空间中storageclass-c的配额设为0:\n")
		fmt.Fprintf(os.Stderr, "  	%s -m set-zero -s storageclass-c -n prometheus\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n说明: \n")
		fmt.Fprintf(os.Stderr, "  migrate模式: 将现有的requests.storage配额设置给old-storageclass，并将new-storageclass的配额设为0\n")
		fmt.Fprintf(os.Stderr, "  set-zero模式: 将指定存储类的配额设置为0，用于新建存储类的初始化\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		pflag.PrintDefaults()
	}
	pflag.Parse()

	if config.namespace == "" {
		config.namespace = metav1.NamespaceAll
	}

	// 验证模式和参数
	switch config.mode {
	case "migrate":
		if config.oldStorageclass == "" {
			klog.Exitln("migrate mode requires old-storageclass, please specify with -o")
		}
		if config.newStorageclass == "" {
			klog.Exitln("migrate mode requires new-storageclass, please specify with -s")
		}
		if config.oldStorageclass == config.newStorageclass {
			klog.Exitln("old-storageclass and new-storageclass cannot be the same")
		}
	case "set-zero":
		if config.newStorageclass == "" {
			klog.Exitln("set-zero mode requires storageclass, please specify with -s")
		}
	default:
		klog.Exitf("invalid mode: %s, must be 'migrate' or 'set-zero'", config.mode)
	}

	config.context = context.TODO()

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
	if c.mode == "migrate" {
		// 检查原存储类是否存在
		_, err := c.client.StorageV1().StorageClasses().Get(c.context, c.oldStorageclass, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				klog.Exitf("old storageclass %s does not exist", c.oldStorageclass)
			}
			klog.Exitf("error happened when getting old storageclass %s, error: %v", c.oldStorageclass, err.Error())
		}
	}

	// 检查新存储类/目标存储类是否存在
	_, err := c.client.StorageV1().StorageClasses().Get(c.context, c.newStorageclass, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Exitf("storageclass %s does not exist", c.newStorageclass)
		}
		klog.Exitf("error happened when getting storageclass %s, error: %v", c.newStorageclass, err.Error())
	}

	if c.mode == "migrate" {
		klog.Infof("both storageclasses validated: %s (old) -> %s (new)", c.oldStorageclass, c.newStorageclass)
	} else {
		klog.Infof("target storageclass validated: %s", c.newStorageclass)
	}
}

func (c *Config) MigrateStorageclassQuota() error {
	var errorList []error
	rqs, err := c.client.CoreV1().ResourceQuotas(c.namespace).List(c.context, metav1.ListOptions{})
	if err != nil {
		return err
	}

	if len(rqs.Items) == 0 {
		return fmt.Errorf("no ResourceQuota found in namespace/%s", c.namespace)
	}

	for _, rq := range rqs.Items {
		// 获取现有的requests.storage配额
		quotaSize := c.getExistingStorageQuota(&rq)

		if quotaSize == "" {
			klog.Warningf("no requests.storage quota found in ResourceQuota %s/%s, skipping", rq.Namespace, rq.Name)
			continue
		}

		// 执行迁移：将现有配额设置给原存储类，新存储类设为0
		patchData := fmt.Sprintf(patchMigrateTemplate, c.oldStorageclass, quotaSize, c.newStorageclass)

		patchType := types.StrategicMergePatchType
		_, err = c.client.CoreV1().ResourceQuotas(rq.Namespace).Patch(c.context, rq.Name, patchType, []byte(patchData), metav1.PatchOptions{
			FieldManager: "storageclass-migration",
		})
		if err != nil {
			klog.Warningf("failed to migrate storageclass quota in namespace/%s: %v", rq.Namespace, err)
			errorList = append(errorList, err)
			continue
		}

		klog.Infof("successfully migrated quota %s: %s -> %s, %s -> 0 in namespace/%s",
			quotaSize, c.newStorageclass, c.oldStorageclass, c.newStorageclass, rq.Namespace)
	}

	return utilerrors.NewAggregate(errorList)
}

// SetStorageclassQuotaToZero 将指定存储类的配额设置为0
func (c *Config) SetStorageclassQuotaToZero() error {
	var errorList []error
	rqs, err := c.client.CoreV1().ResourceQuotas(c.namespace).List(c.context, metav1.ListOptions{})
	if err != nil {
		return err
	}

	if len(rqs.Items) == 0 {
		return fmt.Errorf("no ResourceQuota found in namespace/%s", c.namespace)
	}

	for _, rq := range rqs.Items {
		// 检查是否已经存在该存储类的配额
		quotaKey := fmt.Sprintf("%s.storageclass.storage.k8s.io/requests.storage", c.newStorageclass)
		if existingQuota, exists := rq.Spec.Hard[corev1.ResourceName(quotaKey)]; exists {
			if existingQuota.String() == "0" {
				klog.V(2).Infof("storageclass %s quota already set to 0 in namespace/%s, skipping", c.newStorageclass, rq.Namespace)
				continue
			}
		}

		// 设置存储类配额为0
		patchData := fmt.Sprintf(patchSetZeroTemplate, c.newStorageclass)

		patchType := types.StrategicMergePatchType
		_, err = c.client.CoreV1().ResourceQuotas(rq.Namespace).Patch(c.context, rq.Name, patchType, []byte(patchData), metav1.PatchOptions{
			FieldManager: "storageclass-quota-zero",
		})
		if err != nil {
			klog.Warningf("failed to set storageclass %s quota to zero in namespace/%s: %v", c.newStorageclass, rq.Namespace, err)
			errorList = append(errorList, err)
			continue
		}

		klog.Infof("successfully set storageclass %s quota to 0 in namespace/%s", c.newStorageclass, rq.Namespace)
	}

	return utilerrors.NewAggregate(errorList)
}

// getExistingStorageQuota 从ResourceQuota中获取现有的requests.storage配额
func (c *Config) getExistingStorageQuota(rq *corev1.ResourceQuota) string {
	// 直接获取requests.storage配额
	if quota, exists := rq.Spec.Hard["requests.storage"]; exists {
		klog.V(5).Infof("found requests.storage quota: %s in namespace/%s", quota.String(), rq.Namespace)
		return quota.String()
	}

	return ""
}