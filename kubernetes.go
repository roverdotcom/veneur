package veneur

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesDiscoverer struct {
	clientset *kubernetes.Clientset
}

func NewKubernetesDiscoverer() (*KubernetesDiscoverer, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &KubernetesDiscoverer{clientset}, nil
}

func (kd *KubernetesDiscoverer) GetDestinationsForService(serviceName string) ([]string, error) {
	// It looks like the serviceName is never actually consulted by the
	// kubernetes integration. We'll see if we can help finish this integration
	// and get it real configuration options. For now this continues the hack by
	// looking for the string grpc in the servicename and then finding a port
	// named grpc in the container spec.
	isGrpc := strings.Contains(strings.ToLower(serviceName), "grpc")
	pods, err := kd.clientset.CoreV1().Pods(metav1.NamespaceAll).List(metav1.ListOptions{
		LabelSelector: "app=veneur-global",
	})
	if err != nil {
		return nil, err
	}

	ips := make([]string, 0, len(pods.Items))
	for podIndex, pod := range pods.Items {

		var forwardPort string

		if pod.Status.Phase != v1.PodRunning {
			continue
		}

		// TODO don't assume there is only one container for the veneur global
		if len(pod.Spec.Containers) > 0 {
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					// Still hacking this right now. We'll check to see if we
					// can help finish wiring up the kubernetes integration into
					// the config structure. This should probably use a service
					// name and find the pods from that selector, doing a
					// similar port search as before.
					if port.Name == "grpc" {
						if isGrpc {
							forwardPort = strconv.Itoa(int(port.ContainerPort))
							log.WithField("port", forwardPort).Debug("Found grpc port")
							break
						}
						continue
					}

					if port.Name == "http" {
						forwardPort = strconv.Itoa(int(port.ContainerPort))
						log.WithField("port", forwardPort).Debug("Found http port")
						break
					}

					// TODO don't assume all TCP ports are for importing
					if port.Protocol == "TCP" {
						forwardPort = strconv.Itoa(int(port.ContainerPort))
						log.WithField("port", forwardPort).Debug("Found TCP port")
					}
				}
			}
		}

		if forwardPort == "" || forwardPort == "0" {
			log.WithFields(logrus.Fields{
				"podIndex":    podIndex,
				"PodIP":       pod.Status.PodIP,
				"forwardPort": forwardPort,
			}).Error("Could not find valid port for forwarding")
			continue
		}

		if pod.Status.PodIP == "" {
			log.WithFields(logrus.Fields{
				"podIndex":    podIndex,
				"PodIP":       pod.Status.PodIP,
				"forwardPort": forwardPort,
			}).Error("Could not find valid podIP for forwarding")
			continue
		}

		// prepend with // so that it is a valid URL parseable by url.Parse
		var podIP string
		if isGrpc {
			podIP = fmt.Sprintf("%s:%s", pod.Status.PodIP, forwardPort)
		} else {
			podIP = fmt.Sprintf("http://%s:%s", pod.Status.PodIP, forwardPort)
		}

		ips = append(ips, podIP)
	}
	return ips, nil
}
