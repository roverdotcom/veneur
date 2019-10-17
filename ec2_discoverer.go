package veneur

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type Ec2Discoverer struct {
	awsSession         *session.Session
	grpcPort, httpPort string
}

func NewEc2Discoverer() (*Ec2Discoverer, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	disc := &Ec2Discoverer{
		awsSession: sess,
		httpPort:   "8127",
		grpcPort:   "8128",
	}
	return disc, nil
}

func (d *Ec2Discoverer) GetDestinationsForService(serviceName string) ([]string, error) {
	isGrpc := strings.Contains(strings.ToLower(serviceName), "grpc")
	var ids []*string
	instancesIps := map[string]string{}
	var out []string

	sess, _ := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	ec2Svc := ec2.New(sess)
	autoscalingSvc := autoscaling.New(sess)
	instances, err := ec2Svc.DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("tag-key"),
				Values: []*string{
					aws.String(serviceName),
				},
			},
		},
	})
	if err != nil {
		return out, err
	}
	for _, reservation := range instances.Reservations {
		for _, instance := range reservation.Instances {
			ids = append(ids, instance.InstanceId)
			instancesIps[*instance.InstanceId] = *instance.PrivateIpAddress
		}
	}

	// If we can't find any instances then just return
	if len(ids) == 0 {
		return out, nil
	}

	autoscalingInstances, err := autoscalingSvc.DescribeAutoScalingInstances(&autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: ids,
	})

	if err != nil {
		return out, err
	}

	for _, instance := range autoscalingInstances.AutoScalingInstances {
		if *instance.LifecycleState == "InService" {
			var node string
			ip := instancesIps[*instance.InstanceId]
			if isGrpc {
				node = fmt.Sprintf("%s:%s", ip, d.grpcPort)
			} else {
				node = fmt.Sprintf("http://%s:%s", ip, d.httpPort)
			}
			out = append(out, node)
		}
	}

	sort.Strings(out)

	return out, nil
}
