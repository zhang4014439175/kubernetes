/*
Copyright 2014 The Kubernetes Authors.

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

package core

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/errors"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1beta1"
	"k8s.io/client-go/util/workqueue"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/priorities"
	extenderv1 "k8s.io/kubernetes/pkg/scheduler/apis/extender/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/migration"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	nodeinfosnapshot "k8s.io/kubernetes/pkg/scheduler/nodeinfo/snapshot"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
	utiltrace "k8s.io/utils/trace"
)

const (
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind is the minimum percentage of nodes that
	// would be scored in each scheduling cycle. This is a semi-arbitrary value
	// to ensure that a certain minimum of nodes are checked for feasibility.
	// This in turn helps ensure a minimum level of spreading.
	// 是在每个调度周期中评分的最小节点百分比。这是一个半任意值，以确保检查某个最小节点的可行性。这反过来又有助于确保最小的传播水平。
	minFeasibleNodesPercentageToFind = 5
)

// FailedPredicateMap declares a map[string][]algorithm.PredicateFailureReason type.
type FailedPredicateMap map[string][]predicates.PredicateFailureReason

// FitError describes a fit error of a pod.
type FitError struct {
	Pod                   *v1.Pod
	NumAllNodes           int
	FailedPredicates      FailedPredicateMap
	FilteredNodesStatuses framework.NodeToStatusMap
}

// ErrNoNodesAvailable is used to describe the error that no nodes available to schedule pods.
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

const (
	// NoNodeAvailableMsg is used to format message when no nodes available.
	NoNodeAvailableMsg = "0/%v nodes are available"
)

// Error returns detailed information of why the pod failed to fit on each node
func (f *FitError) Error() string {
	reasons := make(map[string]int)
	for _, predicates := range f.FailedPredicates {
		for _, pred := range predicates {
			reasons[pred.GetReason()]++
		}
	}

	for _, status := range f.FilteredNodesStatuses {
		reasons[status.Message()]++
	}

	sortReasonsHistogram := func() []string {
		reasonStrings := []string{}
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}
	reasonMsg := fmt.Sprintf(NoNodeAvailableMsg+": %v.", f.NumAllNodes, strings.Join(sortReasonsHistogram(), ", "))
	return reasonMsg
}

// ScheduleAlgorithm is an interface implemented by things that know how to schedule pods
// onto machines.
// TODO: Rename this type.
type ScheduleAlgorithm interface {
	Schedule(context.Context, *framework.CycleState, *v1.Pod) (scheduleResult ScheduleResult, err error)
	// Preempt receives scheduling errors for a pod and tries to create room for
	// the pod by preempting lower priority pods if possible.
	// It returns the node where preemption happened, a list of preempted pods, a
	// list of pods whose nominated node name should be removed, and error if any.
	Preempt(context.Context, *framework.CycleState, *v1.Pod, error) (selectedNode *v1.Node, preemptedPods []*v1.Pod, cleanupNominatedPods []*v1.Pod, err error)
	// Predicates() returns a pointer to a map of predicate functions. This is
	// exposed for testing.
	Predicates() map[string]predicates.FitPredicate
	// Prioritizers returns a slice of priority config. This is exposed for
	// testing.
	Prioritizers() []priorities.PriorityConfig
	// Extenders returns a slice of extender config. This is exposed for
	// testing.
	Extenders() []algorithm.SchedulerExtender
	// GetPredicateMetadataProducer returns the predicate metadata producer. This is needed
	// for cluster autoscaler integration.
	// TODO(#85691): remove this once CA migrates to creating a Framework instead of a full scheduler.
	PredicateMetadataProducer() predicates.MetadataProducer
	// Snapshot snapshots scheduler cache and node infos. This is needed
	// for cluster autoscaler integration.
	// TODO(#85691): remove this once CA migrates to creating a Framework instead of a full scheduler.
	Snapshot() error
}

// ScheduleResult represents the result of one pod scheduled. It will contain
// the final selected Node, along with the selected intermediate information.
type ScheduleResult struct {
	// Name of the scheduler suggest host
	SuggestedHost string
	// Number of nodes scheduler evaluated on one pod scheduled
	EvaluatedNodes int
	// Number of feasible nodes on one pod scheduled
	FeasibleNodes int
}

type genericScheduler struct {
	cache                    internalcache.Cache
	schedulingQueue          internalqueue.SchedulingQueue
	predicates               map[string]predicates.FitPredicate
	priorityMetaProducer     priorities.MetadataProducer
	predicateMetaProducer    predicates.MetadataProducer
	prioritizers             []priorities.PriorityConfig
	framework                framework.Framework
	extenders                []algorithm.SchedulerExtender
	alwaysCheckAllPredicates bool
	nodeInfoSnapshot         *nodeinfosnapshot.Snapshot
	volumeBinder             *volumebinder.VolumeBinder
	pvcLister                corelisters.PersistentVolumeClaimLister
	pdbLister                policylisters.PodDisruptionBudgetLister
	disablePreemption        bool
	percentageOfNodesToScore int32
	enableNonPreempting      bool
	nextStartNodeIndex       int
}

// snapshot snapshots scheduler cache and node infos for all fit and priority
// functions.
func (g *genericScheduler) Snapshot() error {
	// Used for all fit and priority funcs.
	return g.cache.UpdateNodeInfoSnapshot(g.nodeInfoSnapshot)
}

// GetPredicateMetadataProducer returns the predicate metadata producer. This is needed
// for cluster autoscaler integration.
func (g *genericScheduler) PredicateMetadataProducer() predicates.MetadataProducer {
	return g.predicateMetaProducer
}

// Schedule tries to schedule the given pod to one of the nodes in the node list.
// If it succeeds, it will return the name of the node.
// If it fails, it will return a FitError error with reasons.
func (g *genericScheduler) Schedule(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (result ScheduleResult, err error) {
	trace := utiltrace.New("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	defer trace.LogIfLong(100 * time.Millisecond)

	if err := podPassesBasicChecks(pod, g.pvcLister); err != nil {
		return result, err
	}
	trace.Step("Basic checks done")

	// 获取node节点信息，方便findNodesThatFit里面使用
	if err := g.Snapshot(); err != nil {
		return result, err
	}
	trace.Step("Snapshoting scheduler cache and node infos done")

	if len(g.nodeInfoSnapshot.NodeInfoList) == 0 {
		return result, ErrNoNodesAvailable
	}

	// Run "prefilter" plugins.
	preFilterStatus := g.framework.RunPreFilterPlugins(ctx, state, pod)
	if !preFilterStatus.IsSuccess() {
		return result, preFilterStatus.AsError()
	}
	trace.Step("Running prefilter plugins done")

	// 1、预选node
	startPredicateEvalTime := time.Now()
	filteredNodes, failedPredicateMap, filteredNodesStatuses, err := g.findNodesThatFit(ctx, state, pod)
	if err != nil {
		return result, err
	}
	trace.Step("Computing predicates done")

	// Run "postfilter" plugins.
	postfilterStatus := g.framework.RunPostFilterPlugins(ctx, state, pod, filteredNodes, filteredNodesStatuses)
	if !postfilterStatus.IsSuccess() {
		return result, postfilterStatus.AsError()
	}

	if len(filteredNodes) == 0 {
		return result, &FitError{
			Pod:                   pod,
			NumAllNodes:           len(g.nodeInfoSnapshot.NodeInfoList),
			FailedPredicates:      failedPredicateMap,
			FilteredNodesStatuses: filteredNodesStatuses,
		}
	}
	trace.Step("Running postfilter plugins done")
	metrics.SchedulingAlgorithmPredicateEvaluationDuration.Observe(metrics.SinceInSeconds(startPredicateEvalTime))
	metrics.DeprecatedSchedulingAlgorithmPredicateEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPredicateEvalTime))
	metrics.SchedulingLatency.WithLabelValues(metrics.PredicateEvaluation).Observe(metrics.SinceInSeconds(startPredicateEvalTime))
	metrics.DeprecatedSchedulingLatency.WithLabelValues(metrics.PredicateEvaluation).Observe(metrics.SinceInSeconds(startPredicateEvalTime))

	startPriorityEvalTime := time.Now()
	// When only one node after predicate, just use it.
	if len(filteredNodes) == 1 {
		metrics.SchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInSeconds(startPriorityEvalTime))
		metrics.DeprecatedSchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPriorityEvalTime))
		return ScheduleResult{
			SuggestedHost:  filteredNodes[0].Name,
			EvaluatedNodes: 1 + len(failedPredicateMap) + len(filteredNodesStatuses),
			FeasibleNodes:  1,
		}, nil
	}

	// 2、优先算法
	metaPrioritiesInterface := g.priorityMetaProducer(pod, filteredNodes, g.nodeInfoSnapshot)
	priorityList, err := g.prioritizeNodes(ctx, state, pod, metaPrioritiesInterface, filteredNodes)
	if err != nil {
		return result, err
	}

	metrics.SchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInSeconds(startPriorityEvalTime))
	metrics.DeprecatedSchedulingAlgorithmPriorityEvaluationDuration.Observe(metrics.SinceInMicroseconds(startPriorityEvalTime))
	metrics.SchedulingLatency.WithLabelValues(metrics.PriorityEvaluation).Observe(metrics.SinceInSeconds(startPriorityEvalTime))
	metrics.DeprecatedSchedulingLatency.WithLabelValues(metrics.PriorityEvaluation).Observe(metrics.SinceInSeconds(startPriorityEvalTime))

	// 3、调度
	host, err := g.selectHost(priorityList)
	trace.Step("Prioritizing done")

	return ScheduleResult{
		SuggestedHost:  host,
		EvaluatedNodes: len(filteredNodes) + len(failedPredicateMap) + len(filteredNodesStatuses),
		FeasibleNodes:  len(filteredNodes),
	}, err
}

// Prioritizers returns a slice containing all the scheduler's priority
// functions and their config. It is exposed for testing only.
func (g *genericScheduler) Prioritizers() []priorities.PriorityConfig {
	return g.prioritizers
}

// Predicates returns a map containing all the scheduler's predicate
// functions. It is exposed for testing only.
func (g *genericScheduler) Predicates() map[string]predicates.FitPredicate {
	return g.predicates
}

func (g *genericScheduler) Extenders() []algorithm.SchedulerExtender {
	return g.extenders
}

// selectHost takes a prioritized list of nodes and then picks one
// in a reservoir sampling manner from the nodes that had the highest score.
func (g *genericScheduler) selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	cntOfMaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// Replace the candidate with probability of 1/cntOfMaxScore
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

// preempt finds nodes with pods that can be preempted to make room for "pod" to
// schedule. It chooses one of the nodes and preempts the pods on the node and
// returns 1) the node, 2) the list of preempted pods if such a node is found,
// 3) A list of pods whose nominated node name should be cleared, and 4) any
// possible error.
// Preempt does not update its snapshot. It uses the same snapshot used in the
// scheduling cycle. This is to avoid a scenario where preempt finds feasible
// nodes without preempting any pod. When there are many pending pods in the
// scheduling queue a nominated pod will go back to the queue and behind
// other pods with the same priority. The nominated pod prevents other pods from
// using the nominated resources and the nominated pod could take a long time
// before it is retried after many other pending pods.
// Preempt 在 pod 调度发生失败的时候尝试抢占低优先级的 pod.
// 返回发生 preemption 的 node, 被 preempt的 pods 列表,
// nominated node name 需要被移除的 pods 列表，一个 error 信息.
func (g *genericScheduler) Preempt(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scheduleErr error) (*v1.Node, []*v1.Pod, []*v1.Pod, error) {
	// Scheduler may return various types of errors. Consider preemption only if
	// the error is of type FitError.
	fitError, ok := scheduleErr.(*FitError)
	if !ok || fitError == nil {
		return nil, nil, nil, nil
	}
	// 判断执行驱逐操作是否合适，如何判断是否适合抢占？
	// 做的事情是判断一个 pod 是否应该去抢占其他 pods. 如果这个 pod 已经抢占过其他 pods，那些 pods 还在 graceful termination period 中，那就不应该再次发生抢占。
	// 如果一个 node 已经被这个 pod nominated，并且这个 node 上有处于 terminating 状态的 pods，那么就不考虑驱逐更多的 pods.
	if !podEligibleToPreemptOthers(pod, g.nodeInfoSnapshot.NodeInfoMap, g.enableNonPreempting) {
		klog.V(5).Infof("Pod %v/%v is not eligible for more preemption.", pod.Namespace, pod.Name)
		return nil, nil, nil, nil
	}
	if len(g.nodeInfoSnapshot.NodeInfoMap) == 0 {
		return nil, nil, nil, ErrNoNodesAvailable
	}
	// 计算潜在的执行驱逐后能够用于跑 pod 的 nodes，怎么寻找能够用于 preempt 的 nodes？
	// 要做的事情是寻找 predicates 阶段失败但是通过抢占也许能够调度成功的 nodes.
	potentialNodes := nodesWherePreemptionMightHelp(g.nodeInfoSnapshot.NodeInfoMap, fitError)
	if len(potentialNodes) == 0 {
		klog.V(3).Infof("Preemption will not help schedule pod %v/%v on any node.", pod.Namespace, pod.Name)
		// In this case, we should clean-up any existing nominated node name of the pod.
		return nil, nil, []*v1.Pod{pod}, nil
	}
	var (
		pdbs []*policy.PodDisruptionBudget
		err  error
	)

	// 列出 pdb 对象
	if g.pdbLister != nil {
		pdbs, err = g.pdbLister.List(labels.Everything())
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// 计算所有 node 需要驱逐的 pods 有哪些等，后面细讲，这个过程计算的是什么？
	// 这个函数会并发计算所有的 nodes 是否通过驱逐实现 pod 抢占。
	nodeToVictims, err := g.selectNodesForPreemption(ctx, state, pod, potentialNodes, pdbs)
	if err != nil {
		return nil, nil, nil, err
	}

	// We will only check nodeToVictims with extenders that support preemption.
	// Extenders which do not support preemption may later prevent preemptor from being scheduled on the nominated
	// node. In that case, scheduler will find a different host for the preemptor in subsequent scheduling cycles.
	// 拓展调度的逻辑
	nodeToVictims, err = g.processPreemptionWithExtenders(pod, nodeToVictims)
	if err != nil {
		return nil, nil, nil, err
	}

	// 选择1个 node 用于 schedule，怎么从选择最合适被抢占的 node？
	//  要从给定的 nodes 中选择一个 node，这个函数假设给定的 map 中 value 部分是以 priority 降序排列的。这里选择 node 的标准是：
	// 最少的 PDB violations
	// 最少的高优先级 victim
	// 优先级总数字最小
	// victim 总数最小
	// 直接返回第一个
	candidateNode := pickOneNodeForPreemption(nodeToVictims)
	if candidateNode == nil {
		return nil, nil, nil, nil
	}

	// Lower priority pods nominated to run on this node, may no longer fit on
	// this node. So, we should remove their nomination. Removing their
	// nomination updates these pods and moves them to the active queue. It
	// lets scheduler find another place for them.
	// 低优先级的被 nominate 到这个 node 的 pod 很可能已经不再 fit 这个 node 了，所以
	// 需要移除这些 pod 的 nomination，更新这些 pod，挪动到 activeQ 中，让调度器
	// 得以寻找另外一个 node 给这些 pod
	nominatedPods := g.getLowerPriorityNominatedPods(pod, candidateNode.Name)
	if nodeInfo, ok := g.nodeInfoSnapshot.NodeInfoMap[candidateNode.Name]; ok {
		return nodeInfo.Node(), nodeToVictims[candidateNode].Pods, nominatedPods, nil
	}

	return nil, nil, nil, fmt.Errorf(
		"preemption failed: the target node %s has been deleted from scheduler cache",
		candidateNode.Name)
}

// processPreemptionWithExtenders processes preemption with extenders
func (g *genericScheduler) processPreemptionWithExtenders(
	pod *v1.Pod,
	nodeToVictims map[*v1.Node]*extenderv1.Victims,
) (map[*v1.Node]*extenderv1.Victims, error) {
	if len(nodeToVictims) > 0 {
		for _, extender := range g.extenders {
			if extender.SupportsPreemption() && extender.IsInterested(pod) {
				newNodeToVictims, err := extender.ProcessPreemption(
					pod,
					nodeToVictims,
					g.nodeInfoSnapshot.NodeInfoMap,
				)
				if err != nil {
					if extender.IsIgnorable() {
						klog.Warningf("Skipping extender %v as it returned error %v and has ignorable flag set",
							extender, err)
						continue
					}
					return nil, err
				}

				// Replace nodeToVictims with new result after preemption. So the
				// rest of extenders can continue use it as parameter.
				nodeToVictims = newNodeToVictims

				// If node list becomes empty, no preemption can happen regardless of other extenders.
				if len(nodeToVictims) == 0 {
					break
				}
			}
		}
	}

	return nodeToVictims, nil
}

// getLowerPriorityNominatedPods returns pods whose priority is smaller than the
// priority of the given "pod" and are nominated to run on the given node.
// Note: We could possibly check if the nominated lower priority pods still fit
// and return those that no longer fit, but that would require lots of
// manipulation of NodeInfo and PredicateMeta per nominated pod. It may not be
// worth the complexity, especially because we generally expect to have a very
// small number of nominated pods per node.
func (g *genericScheduler) getLowerPriorityNominatedPods(pod *v1.Pod, nodeName string) []*v1.Pod {
	pods := g.schedulingQueue.NominatedPodsForNode(nodeName)

	if len(pods) == 0 {
		return nil
	}

	var lowerPriorityPods []*v1.Pod
	podPriority := podutil.GetPodPriority(pod)
	for _, p := range pods {
		if podutil.GetPodPriority(p) < podPriority {
			lowerPriorityPods = append(lowerPriorityPods, p)
		}
	}
	return lowerPriorityPods
}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
// 确定在调度过程中需要检查的节点数量。这是为了在大规模集群中优化调度效率，避免每次调度都遍历所有节点，从而节省资源和时间。
// 用于确定在一个调度周期内，应该检查多少个节点以找到适合的节点进行 Pod 的调度。其主要目的是提高调度效率，避免在大规模集群中浪费过多的计算资源。
// 假设一个集群有 5000 个节点，percentageOfNodesToScore 设置为 10%，则调度器只需要检查 500 个节点而不是全部 5000 个节点。这样可以大大提高调度效率。
// 当 percentageOfNodesToScore 计算出的节点数小于 100（即 minFeasibleNodesToFind），调度器仍会检查至少 100 个节点，以确保调度的充分性和公平性。
// 这个设计帮助调度器在大规模集群中避免因检查过多节点而导致的性能问题，同时确保在小规模集群中也能进行充分的调度决策。
//
// 在 Kubernetes 集群中，节点（Node）是运行容器化应用程序的物理或虚拟机器。
// 当我们说一个集群有 5000 个节点时，意思是这个 Kubernetes 集群中有 5000 台机器（可以是物理服务器或者虚拟机），
// 每台机器都可以运行 Pods（Kubernetes 中的最小可调度单元）。
func (g *genericScheduler) numFeasibleNodesToFind(numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind || g.percentageOfNodesToScore >= 100 {
		return numAllNodes
	}

	// 如果 adaptivePercentage 小于或等于 0，意味着需要进行自适应调整。
	// 设置基本百分比 (basePercentageOfNodesToScore):
	// •通过集群节点总数 numAllNodes 自适应地调整百分比值。具体公式是 basePercentageOfNodesToScore - numAllNodes/125。
	// •例如，如果 numAllNodes 为 5000，计算结果为 50 - 5000/125 = 50 - 40 = 10。
	// 这段代码旨在根据集群规模动态调整需要检查的节点百分比，以优化调度效率和资源使用。具体来说：
	//	•在节点较少时，保持较高的检查比例（最多 50%）。
	//	•在节点较多时，减少检查比例，避免浪费资源。
	//	•确保检查比例不低于预设的最小值，以保证调度的充分性。
	//通过这种自适应调整，可以在不同规模的集群中实现高效和灵活的调度策略。
	adaptivePercentage := g.percentageOfNodesToScore
	if adaptivePercentage <= 0 {
		basePercentageOfNodesToScore := int32(50)
		adaptivePercentage = basePercentageOfNodesToScore - numAllNodes/125
		if adaptivePercentage < minFeasibleNodesPercentageToFind {
			adaptivePercentage = minFeasibleNodesPercentageToFind
		}
	}

	// 	•计算需要参与调度的节点数。这里的 g.percentageOfNodesToScore 表示一个百分比，取值范围在 1 到 100 之间。
	//	•举例：如果集群中有 5000 个节点，percentageOfNodesToScore 设置为 10%，则计算结果为 5000 * 10 / 100 = 500，即 500 个节点参与调度。
	numNodes = numAllNodes * adaptivePercentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}

// Filters the nodes to find the ones that fit based on the given predicate functions
// Each node is passed through the predicate functions to determine if it is a fit
// state 跨调度插件保存特定于周期的数据。
// •此方法基于谓词和可能的扩展程序过滤节点。
// •并行处理节点以提高性能。
// •它处理错误并相应地更新状态。
// •结果包括适合的节点、谓词失败的节点和节点的状态。
func (g *genericScheduler) findNodesThatFit(ctx context.Context, state *framework.CycleState, pod *v1.Pod) ([]*v1.Node, FailedPredicateMap, framework.NodeToStatusMap, error) {
	var filtered []*v1.Node
	failedPredicateMap := FailedPredicateMap{}
	filteredNodesStatuses := framework.NodeToStatusMap{}

	// 检查是否有谓词和过滤插件：
	if len(g.predicates) == 0 && !g.framework.HasFilterPlugins() {
		filtered = g.nodeInfoSnapshot.ListNodes()
	} else {
		// 定义需要找到的节点数量：
		// 确定需要检查的节点数量
		// numNodesToFind 根据某种逻辑找到的可行节点数。
		allNodes := len(g.nodeInfoSnapshot.NodeInfoList)
		numNodesToFind := g.numFeasibleNodesToFind(int32(allNodes))

		// Create filtered list with enough space to avoid growing it
		// and allow assigning.
		// errCh:在节点检查期间处理错误的错误通道。
		filtered = make([]*v1.Node, numNodesToFind)
		errCh := util.NewErrorChannel()
		var (
			predicateResultLock sync.Mutex // predicateResultLock:同步访问共享变量的互斥锁。
			filteredLen         int32      // filteredLen:过滤节点列表的长度。
		)

		// ctx, cancel:具有取消功能的上下文。
		ctx, cancel := context.WithCancel(ctx)

		// We can use the same metadata producer for all nodes.
		// 元数据和节点检查功能
		// meta: Metadata for the predicates.
		// 为调度谓词生成元数据并将其写入调度周期状态。
		// 用于生成与调度谓词相关的元数据（metadata）。调度谓词是 Kubernetes 调度器用来确定 Pod 是否可以在某个节点上运行的规则和条件。
		// 这个方法会基于当前正在调度的 Pod 和节点信息快照（g.nodeInfoSnapshot）生成这些元数据。
		// 方法用于生成调度器在节点筛选过程中所需的元数据（metadata）。这些元数据包含了关于待调度 Pod 及节点快照的信息，供调度器在运行预选（predicate）函数时使用。
		// meta 是一个实现了 predicates.Metadata 接口的对象，包含了对当前 Pod 调度所需的所有上下文信息。这些信息包括但不限于：
		//	•	Pod 的详细信息
		//	•	节点的资源使用情况
		//	•	节点上已有的 Pods 信息
		//	•	其他可能影响调度决策的状态数据
		// meta获取是在 register_predicates 里面29行
		meta := g.predicateMetaProducer(pod, g.nodeInfoSnapshot)
		// 这一行代码将生成的元数据写入调度周期的状态（CycleState）中。CycleState 是一个数据结构，用于在调度周期内存储和共享数据。
		// 这里，它将元数据存储在一个特定的键（migration.PredicatesStateKey）下，以便在调度周期的其他部分可以访问这些数据
		// state.Write 方法将 meta 写入到调度周期的状态对象 state 中。这一步是为了在后续的调度过程中能够方便地访问和利用这些预先计算好的元数据。
		// 这样做的目的是为了在调度的不同阶段能够访问和使用这些元数据。例如，在运行预选插件或函数时，可以直接从 state 中读取这些信息，避免重复计算，提升调度效率。
		// 目的:
		// 	•优化调度过程：通过预先生成和存储元数据，减少在调度过程中实时计算的负担，提升调度效率。
		//	•数据共享：利用 state 对象，实现调度周期内不同插件和函数之间的数据共享，确保一致性和便利性。
		//	•可扩展性：这种设计允许调度器灵活地扩展和添加新的预选（predicate）逻辑，而无需修改已有的代码，只需确保元数据生成和存储的一致性即可。
		// 将元数据写入 state 对象的主要意义在于数据共享、提高代码可读性、避免重复计算、增强扩展性和确保状态一致性。
		// 这种设计使得 Kubernetes 调度器在处理复杂调度逻辑时，能够保持高效、灵活和可维护。直接传递参数虽然也能实现功能，但在大型系统中容易导致参数传递复杂化和数据不一致等问题。
		state.Write(migration.PredicatesStateKey, &migration.PredicatesStateData{Reference: meta})

		// 并行检查节点是否适合：
		// checkNode:检查节点是否适合谓词的函数。
		checkNode := func(i int) {
			// 我们从上一个调度周期中停止的地方开始检查节点，这是为了确保所有节点在pod中都有相同的检查机会。
			// We check the nodes starting from where we left off in the previous scheduling cycle,
			// this is to make sure all nodes have the same chance of being examined across pods.
			// 这是一个整数，表示在上一个调度周期中检查节点的位置索引。在新的一轮检查中，将从这个索引位置开始。
			nodeInfo := g.nodeInfoSnapshot.NodeInfoList[(g.nextStartNodeIndex+i)%allNodes]

			//fits: pod是否适合这个节点。
			//failedPredicates:表示节点失败。
			//status:节点的状态。
			fits, failedPredicates, status, err := g.podFitsOnNode(
				ctx,
				state,
				pod,
				meta,
				nodeInfo,
				g.alwaysCheckAllPredicates,
			)
			//发送错误并取消上下文，停止进一步的检查
			if err != nil {
				errCh.SendErrorWithCancel(err, cancel)
				return
			}
			if fits {
				// 合适长度，大于AddInt32的话取消
				// 如果适合的节点数量超过了 numNodesToFind，则取消上下文，停止进一步的检查。
				// 否则，将节点添加到 filtered 列表中。
				length := atomic.AddInt32(&filteredLen, 1)
				if length > numNodesToFind {
					cancel()
					atomic.AddInt32(&filteredLen, -1)
				} else {
					filtered[length-1] = nodeInfo.Node()
				}
			} else {
				// 记录状态和失败原因：
				//	•	如果 status 不是成功状态，将其记录到 filteredNodesStatuses 中。
				//	•	如果 failedPredicates 不为空，将其记录到 failedPredicateMap 中。
				predicateResultLock.Lock()
				if !status.IsSuccess() {
					filteredNodesStatuses[nodeInfo.Node().Name] = status
				}
				if len(failedPredicates) != 0 {
					failedPredicateMap[nodeInfo.Node().Name] = failedPredicates
				}
				predicateResultLock.Unlock()
			}
		}

		// Stops searching for more nodes once the configured number of feasible nodes
		// are found.
		// 定义一个 checkNode 函数，用于检查单个节点是否适合。并行地检查所有节点：
		// 并行节点检查
		// workqueue。parallelelizeuntil:并行运行checkNode函数，直到上下文被取消或所有节点都被处理。
		// processedNodes:已处理的节点总数 = 适合调度的节点数量 + 过滤节点状态 + 失败的预选
		//	•filteredLen：表示适合调度的节点数量。
		//	•filteredNodesStatuses：包含所有不适合调度节点的状态信息的映射。
		//	•failedPredicateMap：包含所有不适合调度节点的失败预选条件原因的映射。
		// g.nextstartnodeindex:更新下一个调度周期的索引。
		//  •上次开始调度节点索引 + 当前调度周期中处理的节点数 % allNodes，防止一直重复调度
		workqueue.ParallelizeUntil(ctx, 16, allNodes, checkNode)
		processedNodes := int(filteredLen) + len(filteredNodesStatuses) + len(failedPredicateMap)
		g.nextStartNodeIndex = (g.nextStartNodeIndex + processedNodes) % allNodes

		//filtered:将过滤后的节点切片为实际长度。
		//通过将 filtered 切片到 filteredLen 的长度，可以得到一个实际适合调度的节点列表。
		// 0 -> filteredLen
		// 从错误通道中接收错误信息
		filtered = filtered[:filteredLen]
		if err := errCh.ReceiveError(); err != nil {
			return []*v1.Node{}, FailedPredicateMap{}, framework.NodeToStatusMap{}, err
		}
	}

	// 调用扩展插件过滤节点：
	// 扩展过滤
	// 扩展器:可以过滤节点的附加组件。
	// filtered:适合的节点数
	if len(filtered) > 0 && len(g.extenders) != 0 {
		for _, extender := range g.extenders {
			// 如果扩展器对当前 Pod 不感兴趣，跳过该扩展器。
			if !extender.IsInterested(pod) {
				continue
			}
			// filteredList, failedMap, err:扩展程序过滤的结果。
			//	•调用扩展器的 Filter 方法，传入当前 Pod、过滤后的节点列表以及节点信息快照。
			//	•该方法返回过滤后的节点列表（filteredList）、失败节点映射（failedMap）和错误（err）。
			filteredList, failedMap, err := extender.Filter(pod, filtered, g.nodeInfoSnapshot.NodeInfoMap)
			if err != nil {
				// 如果扩展器返回错误且该错误可忽略，记录警告日志并跳过该扩展器。
				if extender.IsIgnorable() {
					klog.Warningf("Skipping extender %v as it returned error %v and has ignorable flag set",
						extender, err)
					continue
				}
				//如果错误不可忽略，返回空节点列表、空的失败预选条件映射和空的节点状态映射，并附带错误信息。
				return []*v1.Node{}, FailedPredicateMap{}, framework.NodeToStatusMap{}, err
			}

			// failedPredicateMap:更新了来自扩展器的失败。
			// 遍历扩展器返回的失败节点映射，将失败原因更新到 failedPredicateMap。
			for failedNodeName, failedMsg := range failedMap {
				if _, found := failedPredicateMap[failedNodeName]; !found {
					failedPredicateMap[failedNodeName] = []predicates.PredicateFailureReason{}
				}
				failedPredicateMap[failedNodeName] = append(failedPredicateMap[failedNodeName], predicates.NewFailureReason(failedMsg))
			}

			//filtered:使用通过扩展程序过滤的节点进行更新。
			//	•更新 filtered 为扩展器返回的过滤后的节点列表。
			//	•如果过滤后的节点列表为空，停止进一步的扩展器过滤。
			filtered = filteredList
			if len(filtered) == 0 {
				break
			}
		}
	}
	return filtered, failedPredicateMap, filteredNodesStatuses, nil
}

// addNominatedPods adds pods with equal or greater priority which are nominated
// to run on the node given in nodeInfo to meta and nodeInfo. It returns 1) whether
// any pod was added, 2) augmented metadata, 3) augmented CycleState 4) augmented nodeInfo.
func (g *genericScheduler) addNominatedPods(ctx context.Context, pod *v1.Pod, meta predicates.Metadata, state *framework.CycleState,
	nodeInfo *schedulernodeinfo.NodeInfo) (bool, predicates.Metadata,
	*framework.CycleState, *schedulernodeinfo.NodeInfo, error) {
	if g.schedulingQueue == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		// This may happen only in tests.
		return false, meta, state, nodeInfo, nil
	}
	// 这一行代码从调度队列中获取提名的 Pods。具体方法是 NominatedPodsForNode，它接受一个节点名称作为参数，并返回所有提名在该节点上的 Pods 列表。
	//	•g.schedulingQueue：调度队列，用于管理和存储待调度的 Pods。
	//	•NominatedPodsForNode：这是调度队列的一个方法，用于获取提名在指定节点上的 Pods。
	//	•nodeInfo.Node().Name：当前节点的名称，用于查找提名在该节点上的 Pods。
	//在 Kubernetes 中，提名 Pods 是那些已经被某些调度算法认为适合某个节点，但还未真正被调度和绑定到该节点的 Pods。这些 Pods 可能正在等待资源释放或等待其他原因而无法立即调度。
	//	•优化调度过程：通过提名前选取合适的 Pods，可以减少调度冲突和资源竞争，提高调度效率。
	//	•资源利用：提名 Pods 允许调度器暂时预留节点资源，以便在资源释放或其他条件满足时快速调度这些 Pods。
	//	•保证公平性：在多个 Pods 等待同一节点资源时，提名机制可以帮助调度器按照优先级或其他策略决定哪些 Pods 应该优先调度。
	//总结：通过从调度队列中获取提名的 Pods 并进行检查，调度器能够在调度决策中考虑到这些预先选定的 Pods，从而提高调度效率和资源利用率。
	//如果没有提名的 Pods，方法会立即返回，表示当前节点上没有预定的 Pods 需要考虑。
	nominatedPods := g.schedulingQueue.NominatedPodsForNode(nodeInfo.Node().Name)
	if len(nominatedPods) == 0 {
		return false, meta, state, nodeInfo, nil
	}
	nodeInfoOut := nodeInfo.Clone()
	var metaOut predicates.Metadata
	if meta != nil {
		metaOut = meta.ShallowCopy()
	}
	stateOut := state.Clone()
	stateOut.Write(migration.PredicatesStateKey, &migration.PredicatesStateData{Reference: metaOut})
	podsAdded := false
	for _, p := range nominatedPods {
		if podutil.GetPodPriority(p) >= podutil.GetPodPriority(pod) && p.UID != pod.UID {
			nodeInfoOut.AddPod(p)
			if metaOut != nil {
				if err := metaOut.AddPod(p, nodeInfoOut.Node()); err != nil {
					return false, meta, state, nodeInfo, err
				}
			}
			status := g.framework.RunPreFilterExtensionAddPod(ctx, stateOut, pod, p, nodeInfoOut)
			if !status.IsSuccess() {
				return false, meta, state, nodeInfo, status.AsError()
			}
			podsAdded = true
		}
	}
	return podsAdded, metaOut, stateOut, nodeInfoOut, nil
}

// podFitsOnNode checks whether a node given by NodeInfo satisfies the given predicate functions.
// For given pod, podFitsOnNode will check if any equivalent pod exists and try to reuse its cached
// predicate results as possible.
// This function is called from two different places: Schedule and Preempt.
// When it is called from Schedule, we want to test whether the pod is schedulable
// on the node with all the existing pods on the node plus higher and equal priority
// pods nominated to run on the node.
// When it is called from Preempt, we should remove the victims of preemption and
// add the nominated pods. Removal of the victims is done by SelectVictimsOnNode().
// It removes victims from meta and NodeInfo before calling this function.
func (g *genericScheduler) podFitsOnNode(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	meta predicates.Metadata,
	info *schedulernodeinfo.NodeInfo,
	alwaysCheckAllPredicates bool,
) (bool, []predicates.PredicateFailureReason, *framework.Status, error) {
	var failedPredicates []predicates.PredicateFailureReason
	var status *framework.Status

	// In the case where a node delete event is received before all pods on the node are deleted, a NodeInfo may have
	// a nil Node(). This should not be an issue in 1.18 and later, but we need to check it here. See https://github.com/kubernetes/kubernetes/issues/89006
	if info.Node() == nil {
		return false, []predicates.PredicateFailureReason{}, framework.NewStatus(framework.UnschedulableAndUnresolvable, "node being deleted"), nil
	}
	// podsAdded主要用于标识当前是否有提议的pod如果没有提议的pod则就不需要再进行一轮筛选了。
	podsAdded := false
	// We run predicates twice in some cases. If the node has greater or equal priority
	// nominated pods, we run them when those pods are added to meta and nodeInfo.
	// If all predicates succeed in this pass, we run them again when these
	// nominated pods are not added. This second pass is necessary because some
	// predicates such as inter-pod affinity may not pass without the nominated pods.
	// If there are no nominated pods for the node or if the first run of the
	// predicates fail, we don't run the second pass.
	// We consider only equal or higher priority pods in the first pass, because
	// those are the current "pod" must yield to them and not take a space opened
	// for running them. It is ok if the current "pod" take resources freed for
	// lower priority pods.
	// Requiring that the new pod is schedulable in both circumstances ensures that
	// we are making a conservative decision: predicates like resources and inter-pod
	// anti-affinity are more likely to fail when the nominated pods are treated
	// as running, while predicates like pod affinity are more likely to fail when
	// the nominated pods are treated as not running. We can't just assume the
	// nominated pods are running because they are not running right now and in fact,
	// they may end up getting scheduled to a different node.
	//出于某些原因考虑我们需要运行两次predicate. 如果node上有更高或者相同优先级的“指定pods”（这里的“指定pods”指的是通过schedule计算后指定要跑在一个
	//node上但是还未真正运行到那个node上的pods），我们将这些pods加入到meta和nodeInfo后执行一次计算过程。 如果这个过程所有的predicates都成功了，
	//我们再假设这些“指定pods”不会跑到node上再运行一次。第二次计算是必须的，因为有一些predicates比如pod亲和性，也许在“指定pods”没有成功跑到node的情况下会不满足。
	//如果没有“指定pods”或者第一次计算过程失败了，那么第二次计算不会进行。
	//我们在第一次调度的时候只考虑相等或者更高优先级的pods，因为这些pod是当前pod必须“臣服”的，也就是说不能够从这些pod中抢到资源，这些pod不会被当前pod“抢占”；
	//这样当前pod也就能够安心从低优先级的pod手里抢资源了。
	//新pod在上述2种情况下都可调度基于一个保守的假设：资源和pod反亲和性等的predicate在“指定pods”被处理为Running时更容易失败；
	//pod亲和性在“指定pods”被处理为Not Running时更加容易失败。
	//我们不能假设“指定pods”是Running的因为它们当前还没有运行，而且事实上，它们确实有可能最终又被调度到其他node上了。
	//待检查的 Node 是一个即将被抢占的节点，调度器就会对这个Node用同样的 Predicates 算法运行两遍。
	//在第一次循环中，代码的重点是处理与提名 Pod 相关的逻辑。调用 addNominatedPods 方法来检查是否有提名的 Pod 需要被考虑调度，并更新相关的元数据和状态。
	//第二次循环的重点是实际执行预选（predicate）和过滤（filter）逻辑。只有在以下条件下才会进入第二次循环：
	for i := 0; i < 2; i++ {
		metaToUse := meta
		stateToUse := state
		nodeInfoToUse := info
		if i == 0 {
			var err error
			//addNominatedPods：该方法会检查是否有其他优先级相同或更高的 Pod 被提名到当前节点上。如果有，这些 Pod 会被添加到节点信息中，从而在后续的预选阶段考虑这些提名的 Pod。
			//更新元数据和状态：调用 addNominatedPods 后，元数据 metaToUse、状态 stateToUse 和节点信息 nodeInfoToUse 可能会被更新，以反映新的调度环境。
			podsAdded, metaToUse, stateToUse, nodeInfoToUse, err = g.addNominatedPods(ctx, pod, meta, state, info)
			if err != nil {
				return false, []predicates.PredicateFailureReason{}, nil, err
			}
		} else if !podsAdded || len(failedPredicates) != 0 || !status.IsSuccess() {
			break
		}

		// predicates.Ordering()得到的是一个[]string，predicate名字集合
		for _, predicateKey := range predicates.Ordering() {
			var (
				fit     bool
				reasons []predicates.PredicateFailureReason
				err     error
			)

			// 如果predicateFuncs有这个key，则调用这个predicate；也就是说predicateFuncs如果定义了一堆乱七八遭的名字，会被忽略调，因为predicateKey是内置的。
			// 循环执行所有配置的Predicates Polic对应的predicateFunc。
			//		只有全部策略都通过，该node才符合要求
			//		具体的Predicate Policy对应的PredicateFunc都定义在plugin/pkg/scheduler/algorithm/predicates/predicates.Go中
			// scheduler.RegisterFitPredicate这个方法来的
			if predicate, exist := g.predicates[predicateKey]; exist {
				// 真正调用predicate函数了！！！！！！！！！
				fit, reasons, err = predicate(pod, metaToUse, nodeInfoToUse)
				if err != nil {
					return false, []predicates.PredicateFailureReason{}, nil, err
				}

				// 不合适，记录失败原因，
				if !fit {
					// eCache is available and valid, and predicates result is unfit, record the fail reasons
					failedPredicates = append(failedPredicates, reasons...)
					// if alwaysCheckAllPredicates is false, short circuit all predicates when one predicate fails.
					// 	•	alwaysCheckAllPredicates = true：即使某个预选条件失败，调度器也会继续检查剩余的所有预选条件。这意味着调度器会收集所有可能的失败原因，
					//		而不是在遇到第一个失败的条件时就停止检查。这种方式适用于需要全面了解所有可能的调度失败原因的场景，例如调试或详细分析调度失败的原因。
					//	•	alwaysCheckAllPredicates = false：一旦某个预选条件失败，调度器会立即停止检查其他预选条件，并认为该节点不适合调度当前的 Pod。
					//		这种方式可以节省资源和时间，因为不需要检查剩余的预选条件。
					// 	•	alwaysCheckAllPredicates = true 适用于需要详细调试和分析调度失败原因的场景，特别是在开发和测试过程中。
					//	•	alwaysCheckAllPredicates = false 适用于生产环境，尤其是在调度效率和资源利用率优先的场景下。
					if !alwaysCheckAllPredicates {
						klog.V(5).Infoln("since alwaysCheckAllPredicates has not been set, the predicate " +
							"evaluation is short circuited and there are chances " +
							"of other predicates failing as well.")
						break
					}
				}
			}
		}

		// factory文件中CreateFromKeys方法plugins.Append(pluginsForPredicates)添加插件
		// todo 插件
		status = g.framework.RunFilterPlugins(ctx, stateToUse, pod, nodeInfoToUse)
		if !status.IsSuccess() && !status.IsUnschedulable() {
			return false, failedPredicates, status, status.AsError()
		}
	}

	return len(failedPredicates) == 0 && status.IsSuccess(), failedPredicates, status, nil
}

// prioritizeNodes prioritizes the nodes by running the individual priority functions in parallel.
// Each priority function is expected to set a score of 0-10
// 0 is the lowest priority score (least preferred node) and 10 is the highest
// Each priority function can also have its own weight
// The node scores returned by the priority function are multiplied by the weights to get weighted scores
// All scores are finally combined (added) to get the total weighted scores of all nodes
func (g *genericScheduler) prioritizeNodes(
	ctx context.Context,
	state *framework.CycleState, //调度周期状态，包含当前调度周期中的临时数据
	pod *v1.Pod, // pod正在调度的 Pod。
	meta interface{},
	nodes []*v1.Node, // 可供选择的节点列表
) (framework.NodeScoreList, error) {
	// If no priority configs are provided, then all nodes will have a score of one.
	// This is required to generate the priority list in the required format
	// 1、无评分插件情况处理：
	// 首先检查是否没有配置任何评分插件 (g.prioritizers 为空且 g.extenders 为空且没有其他评分插件)。
	// 如果没有评分插件，则所有节点的得分默认为 1，并返回结果。
	if len(g.prioritizers) == 0 && len(g.extenders) == 0 && !g.framework.HasScorePlugins() {
		result := make(framework.NodeScoreList, 0, len(nodes))
		for i := range nodes {
			result = append(result, framework.NodeScore{
				Name:  nodes[i].Name,
				Score: 1,
			})
		}
		return result, nil
	}

	// 2、并发处理节点评分计算：
	// 初始化并发控制相关的 sync.Mutex、sync.WaitGroup 和错误集合 errs。
	// 使用 workqueue.ParallelizeUntil 并行处理节点列表，每个节点并发计算其在各个评分器下的得分。
	// 每个评分器的结果存储在 results 数组中，每个元素是一个 framework.NodeScoreList，表示每个节点的评分结果。
	// 这里只是简单定义3个变量，一把锁，一个并发等待相关的wg，一个错误集合errs；
	var (
		mu   = sync.Mutex{}
		wg   = sync.WaitGroup{}
		errs []error
	)
	// 这里定义了一个appendError小函数，逻辑很简单，并发场景下将错误信息收集到errs中；
	appendError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}

	results := make([]framework.NodeScoreList, len(g.prioritizers))

	for i := range g.prioritizers {
		results[i] = make(framework.NodeScoreList, len(nodes))
	}

	workqueue.ParallelizeUntil(context.TODO(), 16, len(nodes), func(index int) {
		nodeInfo := g.nodeInfoSnapshot.NodeInfoMap[nodes[index].Name]
		for i := range g.prioritizers {
			var err error
			results[i][index], err = g.prioritizers[i].Map(pod, meta, nodeInfo)
			if err != nil {
				appendError(err)
				results[i][index].Name = nodes[index].Name
			}
		}
	})

	// 3、评分器 Reduce 操作：
	//
	//对每个评分器调用其 Reduce 方法，将之前计算的各个节点的评分结果进行汇总处理。
	//使用并发 goroutine 处理 Reduce 操作，最后将每个评分器处理后的结果更新到 results 中。
	for i := range g.prioritizers {
		if g.prioritizers[i].Reduce == nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			metrics.SchedulerGoroutines.WithLabelValues("prioritizing_mapreduce").Inc()
			defer func() {
				metrics.SchedulerGoroutines.WithLabelValues("prioritizing_mapreduce").Dec()
				wg.Done()
			}()
			if err := g.prioritizers[index].Reduce(pod, meta, g.nodeInfoSnapshot, results[index]); err != nil {
				appendError(err)
			}
			if klog.V(10) {
				for _, hostPriority := range results[index] {
					klog.Infof("%v -> %v: %v, Score: (%d)", util.GetPodFullName(pod), hostPriority.Name, g.prioritizers[index].Name, hostPriority.Score)
				}
			}
		}(i)
	}
	// Wait for all computations to be finished.
	wg.Wait()
	if len(errs) != 0 {
		return framework.NodeScoreList{}, errors.NewAggregate(errs)
	}

	// 4、Run the Score plugins.
	// 运行评分插件：
	//
	//调用 g.framework.RunScorePlugins 方法，运行评分插件来获取额外的节点评分信息。
	//将评分插件的得分合并到最终的评分结果中。
	state.Write(migration.PrioritiesStateKey, &migration.PrioritiesStateData{Reference: meta})
	scoresMap, scoreStatus := g.framework.RunScorePlugins(ctx, state, pod, nodes)
	if !scoreStatus.IsSuccess() {
		return framework.NodeScoreList{}, scoreStatus.AsError()
	}

	// 5、计算最终节点得分：
	//
	// 根据各个评分器和评分插件的得分，计算每个节点的最终得分，并将结果保存在 result 中。
	// Summarize all scores.
	// 这个result和前面的results类似，result用于存储每个node的Score，到这里已经没有必要区分算法了；
	result := make(framework.NodeScoreList, 0, len(nodes))

	for i := range nodes {
		// 先在result中塞满所有node的Name，Score初始化为0
		result = append(result, framework.NodeScore{Name: nodes[i].Name, Score: 0})
		// 执行了多少个priorityConfig就有多少个Score，所以这里遍历len(priorityConfigs)次
		for j := range g.prioritizers {
			// 每个算法对应第i个node的结果分值加权后累加；
			result[i].Score += results[j][i].Score * g.prioritizers[j].Weight
		}

		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	// 6、处理 Extender 扩展插件：
	//
	//如果存在扩展插件 (g.extenders 不为空)，则对每个符合条件的扩展插件进行处理。
	//扩展插件可能会返回额外的节点优先级，将其加权合并到节点的最终得分中。
	if len(g.extenders) != 0 && nodes != nil {
		combinedScores := make(map[string]int64, len(g.nodeInfoSnapshot.NodeInfoList))
		for i := range g.extenders {
			if !g.extenders[i].IsInterested(pod) {
				continue
			}
			wg.Add(1)
			go func(extIndex int) {
				metrics.SchedulerGoroutines.WithLabelValues("prioritizing_extender").Inc()
				defer func() {
					metrics.SchedulerGoroutines.WithLabelValues("prioritizing_extender").Dec()
					wg.Done()
				}()
				prioritizedList, weight, err := g.extenders[extIndex].Prioritize(pod, nodes)
				if err != nil {
					// Prioritization errors from extender can be ignored, let k8s/other extenders determine the priorities
					return
				}
				mu.Lock()
				for i := range *prioritizedList {
					host, score := (*prioritizedList)[i].Host, (*prioritizedList)[i].Score
					if klog.V(10) {
						klog.Infof("%v -> %v: %v, Score: (%d)", util.GetPodFullName(pod), host, g.extenders[extIndex].Name(), score)
					}
					combinedScores[host] += score * weight
				}
				mu.Unlock()
			}(i)
		}
		// wait for all go routines to finish
		wg.Wait()
		for i := range result {
			// MaxExtenderPriority may diverge from the max priority used in the scheduler and defined by MaxNodeScore,
			// therefore we need to scale the score returned by extenders to the score range used by the scheduler.
			result[i].Score += combinedScores[result[i].Name] * (framework.MaxNodeScore / extenderv1.MaxExtenderPriority)
		}
	}

	if klog.V(10) {
		for i := range result {
			klog.Infof("Host %s => Score %d", result[i].Name, result[i].Score)
		}
	}
	return result, nil
}

// pickOneNodeForPreemption chooses one node among the given nodes. It assumes
// pods in each map entry are ordered by decreasing priority.
// It picks a node based on the following criteria:
// 1. A node with minimum number of PDB violations.
// 2. A node with minimum highest priority victim is picked.
// 3. Ties are broken by sum of priorities of all victims.
// 4. If there are still ties, node with the minimum number of victims is picked.
// 5. If there are still ties, node with the latest start time of all highest priority victims is picked.
// 6. If there are still ties, the first such node is picked (sort of randomly).
// The 'minNodes1' and 'minNodes2' are being reused here to save the memory
// allocation and garbage collection time.
func pickOneNodeForPreemption(nodesToVictims map[*v1.Node]*extenderv1.Victims) *v1.Node {
	if len(nodesToVictims) == 0 {
		return nil
	}
	// 初始化为最大值
	minNumPDBViolatingPods := int64(math.MaxInt32)
	var minNodes1 []*v1.Node
	lenNodes1 := 0
	// 这个循环要找到 PDBViolatingPods 最少的 node，如果有多个，就全部存在 minNodes1 中
	for node, victims := range nodesToVictims {
		if len(victims.Pods) == 0 {
			// We found a node that doesn't need any preemption. Return it!
			// This should happen rarely when one or more pods are terminated between
			// the time that scheduler tries to schedule the pod and the time that
			// preemption logic tries to find nodes for preemption.
			return node
		}
		numPDBViolatingPods := victims.NumPDBViolations
		if numPDBViolatingPods < minNumPDBViolatingPods {
			minNumPDBViolatingPods = numPDBViolatingPods
			minNodes1 = nil
			lenNodes1 = 0
		}
		if numPDBViolatingPods == minNumPDBViolatingPods {
			minNodes1 = append(minNodes1, node)
			lenNodes1++
		}
	}
	// 如果只找到1个 PDB violations 最少的 node，那就直接返回这个 node 就 ok 了
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are more than one node with minimum number PDB violating pods. Find
	// the one with minimum highest priority victim.
	// 还剩下多个 node，那就寻找 highest priority victim 最小的 node
	minHighestPriority := int32(math.MaxInt32)
	var minNodes2 = make([]*v1.Node, lenNodes1)
	lenNodes2 := 0
	// 这个循环要做的事情是看2个 node 上 victims 中最高优先级的 pod 哪个优先级更高
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		victims := nodesToVictims[node]
		// highestPodPriority is the highest priority among the victims on this node.
		highestPodPriority := podutil.GetPodPriority(victims.Pods[0])
		if highestPodPriority < minHighestPriority {
			minHighestPriority = highestPodPriority
			lenNodes2 = 0
		}
		if highestPodPriority == minHighestPriority {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	// 发现只有1个，那就直接返回
	if lenNodes2 == 1 {
		return minNodes2[0]
	}

	// There are a few nodes with minimum highest priority victim. Find the
	// smallest sum of priorities.
	// 这时候还没有抉择出一个 node，那就开始计算优先级总和了，看哪个更低
	minSumPriorities := int64(math.MaxInt64)
	lenNodes1 = 0
	for i := 0; i < lenNodes2; i++ {
		var sumPriorities int64
		node := minNodes2[i]
		for _, pod := range nodesToVictims[node].Pods {
			// We add MaxInt32+1 to all priorities to make all of them >= 0. This is
			// needed so that a node with a few pods with negative priority is not
			// picked over a node with a smaller number of pods with the same negative
			// priority (and similar scenarios).
			// 这里的累加考虑到了先把优先级搞成正数。不然会出现1个 node 上有1优先级为 -3 的 pod，
			// 另外一个 node 上有2个优先级为 -3 的 pod，结果 -3>-6，有2个 pod 的 node 反而被认为总优先级更低！
			sumPriorities += int64(podutil.GetPodPriority(pod)) + int64(math.MaxInt32+1)
		}
		if sumPriorities < minSumPriorities {
			minSumPriorities = sumPriorities
			lenNodes1 = 0
		}
		if sumPriorities == minSumPriorities {
			minNodes1[lenNodes1] = node
			lenNodes1++
		}
	}
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are a few nodes with minimum highest priority victim and sum of priorities.
	// Find one with the minimum number of pods.
	// 还是没有分出胜负，于是开始用 pod 总数做比较
	minNumPods := math.MaxInt32
	lenNodes2 = 0
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		numPods := len(nodesToVictims[node].Pods)
		if numPods < minNumPods {
			minNumPods = numPods
			lenNodes2 = 0
		}
		if numPods == minNumPods {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	if lenNodes2 == 1 {
		return minNodes2[0]
	}

	// There are a few nodes with same number of pods.
	// Find the node that satisfies latest(earliestStartTime(all highest-priority pods on node))
	latestStartTime := util.GetEarliestPodStartTime(nodesToVictims[minNodes2[0]])
	if latestStartTime == nil {
		// If the earliest start time of all pods on the 1st node is nil, just return it,
		// which is not expected to happen.
		klog.Errorf("earliestStartTime is nil for node %s. Should not reach here.", minNodes2[0])
		return minNodes2[0]
	}
	nodeToReturn := minNodes2[0]
	for i := 1; i < lenNodes2; i++ {
		node := minNodes2[i]
		// Get earliest start time of all pods on the current node.
		earliestStartTimeOnNode := util.GetEarliestPodStartTime(nodesToVictims[node])
		if earliestStartTimeOnNode == nil {
			klog.Errorf("earliestStartTime is nil for node %s. Should not reach here.", node)
			continue
		}
		if earliestStartTimeOnNode.After(latestStartTime.Time) {
			latestStartTime = earliestStartTimeOnNode
			nodeToReturn = node
		}
	}

	return nodeToReturn
}

// selectNodesForPreemption finds all the nodes with possible victims for
// preemption in parallel.
func (g *genericScheduler) selectNodesForPreemption(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	potentialNodes []*v1.Node,
	pdbs []*policy.PodDisruptionBudget,
) (map[*v1.Node]*extenderv1.Victims, error) {
	nodeToVictims := map[*v1.Node]*extenderv1.Victims{}
	var resultLock sync.Mutex

	// We can use the same metadata producer for all nodes.
	meta := g.predicateMetaProducer(pod, g.nodeInfoSnapshot)
	checkNode := func(i int) {
		nodeName := potentialNodes[i].Name
		if g.nodeInfoSnapshot.NodeInfoMap[nodeName] == nil {
			return
		}
		nodeInfoCopy := g.nodeInfoSnapshot.NodeInfoMap[nodeName].Clone()
		var metaCopy predicates.Metadata
		if meta != nil {
			metaCopy = meta.ShallowCopy()
		}
		stateCopy := state.Clone()
		stateCopy.Write(migration.PredicatesStateKey, &migration.PredicatesStateData{Reference: metaCopy})
		// 这里有一个子过程调用，下面单独介绍
		pods, numPDBViolations, fits := g.selectVictimsOnNode(ctx, stateCopy, pod, metaCopy, nodeInfoCopy, pdbs)
		if fits {
			resultLock.Lock()
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			// 如果 fit，就添加到 nodeToVictims 中，也就是最后的返回值
			nodeToVictims[potentialNodes[i]] = &victims
			resultLock.Unlock()
		}
	}
	workqueue.ParallelizeUntil(context.TODO(), 16, len(potentialNodes), checkNode)
	return nodeToVictims, nil
}

// filterPodsWithPDBViolation groups the given "pods" into two groups of "violatingPods"
// and "nonViolatingPods" based on whether their PDBs will be violated if they are
// preempted.
// This function is stable and does not change the order of received pods. So, if it
// receives a sorted list, grouping will preserve the order of the input list.
func filterPodsWithPDBViolation(pods []*v1.Pod, pdbs []*policy.PodDisruptionBudget) (violatingPods, nonViolatingPods []*v1.Pod) {
	for _, obj := range pods {
		pod := obj
		pdbForPodIsViolated := false
		// A pod with no labels will not match any PDB. So, no need to check.
		if len(pod.Labels) != 0 {
			for _, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					continue
				}
				// A PDB with a nil or empty selector matches nothing.
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}
				// We have found a matching PDB.
				if pdb.Status.PodDisruptionsAllowed <= 0 {
					pdbForPodIsViolated = true
					break
				}
			}
		}
		if pdbForPodIsViolated {
			violatingPods = append(violatingPods, pod)
		} else {
			nonViolatingPods = append(nonViolatingPods, pod)
		}
	}
	return violatingPods, nonViolatingPods
}

// selectVictimsOnNode finds minimum set of pods on the given node that should
// be preempted in order to make enough room for "pod" to be scheduled. The
// minimum set selected is subject to the constraint that a higher-priority pod
// is never preempted when a lower-priority pod could be (higher/lower relative
// to one another, not relative to the preemptor "pod").
// The algorithm first checks if the pod can be scheduled on the node when all the
// lower priority pods are gone. If so, it sorts all the lower priority pods by
// their priority and then puts them into two groups of those whose PodDisruptionBudget
// will be violated if preempted and other non-violating pods. Both groups are
// sorted by priority. It first tries to reprieve as many PDB violating pods as
// possible and then does them same for non-PDB-violating pods while checking
// that the "pod" can still fit on the node.
// NOTE: This function assumes that it is never called if "pod" cannot be scheduled
// due to pod affinity, node affinity, or node anti-affinity reasons. None of
// these predicates can be satisfied by removing more pods from the node.
func (g *genericScheduler) selectVictimsOnNode(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	meta predicates.Metadata,
	nodeInfo *schedulernodeinfo.NodeInfo,
	pdbs []*policy.PodDisruptionBudget,
) ([]*v1.Pod, int, bool) {
	var potentialVictims []*v1.Pod

	// 定义删除 pod 函数
	removePod := func(rp *v1.Pod) error {
		if err := nodeInfo.RemovePod(rp); err != nil {
			return err
		}
		if meta != nil {
			if err := meta.RemovePod(rp, nodeInfo.Node()); err != nil {
				return err
			}
		}
		status := g.framework.RunPreFilterExtensionRemovePod(ctx, state, pod, rp, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}

	// 定义添加 pod 函数
	addPod := func(ap *v1.Pod) error {
		nodeInfo.AddPod(ap)
		if meta != nil {
			if err := meta.AddPod(ap, nodeInfo.Node()); err != nil {
				return err
			}
		}
		status := g.framework.RunPreFilterExtensionAddPod(ctx, state, pod, ap, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	// As the first step, remove all the lower priority pods from the node and
	// check if the given pod can be scheduled.
	// 删除所有的低优先级 pod 看是不是能够满足调度需求了
	podPriority := podutil.GetPodPriority(pod)
	for _, p := range nodeInfo.Pods() {
		if podutil.GetPodPriority(p) < podPriority {
			potentialVictims = append(potentialVictims, p)
			if err := removePod(p); err != nil {
				return nil, 0, false
			}
		}
	}

	// If the new pod does not fit after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption. The only
	// condition that we could check is if the "pod" is failing to schedule due to
	// inter-pod affinity to one or more victims, but we have decided not to
	// support this case for performance reasons. Having affinity to lower
	// priority pods is not a recommended configuration anyway.
	// 如果删除了所有的低优先级 pods 之后还不能跑这个新 pod，那么差不多就可以判断这个 node 不适合 preemption 了，
	// 还有一点点需要考虑的是这个“pod”的不 fit 的原因是由于 pod affinity 不满足了。 后续可能会增加当前 pod 和低优先级 pod 之间的 优先级检查。
	// 这个函数调用其实就是之前讲到过的预选函数的调用逻辑，判断这个 pod 是否合适跑在这个 node 上。
	if fits, _, _, err := g.podFitsOnNode(ctx, state, pod, meta, nodeInfo, false); !fits {
		if err != nil {
			klog.Warningf("Encountered error while selecting victims on node %v: %v", nodeInfo.Node().Name, err)
		}

		return nil, 0, false
	}
	var victims []*v1.Pod
	numViolatingVictim := 0
	sort.Slice(potentialVictims, func(i, j int) bool { return util.MoreImportantPod(potentialVictims[i], potentialVictims[j]) })
	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest priority victims.
	// 尝试尽量多地释放这些 pods，也就是说能少杀就少杀；
	// 这里先从 PDB violating victims 中释放，再从 PDB non-violating victims 中释放；
	// 两个组都是从高优先级的 pod 开始释放。
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims, pdbs)

	// 释放 pods 的函数，来一个放一个
	reprievePod := func(p *v1.Pod) (bool, error) {
		if err := addPod(p); err != nil {
			return false, err
		}
		fits, _, _, _ := g.podFitsOnNode(ctx, state, pod, meta, nodeInfo, false)
		if !fits {
			if err := removePod(p); err != nil {
				return false, err
			}
			victims = append(victims, p)
			klog.V(5).Infof("Pod %v/%v is a potential preemption victim on node %v.", p.Namespace, p.Name, nodeInfo.Node().Name)
		}
		return fits, nil
	}

	// 释放 violatingVictims 中元素的同时会记录放了多少个
	for _, p := range violatingVictims {
		if fits, err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, 0, false
		} else if !fits {
			numViolatingVictim++
		}
	}

	// Now we try to reprieve non-violating victims.
	// 开始释放 non-violating victims.
	for _, p := range nonViolatingVictims {
		if _, err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, 0, false
		}
	}
	return victims, numViolatingVictim, true
}

// nodesWherePreemptionMightHelp returns a list of nodes with failed predicates
// that may be satisfied by removing pods from the node.
func nodesWherePreemptionMightHelp(nodeNameToInfo map[string]*schedulernodeinfo.NodeInfo, fitErr *FitError) []*v1.Node {
	// 潜力 node， 用于存储返回值的 slice
	potentialNodes := []*v1.Node{}
	for name, node := range nodeNameToInfo {
		if fitErr.FilteredNodesStatuses[name].Code() == framework.UnschedulableAndUnresolvable {
			continue
		}
		failedPredicates := fitErr.FailedPredicates[name]

		// If we assume that scheduler looks at all nodes and populates the failedPredicateMap
		// (which is the case today), the !found case should never happen, but we'd prefer
		// to rely less on such assumptions in the code when checking does not impose
		// significant overhead.
		// Also, we currently assume all failures returned by extender as resolvable.
		if predicates.UnresolvablePredicateExists(failedPredicates) == nil {
			klog.V(3).Infof("Node %v is a potential node for preemption.", name)
			potentialNodes = append(potentialNodes, node.Node())
		}
	}
	return potentialNodes
}

// podEligibleToPreemptOthers determines whether this pod should be considered
// for preempting other pods or not. If this pod has already preempted other
// pods and those are in their graceful termination period, it shouldn't be
// considered for preemption.
// We look at the node that is nominated for this pod and as long as there are
// terminating pods on the node, we don't consider this for preempting more pods.
func podEligibleToPreemptOthers(pod *v1.Pod, nodeNameToInfo map[string]*schedulernodeinfo.NodeInfo, enableNonPreempting bool) bool {
	if enableNonPreempting && pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		klog.V(5).Infof("Pod %v/%v is not eligible for preemption because it has a preemptionPolicy of %v", pod.Namespace, pod.Name, v1.PreemptNever)
		return false
	}
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		// 被 nominate 的 node
		if nodeInfo, found := nodeNameToInfo[nomNodeName]; found {
			podPriority := podutil.GetPodPriority(pod)
			for _, p := range nodeInfo.Pods() {
				// 有低优先级的 pod 处于删除中状态，就返回 false
				if p.DeletionTimestamp != nil && podutil.GetPodPriority(p) < podPriority {
					// There is a terminating pod on the nominated node.
					return false
				}
			}
		}
	}
	return true
}

// podPassesBasicChecks makes sanity checks on the pod if it can be scheduled.
func podPassesBasicChecks(pod *v1.Pod, pvcLister corelisters.PersistentVolumeClaimLister) error {
	// Check PVCs used by the pod
	namespace := pod.Namespace
	manifest := &(pod.Spec)
	for i := range manifest.Volumes {
		volume := &manifest.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			// Volume is not a PVC, ignore
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvc, err := pvcLister.PersistentVolumeClaims(namespace).Get(pvcName)
		if err != nil {
			// The error has already enough context ("persistentvolumeclaim "myclaim" not found")
			return err
		}

		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}
	}

	return nil
}

// NewGenericScheduler creates a genericScheduler object.
func NewGenericScheduler(
	cache internalcache.Cache,
	podQueue internalqueue.SchedulingQueue,
	predicates map[string]predicates.FitPredicate,
	predicateMetaProducer predicates.MetadataProducer,
	prioritizers []priorities.PriorityConfig,
	priorityMetaProducer priorities.MetadataProducer,
	nodeInfoSnapshot *nodeinfosnapshot.Snapshot,
	framework framework.Framework,
	extenders []algorithm.SchedulerExtender,
	volumeBinder *volumebinder.VolumeBinder,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pdbLister policylisters.PodDisruptionBudgetLister,
	alwaysCheckAllPredicates bool,
	disablePreemption bool,
	percentageOfNodesToScore int32,
	enableNonPreempting bool) ScheduleAlgorithm {
	return &genericScheduler{
		cache:                    cache,
		schedulingQueue:          podQueue,
		predicates:               predicates,
		predicateMetaProducer:    predicateMetaProducer,
		prioritizers:             prioritizers,
		priorityMetaProducer:     priorityMetaProducer,
		framework:                framework,
		extenders:                extenders,
		nodeInfoSnapshot:         nodeInfoSnapshot,
		volumeBinder:             volumeBinder,
		pvcLister:                pvcLister,
		pdbLister:                pdbLister,
		alwaysCheckAllPredicates: alwaysCheckAllPredicates,
		disablePreemption:        disablePreemption,
		percentageOfNodesToScore: percentageOfNodesToScore,
		enableNonPreempting:      enableNonPreempting,
	}
}
