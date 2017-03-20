package hawkular

import (
	"strings"

	"fmt"

	"bytes"

	"github.com/hawkular/hawkular-client-go/metrics"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/pkg/api"
	"k8s.io/custom-metrics-boilerplate/pkg/provider"
	"k8s.io/metrics/pkg/apis/custom_metrics"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	resourceTag               string = "type"
	descriptorTag             string = "descriptor_name"
	podLabelsDefaultTagPrefix string = "labels."
)

type HawkularProvider struct {
	client *metrics.Client

	// TODO Mutex? This could take a while with current API
	cachedMetrics []provider.MetricInfo
	labelPrefix   string
}

func New() (*provider.CustomMetricsProvider, error) {
	// Parse parameters first

	p := metrics.Parameters{
		Tenant: "heapster", // TODO We need default tenant configurable
		// Url:         h.uri.String(),
		// Concurrency: concurrencyDefault,
	}

	c, err := metrics.NewHawkularClient(p)
	if err != nil {
		return nil, err
	}

	p := &HawkularProvider{
		client:        c,
		cachedMetrics: []provider.MetricInfo{},
		labelPrefix:   podLabelsDefaultTagPrefix, // Make configurable
	}

	// Update the cache on the background
	go func() {
		p.ListAllMetrics()
	}()

	return p, nil
}

/*
{
  "tags": {
    "resource_id_description": "Identifier(s) specific to a metric",
    "labels": "deployment:router-1,deploymentconfig:router,router:router",
    "nodename": "10.16.89.58",
    "resource_id": "/",
    "type": "pod_container",
    "hostname": "10.16.89.58",
    "container_base_image": "openshift/origin-haproxy-router:latest",
    "namespace_id": "ef59e1bb-ea0d-11e6-9dc8-a0d3c1f893c0",
    "descriptor_name": "filesystem/usage",
    "pod_name": "router-1-bwvdt",
    "container_name": "router",
    "units": "bytes",
    "host_id": "10.16.89.58",
    "group_id": "router/filesystem/usage",
    "pod_namespace": "default",
    "pod_id": "fe42efce-ea0d-11e6-9dc8-a0d3c1f893c0",
    "namespace_name": "default"
  },
  "tenantId": "default",
  "dataRetention": 7,
  "minTimestamp": 1488967200000,
  "type": "gauge",
  "id": "router/fe42efce-ea0d-11e6-9dc8-a0d3c1f893c0/filesystem/usage//",
  "maxTimestamp": 1489581220000
}
*/

/*
GET /apis/custom-metrics/v1alpha1/namespaces/webapp/ingress.extensions//hits-per-second?labelSelector=app%3Dfrontend`

---

Verb: GET
Namespace: webapp
APIGroup: custom-metrics
APIVersion: v1alpha1
Resource: ingress.extensions
Subresource: hits-per-second
Name: ResourceAll(*)
*/

/*
Extracting:

APIGroup -> custom-metrics
Resource -> type?
label_selector -> labels (and/or/extracted/labels)
Name <-> resource_id ? Not likely. group_id perhaps without the first part?

descriptor_name <-> metricName (yep .. tätä pitää varmaan "ListAllMetrics()" kohdassa hakea, eli tag values tälle)

type = {pod, pod_container} etc

If namespace is used and "metrics"" is the ResourceName, then name is the resource_id?

*/

func valueConverter(dp *metrics.Datapoint, t metrics.MetricType, unit string) (*resource.Quantity, error) {
	var int64 val
	// TODO Reoccurring theme.. this should be handled in the metrics client
	switch t {
	case metrics.Counter:
		val = dp.Value.(int64)
	case metrics.Gauge:
		f := dp.Value.(float64)
		val = int64(f) // We loose some precision, but that's acceptable in this case
	}

	// TODO Use unit (such as bytes) for the correct measurement scaling
	return *resource.NewMilliQuantity(value*100, resource.DecimalSI), nil
}

func definitionToMetricValue(md *metrics.MetricDefinition, groupResource schema.GroupResource) (*custom_metrics.MetricValue, error) {
	// Fetch async
	dp, err := h.client.ReadRaw(md.Type, md.ID, metrics.LimitFilter(1), metrics.OrderFilter(metrics.DESC))
	if err != nil {
		return nil, err
	}

	if len(dp) > 0 {
		// Fetch async, cache the result
		group, err := api.Registry.Group(groupResource.Group)
		if err != nil {
			return nil, err
		}

		kind, err := api.Registry.RESTMapper().KindFor(groupResource.WithVersion(group.GroupVersion.Version))
		if err != nil {
			return nil, err
		}

		val, err := valueConverter(dp[0], d.Type, d.Tags["units"])
		if err != nil {
			return nil, err
		}

		custom_metrics.MetricValue{
			DescribedObject: api.ObjectReference{
				APIVersion: groupResource.Group + "/" + runtime.APIVersionInternal,
				Kind:       kind.Kind,
				Name:       md.Tags[descriptorTag],
				Namespace:  md.Tenant, // Or do we need the id instead of the name?
			},
			Timestamp: metav1.Time{dp[0].Timestamp},
			Value:     val,
		}
	}

}

func tagsQueryBuilder(groupResource schema.GroupResource, selector labels.Selector, metricName string) metrics.TagsQueryFilter {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s = %s AND %s = %s", descriptorTag, metricName, resourceTag, groupResource.Resource))

	if !selector.Empty() {
		buffer.WriteString(" AND ")
		buffer.WriteString(h.labelSelectorToHawkularTagsQuery(selector))
	}

	return metrics.TagsQueryFilter(buffer.String())
}

func (h *HawkularProvider) GetRootScopedMetricByName(groupResource schema.GroupResource, name string, metricName string) (*custom_metrics.MetricValue, error) {

}

// GetRootScopedMetricBySelector fetches a particular metric for a set of root-scoped objects
// matching the given label selector.
func (h *HawkularProvider) GetRootScopedMetricBySelector(groupResource schema.GroupResource, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// We fetch definitions first to be able to do manual label selection for procedures we don't support
	defs, err := h.client.Definitions(tagsQueryBuilder(groupResource, selector, metricName))
	if err != nil {
		return nil, err
	}

	// TODO Now filter here if there was some integer label selectors..

	// TODO Modify this to fetch all the metrics with a single call to Hawkular-Metrics once we know the real split between rate selection & gauge selection
	// We can request rates for each metric if we wanted, but when is it necessary? There's no specifications or metadata to make that selection.
	// For example, are counter types always with rate and gauge with always exact? There are situations where "gauges" can actually give some rate, such as
	// filesystem usage growth rate (while filesystem usage is actually a gauge metric)

	mv := make([]custom_metrics.MetricValue, 0, len(defs))

	// Fetch the requested values
	// TODO Turn into its own function to allow reuse
	for _, d := range defs {
		mv = append(mv, definitionToMetricValue(d, groupResource))
	}

	return &custom_metrics.MetricValueList{
		Items: mv,
	}, nil
}

// GetNamespacedMetricByName fetches a particular metric for a particular namespaced object.
func (h *HawkularProvider) GetNamespacedMetricByName(groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValue, error) {
	// TODO How are we going to select the metricName here properly? How are we storing the metric?
	// if name is * we do something and if "metrics" something else.. or ?
}

// GetNamespacedMetricBySelector fetches a particular metric for a set of namespaced objects
// matching the given label selector.
func (h *HawkularProvider) GetNamespacedMetricBySelector(groupResource schema.GroupResource, namespace string, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// We fetch definitions first to be able to do manual label selection for procedures we don't support
	defs, err := h.client.Definitions(metrics.Tenant(namespace), tagsQueryBuilder(groupResource, selector, metricName))
	if err != nil {
		return nil, err
	}

	// TODO Refactor following to reusable function (copy from other methods) as soon as name versioned ones are implemented

	mv := make([]custom_metrics.MetricValue, 0, len(defs))

	// Fetch the requested values
	// TODO Turn into its own function to allow reuse
	for _, d := range defs {
		mv = append(mv, definitionToMetricValue(d, groupResource))
	}

	return &custom_metrics.MetricValueList{
		Items: mv,
	}, nil
}

// ListAllMetrics provides a list of all available metrics at
// the current time.  Note that this is not allowed to return
// an error, so it is reccomended that implementors cache and
// periodically update this list, instead of querying every time.
func (h *HawkularProvider) ListAllMetrics() []provider.MetricInfo {
	q := make(map[string]string)

	q[resourceTag] = "*"

	types, err := h.client.TagValues(q)
	if err != nil {
		// Return cached version
		return h.cachedMetrics
	}

	// Seek all the available metricNames for each type
	q[descriptorTag] = "*"

	providers := make([]provider.MetricInfo, 0, len(types))
	for _, v := range types[resourceTag] {
		del(q, resourceTag)
		q[resourceTag] = v
		names, err := h.client.TagValues(q)
		if err != nil {
			// Return cached version
			return h.cachedMetrics
		}
		for _, name := range names[descriptorTag] {
			providers = append(providers, provider.MetricInfo{
				GroupResource: schema.GroupResource{Group: "", Resource: v},
				Metric:        name,
				Namespaced:    true, // Everything in Hawkular+Openshift is namespaced
			})
		}
	}

	h.cachedMetrics = providers
	return providers
}

func quoteValues(vals []string) string {
	quotedVals := make([]string, 0, len(vals))
	for _, val := range vals {
		quotedVals = append(quotedVals, fmt.Sprintf("'%s'", val))
	}
	return strings.Join(quotedVals, ",")
}

// labelSelectorToHawkularTagsQuery transforms labels.Selector to Hawkular's TagQL (new style)
func (h *HawkularProvider) labelSelectorToHawkularTagsQuery(selector labels.Selector) string {
	reqs := selector.Requirements()
	queries := make([]string, 0, len(reqs))
	// postReq := make([]selector.Requirement)

	for _, req := range reqs {
		key := fmt.Sprintf("%s%s", h.labelPrefix, req.Key)
		switch req.operator {
		case selection.In:
			queries = append(queries, fmt.Sprintf("%s IN [%s]", key, quoteValues(req.Values)))
		case selection.NotIn:
			queries = append(queries, fmt.Sprintf("%s NOT IN [%s]", key, quoteValues(req.Values)))
		case selection.Equals, selection.DoubleEquals:
			// More than one value would make no sense, use IN
			queries = append(queries, fmt.Sprintf("%s = '%s'", key, req.Values[0]))
		case selection.DoesNotExist:
			queries = append(queries, fmt.Sprintf("NOT %s", key))
		case selection.NotEquals:
			// More than one value would make no sense, use NOT IN
			queries = append(queries, fmt.Sprintf("%s != '%s'", key, reasdsaq.Values[0]))
		case selection.Exists:
			queries = append(queries, req.Key)
		case selection.GreaterThan, selection.LessThan:
			// These are not supported (Hawkular tags are string typed), need manual filtering
			// We could return a function for "post-processing" that would be run after fetching the definitions?
			// postReq = append(postReq, req)
		}
	}

	return strings.Join(queries, " AND ")
}
