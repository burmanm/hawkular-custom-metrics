package hawkular

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"

	"github.com/hawkular/hawkular-client-go/metrics"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/custom-metrics-boilerplate/pkg/provider"
	"k8s.io/metrics/pkg/apis/custom_metrics"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
)

const (
	resourceTag               string = "type"
	descriptorTag             string = "descriptor_name"
	podLabelsDefaultTagPrefix string = "labels."
	defaultServiceAccountFile string = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

type hawkularProvider struct {
	client *metrics.Client

	// TODO Mutex? This could take a while with current API
	cachedMetrics []provider.MetricInfo
	labelPrefix   string
}

func NewHawkularCustomMetricsProvider(client coreclient.CoreV1Interface, uri string) (provider.CustomMetricsProvider, error) {
	// Parse parameters first, same uri format as with Heapster & InitialResources
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	p := metrics.Parameters{
		Tenant: "heapster", // TODO We need default tenant configurable
		Url:    u.String(),
		// Concurrency: concurrencyDefault,
	}

	opts := u.Query()

	if v, found := opts["useServiceAccount"]; found {
		if b, _ := strconv.ParseBool(v[0]); b {
			// If a readable service account token exists, then use it
			if contents, err := ioutil.ReadFile(defaultServiceAccountFile); err == nil {
				p.Token = string(contents)
			}
		}
	}

	// Authentication / Authorization parameters
	tC := &tls.Config{}

	if v, found := opts["auth"]; found {
		if _, f := opts["caCert"]; f {
			return nil, fmt.Errorf("Both auth and caCert files provided, combination is not supported")
		}
		if len(v[0]) > 0 {
			// Authfile
			kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{
				ExplicitPath: v[0]},
				&clientcmd.ConfigOverrides{}).ClientConfig()
			if err != nil {
				return nil, err
			}
			tC, err = restclient.TLSConfigFor(kubeConfig)
			if err != nil {
				return nil, err
			}
		}
	}

	if u, found := opts["user"]; found {
		if _, wrong := opts["useServiceAccount"]; wrong {
			return nil, fmt.Errorf("If user and password are used, serviceAccount cannot be used")
		}
		if pass, f := opts["pass"]; f {
			p.Username = u[0]
			p.Password = pass[0]
		}
	}

	if v, found := opts["caCert"]; found {
		caCert, err := ioutil.ReadFile(v[0])
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tC.RootCAs = caCertPool
	}

	// Concurrency limitations
	if v, found := opts["concurrencyLimit"]; found {
		cs, err := strconv.Atoi(v[0])
		if err != nil || cs < 0 {
			return nil, fmt.Errorf("Supplied concurrency value of %s is invalid", v[0])
		}
		p.Concurrency = cs
	}

	c, err := metrics.NewHawkularClient(p)
	if err != nil {
		return nil, err
	}

	hp := &hawkularProvider{
		client:        c,
		cachedMetrics: []provider.MetricInfo{},
		labelPrefix:   podLabelsDefaultTagPrefix, // Make configurable
	}

	// Update the cache on the background
	go func() {
		hp.ListAllMetrics()
	}()

	return hp, nil
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
	var val int64
	// TODO Reoccurring theme.. this should be handled in the metrics client
	switch t {
	case metrics.Counter:
		val = dp.Value.(int64)
	case metrics.Gauge:
		f := dp.Value.(float64)
		val = int64(f) // We loose some precision, but that's acceptable in this case
	}

	// TODO Use unit (such as bytes) for the correct measurement scaling
	return resource.NewMilliQuantity(val*100, resource.DecimalSI), nil
}

func (h *hawkularProvider) definitionToMetricValue(md *metrics.MetricDefinition, groupResource schema.GroupResource) (*custom_metrics.MetricValue, error) {
	// Fetch async
	dp, err := h.client.ReadRaw(md.Type, md.ID, metrics.Filters(metrics.LimitFilter(1), metrics.OrderFilter(metrics.DESC)))
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

		val, err := valueConverter(dp[0], md.Type, md.Tags["units"])
		if err != nil {
			return nil, err
		}

		return &custom_metrics.MetricValue{
			DescribedObject: api.ObjectReference{
				APIVersion: groupResource.Group + "/" + runtime.APIVersionInternal,
				Kind:       kind.Kind,
				Name:       md.Tags[descriptorTag],
				Namespace:  md.Tenant, // Or do we need the id instead of the name?
			},
			Timestamp: metav1.Time{dp[0].Timestamp},
			Value:     *val,
		}, nil
	}
	return &custom_metrics.MetricValue{}, nil
}

func (h *hawkularProvider) tagsQueryFilter(groupResource schema.GroupResource, selector labels.Selector, metricName string) metrics.Filter {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s = %s AND %s = %s", descriptorTag, metricName, resourceTag, groupResource.Resource))

	if !selector.Empty() {
		buffer.WriteString(" AND ")
		buffer.WriteString(h.labelSelectorToHawkularTagsQuery(selector))
	}

	return metrics.TagsQueryFilter(buffer.String())
}

func (h *hawkularProvider) GetRootScopedMetricByName(groupResource schema.GroupResource, name string, metricName string) (*custom_metrics.MetricValue, error) {
	return nil, nil
}

// GetRootScopedMetricBySelector fetches a particular metric for a set of root-scoped objects
// matching the given label selector.
func (h *hawkularProvider) GetRootScopedMetricBySelector(groupResource schema.GroupResource, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// We fetch definitions first to be able to do manual label selection for procedures we don't support
	defs, err := h.client.Definitions(metrics.Filters(h.tagsQueryFilter(groupResource, selector, metricName)))
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
		dmv, err := h.definitionToMetricValue(d, groupResource)
		if err != nil {
			return nil, err
		}
		mv = append(mv, *dmv)
	}

	return &custom_metrics.MetricValueList{
		Items: mv,
	}, nil
}

// GetNamespacedMetricByName fetches a particular metric for a particular namespaced object.
func (h *hawkularProvider) GetNamespacedMetricByName(groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValue, error) {
	// TODO How are we going to select the metricName here properly? How are we storing the metric?
	// if name is * we do something and if "metrics" something else.. or ?
	return nil, nil
}

// GetNamespacedMetricBySelector fetches a particular metric for a set of namespaced objects
// matching the given label selector.
func (h *hawkularProvider) GetNamespacedMetricBySelector(groupResource schema.GroupResource, namespace string, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// We fetch definitions first to be able to do manual label selection for procedures we don't support
	defs, err := h.client.Definitions(metrics.Tenant(namespace), metrics.Filters(h.tagsQueryFilter(groupResource, selector, metricName)))
	if err != nil {
		return nil, err
	}

	// TODO Refactor following to reusable function (copy from other methods) as soon as name versioned ones are implemented

	mv := make([]custom_metrics.MetricValue, 0, len(defs))

	// Fetch the requested values
	// TODO Turn into its own function to allow reuse
	for _, d := range defs {
		dmv, err := h.definitionToMetricValue(d, groupResource)
		if err != nil {
			return nil, err
		}
		mv = append(mv, *dmv)
	}

	return &custom_metrics.MetricValueList{
		Items: mv,
	}, nil
}

// ListAllMetrics provides a list of all available metrics at
// the current time.  Note that this is not allowed to return
// an error, so it is reccomended that implementors cache and
// periodically update this list, instead of querying every time.
func (h *hawkularProvider) ListAllMetrics() []provider.MetricInfo {
	q := make(map[string]string)

	q[resourceTag] = "*"

	types, err := h.client.TagValues(q)
	if err != nil {
		// Return cached version
		return h.cachedMetrics
	}
	delete(q, resourceTag)

	// Seek all the available metricNames for each type
	q[descriptorTag] = "*"
	providers := make([]provider.MetricInfo, 0, len(types))

	for _, v := range types[resourceTag] {
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

func quoteValues(vals sets.String) string {
	quotedVals := make([]string, 0, len(vals))
	for val, _ := range vals {
		quotedVals = append(quotedVals, fmt.Sprintf("'%s'", val))
	}
	return strings.Join(quotedVals, ",")
}

// labelSelectorToHawkularTagsQuery transforms labels.Selector to Hawkular's TagQL (new style)
func (h *hawkularProvider) labelSelectorToHawkularTagsQuery(selector labels.Selector) string {
	reqs, selectable := selector.Requirements()
	if !selectable {
		return ""
	}

	queries := make([]string, 0, len(reqs))
	// postReq := make([]selector.Requirement)

	for _, req := range reqs {
		key := fmt.Sprintf("%s%s", h.labelPrefix, req.Key())
		switch req.Operator() {
		case selection.In:
			queries = append(queries, fmt.Sprintf("%s IN [%s]", key, quoteValues(req.Values())))
		case selection.NotIn:
			queries = append(queries, fmt.Sprintf("%s NOT IN [%s]", key, quoteValues(req.Values())))
		case selection.Equals, selection.DoubleEquals:
			// More than one value would make no sense, use IN
			queries = append(queries, fmt.Sprintf("%s = '%s'", key, req.Values().List()[0]))
		case selection.DoesNotExist:
			queries = append(queries, fmt.Sprintf("NOT %s", key))
		case selection.NotEquals:
			// More than one value would make no sense, use NOT IN
			queries = append(queries, fmt.Sprintf("%s != '%s'", key, req.Values().List()[0]))
		case selection.Exists:
			queries = append(queries, req.Key())
		case selection.GreaterThan, selection.LessThan:
			// These are not supported (Hawkular tags are string typed), need manual filtering
			// We could return a function for "post-processing" that would be run after fetching the definitions?
			// postReq = append(postReq, req)
		}
	}

	return strings.Join(queries, " AND ")
}
