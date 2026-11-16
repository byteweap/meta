package eureka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	statusUp           = "UP"
	statusDown         = "DOWN"
	statusOutOfService = "OUT_OF_SERVICE"
	heartbeatRetry     = 3
	heartbeatTime      = 10 * time.Second
	httpTimeout        = 3 * time.Second
	refreshTime        = 30 * time.Second
)

type Endpoint struct {
	InstanceID     string
	IP             string
	AppID          string
	Port           int
	SecurePort     int
	HomePageURL    string
	StatusPageURL  string
	HealthCheckURL string
	MetaData       map[string]string
}

// ApplicationsRootResponse 对应 /eureka/apps 响应
type ApplicationsRootResponse struct {
	ApplicationsResponse `json:"applications"`
}

type ApplicationsResponse struct {
	Version      string        `json:"versions__delta"`
	AppsHashcode string        `json:"apps__hashcode"`
	Applications []Application `json:"application"`
}

type Application struct {
	Name     string     `json:"name"`
	Instance []Instance `json:"instance"`
}

type RequestInstance struct {
	Instance Instance `json:"instance"`
}

type Instance struct {
	InstanceID     string            `json:"instanceId"`
	HostName       string            `json:"hostName"`
	Port           Port              `json:"port"`
	App            string            `json:"app"`
	IPAddr         string            `json:"ipAddr"`
	VipAddress     string            `json:"vipAddress"`
	Status         string            `json:"status"`
	SecurePort     Port              `json:"securePort"`
	HomePageURL    string            `json:"homePageUrl"`
	StatusPageURL  string            `json:"statusPageUrl"`
	HealthCheckURL string            `json:"healthCheckUrl"`
	DataCenterInfo DataCenterInfo    `json:"dataCenterInfo"`
	Metadata       map[string]string `json:"metadata"`
}

type Port struct {
	Port    int    `json:"$"`
	Enabled string `json:"@enabled"`
}

type DataCenterInfo struct {
	Name  string `json:"name"`
	Class string `json:"@class"`
}

var _ APIInterface = (*Client)(nil)

type APIInterface interface {
	Register(ctx context.Context, ep Endpoint) error
	Deregister(ctx context.Context, appID, instanceID string) error
	FetchApps(ctx context.Context) ([]Application, error)
	FetchAllUpInstances(ctx context.Context) ([]Instance, error)
	FetchAppInstances(ctx context.Context, appID string) (m Application, err error)
	FetchAppUpInstances(ctx context.Context, appID string) ([]Instance, error)
	FetchAppInstance(ctx context.Context, appID string, instanceID string) (m Instance, err error)
	FetchInstance(ctx context.Context, instanceID string) (m Instance, err error)
	Out(ctx context.Context, appID, instanceID string) error
	Down(ctx context.Context, appID, instanceID string) error
}

type ClientOption func(e *Client)

func WithMaxRetry(maxRetry int) ClientOption {
	return func(e *Client) { e.maxRetry = maxRetry }
}

func WithHeartbeatInterval(interval time.Duration) ClientOption {
	return func(e *Client) {
		e.heartbeatInterval = interval
	}
}

func WithClientContext(ctx context.Context) ClientOption {
	return func(e *Client) { e.ctx = ctx }
}

func WithNamespace(path string) ClientOption {
	return func(e *Client) { e.eurekaPath = path }
}

type Client struct {
	ctx               context.Context
	urls              []string
	eurekaPath        string
	maxRetry          int
	heartbeatInterval time.Duration
	client            *http.Client
	registrations     map[instanceKey]*registration
	lock              sync.Mutex
}

type instanceKey struct {
	appID, instanceID string
}

type registration struct {
	gate      chan struct{}
	refs      int
	heartbeat *heartbeat
}

type heartbeat struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

var clientTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	IdleConnTimeout:       30 * time.Second,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   10,
	TLSHandshakeTimeout:   5 * time.Second,
	ResponseHeaderTimeout: 10 * time.Second,
}

func NewClient(urls []string, opts ...ClientOption) *Client {
	e := &Client{
		ctx:               context.Background(),
		urls:              slices.Clone(urls),
		eurekaPath:        "eureka/v2",
		maxRetry:          len(urls),
		heartbeatInterval: heartbeatTime,
		client:            &http.Client{Transport: clientTransport, Timeout: httpTimeout},
		registrations:     make(map[instanceKey]*registration),
	}

	for _, o := range opts {
		o(e)
	}

	return e
}

func (e *Client) FetchApps(ctx context.Context) ([]Application, error) {
	var m ApplicationsRootResponse
	if err := e.do(ctx, http.MethodGet, []string{"apps"}, nil, &m); err != nil {
		return nil, err
	}

	return m.Applications, nil
}

func (e *Client) FetchAppInstances(ctx context.Context, appID string) (m Application, err error) {
	err = e.do(ctx, http.MethodGet, []string{"apps", appID}, nil, &m)
	return
}

func (e *Client) FetchAppUpInstances(ctx context.Context, appID string) ([]Instance, error) {
	app, err := e.FetchAppInstances(ctx, appID)
	if err != nil {
		return nil, err
	}
	return e.filterUp(app), nil
}

func (e *Client) FetchAppInstance(ctx context.Context, appID string, instanceID string) (m Instance, err error) {
	err = e.do(ctx, http.MethodGet, []string{"apps", appID, instanceID}, nil, &m)
	return
}

func (e *Client) FetchInstance(ctx context.Context, instanceID string) (m Instance, err error) {
	err = e.do(ctx, http.MethodGet, []string{"instances", instanceID}, nil, &m)
	return
}

func (e *Client) Out(ctx context.Context, appID, instanceID string) error {
	return e.do(ctx, http.MethodPut, []string{"apps", appID, instanceID, fmt.Sprintf("status?value=%s", statusOutOfService)}, nil, nil)
}

func (e *Client) Down(ctx context.Context, appID, instanceID string) error {
	return e.do(ctx, http.MethodPut, []string{"apps", appID, instanceID, fmt.Sprintf("status?value=%s", statusDown)}, nil, nil)
}

func (e *Client) FetchAllUpInstances(ctx context.Context) ([]Instance, error) {
	apps, err := e.FetchApps(ctx)
	if err != nil {
		return nil, err
	}
	return e.filterUp(apps...), nil
}

func (e *Client) Register(ctx context.Context, ep Endpoint) error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	defer cancel()
	if e.heartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	key := instanceKey{ep.AppID, ep.InstanceID}
	state, release, err := e.acquireRegistration(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	if err := e.stopHeartbeat(ctx, state); err != nil {
		return err
	}
	ep.MetaData = maps.Clone(ep.MetaData)
	if err := e.registerEndpoint(ctx, ep); err != nil {
		return err
	}
	hbCtx, cancel := context.WithCancel(e.ctx)
	hb := &heartbeat{ctx: hbCtx, cancel: cancel, done: make(chan struct{})}
	e.lock.Lock()
	state.heartbeat = hb
	e.lock.Unlock()
	go e.heartbeatLoop(key, state, hb, ep)
	return nil
}

func (e *Client) Deregister(ctx context.Context, appID, instanceID string) error {
	state, release, err := e.acquireRegistration(ctx, instanceKey{appID, instanceID})
	if err != nil {
		return err
	}
	defer release()
	if err := e.stopHeartbeat(ctx, state); err != nil {
		return err
	}
	return e.do(ctx, http.MethodDelete, []string{"apps", appID, instanceID}, nil, nil)
}

// 每个实例独立串行生命周期操作；全局锁不跨网络请求或等待。
func (e *Client) acquireRegistration(ctx context.Context, key instanceKey) (*registration, func(), error) {
	e.lock.Lock()
	state := e.registrations[key]
	if state == nil {
		state = &registration{gate: make(chan struct{}, 1)}
		e.registrations[key] = state
	}
	state.refs++
	e.lock.Unlock()
	unref := func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		state.refs--
		if state.refs == 0 && state.heartbeat == nil {
			delete(e.registrations, key)
		}
	}
	select {
	case <-ctx.Done():
		unref()
		return nil, nil, ctx.Err()
	case state.gate <- struct{}{}:
		release := func() { <-state.gate; unref() }
		if err := ctx.Err(); err != nil {
			release()
			return nil, nil, err
		}
		return state, release, nil
	}
}

func (e *Client) stopHeartbeat(ctx context.Context, state *registration) error {
	e.lock.Lock()
	hb := state.heartbeat
	e.lock.Unlock()
	if hb == nil {
		return nil
	}
	hb.cancel()
	select {
	case <-hb.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Client) registerEndpoint(ctx context.Context, ep Endpoint) error {
	instance := RequestInstance{
		Instance: Instance{
			InstanceID: ep.InstanceID,
			HostName:   ep.AppID,
			Port: Port{
				Port:    ep.Port,
				Enabled: "true",
			},
			App:        ep.AppID,
			IPAddr:     ep.IP,
			VipAddress: ep.AppID,
			Status:     statusUp,
			SecurePort: Port{
				Port:    ep.SecurePort,
				Enabled: "false",
			},
			HomePageURL:    ep.HomePageURL,
			StatusPageURL:  ep.StatusPageURL,
			HealthCheckURL: ep.HealthCheckURL,
			DataCenterInfo: DataCenterInfo{
				Name:  "MyOwn",
				Class: "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo",
			},
			Metadata: ep.MetaData,
		},
	}

	body, err := json.Marshal(instance)
	if err != nil {
		return err
	}
	return e.do(ctx, http.MethodPost, []string{"apps", ep.AppID}, bytes.NewReader(body), nil)
}

func (e *Client) heartbeatLoop(key instanceKey, state *registration, hb *heartbeat, ep Endpoint) {
	defer func() {
		hb.cancel()
		e.lock.Lock()
		if state.heartbeat == hb {
			state.heartbeat = nil
			if state.refs == 0 && e.registrations[key] == state {
				delete(e.registrations, key)
			}
		}
		e.lock.Unlock()
		close(hb.done)
	}()
	ticker := time.NewTicker(e.heartbeatInterval)
	defer ticker.Stop()
	retryCount := 0
	for {
		select {
		case <-hb.ctx.Done():
			return
		case <-ticker.C:
			if err := e.do(hb.ctx, http.MethodPut, []string{"apps", ep.AppID, ep.InstanceID}, nil, nil); err != nil {
				if retryCount++; retryCount > heartbeatRetry {
					_ = e.registerEndpoint(hb.ctx, ep)
					retryCount = 0
				}
			} else {
				retryCount = 0
			}
		}
	}
}

func (e *Client) filterUp(apps ...Application) (res []Instance) {
	for _, app := range apps {
		for _, ins := range app.Instance {
			if ins.Status == statusUp {
				res = append(res, ins)
			}
		}
	}

	return
}

func (e *Client) buildAPI(server string, params ...string) string {
	params = append([]string{server, e.eurekaPath}, params...)
	return strings.Join(params, "/")
}

func (e *Client) request(ctx context.Context, server, method string, params []string, input io.Reader, output any) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, method, e.buildAPI(server, params...), input)
	if err != nil {
		return false, err
	}
	request.Header.Add("User-Agent", "go-eureka-client")
	request.Header.Add("Accept", "application/json;charset=UTF-8")
	request.Header.Add("Content-Type", "application/json;charset=UTF-8")
	resp, err := e.client.Do(request)
	if err != nil {
		return true, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if output != nil && resp.StatusCode/100 == 2 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		err = json.Unmarshal(data, output)
		if err != nil {
			return false, err
		}
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return false, fmt.Errorf("response Error %d", resp.StatusCode)
	}

	return false, nil
}

func (e *Client) do(ctx context.Context, method string, params []string, input io.Reader, output any) error {
	if len(e.urls) == 0 || e.maxRetry <= 0 {
		return fmt.Errorf("Eureka requires a server and a positive request attempt count")
	}
	var body []byte
	if input != nil {
		var err error
		body, err = io.ReadAll(input)
		if err != nil {
			return err
		}
	}
	servers := slices.Clone(e.urls)
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	var lastErr error
	for i := 0; i < e.maxRetry; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		retry, err := e.request(ctx, servers[i%len(servers)], method, params, bytes.NewReader(body), output)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retry {
			return err
		}
		lastErr = err
	}
	return lastErr
}
