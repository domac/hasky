package app

import (
	"bytes"
	"errors"
	log "github.com/alecthomas/log4go"
	"github.com/julienschmidt/httprouter"
	"github.com/olekukonko/tablewriter"
	"net/http"
	"net/http/pprof"
)

type httpServer struct {
	ctx    *context
	router http.Handler
}

//HTTP 服务
func newHTTPServer(ctx *context) *httpServer {

	log := Log(ctx.appd.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = LogPanicHandler(ctx.appd.opts.Logger)
	router.NotFound = LogNotFoundHandler(ctx.appd.opts.Logger)
	router.MethodNotAllowed = LogMethodNotAllowedHandler(ctx.appd.opts.Logger)

	s := &httpServer{
		ctx:    ctx,
		router: router,
	}
	//内置监控
	router.GET("/debug/pprof/*pprof", innerPprofHandler)

	//在这里注册路由服务
	router.Handle("GET", "/version", Decorate(s.versionHandler, log, Default))
	router.Handle("GET", "/workers", Decorate(s.displayWorkersHandler, log, PlainText))
	router.Handle("GET", "/update", Decorate(s.agentUpdateHandler, log, PlainText))
	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

//调用内置的pprof
func innerPprofHandler(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	switch p.ByName("pprof") {
	case "/cmdline":
		pprof.Cmdline(w, r)
	case "/profile":
		pprof.Profile(w, r)
	case "/symbol":
		pprof.Symbol(w, r)
	default:
		pprof.Index(w, r)
	}
}

func (s *httpServer) versionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	res := NewResult(RESULT_CODE_FAIL, true, "", VerString())
	return res, nil
}

//展示工作节点列表
func (s *httpServer) displayWorkersHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	workers := s.ctx.appd.etcdRegistry.GetWorkers()

	buff := bytes.Buffer{}
	table := tablewriter.NewWriter(&buff)
	table.SetHeader([]string{"worker node", "Last Keepalive"})

	for group, worker := range workers {
		data := []string{group, worker.LastKeepalive.Format("2006-01-02 15:04:05")}
		table.Append(data)
	}
	table.Render()
	return buff.String(), nil
}

//TODO: Agent更新
func (s *httpServer) agentUpdateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	paramReq, err := NewReqParams(req)
	if err != nil {
		return nil, err
	}
	group, _ := paramReq.Get("group")
	agent, _ := paramReq.Get("agent")
	if group == "" || agent == "" {
		return nil, errors.New("group or agent must not be null !")
	}
	log.Info("update info : GROUP: %s , AGENT: %s", group, agent)
	return nil, nil
}
