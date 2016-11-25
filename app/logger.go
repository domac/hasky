package app

//日志器接口
type Logger interface {
	Output(maxdepth int, s string) error
}
