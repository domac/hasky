package app

import (
	"fmt"
	"runtime"
)

const Binary = "0.0.1"

//输出版本号信息
func VerString() string {
	return fmt.Sprintf("%s v%s (built w/%s)", "husky", Binary, runtime.Version())
}
