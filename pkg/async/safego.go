package async

// Go 在一个goroutine中运行一个函数，并在发生panic时进行recover
// 如果发生panic，会调用第一个handler
func Go(fn func(), handler func(r any)) {
	if fn == nil {
		return
	}
	go func() {
		defer Recover(handler)
		fn()
	}()
}

// Recover 捕获 panic 并调用 handler，必须直接以 defer Recover(handler) 调用。
func Recover(handler func(r any)) {
	if r := recover(); r != nil && handler != nil {
		handler(r)
	}
}
