package main

// import "fmt"

// func test(a int) error {
// 	defer fmt.Printf("b\n")
// 	if a != 0 {
// 		fmt.Printf("c\n")
// 		return nil
// 	}
// 	fmt.Printf("a\n")
// 	return nil
// }

// func main() {
// 	d := [...]int{5, 9: 10}
// 	fmt.Printf("%d\n", d)
// }

import (
	"fmt"
	"os"
	"syscall"
)

// 文件锁
type FileLock struct {
	dir string
	f   *os.File
}

func New(dir string) *FileLock {
	return &FileLock{
		dir: dir,
	}
}

// 加锁
func (l *FileLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
	}
	return nil
}

// 释放锁
func (l *FileLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
func main() {
	// test_file_path, _ := os.Getwd()
	var a float64 = 5
	var b float64 = 6
	fmt.Printf("test_file_path:%d\n", int((a/b)*100))
	// locked_file := "/home/pwq/kvdb/cmd/test"
	// wg := sync.WaitGroup{}
	// for i := 0; i < 10; i++ {
	// 	wg.Add(1)
	// 	go func(num int) {
	// 		flock := New(locked_file)
	// 		err := flock.Lock()
	// 		if err != nil {
	// 			wg.Done()
	// 			fmt.Println(err.Error())
	// 			return
	// 		}
	// 		fmt.Printf("output : %d\n", num)
	// 		wg.Done()
	// 	}(i)
	// }
	// wg.Wait()
	// time.Sleep(2 * time.Second)

}
