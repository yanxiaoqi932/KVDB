package file

import (
	"os"
	"syscall"
)

// 用于管理对文件夹的锁
type FileLockGuard struct {
	Fd *os.File // 这里的*os.File代表的是目录
}

// https://segmentfault.com/a/1190000020671608?sort=votes, http://c.biancheng.net/view/5730.html，
// Flock从每个进程上对db所在的文件夹上锁，使得多个进程不能同时修改文件夹
func AcquireFlock(path string, readOnly bool) (*FileLockGuard, error) {
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	// 打开文件
	file, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}

	// syscall.LOCK_SH	放置共享锁
	// syscall.LOCK_EX	放置互斥锁
	// syscall.LOCK_UN	解锁
	// syscall.LOCK_NB	非阻塞锁请求，在默认情况下如果另一个进程持有了一把锁，
	// 那么调用syscall.Flock会被阻塞，如果设置为syscall.LOCK_NB就不会阻塞而是直接返回error。
	how := syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly {
		how = syscall.LOCK_SH | syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), how); err != nil {
		return nil, err
	}
	return &FileLockGuard{Fd: file}, nil
}

// 数据库关闭过程中，即将关闭文件夹时，启动该方法
func (fl *FileLockGuard) ReleaseFlock() error {
	how := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.Fd.Fd()), how); err != nil {
		return err
	}
	return fl.Fd.Close()
}
